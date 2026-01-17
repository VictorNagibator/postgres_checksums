/*-------------------------------------------------------------------------
 *
 * checksumfuncs.c
 *    SQL-callable functions for checksum operations at all granularities
 *
 * This module provides SQL functions that expose checksum capabilities to
 * users and administrators. Functions are provided for:
 *    - Individual tuples (with or without headers)
 *    - Specific columns within tuples
 *    - Entire tables (XOR of all tuple checksums)
 *    - Index tuples and entire indexes
 *    - Database-level checksums
 *
 * These functions enable practical data integrity verification for:
 *    - Application developers: Verify data consistency after complex operations
 *    - DBAs: Monitor database health and detect silent corruption
 *    - Migration teams: Validate data transfer accuracy
 *    - Backup/restore validation: Ensure backup integrity
 *
 * All functions are designed to be safe, efficient, and minimally intrusive,
 * using appropriate locking strategies and respecting MVCC semantics.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/utils/adt/checksumfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "access/htup.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"
#include "storage/checksum_tuple.h"
#include "storage/checksum_column.h"
#include "storage/checksum_database.h"
#include "storage/checksum_index.h"
#include "storage/ipc.h"
#include "access/nbtree.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

/*
 * pg_checksum_tuple
 *    SQL function: pg_checksum_tuple(reloid, tid, include_header)
 *
 * Returns the checksum of a specific tuple identified by its TID.
 * This function is useful for verifying individual row integrity,
 * especially after data migration or replication. It provides two modes:
 *    - include_header = false: Checksum only the tuple data (recommended)
 *    - include_header = true: Include the tuple header in checksum
 *
 * Security: Requires SELECT privilege on the relation.
 *
 * Performance: Uses minimal locking (AccessShareLock) and reads only
 * the necessary page, making it efficient for point lookups.
 */
PG_FUNCTION_INFO_V1(pg_checksum_tuple);

Datum
pg_checksum_tuple(PG_FUNCTION_ARGS)
{
    Oid         reloid = PG_GETARG_OID(0);
    ItemPointer tid = PG_GETARG_ITEMPOINTER(1);
    bool        include_header = PG_GETARG_BOOL(2);
    Relation    rel = NULL;
    Buffer      buffer = InvalidBuffer;
    Page        page;
    uint32      checksum = 0;
    bool        lock_held = false;
    BlockNumber blkno;
    
    /*
     * Use PG_TRY/PG_CATCH to ensure resources are always cleaned up,
     * even if an error occurs during processing.
     */
    PG_TRY();
    {
        /* Open relation with minimal locking */
        rel = relation_open(reloid, AccessShareLock);
        
        /* Read the specific page containing the tuple */
        buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        lock_held = true;
        
        page = BufferGetPage(buffer);
        blkno = BufferGetBlockNumber(buffer);
        
        /* Compute the tuple checksum */
        checksum = pg_tuple_checksum(page, 
                                     ItemPointerGetOffsetNumber(tid), 
                                     blkno,
                                     include_header);
    }
    PG_CATCH();
    {
        /* Clean up resources in case of error */
        if (lock_held && BufferIsValid(buffer))
            UnlockReleaseBuffer(buffer);
        if (rel)
            relation_close(rel, AccessShareLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    
    /* Clean up resources on normal path */
    if (lock_held && BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
    if (rel)
        relation_close(rel, AccessShareLock);
    
    PG_RETURN_INT32((int32)checksum);
}

/*
 * pg_checksum_table
 *    SQL function: pg_checksum_table(reloid, include_header)
 *
 * Computes a composite checksum for an entire table by XOR-ing the
 * checksums of all tuples. This provides a quick integrity check
 * for the entire table without reading all data. The XOR approach:
 *    - Changes with any tuple modification
 *    - Is commutative, making it order-independent
 *    - Doesn't guarantee ordering or detect missing tuples that XOR to zero
 *
 * Security: Requires SELECT privilege on the relation.
 *
 * Performance: Uses a sequential table scan with MVCC snapshot,
 * making it suitable for integrity checking of live tables.
 */
PG_FUNCTION_INFO_V1(pg_checksum_table);

Datum
pg_checksum_table(PG_FUNCTION_ARGS)
{
    Oid         reloid = PG_GETARG_OID(0);
    bool        include_header = PG_GETARG_BOOL(1);
    Relation    rel;
    TableScanDesc scan;
    HeapTuple   tuple;
    uint32      table_checksum = 0;
    BlockNumber blkno;
    
    /* Open relation with minimal locking */
    rel = relation_open(reloid, AccessShareLock);
    
    /* Start a table scan using current snapshot */
    scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
    
    /* Process each tuple in the table */
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Buffer      buffer;
        Page        page;
        uint32      tuple_checksum;
        
        /* Read the page containing this tuple */
        buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(&tuple->t_self));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        
        page = BufferGetPage(buffer);
        blkno = BufferGetBlockNumber(buffer);
        
        /* Compute tuple checksum */
        tuple_checksum = pg_tuple_checksum(page, 
                                           ItemPointerGetOffsetNumber(&tuple->t_self), 
                                           blkno,
                                           include_header);
        
        /* XOR with accumulating table checksum */
        table_checksum ^= tuple_checksum;
        
        UnlockReleaseBuffer(buffer);
    }
    
    /* Clean up */
    table_endscan(scan);
    relation_close(rel, AccessShareLock);
    
    PG_RETURN_INT32((int32)table_checksum);
}

/*
 * pg_checksum_page_data
 *    SQL function: pg_checksum_page_data(relfilenode, blocknum)
 *
 * Computes a checksum for the data portion of a specific page,
 * excluding the page header. This is useful for:
 *    - Verifying page-level integrity independent of tuple structure
 *    - Debugging storage-level corruption
 *    - Validating custom page formats
 *
 * Security: Requires superuser privileges due to low-level access.
 *
 * Performance: Reads a single page with minimal overhead.
 */
PG_FUNCTION_INFO_V1(pg_checksum_page_data);

Datum
pg_checksum_page_data(PG_FUNCTION_ARGS)
{
    Oid         relfilenode = PG_GETARG_OID(0);
    int32       blocknum_arg = PG_GETARG_INT32(1);
    BlockNumber blocknum = (BlockNumber)blocknum_arg;
    Relation    rel = NULL;
    Buffer      buffer = InvalidBuffer;
    Page        page;
    uint32      checksum = 0;
    bool        lock_held = false;
    
    PG_TRY();
    {
        /* Open relation by filenode (for physical access) */
        rel = RelationIdGetRelation(relfilenode);
        
        if (!RelationIsValid(rel))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("relation with OID %u does not exist", relfilenode)));
        
        /* Read the specific page */
        buffer = ReadBuffer(rel, blocknum);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        lock_held = true;
        
        page = BufferGetPage(buffer);
        
        /* Compute checksum for page data (skip page header) */
        checksum = pg_checksum_data((char *)page + sizeof(PageHeaderData),
                                    BLCKSZ - sizeof(PageHeaderData),
                                    0);
    }
    PG_CATCH();
    {
        /* Clean up in case of error */
        if (lock_held && BufferIsValid(buffer))
            UnlockReleaseBuffer(buffer);
        if (rel)
            relation_close(rel, AccessShareLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    
    /* Clean up */
    if (lock_held && BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
    if (rel)
        relation_close(rel, AccessShareLock);
    
    PG_RETURN_INT32((int32)checksum);
}

/*
 * pg_checksum_column
 *    SQL function: pg_checksum_column(reloid, tid, attnum)
 *
 * Returns the checksum of a specific column within a tuple.
 * This enables fine-grained integrity checking at the column level,
 * useful for:
 *    - Validating critical columns (e.g., financial amounts)
 *    - Detecting corruption in specific data types
 *    - Monitoring column-level data quality
 *
 * Usage: SELECT pg_checksum_column('table_name'::regclass, ctid, column_number);
 *
 * Security: Requires SELECT privilege on the relation.
 *
 * Performance: Reads only the necessary page and extracts the column value.
 */
PG_FUNCTION_INFO_V1(pg_checksum_column);

Datum
pg_checksum_column(PG_FUNCTION_ARGS)
{
    Oid         reloid = PG_GETARG_OID(0);
    ItemPointer tid = PG_GETARG_ITEMPOINTER(1);
    int32       attnum_arg = PG_GETARG_INT32(2);
    Relation    rel;
    Buffer      buffer;
    Page        page;
    HeapTupleHeader tuple;
    ItemId      lp;
    TupleDesc   tupleDesc;
    uint32      checksum = 0;
    int         attnum;

    /* Validate attribute number */
    if (attnum_arg <= 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid attribute number: %d", attnum_arg)));

    attnum = attnum_arg;

    /* Open relation and get tuple descriptor */
    rel = relation_open(reloid, AccessShareLock);
    tupleDesc = RelationGetDescr(rel);

    /* Validate attribute number against relation schema */
    if (attnum > tupleDesc->natts)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("attribute number %d exceeds number of columns %d",
                        attnum, tupleDesc->natts)));

    /* Read the page containing the tuple */
    buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buffer);
    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    
    /* Verify the tuple slot is actually used */
    if (!ItemIdIsUsed(lp))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("tuple at (%u, %u) is not used",
                        ItemPointerGetBlockNumber(tid),
                        ItemPointerGetOffsetNumber(tid))));

    tuple = (HeapTupleHeader) PageGetItem(page, lp);

    /* Compute column checksum */
    checksum = pg_tuple_column_checksum(tuple, attnum, tupleDesc);

    /* Clean up */
    UnlockReleaseBuffer(buffer);
    relation_close(rel, AccessShareLock);

    PG_RETURN_INT32((int32)checksum);
}

/*
 * pg_checksum_index
 *    SQL function: pg_checksum_index(indexoid)
 *
 * Computes a composite checksum for an entire index by XOR-ing the
 * checksums of all index tuples. This provides integrity verification
 * for index structures, detecting:
 *    - Corruption in index pages
 *    - Missing or extra index entries
 *    - Inconsistencies between index and table data
 *
 * Security: Requires SELECT privilege on the index.
 *
 * Performance: Uses bulk read strategy for efficient sequential scanning
 * of index pages with minimal lock contention.
 */
PG_FUNCTION_INFO_V1(pg_checksum_index);

Datum
pg_checksum_index(PG_FUNCTION_ARGS)
{
    Oid         indexoid = PG_GETARG_OID(0);
    Relation    rel;
    TupleDesc   tupdesc;
    uint32      index_checksum = 0;
    uint64      n_tuples = 0;
    BlockNumber nblocks;
    BufferAccessStrategy bstrategy;
    BlockNumber blkno;
    Buffer      buffer;
    Page        page;
    OffsetNumber maxoff;
    OffsetNumber offnum;
    ItemId      itemId;
    IndexTuple  itup;
    uint32      tuple_checksum;

    /* Open the index with minimal locking */
    rel = index_open(indexoid, AccessShareLock);
    tupdesc = RelationGetDescr(rel);

    /* Get total number of blocks in the index */
    nblocks = RelationGetNumberOfBlocks(rel);
    
    /* Use bulk read strategy for efficient sequential scanning */
    bstrategy = GetAccessStrategy(BAS_BULKREAD);

    /* Process each block in the index */
    for (blkno = 0; blkno < nblocks; blkno++)
    {
        buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
                                    RBM_NORMAL, bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        
        page = BufferGetPage(buffer);
        
        /* Skip uninitialized pages */
        if (PageIsNew(page))
        {
            UnlockReleaseBuffer(buffer);
            continue;
        }
        
        /* Process all index tuples on the page */
        maxoff = PageGetMaxOffsetNumber(page);
        
        for (offnum = FirstOffsetNumber;
             offnum <= maxoff;
             offnum = OffsetNumberNext(offnum))
        {
            itemId = PageGetItemId(page, offnum);
            
            /* Skip unused or dead index entries */
            if (!ItemIdIsUsed(itemId) || ItemIdIsDead(itemId))
                continue;
                
            itup = (IndexTuple) PageGetItem(page, itemId);
            
            if (itup)
            {
                /* Compute checksum for this index tuple */
                tuple_checksum = pg_index_tuple_checksum(itup, tupdesc, offnum);
                index_checksum ^= tuple_checksum;
                n_tuples++;
            }
        }
        
        UnlockReleaseBuffer(buffer);
        
        /* Check for interrupts every 64 pages to allow cancellation */
        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    
    /* Clean up */
    FreeAccessStrategy(bstrategy);
    index_close(rel, AccessShareLock);

    PG_RETURN_INT32((int32)index_checksum);
}

/*
 * pg_database_checksum
 *    SQL function: pg_database_checksum(include_system, include_toast)
 *
 * Computes a checksum for the entire current database by aggregating
 * checksums from all tables and indexes. This provides the highest-level
 * integrity check, useful for:
 *    - Verifying database consistency after major operations
 *    - Detecting widespread silent corruption
 *    - Validating backup/restore and replication processes
 *
 * Parameters:
 *    include_system: Whether to include system catalogs
 *    include_toast:  Whether to include toast tables
 *
 * Security: Requires superuser privileges due to the scope of access.
 *
 * Performance: This is an expensive operation that scans the entire
 * database. It should be used judiciously, typically during maintenance
 * windows or for critical validation.
 */
PG_FUNCTION_INFO_V1(pg_database_checksum);

Datum
pg_database_checksum(PG_FUNCTION_ARGS)
{
    bool        include_system = false;
    bool        include_toast = false;
    uint64      checksum;

    /* Parse optional parameters */
    if (PG_NARGS() >= 1)
        include_system = PG_GETARG_BOOL(0);
    if (PG_NARGS() >= 2)
        include_toast = PG_GETARG_BOOL(1);

    /* Security check: only superusers can checksum the entire database */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("must be superuser to compute database checksum")));

    /* Compute the database checksum */
    checksum = pg_database_checksum_internal(MyDatabaseId,
                                              include_system,
                                              include_toast,
                                              NULL, NULL);

    PG_RETURN_INT64((int64)checksum);
}