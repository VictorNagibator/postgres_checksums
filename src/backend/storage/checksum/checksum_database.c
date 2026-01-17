/*-------------------------------------------------------------------------
 *
 * checksum_database.c
 *    Database-level checksum implementation
 *
 * This module provides functions for computing a checksum for an entire
 * database by aggregating checksums from all tables and indexes. This
 * provides a high-level integrity check useful for:
 *    - Verifying database consistency after backup/restore
 *    - Detecting silent data corruption across the entire database
 *    - Validating replication and migration processes
 *
 * The implementation scans all relations in the database, computing
 * checksums for each tuple/index entry and combining them using XOR.
 * This approach provides several benefits:
 *    - Efficient: Processes data in bulk using sequential scans
 *    - Scalable: Handles large databases with minimal memory overhead
 *    - Flexible: Can include/exclude system catalogs and toast tables
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/checksum/checksum_database.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "nodes/memnodes.h"
#include "storage/bufmgr.h"
#include "storage/checksum_database.h"
#include "storage/checksum_tuple.h"
#include "storage/checksum_index.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"

/*
 * DatabaseChecksumState
 *    State maintained during database checksum computation.
 *
 * This structure tracks progress and accumulates results while scanning
 * the database. It's passed to callback functions for progress reporting
 * and error handling.
 */
typedef struct DatabaseChecksumState
{
    uint64      checksum;          /* Current checksum value */
    uint64      n_tuples;          /* Number of tuples processed */
    uint64      n_pages;           /* Number of pages processed */
    Oid         current_relid;     /* OID of relation being processed */
    char        current_relkind;   /* Relation kind (r = table, i = index, etc.) */
    bool        include_toast;     /* Whether to include toast tables */
    bool        include_system;    /* Whether to include system catalogs */
} DatabaseChecksumState;

/*
 * process_index_for_checksum
 *    Process an index relation by reading its pages directly.
 *
 * Indexes are processed differently from heap relations because:
 *    - They contain IndexTuples rather than HeapTuples
 *    - They may have different storage characteristics
 *    - They require direct page access for efficiency
 *
 * This function reads each page of the index, extracts all valid
 * index tuples, computes their checksums, and XORs them into the
 * database checksum.
 *
 * Parameters:
 *    idxRel:   Index relation to process
 *    state:    Database checksum state (updated in place)
 */
static void
process_index_for_checksum(Relation idxRel, DatabaseChecksumState *state)
{
    BlockNumber nblocks;
    TupleDesc   tupdesc;
    BufferAccessStrategy bstrategy;

    /* Get the index's tuple descriptor for checksum computation */
    tupdesc = RelationGetDescr(idxRel);
    
    /* Determine how many blocks we need to process */
    nblocks = RelationGetNumberOfBlocks(idxRel);
    
    /*
     * Use a bulk read buffer strategy for efficient sequential scanning.
     * This reduces lock contention and improves I/O performance.
     */
    bstrategy = GetAccessStrategy(BAS_BULKREAD);

    /* Process each block in the index */
    for (BlockNumber blkno = 0; blkno < nblocks; blkno++)
    {
        Buffer      buffer;
        Page        page;
        OffsetNumber maxoff;
        
        /* Read the buffer using our bulk read strategy */
        buffer = ReadBufferExtended(idxRel, MAIN_FORKNUM, blkno,
                                    RBM_NORMAL, bstrategy);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        
        page = BufferGetPage(buffer);
        
        /* Skip uninitialized (new) pages */
        if (PageIsNew(page))
        {
            UnlockReleaseBuffer(buffer);
            continue;
        }
        
        /* Process all index tuples on this page */
        maxoff = PageGetMaxOffsetNumber(page);
        
        for (OffsetNumber offnum = FirstOffsetNumber;
             offnum <= maxoff;
             offnum = OffsetNumberNext(offnum))
        {
            ItemId      itemId;
            IndexTuple  itup;
            
            itemId = PageGetItemId(page, offnum);
            
            /* Skip unused or dead index entries */
            if (!ItemIdIsUsed(itemId) || ItemIdIsDead(itemId))
                continue;
                
            itup = (IndexTuple) PageGetItem(page, itemId);
            
            if (itup)
            {
                uint32 idx_checksum;
                
                /* Compute checksum for this index tuple */
                idx_checksum = pg_index_tuple_checksum(itup, tupdesc, offnum);
                
                /*
                 * Incorporate the checksum into the database checksum.
                 * We combine the tuple checksum with the relation OID
                 * to ensure different relations contribute differently
                 * even if they have identical tuples.
                 */
                state->checksum ^= ((uint64)idx_checksum << 32) | 
                                   (uint64)idxRel->rd_id;
                state->n_tuples++;
            }
        }
        
        UnlockReleaseBuffer(buffer);
        state->n_pages++;
        
        /* Check for interrupts periodically to allow query cancellation */
        if ((blkno & 63) == 0)
            CHECK_FOR_INTERRUPTS();
    }
    
    FreeAccessStrategy(bstrategy);
}

/*
 * process_relation_for_checksum
 *    Process a single relation (table or index) for checksum computation.
 *
 * This function handles both heap relations and indexes, delegating
 * to the appropriate processing function based on the relation type.
 *
 * Parameters:
 *    relid:   OID of the relation to process
 *    state:   Database checksum state (updated in place)
 */
static void
process_relation_for_checksum(Oid relid, DatabaseChecksumState *state)
{
    Relation    rel;
    TableScanDesc scan;
    HeapTuple   tuple;
    Snapshot    snapshot;
    bool        is_index;

    /* Open the relation with minimal locking (AccessShareLock) */
    rel = relation_open(relid, AccessShareLock);
    
    /*
     * Filter relations we don't want to process:
     *   - Only regular tables, indexes, materialized views, sequences, and toast
     *   - Skip other relation kinds (views, foreign tables, etc.)
     */
    if (rel->rd_rel->relkind != RELKIND_RELATION &&
        rel->rd_rel->relkind != RELKIND_INDEX &&
        rel->rd_rel->relkind != RELKIND_MATVIEW &&
        rel->rd_rel->relkind != RELKIND_SEQUENCE &&
        rel->rd_rel->relkind != RELKIND_TOASTVALUE)
    {
        relation_close(rel, AccessShareLock);
        return;
    }

    /* Update state for progress reporting */
    state->current_relid = relid;
    state->current_relkind = rel->rd_rel->relkind;

    is_index = (rel->rd_rel->relkind == RELKIND_INDEX);

    /* Use a consistent snapshot for the entire scan */
    snapshot = GetActiveSnapshot();

    if (!is_index)
    {
        /*
         * Process heap relation using a table scan.
         * This approach is efficient for sequential access and respects MVCC.
         */
        scan = table_beginscan(rel, snapshot, 0, NULL);
        
        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
        {
            Buffer      buffer;
            Page        page;
            uint32      tuple_checksum;
            BlockNumber blkno;
            
            /* Read the page containing this tuple */
            buffer = ReadBuffer(rel, 
                               ItemPointerGetBlockNumber(&tuple->t_self));
            LockBuffer(buffer, BUFFER_LOCK_SHARE);
            
            page = BufferGetPage(buffer);
            blkno = BufferGetBlockNumber(buffer);

            /* Compute checksum for this tuple (excluding header) */
            tuple_checksum = pg_tuple_checksum(page,
                ItemPointerGetOffsetNumber(&tuple->t_self), blkno, false);
            
            /*
             * Incorporate tuple checksum into database checksum.
             * Combine with relation OID to differentiate between relations.
             */
            state->checksum ^= ((uint64)tuple_checksum << 32) | 
                               (uint64)relid;
            state->n_tuples++;
            
            UnlockReleaseBuffer(buffer);
        }
        
        table_endscan(scan);
    }
    else
    {
        /* Process index by reading pages directly */
        process_index_for_checksum(rel, state);
    }

    relation_close(rel, AccessShareLock);
}

/*
 * pg_database_checksum_internal
 *    Compute a checksum for the entire database.
 *
 * This is the main entry point for database-level checksum computation.
 * It scans pg_class to find all relations in the database, filters them
 * based on inclusion criteria, and processes each one.
 *
 * Parameters:
 *    dboid:              OID of database to checksum (must be current database)
 *    include_system:     Whether to include system catalogs
 *    include_toast:      Whether to include toast tables
 *    progress_callback:  Optional callback for progress reporting
 *    callback_arg:       User data passed to progress callback
 *
 * Returns:
 *    64-bit checksum representing the entire database state
 *
 * Notes:
 *    - Only superusers can call this function (enforced by SQL wrapper)
 *    - Runs in a dedicated memory context to control memory usage
 *    - Respects snapshot isolation for consistent results
 *    - Periodically checks for interrupts to allow cancellation
 */
uint64
pg_database_checksum_internal(Oid dboid,
                              bool include_system,
                              bool include_toast,
                              checksum_progress_callback progress_callback,
                              void *callback_arg)
{
    DatabaseChecksumState state;
    Relation    pg_class_rel;
    TableScanDesc scan;
    HeapTuple   classTuple;
    Snapshot    snapshot;
    MemoryContext oldcontext;
    MemoryContext checksum_context;

    /* Initialize state structure */
    memset(&state, 0, sizeof(DatabaseChecksumState));
    state.include_system = include_system;
    state.include_toast = include_toast;

    /*
     * Create a dedicated memory context for the checksum operation.
     * This ensures we clean up all memory even on error and allows
     * better memory usage tracking.
     */
    checksum_context = AllocSetContextCreate(CurrentMemoryContext,
                                             "Database Checksum",
                                             ALLOCSET_DEFAULT_SIZES);
    oldcontext = MemoryContextSwitchTo(checksum_context);

    /*
     * Security check: Only allow checksumming the current database.
     * Cross-database operations would require additional permissions
     * and snapshot management.
     */
    if (OidIsValid(dboid) && dboid != MyDatabaseId)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cross-database checksum not supported from this context")));

    /* Use a consistent snapshot for the entire operation */
    snapshot = GetActiveSnapshot();

    /* Scan pg_class to find all relations in the database */
    pg_class_rel = table_open(RelationRelationId, AccessShareLock);
    scan = table_beginscan(pg_class_rel, snapshot, 0, NULL);

    while ((classTuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classTuple);
        Oid         relid = classForm->oid;
        Oid         relnamespace = classForm->relnamespace;
        char        relkind = classForm->relkind;
        char        relpersistence = classForm->relpersistence;

        /*
         * Apply inclusion filters:
         *   - Skip system catalogs if include_system is false
         *   - Skip toast tables if include_toast is false
         *   - Always skip unlogged relations (they're not crash-safe)
         */
        if (!include_system && 
            (relnamespace == PG_CATALOG_NAMESPACE ||
             relnamespace == PG_TOAST_NAMESPACE))
            continue;

        if (!include_toast && relkind == RELKIND_TOASTVALUE)
            continue;

        if (relpersistence == RELPERSISTENCE_UNLOGGED)
            continue;

        /* Process this relation */
        process_relation_for_checksum(relid, &state);

        /* Call progress callback if provided */
        if (progress_callback)
            progress_callback(&state, callback_arg);

        CHECK_FOR_INTERRUPTS();
    }

    /* Clean up */
    table_endscan(scan);
    table_close(pg_class_rel, AccessShareLock);

    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(checksum_context);

    return state.checksum;
}