/*-------------------------------------------------------------------------
 *
 * checksum_index.c
 *    Index-level checksum implementation
 *
 * This module provides functions for computing checksums at the index level,
 * including both individual index tuples and entire indexes. Index checksums
 * help detect corruption in index structures and ensure index consistency
 * with table data. They are particularly important for:
 *    - Verifying B-tree integrity after crash recovery
 *    - Detecting index corruption that could lead to wrong query results
 *    - Validating index builds and rebuilds
 *
 * The implementation handles different index types (B-tree, hash, etc.)
 * and incorporates index-specific metadata like heap TIDs for B-trees.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/checksum/checksum_index.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "catalog/index.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_index.h"
#include "storage/checksum_column.h"
#include "utils/rel.h"

/*
 * pg_index_tuple_checksum
 *    Compute checksum for an individual index tuple.
 *
 * This function calculates a 32-bit checksum for an index tuple, taking into
 * account index-specific metadata. For B-tree indexes, the heap TID
 * is included in the checksum to maintain the index-to-heap relationship.
 *
 * Parameters:
 *    itup:          Index tuple to checksum
 *    indexTupDesc:  Tuple descriptor for the index
 *    attno:         Offset number within the index page (for uniqueness)
 *
 * Returns:
 *    32-bit checksum for the index tuple
 *
 * Notes:
 *    - For B-tree indexes, the heap TID (ItemPointer) is included
 *      to bind index entries to their corresponding heap tuples
 *    - The offset number ensures tuples at different positions have
 *      different checksums even if their data is identical
 *    - Index tuple size must be valid (verified by IndexTupleSize)
 */
uint32
pg_index_tuple_checksum(IndexTuple itup, TupleDesc indexTupDesc,
                        OffsetNumber attno)
{
    uint32      len;
    uint32      checksum;

    /* Get the index tuple size (includes header and data) */
    len = IndexTupleSize(itup);
    
    /* Basic checksum of the tuple data using offset as initial value */
    checksum = pg_checksum_data((char *) itup, len, attno);

    /*
     * For B-tree indexes, include the heap TID in the checksum.
     * This binds the index entry to its corresponding heap tuple,
     * which is crucial for detecting inconsistencies between
     * indexes and their underlying tables.
     */
    if (indexTupDesc->tdtypeid == BTREE_AM_OID)
    {
        ItemPointer heapTid = &itup->t_tid;
        checksum ^= ItemPointerGetBlockNumber(heapTid);
        checksum ^= ItemPointerGetOffsetNumber(heapTid) << 16;
    }

    /*
     * IMPORTANT: Guarantee that index checksums never equal CHECKSUM_NULL.
     * This prevents collisions with NULL column values in heap tuples.
     */
    if (checksum == CHECKSUM_NULL)
    {
        checksum = (CHECKSUM_NULL ^ attno ^ len) & 0xFFFFFFFE;
    }
    
    return checksum;
}

/*
 * pg_index_page_checksum
 *    Compute checksum for all index tuples on a page.
 *
 * This function aggregates checksums of all valid index tuples on a page
 * using XOR. This provides a page-level integrity check for indexes
 * that can detect:
 *    - Missing or extra index entries on a page
 *    - Corruption of index tuple ordering
 *    - Partial page writes
 *
 * Parameters:
 *    page:          Index page
 *    indexTupDesc:  Tuple descriptor for the index
 *
 * Returns:
 *    32-bit composite checksum for the entire page
 *
 * Notes:
 *    - Only valid (used and not dead) index tuples are included
 *    - The XOR operation is commutative, making the checksum
 *      order-independent but sensitive to the set of tuples
 *    - This complements but doesn't replace page-level checksums
 */
uint32
pg_index_page_checksum(Page page, TupleDesc indexTupDesc)
{
    OffsetNumber maxoff;
    OffsetNumber offnum;
    uint32      page_checksum = 0;

    /* Get the maximum offset number on this page */
    maxoff = PageGetMaxOffsetNumber(page);
    
    /* Process all index tuples on the page */
    for (offnum = FirstOffsetNumber;
         offnum <= maxoff;
         offnum = OffsetNumberNext(offnum))
    {
        ItemId      itemId;
        IndexTuple  itup;
        
        itemId = PageGetItemId(page, offnum);
        
        /* Skip unused or dead index tuples */
        if (!ItemIdIsUsed(itemId) || ItemIdIsDead(itemId))
            continue;
            
        itup = (IndexTuple) PageGetItem(page, itemId);
        
        /* XOR the tuple checksum into the page checksum */
        page_checksum ^= pg_index_tuple_checksum(itup, indexTupDesc, offnum);
    }
    
    return page_checksum;
}