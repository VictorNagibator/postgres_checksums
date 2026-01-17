/*-------------------------------------------------------------------------
 *
 * checksum_tuple.c
 *    Tuple-level checksum implementation.
 *
 * This module provides functions for computing checksums at the tuple level,
 * which enables data integrity verification for individual rows and index
 * entries. Unlike page-level checksums that protect against storage corruption,
 * tuple-level checksums can detect logical corruption within tuples and
 * are preserved during logical replication. They are particularly useful for:
 *    - Verifying row-level data integrity after migration
 *    - Detecting application-level data corruption
 *    - Providing end-to-end data integrity in replication scenarios
 *
 * The implementation incorporates MVCC information (xmin/xmax) to
 * differentiate between different versions of the same logical row,
 * which is crucial for detecting corruption in MVCC chains.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/page/checksum_tuple.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/itup.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_tuple.h"
#include "storage/checksum_column.h"
#include "utils/rel.h"

/*
 * pg_tuple_checksum
 *    Compute a checksum for a heap tuple.
 *
 * This function calculates a 32-bit checksum for a heap tuple, optionally
 * including the tuple header. The checksum incorporates:
 *    - The tuple's physical location (block number and offset)
 *    - MVCC information (xmin/xmax) when header is not included
 *    - Either the entire tuple or just the data portion
 *
 * Parameters:
 *    page:           Page containing the tuple
 *    offnum:         Offset number of the tuple within the page
 *    blkno:          Block number containing the page
 *    include_header: If true, include the HeapTupleHeader in the calculation;
 *                    if false, calculate checksum only on tuple data
 *
 * Returns:
 *    32-bit checksum, or 0 if the offset is invalid or tuple is not used
 *
 * Notes:
 *    - Checksums are computed even for deleted tuples (old row versions)
 *      to maintain integrity across all MVCC states
 *    - The block number and offset are encoded into a location hash
 *      to bind the tuple to its physical location
 *    - When excluding headers, MVCC information is XORed to differentiate
 *      between different versions of the same logical row
 */
uint32
pg_tuple_checksum(Page page, OffsetNumber offnum, BlockNumber blkno, bool include_header)
{
    ItemId      lp;
    HeapTupleHeader tuple;
    char       *data;
    uint32      len;
    uint32      checksum;
    uint32      location_hash;
    
    /* Validate offset number range */
    if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
        return 0;
    
    lp = PageGetItemId(page, offnum);
    
    /* Skip unused ItemIds (deallocated tuple slots) */
    if (!ItemIdIsUsed(lp))
        return 0;
    
    /*
     * Include deleted tuples (e.g., old versions after UPDATE) in checksum
     * calculation. This is important for verifying integrity of all row
     * versions throughout their lifecycle, especially in MVCC systems.
     */
    
    tuple = (HeapTupleHeader) PageGetItem(page, lp);
    len = ItemIdGetLength(lp);
    
    if (include_header)
    {
        /* Include entire tuple (header + data) in checksum */
        data = (char *) tuple;
    }
    else
    {
        /* Skip header, checksum only the tuple data */
        data = (char *) tuple + tuple->t_hoff;
        len -= tuple->t_hoff;
        
        if (len <= 0)
            return 0;
    }

    /*
     * Create a location hash from block number and offset.
     * This binds the checksum to the tuple's physical location,
     * ensuring that identical tuples at different locations have
     * different checksums.
     */
    location_hash = (blkno << 16) | offnum;
    
    /* Calculate checksum using location_hash as the initial value */
    checksum = pg_checksum_data(data, len, location_hash);
    
    /*
     * Additionally XOR with location_hash to guarantee uniqueness.
     * This extra step ensures that even if pg_checksum_data produces
     * the same result for different locations, the final checksums differ.
     */
    checksum ^= location_hash;
    
    /*
     * Incorporate MVCC information to differentiate between row versions.
     * This ensures that different versions of the same logical row have
     * different checksums, which is essential for detecting corruption
     * in MVCC chains (e.g., when xmin/xmax values are corrupted).
     */
    if (!include_header)
    {
        uint32 mvcc_info = (HeapTupleHeaderGetRawXmin(tuple) ^ 
                           HeapTupleHeaderGetRawXmax(tuple));
        checksum ^= mvcc_info;
    }
    
    /*
     * IMPORTANT: Guarantee that tuple checksums never equal CHECKSUM_NULL.
     * This prevents collisions with NULL column values.
     */
    if (checksum == CHECKSUM_NULL)
    {
        checksum = (CHECKSUM_NULL ^ location_hash) & 0xFFFFFFFE;
    }
    
    return checksum;
}

/*
 * pg_index_checksum
 *    Compute a checksum for an index tuple.
 *
 * This function calculates a 32-bit checksum for an index tuple (itup).
 * Unlike heap tuples, index tuples don't have MVCC information, so the
 * entire tuple content is included in the checksum. The offset number
 * is incorporated to ensure uniqueness.
 *
 * Parameters:
 *    page:   Page containing the index tuple
 *    offnum: Offset number of the index tuple within the page
 *
 * Returns:
 *    32-bit checksum, or 0 if the offset is invalid or tuple is dead
 *
 * Notes:
 *    - Only used, non-dead index tuples are processed
 *    - The offset number ensures uniqueness for identical index entries
 *    - This function provides basic integrity checking for index tuples
 */
uint32
pg_index_checksum(Page page, OffsetNumber offnum)
{
    ItemId      lp;
    IndexTuple  itup;
    uint32      len;
    uint32      checksum;
    
    /* Validate offset number range */
    if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
        return 0;
    
    lp = PageGetItemId(page, offnum);
    
    /* Skip unused or dead index tuples */
    if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
        return 0;
    
    itup = (IndexTuple) PageGetItem(page, lp);
    len = ItemIdGetLength(lp);
    
    /*
     * Calculate checksum for the index tuple.
     * Use the offset number as the initial value to ensure
     * that identical index entries at different positions
     * have different checksums.
     */
    checksum = pg_checksum_data((char *)itup, len, offnum);
    
    /* Additionally XOR with offnum to guarantee uniqueness */
    checksum ^= offnum;
    
    /* Guarantee checksum never equals CHECKSUM_NULL */
    if (checksum == CHECKSUM_NULL)
    {
        checksum = (CHECKSUM_NULL ^ offnum) & 0xFFFFFFFE;
    }
    
    return checksum;
}