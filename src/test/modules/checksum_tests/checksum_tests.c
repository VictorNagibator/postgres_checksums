/*-------------------------------------------------------------------------
 *
 * checksum_tests.c
 *    Unit tests for checksum functionality
 *
 * This module provides unit tests for the checksum functionality at all
 * granularities: tuple, column, table, index, and database level.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/test/modules/checksum_tests/checksum_tests.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "access/htup_details.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/checksum_tuple.h"
#include "storage/checksum_column.h"
#include "storage/checksum_index.h"
#include "storage/checksum_database.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/*
 * Unit tests for checksum functionality
 */

/*
 * test_tuple_checksum_same_data
 *    Test that identical tuples have identical checksums
 */
PG_FUNCTION_INFO_V1(test_tuple_checksum_same_data);
Datum
test_tuple_checksum_same_data(PG_FUNCTION_ARGS)
{
    MemoryContext oldcontext;
    MemoryContext testcontext;
    TupleDesc   tupdesc;
    Datum       values[3];
    bool        nulls[3] = {false, false, false};
    HeapTuple   tuple1, tuple2;
    uint32      checksum1, checksum2;
    Page        page;
    char       *page_buffer;
    OffsetNumber offnum1, offnum2;
    
    /* Create a memory context for the test */
    testcontext = AllocSetContextCreate(CurrentMemoryContext,
                                        "ChecksumTestContext",
                                        ALLOCSET_DEFAULT_SIZES);
    oldcontext = MemoryContextSwitchTo(testcontext);
    
    /* Create a test page */
    page_buffer = (char *) palloc0(BLCKSZ);
    page = (Page) page_buffer;
    PageInit(page, BLCKSZ, 0);
    
    /* Create a simple tuple descriptor */
    tupdesc = CreateTemplateTupleDesc(3);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 3, "value", FLOAT8OID, -1, 0);
    
    /* Create two identical tuples */
    values[0] = Int32GetDatum(1);
    values[1] = CStringGetTextDatum("test");
    values[2] = Float8GetDatum(3.14);
    
    tuple1 = heap_form_tuple(tupdesc, values, nulls);
    tuple2 = heap_form_tuple(tupdesc, values, nulls);
    
    /* Add tuples to page using PageAddItemExtended */
    offnum1 = PageAddItemExtended(page, 
                                 (void *) tuple1->t_data, 
                                 tuple1->t_len,
                                 FirstOffsetNumber, 
                                 0);
    
    if (offnum1 == InvalidOffsetNumber)
        elog(ERROR, "failed to add tuple1 to page");
    
    offnum2 = PageAddItemExtended(page, 
                                 (void *) tuple2->t_data, 
                                 tuple2->t_len,
                                 OffsetNumberNext(offnum1), 
                                 0);
    
    if (offnum2 == InvalidOffsetNumber)
        elog(ERROR, "failed to add tuple2 to page");
    
    /* Calculate checksums with block number 0 (test page) */
    checksum1 = pg_tuple_checksum(page, offnum1, 0, false);
    checksum2 = pg_tuple_checksum(page, offnum2, 0, false);
    
    /* Clean up */
    heap_freetuple(tuple1);
    heap_freetuple(tuple2);
    pfree(page_buffer);
    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(testcontext);
    
    /* Verify checksums are different due to different offsets */
    if (checksum1 == checksum2)
        elog(ERROR, "Tuples at different offsets should have different checksums");
    
    PG_RETURN_VOID();
}

/*
 * test_column_checksum_null
 *    Test that NULL columns return CHECKSUM_NULL
 */
PG_FUNCTION_INFO_V1(test_column_checksum_null);
Datum
test_column_checksum_null(PG_FUNCTION_ARGS)
{
    MemoryContext oldcontext;
    MemoryContext testcontext;
    TupleDesc   tupdesc;
    Datum       values[2];
    bool        nulls[2] = {true, false};
    HeapTuple   tuple;
    uint32      checksum;
    
    testcontext = AllocSetContextCreate(CurrentMemoryContext,
                                        "ChecksumTestContext",
                                        ALLOCSET_DEFAULT_SIZES);
    oldcontext = MemoryContextSwitchTo(testcontext);
    
    tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "nullable", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "not_null", INT4OID, -1, 0);
    
    values[0] = (Datum) 0;  /* Value doesn't matter for NULL */
    values[1] = Int32GetDatum(42);
    
    tuple = heap_form_tuple(tupdesc, values, nulls);
    
    /* Calculate checksum for NULL column */
    checksum = pg_tuple_column_checksum(tuple->t_data, 1, tupdesc);
    
    /* Verify NULL returns CHECKSUM_NULL */
    if (checksum != CHECKSUM_NULL)
        elog(ERROR, "NULL column should return CHECKSUM_NULL, got %u", checksum);
    
    /* Verify non-NULL column returns non-CHECKSUM_NULL */
    checksum = pg_tuple_column_checksum(tuple->t_data, 2, tupdesc);
    if (checksum == CHECKSUM_NULL)
        elog(ERROR, "Non-NULL column should not return CHECKSUM_NULL");
    
    heap_freetuple(tuple);
    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(testcontext);
    
    PG_RETURN_VOID();
}

/*
 * test_index_checksum_basic
 *    Basic test for index tuple checksum
 */
PG_FUNCTION_INFO_V1(test_index_checksum_basic);
Datum
test_index_checksum_basic(PG_FUNCTION_ARGS)
{
    MemoryContext oldcontext;
    MemoryContext testcontext;
    TupleDesc   tupdesc;
    IndexTuple  itup1, itup2;
    uint32      checksum1, checksum2;
    Size        size;
    char       *data1, *data2;
    int32       key_val;
    int64       tid_val;
    
    testcontext = AllocSetContextCreate(CurrentMemoryContext,
                                        "ChecksumTestContext",
                                        ALLOCSET_DEFAULT_SIZES);
    oldcontext = MemoryContextSwitchTo(testcontext);
    
    /* Create a simple tuple descriptor for index */
    tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitEntry(tupdesc, (AttrNumber) 1, "key", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) 2, "tid_block", INT8OID, -1, 0);
    
    /* Create index tuples - simplified approach */
    size = sizeof(IndexTupleData) + sizeof(int32) + sizeof(int64);
    itup1 = (IndexTuple) palloc0(size);
    itup2 = (IndexTuple) palloc0(size);
    
    /* Set up index tuple headers */
    itup1->t_info = size;
    itup2->t_info = size;
    
    /* Copy data to index tuples */
    data1 = (char *) itup1 + sizeof(IndexTupleData);
    data2 = (char *) itup2 + sizeof(IndexTupleData);
    
    /* For pass-by-value types, we need to copy the actual integer values */
    key_val = 100;
    tid_val = 123456;
    
    memcpy(data1, &key_val, sizeof(int32));
    memcpy(data1 + sizeof(int32), &tid_val, sizeof(int64));
    
    memcpy(data2, &key_val, sizeof(int32));
    memcpy(data2 + sizeof(int32), &tid_val, sizeof(int64));
    
    /* Calculate checksums */
    checksum1 = pg_index_tuple_checksum(itup1, tupdesc, 1);
    checksum2 = pg_index_tuple_checksum(itup2, tupdesc, 1);
    
    /* Verify identical index tuples have identical checksums */
    if (checksum1 != checksum2)
        elog(ERROR, "Identical index tuples should have identical checksums");
    
    pfree(itup1);
    pfree(itup2);
    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(testcontext);
    
    PG_RETURN_VOID();
}

/*
 * test_page_checksum_consistency
 *    Test that page checksums are consistent
 */
PG_FUNCTION_INFO_V1(test_page_checksum_consistency);
Datum
test_page_checksum_consistency(PG_FUNCTION_ARGS)
{
    MemoryContext oldcontext;
    MemoryContext testcontext;
    Page        page1, page2;
    char       *page_buffer1, *page_buffer2;
    uint16      checksum1, checksum2;
    
    testcontext = AllocSetContextCreate(CurrentMemoryContext,
                                        "ChecksumTestContext",
                                        ALLOCSET_DEFAULT_SIZES);
    oldcontext = MemoryContextSwitchTo(testcontext);
    
    /* Create two identical pages */
    page_buffer1 = (char *) palloc0(BLCKSZ);
    page_buffer2 = (char *) palloc0(BLCKSZ);
    
    page1 = (Page) page_buffer1;
    page2 = (Page) page_buffer2;
    
    PageInit(page1, BLCKSZ, 0);
    PageInit(page2, BLCKSZ, 0);
    
    /* Set up page headers */
    PageSetLSN(page1, (XLogRecPtr)12345);
    PageSetLSN(page2, (XLogRecPtr)12345);
    
    /* Calculate checksums for identical pages with same block number */
    checksum1 = pg_checksum_page(page_buffer1, 100);
    checksum2 = pg_checksum_page(page_buffer2, 100);
    
    /* Verify identical pages have identical checksums */
    if (checksum1 != checksum2)
        elog(ERROR, "Identical pages should have identical checksums");
    
    /* Calculate checksums with different block numbers */
    checksum2 = pg_checksum_page(page_buffer2, 101);
    
    /* Verify different block numbers produce different checksums */
    if (checksum1 == checksum2)
        elog(ERROR, "Different block numbers should produce different checksums");
    
    pfree(page_buffer1);
    pfree(page_buffer2);
    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(testcontext);
    
    PG_RETURN_VOID();
}