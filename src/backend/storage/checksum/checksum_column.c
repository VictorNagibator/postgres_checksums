/*-------------------------------------------------------------------------
 *
 * checksum_column.c
 *    Column-level checksum implementation
 *
 * This module provides functions for computing checksums at the column level.
 * Column checksums enable fine-grained data integrity verification, allowing
 * detection of corruption in individual column values within tuples. These
 * checksums are particularly useful for:
 *    - Validating data migrations and replication
 *    - Detecting hardware-induced data corruption
 *    - Providing integrity guarantees for specific columns
 *
 * The implementation handles all PostgreSQL data types, including:
 *    - Pass-by-value types (integers, floats)
 *    - Variable-length types (text, arrays)
 *    - Fixed-length pass-by-reference types
 *    - NULL values (returns special CHECKSUM_NULL value)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/checksum/checksum_column.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "storage/checksum.h"
#include "storage/checksum_column.h"

/*
 * pg_column_checksum_internal
 *    Compute a 32-bit checksum for a single column value.
 *
 * This function calculates a checksum for an individual column value,
 * taking into account the data type and storage characteristics. NULL
 * values return the special CHECKSUM_NULL value (0xFFFFFFFF).
 *
 * Parameters:
 *    value:    The column value as a Datum
 *    isnull:   Whether the value is NULL
 *    typid:    OID of the column's data type
 *    typmod:   Type modifier (for varlena types)
 *    attnum:   Attribute number (1-indexed) for uniqueness
 *
 * Returns:
 *    32-bit checksum, or CHECKSUM_NULL (0xFFFFFFFF) for NULL values
 *
 * Notes:
 *    - For pass-by-value types, the actual value bytes are checksummed
 *    - For varlena types, the toast pointer is dereferenced first
 *    - For cstring types, the null-terminated string is checksummed
 *    - The attribute number is incorporated to differentiate columns
 *    - We guarantee non-NULL values never return CHECKSUM_NULL
 */
uint32
pg_column_checksum_internal(Datum value, bool isnull, Oid typid,
                            int32 typmod, int attnum)
{
    Form_pg_type typeForm;
    HeapTuple   typeTuple;
    char       *data;
    int         len;
    uint32      checksum;

    /* Handle NULL values by returning the special NULL checksum */
    if (isnull)
        return CHECKSUM_NULL;

    /*
     * Look up type information to determine how to handle this value.
     * PostgreSQL supports multiple storage strategies:
     *    - typbyval: Pass-by-value types (int4, float8, etc.)
     *    - typlen = -1: Variable-length types (text, bytea, arrays)
     *    - typlen = -2: C string types
     *    - typlen > 0: Fixed-length pass-by-reference types
     */
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(typeTuple))
        elog(ERROR, "cache lookup failed for type %u", typid);
    
    typeForm = (Form_pg_type) GETSTRUCT(typeTuple);

    if (typeForm->typbyval && typeForm->typlen > 0)
    {
        /*
         * Fixed-length pass-by-value type (e.g., int4, float8).
         * The value is stored directly in the Datum, so we checksum
         * the bytes of the Datum itself.
         */
        data = (char *) &value;
        len = typeForm->typlen;
        checksum = pg_checksum_data(data, len, attnum);
    }
    else if (typeForm->typlen == -1)
    {
        /*
         * Variable-length type (varlena). These types have a header
         * that includes length information. We must detoast if the
         * value has been toasted (compressed or out-of-line).
         */
        struct varlena *varlena;
        
        /* Detoast if necessary - this may involve decompression */
        varlena = PG_DETOAST_DATUM(value);
        
        data = (char *) varlena;
        len = VARSIZE_ANY(varlena);  /* Get actual length including header */
        
        checksum = pg_checksum_data(data, len, attnum);
        
        /* Free the detoasted copy if we created one */
        if (varlena != (struct varlena *) DatumGetPointer(value))
            pfree(varlena);
    }
    else if (typeForm->typlen == -2)
    {
        /*
         * C string type. These are null-terminated strings stored
         * as pointers. We checksum the entire string.
         */
        data = DatumGetCString(value);
        len = strlen(data);
        checksum = pg_checksum_data(data, len, attnum);
    }
    else
    {
        /*
         * Fixed-length pass-by-reference type (e.g., char(N)).
         * These are stored as pointers to fixed-size buffers.
         */
        data = DatumGetPointer(value);
        len = typeForm->typlen;
        
        if (data == NULL)
            elog(ERROR, "invalid pointer for fixed-length reference type");
            
        checksum = pg_checksum_data(data, len, attnum);
    }

    ReleaseSysCache(typeTuple);
    
    /*
     * IMPORTANT: Guarantee that non-NULL values never return CHECKSUM_NULL.
     * This prevents collisions between NULL and non-NULL values that might
     * accidentally hash to CHECKSUM_NULL. We XOR with type information
     * to ensure uniqueness while preserving the guarantee.
     */
    if (checksum == CHECKSUM_NULL)
    {
        checksum = (CHECKSUM_NULL ^ attnum ^ typid) & 0xFFFFFFFE;
    }
    
    return checksum;
}

/*
 * pg_tuple_column_checksum
 *    Compute checksum for a specific column in a heap tuple.
 *
 * This function extracts a column value from a heap tuple and computes
 * its checksum using pg_column_checksum_internal(). It handles the
 * tuple descriptor lookup and value extraction.
 *
 * Parameters:
 *    tuple:     Heap tuple header containing the data
 *    attnum:    Attribute number (1-indexed) to checksum
 *    tupleDesc: Tuple descriptor describing the tuple's structure
 *
 * Returns:
 *    32-bit checksum for the specified column
 *
 * Notes:
 *    - The function validates the attribute number
 *    - Uses heap_getattr() to extract the value (handles NULLs)
 *    - Respects the tuple's MVCC visibility information
 */
uint32
pg_tuple_column_checksum(HeapTupleHeader tuple, int attnum,
                         TupleDesc tupleDesc)
{
    bool        isnull;
    Datum       value;
    Form_pg_attribute attr;
    Oid         typid;
    int32       typmod;
    HeapTupleData heapTuple;

    /* Validate attribute number range */
    if (attnum <= 0 || attnum > tupleDesc->natts)
        elog(ERROR, "invalid attribute number %d", attnum);

    /*
     * Create a temporary HeapTuple structure for heap_getattr.
     * This is necessary because heap_getattr expects a HeapTuple,
     * not just a HeapTupleHeader.
     */
    heapTuple.t_len = HeapTupleHeaderGetDatumLength(tuple);
    heapTuple.t_data = tuple;
    heapTuple.t_tableOid = InvalidOid;
    heapTuple.t_self = *((ItemPointer) &tuple->t_ctid);

    /* Extract the attribute value */
    attr = TupleDescAttr(tupleDesc, attnum - 1);
    value = heap_getattr(&heapTuple, attnum, tupleDesc, &isnull);
    
    typid = attr->atttypid;
    typmod = attr->atttypmod;

    /* Delegate to the internal column checksum function */
    return pg_column_checksum_internal(value, isnull, typid, typmod, attnum);
}