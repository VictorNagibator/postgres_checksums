/*-------------------------------------------------------------------------
 *
 * checksum_column.h
 *    Column-level checksum declarations
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum_column.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_COLUMN_H
#define CHECKSUM_COLUMN_H

#include "access/htup.h"

/* Special checksum value for NULL */
#define CHECKSUM_NULL 0xFFFFFFFF

/* Column checksum functions */
extern uint32 pg_column_checksum_internal(Datum value, bool isnull,
                                          Oid typid, int32 typmod,
                                          int attnum);
extern uint32 pg_tuple_column_checksum(HeapTupleHeader tuple, int attnum,
                                       TupleDesc tupleDesc);

#endif /* CHECKSUM_COLUMN_H */