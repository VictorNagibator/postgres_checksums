/*-------------------------------------------------------------------------
 *
 * checksum_index.h
 *    Index-level checksum declarations
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum_index.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_INDEX_H
#define CHECKSUM_INDEX_H

#include "access/itup.h"

/* Index checksum functions */
extern uint32 pg_index_tuple_checksum(IndexTuple itup,
                                      TupleDesc indexTupDesc,
                                      OffsetNumber attno);
extern uint32 pg_index_page_checksum(Page page, TupleDesc indexTupDesc);

#endif /* CHECKSUM_INDEX_H */