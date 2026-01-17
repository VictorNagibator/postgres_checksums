/*-------------------------------------------------------------------------
 *
 * checksum_tuple.h
 *	  Tuple-level checksum declarations
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum_tuple.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_TUPLE_H
#define CHECKSUM_TUPLE_H

#include "storage/bufpage.h"

/* Tuple checksum functions */
extern uint32 pg_tuple_checksum(Page page, OffsetNumber offnum, BlockNumber blkno, bool include_header);
extern uint32 pg_index_checksum(Page page, OffsetNumber offnum);

#endif