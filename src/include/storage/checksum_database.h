/*-------------------------------------------------------------------------
 *
 * checksum_database.h
 *    Database-level checksum declarations
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/checksum_database.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CHECKSUM_DATABASE_H
#define CHECKSUM_DATABASE_H

#include "postgres.h"

typedef void (*checksum_progress_callback)(void *state, void *arg);

/* Database checksum functions */
extern uint64 pg_database_checksum_internal(Oid dboid,
                                            bool include_system,
                                            bool include_toast,
                                            checksum_progress_callback callback,
                                            void *callback_arg);

#endif /* CHECKSUM_DATABASE_H */