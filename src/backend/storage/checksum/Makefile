#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/checksum
#
# IDENTIFICATION
#    src/backend/storage/checksum/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/checksum
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS =  \
	checksum.o \
	checksum_tuple.o \
	checksum_column.o \
	checksum_index.o \
	checksum_database.o

include $(top_srcdir)/src/backend/common.mk

# Provide special optimization flags for checksum.c
checksum.o: CFLAGS += ${CFLAGS_UNROLL_LOOPS} ${CFLAGS_VECTORIZE}
