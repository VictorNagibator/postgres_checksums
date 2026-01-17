# src/test/modules/checksum_tests/Makefile

MODULE_big = checksum_tests
OBJS = \
	$(WIN32RES) \
	checksum_tests.o 
PGFILEDESC = "checksum_tests - tests for checksum functionality"

EXTENSION = checksum_tests
DATA = checksum_tests--1.0.sql

REGRESS = checksum_basic checksum_table checksum_index checksum_database

TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/checksum_tests
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif