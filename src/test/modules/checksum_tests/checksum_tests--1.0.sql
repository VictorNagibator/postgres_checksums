-- checksum_tests--1.0.sql
-- SQL functions for checksum unit tests

-- Unit test functions
CREATE OR REPLACE FUNCTION test_tuple_checksum_same_data()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION test_column_checksum_null()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION test_index_checksum_basic()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION test_page_checksum_consistency()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;