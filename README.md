# PostgreSQL Enhanced Checksums

## Overview

This PostgreSQL fork adds comprehensive checksum functionality at all data granularity levels: column, tuple (row), table, index, and database. Unlike page-level checksums that protect against storage corruption, these checksums operate at the logical level and are preserved during logical replication, providing end-to-end data integrity verification.

## Features

### Granular Checksum Levels

* **Column-level (`pg_checksum_column`)**: Compute checksums for individual column values, with special handling for NULL values

* **Tuple-level (`pg_checksum_tuple`)**: Compute checksums for individual rows, with or without header data

* **Table-level (`pg_checksum_table`)**: Compute aggregate checksums for entire tables using XOR

* **Index-level (`pg_checksum_index`)**: Compute checksums for index structures and entries

* **Database-level (`pg_database_checksum`)**: Compute checksums for entire databases (excluding cluster)

### Key Capabilities

* **MVCC-aware**: Includes transaction visibility information (xmin/xmax) to differentiate between row versions

* **Location-sensitive**: Incorporates physical location (ctid) to ensure unique checksums for identical data at different locations

* **Type-aware**: Handles all PostgreSQL data types (pass-by-value, varlena, cstring, arrays, JSON, composites)

* **NULL handling**: Returns special CHECKSUM_NULL (0xFFFFFFFF) for NULL values

* **Efficient**: Uses optimized algorithms suitable for large-scale data integrity verification

## Implementation

### Checksum Algorithm

The checksum algorithm is based on the FNV-1a (Fowler-Noll-Vo) hash, extended for variable-length data and parallel computation:

```c
#define CHECKSUM_COMP(checksum, value) \
do { \
    uint32 __tmp = (checksum) ^ (value); \
    (checksum) = __tmp * FNV_PRIME ^ (__tmp >> 17); \
} while (0)
```

Key characteristics:

* Parallel computation: 32 parallel hash streams for SIMD optimization

* Variable-length support: Handles arbitrary data sizes with 4-byte alignment

* Collision-resistant: Incorporates context (attnum, offset, block number) to ensure uniqueness

## API Reference (SQL Functions)

### pg_checksum_tuple(reloid regclass, tid tid, include_header bool) RETURNS int4

Computes checksum for a specific tuple identified by TID.

Parameters:

* reloid: OID of the relation

* tid: Tuple identifier (ctid)

* include_header: If true, include tuple header; if false, only tuple data

Returns: 32-bit checksum

Usage:

```sql
SELECT pg_checksum_tuple('mytable'::regclass, ctid, false) FROM mytable WHERE id = 1;
```

### pg_checksum_column(reloid regclass, tid tid, attnum int4) RETURNS int4

Computes checksum for a specific column within a tuple.

Parameters:

* reloid: OID of the relation

* tid: Tuple identifier (ctid)

* attnum: Attribute number (1-indexed)

Returns: 32-bit checksum, or -1 (0xFFFFFFFF) for NULL values

Usage:

```sql
SELECT pg_checksum_column('mytable'::regclass, ctid, 2) FROM mytable WHERE id = 1;
```

### pg_checksum_table(reloid regclass, include_header bool) RETURNS int4

Computes aggregate checksum for an entire table by XOR-ing all tuple checksums.

Parameters:

* reloid: OID of the relation

* include_header: If true, include tuple headers

Returns: 32-bit aggregate checksum

Notes: Returns 0 for empty tables

### pg_checksum_index(indexoid regclass) RETURNS int4

Computes aggregate checksum for an entire index.

Parameters:

* indexoid: OID of the index

Returns: 32-bit aggregate checksum

Supported index types: B-tree, hash, unique, partial, expression

### pg_database_checksum(include_system bool, include_toast bool) RETURNS int8

Computes checksum for the entire current database.

Parameters:

* include_system: Include system catalogs (default: false)

* include_toast: Include TOAST tables (default: false)

Returns: 64-bit database checksum

Security: Requires superuser privileges

## Installation

### Building from Source

```bash
# Configure with TAP test support (recommended)
./configure --prefix=/path/to/installation --enable-tap-tests

# Build and install
make
sudo make install
```

## Testing

### Test Infrastructure

Two types of tests are provided:

* Regression tests (SQL): Located in `src/test/modules/checksum_tests/sql/`

* TAP tests (Perl): Located in `src/test/modules/checksum_tests/t/`

### Running Tests

```bash
# Navigate to test directory
cd src/test/modules/checksum_tests

# Run all tests (requires --enable-tap-tests)
make installcheck

# Or with no installation
make check
```

### Test Coverage

* Basic functionality: All checksum levels work correctly

* NULL handling: NULL columns return CHECKSUM_NULL

* Data modification: Checksums change when data changes

* MVCC behavior: Different row versions have different checksums

* Data types: All PostgreSQL types are supported

## Usage Examples

### Data Integrity Verification

```sql
-- Verify individual row integrity
SELECT
    ctid,
    data,
    pg_checksum_tuple('orders'::regclass, ctid, false) as row_checksum
FROM orders
WHERE order_date = CURRENT_DATE;

-- Verify critical columns
SELECT
    customer_id,
    amount,
    pg_checksum_column('transactions'::regclass, ctid, 3) as amount_checksum
FROM transactions
WHERE verified = false;

-- Detect data corruption
CREATE TABLE checksum_backup AS
SELECT ctid, pg_checksum_tuple('important_table'::regclass, ctid, false) as original
FROM important_table;

-- Later, compare checksums
SELECT COUNT(*) as corrupted_rows
FROM important_table t
JOIN checksum_backup b ON t.ctid = b.ctid
WHERE pg_checksum_tuple('important_table'::regclass, t.ctid, false) != b.original;
```

### Migration Validation

```sql
-- Source database
SELECT pg_checksum_table('customers'::regclass, false) as source_checksum;

-- Target database (after migration)
SELECT pg_checksum_table('customers'::regclass, false) as target_checksum;

-- Verify entire database consistency
SELECT pg_database_checksum(false, false) as db_checksum;
```

### Index Integrity

```sql
-- Verify index structure
SELECT pg_checksum_index('idx_customer_email') as index_checksum;

-- Check for index-table consistency issues
EXPLAIN ANALYZE
SELECT c.customer_id
FROM customers c
LEFT JOIN customer_emails e ON c.customer_id = e.customer_id
WHERE e.customer_id IS NULL;
```

## Development

### Code Organization

```
src/include/storage/checksum*.h        <- Header files
src/backend/storage/checksum/          <- Core implementation
src/backend/utils/adt/checksumfuncs.c  <- SQL-callable functions
src/test/modules/checksum_tests/       <- Test suite
```

## References

### Related PostgreSQL Features

* Page checksums: Physical storage integrity (pg_checksum_page)

* Data checksums: This extension (logical data integrity)

* pg_checksums: Cluster-wide physical checksum tool

* pg_verify_checksums: Physical checksum verification

### Algorithm References

* FNV hash: [https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function](https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function)

* PostgreSQL page checksums: src/include/storage/checksum_impl.h

## License

This extension is distributed under the PostgreSQL License, same as PostgreSQL itself.


