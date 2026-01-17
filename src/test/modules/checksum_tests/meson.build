# src/test/modules/checksum_tests/meson.build
checksum_tests_sources = files(
  'checksum_tests.c',
)

checksum_tests = shared_module('checksum_tests',
  checksum_tests_sources,
  kwargs: pg_mod_args,
)

test_install_data += files(
  'sql/checksum_basic.sql',
  'sql/checksum_table.sql',
  'sql/checksum_index.sql',
  'sql/checksum_database.sql',
  'expected/checksum_basic.out',
  'expected/checksum_table.out',
  'expected/checksum_index.out',
  'expected/checksum_database.out',
)

tests += {
  'name': 'checksum_tests',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'checksum_basic',
      'checksum_table',
      'checksum_index',
      'checksum_database',
    ],
  },
  'tap': {
    'tests': [
      't/001_base.pl'
    ],
  },
}