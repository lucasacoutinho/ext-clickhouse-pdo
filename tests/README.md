# PDO ClickHouse Tests

This directory contains PHPT tests for the PDO ClickHouse driver.

## Running Tests

### Using PHP's run-tests.php

```bash
# From ext/pdo_clickhouse directory
make test

# Or manually
php run-tests.php -d extension=modules/clickhouse.so \
                  -d extension=modules/pdo_clickhouse.so \
                  tests/
```

### Using CMake/CTest

```bash
cd build
ctest
```

## Test Structure

- `001-*` - Basic PDO functionality (connection, queries, etc.)
- `002-*` - Core PDO features (prepared statements, fetch modes, etc.)
- `101-*` - Driver-specific methods (clickhouseGetServerInfo, etc.)

## Prerequisites

- ClickHouse server running (default: localhost:9000)
- Both `clickhouse.so` and `pdo_clickhouse.so` extensions built
- Environment variables (optional):
  - `CLICKHOUSE_HOST` - ClickHouse server host (default: localhost)
  - `CLICKHOUSE_PORT` - ClickHouse native port (default: 9000)

## Test Coverage

### Basic PDO (001-099)
- ✅ Connection (001)
- ✅ Simple queries (002)
- ✅ CREATE TABLE and INSERT (003)
- ✅ Prepared statements with parameters (004)
- ✅ Fetch modes (ASSOC, NUM, OBJ, BOTH) (005)

### Driver-Specific Methods (101-199)
- ✅ clickhouseGetServerInfo() (101)
- ✅ clickhouseGetDatabases() (102)
- ✅ clickhouseDescribeTable() (103)
- ✅ clickhouseSetQuerySetting() / clickhouseClearQuerySettings() (104)

## Adding New Tests

PHPT tests follow this format:

```phpt
--TEST--
Test description
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip');
?>
--FILE--
<?php
// Test code here
?>
--EXPECT--
Expected output
```

See existing tests for examples.
