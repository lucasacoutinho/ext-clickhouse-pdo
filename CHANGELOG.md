# Changelog

All notable changes to the PDO ClickHouse Driver will be documented in this file.

## [0.2.0] - 2025-11-23

### Added

- **Transaction Support** - Standard PDO transaction interface
  - `beginTransaction()` - Start a transaction
  - `commit()` - Commit the transaction
  - `rollback()` - Rollback the transaction
  - `inTransaction()` - Check transaction status
  - Executes actual BEGIN TRANSACTION, COMMIT, ROLLBACK queries
  - Proper validation and error messages
  - Marked EXPERIMENTAL (requires ClickHouse 21.11+ with Atomic database)

### Improved

- **Documentation** - Simplified README with Xdebug-style formatting
- **Error Messages** - Better error messages for transaction failures
- **Code Quality** - Zero memory leaks (Valgrind verified)
- **Static Analysis** - All cppcheck warnings resolved

### Fixed

- Transaction state tracking
- Error handling in transaction methods

### Test Coverage

- All existing PDO tests passing
- Transaction support verified with manual tests
- Zero memory leaks under Valgrind

### Breaking Changes

- None - Fully backward compatible with v0.1.x

### Notes

- Transaction support requires ClickHouse 21.11+ with Atomic database engine
- Transactions work through standard PDO interface
- Compatible with core extension v0.2.0

## [0.1.0] - Initial Release

### Added

- PDO driver for ClickHouse
- Standard PDO interface
- Prepared statements with parameter binding
- ClickHouse-specific methods:
  - `clickhouseInsert()` - Bulk insert
  - `clickhouseInsertFromString()` - Insert from formatted data
  - `clickhouseInsertFromFile()` - Insert from file
  - `clickhouseGetDatabases()` - List databases
  - `clickhouseGetTables()` - List tables
  - `clickhouseDescribeTable()` - Get table schema
- Multiple format support (CSV, TSV, JSONEachRow, etc.)
- Compression support
- SSL/TLS support

---

For detailed documentation, see the [README](README.md).
