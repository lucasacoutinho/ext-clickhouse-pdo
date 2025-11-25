# PDO ClickHouse Driver

A PDO driver for ClickHouse using the native TCP protocol.

## Installation

```bash
mkdir build && cd build
cmake ..
cmake --build .
sudo cmake --install .
```

### Building with phpize

If you prefer `phpize`/`./configure` builds:

```bash
phpize
./configure --with-pdo-clickhouse=/path/to/ext-clickhouse   # use ../clickhouse/src when building from this monorepo
make && sudo make install
```

Point `--with-pdo-clickhouse` to the ClickHouse native extension sources (repo root or its `src/` directory). Without those sources you will hit missing `buffer.c` errors during `make`.

Enable the extension in `php.ini`:
```ini
extension=pdo_clickhouse.so
```

## Requirements

- PHP 8.3+
- ClickHouse native extension (ext/clickhouse)
- ClickHouse server
- CMake 3.15+
- LZ4 library (optional)
- ZSTD library (optional)
- OpenSSL (optional)

## Usage

```php
$pdo = new PDO('clickhouse:host=localhost;port=9000;dbname=default');

// Standard PDO
$stmt = $pdo->query('SELECT version()');
$row = $stmt->fetch(PDO::FETCH_ASSOC);

// Prepared statements
$stmt = $pdo->prepare('SELECT * FROM users WHERE id = ?');
$stmt->execute([123]);

// Transactions (experimental)
$pdo->beginTransaction();
$pdo->exec("INSERT INTO users VALUES (1, 'Alice')");
$pdo->commit();

// Bulk insert (ClickHouse-specific)
$pdo->clickhouseInsert('users', ['id', 'name'], [
    [1, 'Alice'],
    [2, 'Bob']
]);

// Insert from file (ClickHouse-specific)
$pdo->clickhouseInsertFromFile('users', '/path/to/data.csv', 'CSV');
```

## ClickHouse-Specific Methods

- `clickhouseInsert($table, $columns, $rows)` - Bulk insert
- `clickhouseInsertFromString($table, $data, $format)` - Insert from string
- `clickhouseInsertFromFile($table, $path, $format)` - Insert from file
- `clickhouseGetDatabases()` - List databases
- `clickhouseGetTables($database)` - List tables
- `clickhouseDescribeTable($table)` - Get table schema

## Testing

```bash
cd build
ctest
```

## License

MIT License
