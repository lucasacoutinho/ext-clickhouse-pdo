# pdo_clickhouse

A PDO driver for ClickHouse using the native TCP protocol (port 9000). Built on top of [ext-clickhouse](https://github.com/lightprofco/ext-clickhouse) and [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp).

## Requirements

- PHP 8.1+
- [ext-clickhouse](https://github.com/lightprofco/ext-clickhouse) (must be installed first)

## Build

```bash
git clone https://github.com/lightprofco/ext-clickhouse-pdo.git
cd ext-clickhouse-pdo

# ext-clickhouse must be in a sibling directory
ls ../ext-clickhouse/php_clickhouse.h  # verify it's there

phpize
./configure --enable-pdo-clickhouse
make
make install
```

Add to your `php.ini` (after `extension=clickhouse`):

```ini
extension=pdo_clickhouse
```

## Usage

```php
$pdo = new PDO('clickhouse:host=127.0.0.1;port=9000;dbname=default', 'default', '');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

// Query
$stmt = $pdo->query('SELECT number, toString(number) AS str FROM system.numbers LIMIT 5');
$rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

// Prepared statements (emulated)
$stmt = $pdo->prepare('SELECT * FROM my_table WHERE id = :id');
$stmt->bindValue('id', 42);
$stmt->execute();

// DDL
$pdo->query('CREATE TABLE IF NOT EXISTS test (id UInt64, name String) ENGINE = Memory');

// INSERT
$pdo->query("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')");
```

## Docker

Pre-built images are available on GitHub Container Registry:

```bash
docker pull ghcr.io/lightprofco/ext-clickhouse-pdo:php8.4-latest
```

## Running tests

```bash
# Requires a running ClickHouse instance and ext-clickhouse installed
CLICKHOUSE_HOST=127.0.0.1 make test
```

## License

[MIT](LICENSE)
