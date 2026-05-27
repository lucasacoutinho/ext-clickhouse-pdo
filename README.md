# pdo_clickhouse

A PDO driver for ClickHouse using the native TCP protocol (port 9000). Built on top of [ext-clickhouse](https://github.com/lucasacoutinho/ext-clickhouse) and [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp).

## Requirements

- PHP 7.4, 8.0, 8.1, 8.2, 8.3, 8.4, or 8.5
- [ext-clickhouse](https://github.com/lucasacoutinho/ext-clickhouse) (must be installed first)

## PHP version support

CI builds and tests the driver on PHP 7.4, 8.0, 8.1, 8.2, 8.3, 8.4, and 8.5.

`pdo_clickhouse` uses headers installed by `ext-clickhouse`. A sibling `../ext-clickhouse` checkout is still supported for local development, but users should install the native extension first.

PDO releases depend on `ext-clickhouse` releases, not directly on `clickhouse-cpp`. Release images pin the native extension ref instead of building from a floating branch.

## Build

```bash
git clone https://github.com/lucasacoutinho/ext-clickhouse-pdo.git
cd ext-clickhouse-pdo

# ext-clickhouse must be installed first; a sibling ../ext-clickhouse checkout
# is also supported for local development.

phpize
./configure --enable-pdo-clickhouse
make
make install
```

Add to your `php.ini` (after `extension=clickhouse`):

```ini
extension=pdo_clickhouse
```

## Install with PIE

```bash
pie install lucasacoutinho/ext-clickhouse
pie install lucasacoutinho/ext-clickhouse-pdo
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

### TLS DSN Options

Boolean DSN options accept `1`, `0`, `true`, `false`, `yes`, `no`, `on`, and `off` case-insensitively. Invalid values are rejected.

```php
$pdo = new PDO(
    'clickhouse:host=host.example.com;port=9440;dbname=default;ssl=on;ca_file=/path/to/ca.pem;client_cert=/path/to/client.crt;client_key=/path/to/client.key',
    'default',
    'secret'
);
```

Supported TLS options: `ssl`, `skip_verify`, `ca_path`, `ca_file`, `client_cert`, and `client_key`. System CA locations and SNI are enabled when `ssl` is enabled.

## Docker

Pre-built images are available on GitHub Container Registry:

```bash
docker pull ghcr.io/lucasacoutinho/ext-clickhouse-pdo:php8.5-latest
```

## Running tests

```bash
# Requires a running ClickHouse instance and ext-clickhouse installed
CLICKHOUSE_HOST=127.0.0.1 make test
```

## License

[MIT](LICENSE)
