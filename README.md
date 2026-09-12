<div align="center">
  <h1>pdo_clickhouse</h1>
  <p>
    A PDO driver for ClickHouse over the native TCP protocol, built on
    <a href="https://github.com/lucasacoutinho/ext-clickhouse">ext-clickhouse</a>.
  </p>
  <p>
    <a href="https://github.com/lucasacoutinho/ext-clickhouse-pdo/actions/workflows/ci.yml"><img alt="Build status" src="https://img.shields.io/github/actions/workflow/status/lucasacoutinho/ext-clickhouse-pdo/ci.yml?branch=main&style=for-the-badge&labelColor=000000"></a>
    <a href="https://packagist.org/packages/lucasacoutinho/ext-clickhouse-pdo"><img alt="Packagist version" src="https://img.shields.io/packagist/v/lucasacoutinho/ext-clickhouse-pdo?style=for-the-badge&labelColor=000000"></a>
    <a href="#requirements"><img alt="PHP 7.4 through 8.5" src="https://img.shields.io/badge/PHP-7.4%20to%208.5-777BB4?style=for-the-badge&logo=php&logoColor=white&labelColor=000000"></a>
    <a href="https://github.com/lucasacoutinho/ext-clickhouse-pdo/blob/main/LICENSE"><img alt="MIT license" src="https://img.shields.io/github/license/lucasacoutinho/ext-clickhouse-pdo?style=for-the-badge&labelColor=000000"></a>
  </p>
</div>

## Getting started

Install the native extension first, then install the PDO driver with
[PIE](https://github.com/php/pie):

```bash
pie install lucasacoutinho/ext-clickhouse
pie install lucasacoutinho/ext-clickhouse-pdo
```

If the installer does not manage `php.ini`, load the modules in this order:

```ini
extension=clickhouse
extension=pdo_clickhouse
```

Connect through the normal PDO API:

```php
$pdo = new PDO(
    'clickhouse:host=127.0.0.1;port=9000;dbname=default',
    'default',
    '',
    [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
);

$rows = $pdo
    ->query('SELECT number FROM system.numbers LIMIT 5')
    ->fetchAll(PDO::FETCH_COLUMN);
```

The driver uses ClickHouse's native TCP port, usually `9000`. It does not use
the HTTP interface.

SELECT results are buffered before PDO fetches rows. The driver rejects a result
that exceeds either connection limit:

| DSN option | Default | Measures |
| --- | --- | --- |
| `max_buffered_rows` | `1000000` | Rows in one result |
| `max_buffered_bytes` | `67108864` (64 MiB) | Uncompressed serialized column values and result schema |

Both options accept decimal digits representing a positive integer no greater
than `PHP_INT_MAX`. For example:

```php
$pdo = new PDO('clickhouse:host=127.0.0.1;max_buffered_rows=10000;max_buffered_bytes=8388608');
```

Exceeding a limit cancels and drains the query, discards its partial result, and
reports a PDO error. The connection can then execute another query. These limits
apply separately to each buffered statement; they do not silently truncate data.

The byte budget includes nested values and LowCardinality dictionaries. It is
not a limit on PHP process memory: native object overhead, other statements,
and the incoming block decoded by clickhouse-cpp are outside that budget. Use
ClickHouse server limits such as `max_result_bytes` with
`result_overflow_mode='throw'` to restrict results before they reach the client.
Connections must use a trusted server; this driver does not impose allocation
limits on the upstream protocol decoder.

## Requirements

| Component | Supported version |
| --- | --- |
| PHP | 7.4 through 8.5 |
| `ext-pdo` | The version bundled with PHP |
| `ext-clickhouse` | The matching minor release line |

CI builds and tests every supported PHP version.

## Native version pairing

`pdo_clickhouse` includes headers installed by `ext-clickhouse` and constructs
the same C++ types at runtime. The minor release lines must match.

| `pdo_clickhouse` | `ext-clickhouse` |
| --- | --- |
| 1.5.x | 1.5.x |
| 1.4.x | 1.4.x |
| 1.3.x | 1.3.x |
| 1.2.x | 1.2.x |

Composer enforces the 1.5.x pairing for the current release. Published Docker
images also pin the matching native extension tag and verify its runtime API
before they build PDO.

For local development, the build can read headers from a sibling
`../ext-clickhouse` checkout. Installed headers remain the normal path for
users.

## Queries and statements

Prepared statements use PDO's emulated parameter handling:

```php
$statement = $pdo->prepare(
    'SELECT * FROM events WHERE account_id = :account_id LIMIT 10'
);
$statement->bindValue('account_id', 42, PDO::PARAM_INT);
$statement->execute();

$events = $statement->fetchAll(PDO::FETCH_ASSOC);
```

DDL and inserts use the same PDO methods:

```php
$pdo->exec(
    'CREATE TABLE IF NOT EXISTS test '
    . '(id UInt64, name String) ENGINE = Memory'
);

$pdo->exec("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')");
```

### Transactions

ClickHouse does not provide the transaction semantics expected by PDO. The
driver therefore rejects `beginTransaction()`, `commit()`, and `rollBack()`
instead of reporting success for operations that cannot make writes atomic or
revert them. Writes are executed immediately.

## TLS

TLS settings are part of the DSN. Boolean options accept `1`, `0`, `true`,
`false`, `yes`, `no`, `on`, and `off`, without regard to case. The driver
rejects invalid values.

```php
$dsn = 'clickhouse:host=host.example.com;port=9440;dbname=default'
    . ';ssl=on'
    . ';ca_file=/path/to/ca.pem'
    . ';client_cert=/path/to/client.crt'
    . ';client_key=/path/to/client.key';

$pdo = new PDO($dsn, 'default', 'secret');
```

Supported options are `ssl`, `skip_verify`, `ca_path`, `ca_file`,
`client_cert`, and `client_key`. When SSL is enabled, the client uses system CA
locations and SNI unless the DSN overrides them.

## Build from source

Install `ext-clickhouse` first or keep its checkout next to this repository.
Then use the standard PHP extension build flow:

```bash
git clone https://github.com/lucasacoutinho/ext-clickhouse-pdo.git
cd ext-clickhouse-pdo

phpize
./configure --enable-pdo-clickhouse
make
make install
```

## Docker

Versioned and rolling images are published for each supported PHP release:

```bash
docker pull ghcr.io/lucasacoutinho/ext-clickhouse-pdo:php8.5-v1.5.0
docker pull ghcr.io/lucasacoutinho/ext-clickhouse-pdo:php8.5-latest
```

Each image contains the matching `clickhouse` and `pdo_clickhouse` modules.

## Testing

Run the PHPT suite against a live ClickHouse server with `ext-clickhouse`
installed:

```bash
CLICKHOUSE_HOST=127.0.0.1 make test
```

The GitHub Actions matrix also runs integration tests, sanitizers, coverage,
formatting, and clang-tidy on every supported PHP version.

## Contributing

Bug reports and focused pull requests are welcome. Open an
[issue](https://github.com/lucasacoutinho/ext-clickhouse-pdo/issues) with the
PHP version, both extension versions, and a minimal reproduction.

## License

[MIT](LICENSE)
