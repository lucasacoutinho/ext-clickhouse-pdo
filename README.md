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
| 1.3.x | 1.3.x |
| 1.2.x | 1.2.x |

Composer enforces the 1.3.x pairing for the current release. Published Docker
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
docker pull ghcr.io/lucasacoutinho/ext-clickhouse-pdo:php8.5-v1.3.0
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
