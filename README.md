# PDO ClickHouse

Native PDO driver for ClickHouse using the TCP protocol (port 9000).

## Requirements

- PHP 8.3+
- ClickHouse server
- CMake 3.15+
- [ext-clickhouse](https://github.com/lucasacoutinho/ext-clickhouse) - Base extension
- Optional: liblz4-dev, libzstd-dev, libssl-dev

## Build

First, clone the base extension:

```bash
git clone https://github.com/lucasacoutinho/ext-clickhouse ../clickhouse
```

Then build:

```bash
mkdir build && cd build
cmake ..
cmake --build .
sudo cmake --install .
```

## Configure

Add to `php.ini`:

```ini
extension=pdo_clickhouse.so
```

## Usage

```php
// Connect
$pdo = new PDO('clickhouse:host=localhost;port=9000;dbname=default', 'default', '');

// Query
$result = $pdo->query("SELECT * FROM users");
$users = $result->fetchAll(PDO::FETCH_ASSOC);

// Driver-specific methods
$info = $pdo->clickhouseGetServerInfo();
$pdo->clickhouseInsert('events', ['id', 'name'], $data);
```

## DSN Format

```
clickhouse:host=<host>;port=<port>;dbname=<database>
```

Default: `clickhouse:host=localhost;port=9000;dbname=default`

## Testing

```bash
cd build
ctest -V
```

## License

MIT - Copyright (c) 2025 Lucas Coutinho