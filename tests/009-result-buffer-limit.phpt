--TEST--
PDO ClickHouse bounds buffered SELECT results
--SKIPIF--
<?php
require __DIR__ . '/pdo_clickhouse_test.inc';
pdo_clickhouse_skip_if_unavailable();
?>
--FILE--
<?php
require __DIR__ . '/pdo_clickhouse_test.inc';

$dsn = getenv('PDO_CLICKHOUSE_DSN') ?: 'clickhouse:host=127.0.0.1;port=9000;dbname=default';
$pdo = new PDO($dsn . ';max_buffered_rows=2', getenv('PDO_CLICKHOUSE_USER') ?: 'default',
    getenv('PDO_CLICKHOUSE_PASSWORD') ?: '', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

try {
    $pdo->query('SELECT number FROM system.numbers LIMIT 3');
} catch (PDOException $e) {
    echo strpos($e->getMessage(), 'max_buffered_rows') !== false ? "limited\n" : "wrong error\n";
}
?>
--EXPECT--
limited
