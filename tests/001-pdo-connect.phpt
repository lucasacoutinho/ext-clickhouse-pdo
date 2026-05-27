--TEST--
PDO ClickHouse: basic connection and driver name
--EXTENSIONS--
pdo_clickhouse
clickhouse
--SKIPIF--
<?php
require __DIR__ . '/pdo_clickhouse_test.inc';
pdo_clickhouse_test_skip();
?>
--FILE--
<?php
require __DIR__ . '/pdo_clickhouse_test.inc';

$pdo = pdo_clickhouse_test_pdo();
var_dump($pdo->getAttribute(PDO::ATTR_DRIVER_NAME));
var_dump($pdo->getAttribute(PDO::ATTR_SERVER_VERSION) !== '');
var_dump($pdo->getAttribute(PDO::ATTR_CONNECTION_STATUS));

echo "OK\n";
?>
--EXPECTF--
string(10) "clickhouse"
bool(true)
string(%d) "Connected via native TCP"
OK
