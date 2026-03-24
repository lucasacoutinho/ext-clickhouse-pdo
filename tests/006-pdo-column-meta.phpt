--TEST--
PDO ClickHouse: getColumnMeta() returns ClickHouse type info
--EXTENSIONS--
pdo
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

$stmt = $pdo->query("SELECT
    toUInt64(1) AS id,
    'hello' AS name,
    toNullable(toFloat64(3.14)) AS score
");

$stmt->fetch(); // need to fetch at least one row

$meta0 = $stmt->getColumnMeta(0);
var_dump($meta0['name']);
var_dump($meta0['native_type']);

$meta1 = $stmt->getColumnMeta(1);
var_dump($meta1['name']);
var_dump($meta1['native_type']);

$meta2 = $stmt->getColumnMeta(2);
var_dump($meta2['name']);
var_dump($meta2['native_type']);

echo "OK\n";
?>
--EXPECTF--
string(2) "id"
string(6) "UInt64"
string(4) "name"
string(6) "String"
string(5) "score"
string(%d) "Nullable(Float64)"
OK
