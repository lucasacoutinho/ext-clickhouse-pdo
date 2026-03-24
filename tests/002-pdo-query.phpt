--TEST--
PDO ClickHouse: PDO::query() with fetch modes
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

// FETCH_ASSOC
$stmt = $pdo->query("SELECT 1 AS id, 'hello' AS name");
$row = $stmt->fetch(PDO::FETCH_ASSOC);
var_dump($row['id']);
var_dump($row['name']);

// FETCH_NUM
$stmt = $pdo->query("SELECT 42, 'world'");
$row = $stmt->fetch(PDO::FETCH_NUM);
var_dump($row[0]);
var_dump($row[1]);

// FETCH_OBJ
$stmt = $pdo->query("SELECT 99 AS val");
$row = $stmt->fetch(PDO::FETCH_OBJ);
var_dump($row->val);

// Multiple rows
$stmt = $pdo->query("SELECT number FROM system.numbers LIMIT 3");
$rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
var_dump(count($rows));
var_dump($rows[0]['number']);
var_dump($rows[2]['number']);

echo "OK\n";
?>
--EXPECT--
int(1)
string(5) "hello"
int(42)
string(5) "world"
int(99)
int(3)
int(0)
int(2)
OK
