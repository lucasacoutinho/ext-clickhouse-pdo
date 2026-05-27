--TEST--
PDO ClickHouse: PDO::query() with fetch modes
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

// FETCH_ASSOC
$stmt = $pdo->query("SELECT 1 AS id, 'hello' AS name");
$row = $stmt->fetch(PDO::FETCH_ASSOC);
echo $row['id'], "\n";
echo $row['name'], "\n";

// FETCH_NUM
$stmt = $pdo->query("SELECT 42, 'world'");
$row = $stmt->fetch(PDO::FETCH_NUM);
echo $row[0], "\n";
echo $row[1], "\n";

// FETCH_OBJ
$stmt = $pdo->query("SELECT 99 AS val");
$row = $stmt->fetch(PDO::FETCH_OBJ);
echo $row->val, "\n";

// Multiple rows
$stmt = $pdo->query("SELECT number FROM system.numbers LIMIT 3");
$rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
echo count($rows), "\n";
echo $rows[0]['number'], "\n";
echo $rows[2]['number'], "\n";

echo "OK\n";
?>
--EXPECT--
1
hello
42
world
99
3
0
2
OK
