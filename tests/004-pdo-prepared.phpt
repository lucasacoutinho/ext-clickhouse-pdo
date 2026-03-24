--TEST--
PDO ClickHouse: prepared statements with emulated parameters
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

// Named parameters (emulated)
$stmt = $pdo->prepare("SELECT :val AS n, :name AS s");
$stmt->execute(['val' => 42, 'name' => 'hello']);
$row = $stmt->fetch(PDO::FETCH_ASSOC);
var_dump($row['n']);
var_dump($row['s']);

// Positional parameters
$stmt = $pdo->prepare("SELECT ? AS a, ? AS b");
$stmt->execute([100, 'world']);
$row = $stmt->fetch(PDO::FETCH_ASSOC);
var_dump($row['a']);
var_dump($row['b']);

echo "OK\n";
?>
--EXPECT--
int(42)
string(5) "hello"
int(100)
string(5) "world"
OK
