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
echo $row['n'], "\n";
echo $row['s'], "\n";

// Positional parameters
$stmt = $pdo->prepare("SELECT ? AS a, ? AS b");
$stmt->execute([100, 'world']);
$row = $stmt->fetch(PDO::FETCH_ASSOC);
echo $row['a'], "\n";
echo $row['b'], "\n";

// Explicit PDO parameter types
$stmt = $pdo->prepare("SELECT :i AS i, :b AS b, :n AS n, :s AS s");
$stmt->bindValue('i', '42', PDO::PARAM_INT);
$stmt->bindValue('b', false, PDO::PARAM_BOOL);
$stmt->bindValue('n', null, PDO::PARAM_NULL);
$stmt->bindValue('s', "it's ok", PDO::PARAM_STR);
$stmt->execute();
$row = $stmt->fetch(PDO::FETCH_ASSOC);
echo $row['i'], "\n";
echo $row['b'], "\n";
var_dump($row['n'] === null);
echo $row['s'], "\n";

// Null values must override explicit scalar parameter types for this execution only.
$stmt = $pdo->prepare("SELECT :v AS v");
$stmt->bindValue('v', null, PDO::PARAM_INT);
$stmt->execute();
$row = $stmt->fetch(PDO::FETCH_ASSOC);
var_dump($row['v'] === null);

// Inferred null type must not permanently mutate the bound parameter type.
$value = null;
$stmt = $pdo->prepare("SELECT :v AS v");
$stmt->bindParam('v', $value);
$stmt->execute();
$row = $stmt->fetch(PDO::FETCH_ASSOC);
var_dump($row['v'] === null);
$value = 42;
$stmt->execute();
$row = $stmt->fetch(PDO::FETCH_ASSOC);
echo $row['v'], "\n";

echo "OK\n";
?>
--EXPECT--
42
hello
100
world
42
0
bool(true)
it's ok
bool(true)
bool(true)
42
OK
