--TEST--
PDO ClickHouse: type mapping through PDO
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
    toUInt8(255) AS u8,
    toInt32(-42) AS i32,
    toFloat64(3.14) AS f64,
    'hello' AS str,
    toDate('2024-01-15') AS d,
    [1, 2, 3] AS arr,
    toNullable(CAST(NULL AS Nullable(String))) AS nullable_null,
    toNullable(toString('value')) AS nullable_val
");

$row = $stmt->fetch(PDO::FETCH_ASSOC);

var_dump($row['u8']);
var_dump($row['i32']);
var_dump($row['f64']);
var_dump($row['str']);
var_dump($row['d']);
var_dump($row['arr']);
var_dump($row['nullable_null']);
var_dump($row['nullable_val']);

echo "OK\n";
?>
--EXPECT--
int(255)
int(-42)
float(3.14)
string(5) "hello"
string(10) "2024-01-15"
array(3) {
  [0]=>
  int(1)
  [1]=>
  int(2)
  [2]=>
  int(3)
}
NULL
string(5) "value"
OK
