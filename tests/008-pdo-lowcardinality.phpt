--TEST--
PDO ClickHouse: expanded LowCardinality types retain their PHP values
--EXTENSIONS--
pdo_clickhouse
clickhouse
--SKIPIF--
<?php
require __DIR__ . '/pdo_clickhouse_test.inc';
pdo_clickhouse_test_skip();
if (PHP_VERSION_ID < 80100) {
    die('skip PHP 7.4/8.0 PDO fetch API cannot return typed zvals');
}
?>
--FILE--
<?php
require __DIR__ . '/pdo_clickhouse_test.inc';

$pdo = pdo_clickhouse_test_pdo();
$row = $pdo->query("SELECT
    toLowCardinality(toInt32(-42)) AS number,
    toLowCardinality(toUInt64('18446744073709551615')) AS large,
    toLowCardinality(toIPv4('127.0.0.1')) AS ip,
    toLowCardinality(CAST(NULL AS Nullable(Int32))) AS missing,
    toLowCardinality(toNullable(toInt32(7))) AS present
")->fetch(PDO::FETCH_ASSOC);

var_dump($row);
?>
--EXPECT--
array(5) {
  ["number"]=>
  int(-42)
  ["large"]=>
  string(20) "18446744073709551615"
  ["ip"]=>
  string(9) "127.0.0.1"
  ["missing"]=>
  NULL
  ["present"]=>
  int(7)
}
