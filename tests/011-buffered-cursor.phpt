--TEST--
PDO ClickHouse: buffered forward cursor across blocks and executions
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
$stmt = $pdo->prepare('SELECT number AS n, number + 10 AS plus_ten FROM numbers(toUInt64(?)) '
    . 'SETTINGS max_block_size=2, max_threads=1');
$stmt->execute([7]);
var_dump($stmt->rowCount() === 7, $stmt->columnCount() === 2);
$meta = $stmt->getColumnMeta(0);
var_dump($meta['name'] === 'n', $meta['native_type'] === 'UInt64');

$rows = [];
while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
    $rows[] = [(int) $row['n'], (int) $row['plus_ten']];
}
var_dump($rows === [[0, 10], [1, 11], [2, 12], [3, 13], [4, 14], [5, 15], [6, 16]]);
var_dump($stmt->fetch() === false, $stmt->fetchColumn() === false);

$stmt->execute([5]);
var_dump((int) $stmt->fetchColumn(1) === 10);
var_dump($stmt->closeCursor());
$stmt->execute([3]);
var_dump(array_map('intval', $stmt->fetchAll(PDO::FETCH_COLUMN, 0)) === [0, 1, 2]);

$stmt->execute([0]);
var_dump($stmt->rowCount() === 0, $stmt->columnCount() === 2, $stmt->fetch() === false);
$stmt->execute([1]);
$stmt->bindColumn(1, $n);
$stmt->bindColumn(2, $next);
var_dump($stmt->fetch(PDO::FETCH_BOUND), (int) $n === 0, (int) $next === 10);
var_dump($stmt->fetch(PDO::FETCH_BOUND) === false);
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
