--TEST--
PDO ClickHouse: exec and statement row counts include all written progress packets
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
$table = '_pdo_row_count_' . getmypid();

$pdo->exec("DROP TABLE IF EXISTS {$table}");
$pdo->exec("CREATE TABLE {$table} (id UInt64) ENGINE = Memory");

try {
    $written = $pdo->exec("INSERT INTO {$table} VALUES (1), (2), (3)");
    echo "PDO::exec rows: {$written}\n";

    $statement = $pdo->prepare(
        "INSERT INTO {$table} SELECT number + 100 FROM system.numbers LIMIT 1000000"
    );
    $statement->execute();
    echo "Statement rows: " . $statement->rowCount() . "\n";

    $stored = $pdo->query("SELECT count() FROM {$table}")->fetchColumn();
    echo "Stored rows: {$stored}\n";
} finally {
    $pdo->exec("DROP TABLE IF EXISTS {$table}");
}

echo "OK\n";
?>
--EXPECT--
PDO::exec rows: 3
Statement rows: 1000000
Stored rows: 1000003
OK
