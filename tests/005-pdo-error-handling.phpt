--TEST--
PDO ClickHouse: error handling with ERRMODE_EXCEPTION
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

try {
    $pdo->query('SELECT * FROM _nonexistent_pdo_table_xyz');
    echo "FAIL: should have thrown\n";
} catch (PDOException $e) {
    echo "Caught PDOException\n";
    var_dump(strlen($e->getMessage()) > 0);
    var_dump($e->getCode() !== '00000');
}

// Quote function
$quoted = $pdo->quote("it's a test");
var_dump($quoted);

// Transaction rollback should fail
try {
    $pdo->beginTransaction();
    $pdo->rollBack();
    echo "FAIL: rollback should fail\n";
} catch (PDOException $e) {
    echo "Rollback correctly rejected\n";
}

echo "OK\n";
?>
--EXPECTF--
Caught PDOException
bool(true)
bool(true)
string(15) "'it\'s a test'"
Rollback correctly rejected
OK
