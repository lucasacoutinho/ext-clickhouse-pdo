--TEST--
PDO ClickHouse: Transaction support (EXPERIMENTAL)
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip pdo_clickhouse extension not loaded');
if (!getenv('CLICKHOUSE_HOST')) die('skip CLICKHOUSE_HOST not set');

// Check if transactions are supported
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$port = (int)(getenv('CLICKHOUSE_PORT') ?: 9000);
try {
    $pdo = new PDO("clickhouse:host={$host};port={$port};dbname=default", 'default', '');
    $result = $pdo->beginTransaction();
    $pdo->rollback();
} catch (Exception $e) {
    if (stripos($e->getMessage(), 'not supported') !== false ||
        stripos($e->getMessage(), 'Atomic') !== false ||
        stripos($e->getMessage(), 'database engine') !== false) {
        die('skip Transactions not supported by this ClickHouse version/configuration');
    }
}
?>
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$port = (int)(getenv('CLICKHOUSE_PORT') ?: 9000);

$pdo = new PDO("clickhouse:host={$host};port={$port};dbname=default", 'default', '');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

// Test 1: Initial state
echo "Test 1: Initial transaction state\n";
echo "inTransaction: " . ($pdo->inTransaction() ? 'yes' : 'no') . "\n";

// Test 2: Commit without transaction
echo "\nTest 2: Commit without transaction\n";
try {
    $pdo->commit();
    echo "ERROR: Should have thrown exception\n";
} catch (PDOException $e) {
    echo "Correctly rejected\n";
}

// Test 3: Rollback without transaction
echo "\nTest 3: Rollback without transaction\n";
try {
    $pdo->rollback();
    echo "ERROR: Should have thrown exception\n";
} catch (PDOException $e) {
    echo "Correctly rejected\n";
}

// Test 4: Begin transaction
echo "\nTest 4: Begin transaction\n";
try {
    $result = $pdo->beginTransaction();
    echo "Begin result: " . ($result ? 'success' : 'failed') . "\n";
    echo "inTransaction: " . ($pdo->inTransaction() ? 'yes' : 'no') . "\n";

    // Try nested transaction
    try {
        $pdo->beginTransaction();
        echo "ERROR: Should reject nested transaction\n";
    } catch (PDOException $e) {
        echo "Nested transaction rejected\n";
    }

    // Test rollback
    echo "\nTest 5: Rollback\n";
    $result = $pdo->rollback();
    echo "Rollback result: " . ($result ? 'success' : 'failed') . "\n";
    echo "inTransaction: " . ($pdo->inTransaction() ? 'yes' : 'no') . "\n";

} catch (PDOException $e) {
    echo "Transaction not supported (expected)\n";
    echo "Error contains EXPERIMENTAL: " . (stripos($e->getMessage(), 'EXPERIMENTAL') !== false ? 'yes' : 'no') . "\n";
}

echo "\nOK\n";
?>
--EXPECTF--
Test 1: Initial transaction state
inTransaction: no

Test 2: Commit without transaction
Correctly rejected

Test 3: Rollback without transaction
Correctly rejected

Test 4: Begin transaction
%s
%s

Test 5: Rollback
Rollback result: success
inTransaction: no

OK
