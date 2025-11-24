--TEST--
PDO ClickHouse: Execute batch
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip PDO ClickHouse extension not loaded');
?>
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse-server';
$port = getenv('CLICKHOUSE_PORT') ?: '9000';

try {
    $pdo = new PDO("clickhouse:host=$host;port=$port;dbname=default", 'default', '');

    // Create test table
    $pdo->exec("DROP TABLE IF EXISTS test_batch_phpt");
    $pdo->exec("CREATE TABLE test_batch_phpt (id UInt32, value String) ENGINE = Memory");

    // Execute batch of queries
    $queries = [
        "INSERT INTO test_batch_phpt VALUES (1, 'first')",
        "INSERT INTO test_batch_phpt VALUES (2, 'second')",
        "INSERT INTO test_batch_phpt VALUES (3, 'third')"
    ];

    $results = $pdo->clickhouseExecuteBatch($queries);
    echo "Batch execution: " . (is_array($results) ? "success" : "failed") . "\n";
    echo "Queries executed: " . count($results) . "\n";
    echo "All succeeded: " . (array_sum($results) === count($results) ? "yes" : "no") . "\n";

    // Verify data
    $stmt = $pdo->query("SELECT COUNT(*) as cnt FROM test_batch_phpt");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Rows inserted: " . $row['cnt'] . "\n";

    // Verify values
    $stmt = $pdo->query("SELECT * FROM test_batch_phpt ORDER BY id");
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
    echo "First row: id=" . $rows[0]['id'] . ", value=" . $rows[0]['value'] . "\n";
    echo "Last row: id=" . $rows[2]['id'] . ", value=" . $rows[2]['value'] . "\n";

    // Cleanup
    $pdo->exec("DROP TABLE test_batch_phpt");

    echo "Test completed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Batch execution: success
Queries executed: 3
All succeeded: yes
Rows inserted: 3
First row: id=1, value=first
Last row: id=3, value=third
Test completed
