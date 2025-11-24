--TEST--
PDO ClickHouse: Query ID methods
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

    // Test initial state
    $queryId = $pdo->clickhouseGetQueryId();
    echo "Initial query ID: " . ($queryId === null ? "null" : $queryId) . "\n";

    // Set a query ID
    $result = $pdo->clickhouseSetQueryId("test-query-123");
    echo "setQueryId result: " . ($result ? "success" : "failed") . "\n";

    // Get the query ID
    $queryId = $pdo->clickhouseGetQueryId();
    echo "Query ID: " . $queryId . "\n";

    // Execute a query
    $pdo->exec("SELECT 1");

    // Get last query ID (may be null if not tracked during execution)
    $lastId = $pdo->clickhouseGetLastQueryId();
    echo "Last query ID: " . ($lastId !== null ? "tracked" : "null") . "\n";

    // Clear query ID
    $pdo->clickhouseSetQueryId(null);
    $queryId = $pdo->clickhouseGetQueryId();
    echo "After clearing: " . ($queryId === null ? "null" : $queryId) . "\n";

    echo "Test completed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Initial query ID: null
setQueryId result: success
Query ID: test-query-123
Last query ID: null
After clearing: null
Test completed
