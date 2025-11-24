--TEST--
PDO ClickHouse: Query ID methods
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip PDO ClickHouse extension not loaded');
?>
--FILE--
<?php
error_reporting(E_ALL & ~E_DEPRECATED);
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse-server';
$port = getenv('CLICKHOUSE_PORT') ?: '9000';


$class = class_exists('Pdo\\Clickhouse') ? 'Pdo\Clickhouse' : 'PDO';

function ch_call($pdo, string $name, ...$args) {
    if (method_exists($pdo, $name)) {
        return $pdo->$name(...$args);
    }
    $legacy = 'clickhouse' . ucfirst($name);
    return $pdo->$legacy(...$args);
}

try {
    $pdo = new $class("clickhouse:host=$host;port=$port;dbname=default", 'default', '');

    // Test initial state
    $queryId = ch_call($pdo, 'getQueryId');
    echo "Initial query ID: " . ($queryId === null ? "null" : $queryId) . "\n";

    // Set a query ID
    $result = ch_call($pdo, 'setQueryId', "test-query-123");
    echo "setQueryId result: " . ($result ? "success" : "failed") . "\n";

    // Get the query ID
    $queryId = ch_call($pdo, 'getQueryId');
    echo "Query ID: " . $queryId . "\n";

    // Execute a query
    $pdo->exec("SELECT 1");

    // Get last query ID (may be null if not tracked during execution)
    $lastId = ch_call($pdo, 'getLastQueryId');
    echo "Last query ID: " . ($lastId !== null ? "tracked" : "null") . "\n";

    // Clear query ID
    ch_call($pdo, 'setQueryId', null);
    $queryId = ch_call($pdo, 'getQueryId');
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
