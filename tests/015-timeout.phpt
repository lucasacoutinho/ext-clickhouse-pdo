--TEST--
PDO ClickHouse: Timeout configuration
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

    // Get initial timeout
    $timeout = ch_call($pdo, 'getTimeout');
    echo "Initial timeout: " . ($timeout >= 0 ? "OK" : "invalid") . "\n";

    // Set timeout to 5 seconds
    $result = ch_call($pdo, 'setTimeout', 5000);
    echo "setTimeout(5000): " . ($result ? "success" : "failed") . "\n";

    $timeout = ch_call($pdo, 'getTimeout');
    echo "Timeout after setting 5000ms: " . $timeout . "\n";

    // Set timeout to 30 seconds
    ch_call($pdo, 'setTimeout', 30000);
    $timeout = ch_call($pdo, 'getTimeout');
    echo "Timeout after setting 30000ms: " . $timeout . "\n";

    // Execute a query with timeout set
    $stmt = $pdo->query("SELECT 1 as num");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Query with timeout: " . $row['num'] . "\n";

    echo "Test completed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Initial timeout: OK
setTimeout(5000): success
Timeout after setting 5000ms: 5000
Timeout after setting 30000ms: 30000
Query with timeout: 1
Test completed
