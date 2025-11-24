--TEST--
PDO ClickHouse: Timeout configuration
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

    // Get initial timeout
    $timeout = $pdo->clickhouseGetTimeout();
    echo "Initial timeout: " . ($timeout >= 0 ? "OK" : "invalid") . "\n";

    // Set timeout to 5 seconds
    $result = $pdo->clickhouseSetTimeout(5000);
    echo "setTimeout(5000): " . ($result ? "success" : "failed") . "\n";

    $timeout = $pdo->clickhouseGetTimeout();
    echo "Timeout after setting 5000ms: " . $timeout . "\n";

    // Set timeout to 30 seconds
    $pdo->clickhouseSetTimeout(30000);
    $timeout = $pdo->clickhouseGetTimeout();
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
