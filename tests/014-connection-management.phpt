--TEST--
PDO ClickHouse: Connection management methods
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

    // Check if connected
    $connected = ch_call($pdo, 'isConnected');
    echo "Initial connection: " . ($connected ? "yes" : "no") . "\n";

    // Execute a query to ensure connection works
    $stmt = $pdo->query("SELECT 1 as num");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Query before reconnect: " . $row['num'] . "\n";

    // Reconnect
    $result = ch_call($pdo, 'reconnect');
    echo "Reconnect result: " . ($result ? "success" : "failed") . "\n";

    // Check connection after reconnect
    $connected = ch_call($pdo, 'isConnected');
    echo "After reconnect: " . ($connected ? "yes" : "no") . "\n";

    // Execute query after reconnect
    $stmt = $pdo->query("SELECT 2 as num");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Query after reconnect: " . $row['num'] . "\n";

    echo "Test completed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Initial connection: yes
Query before reconnect: 1
Reconnect result: success
After reconnect: yes
Query after reconnect: 2
Test completed
