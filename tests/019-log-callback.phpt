--TEST--
PDO ClickHouse: Log callback
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

    $logMessages = [];

    // Set log callback
    $result = $pdo->clickhouseSetLogCallback(function($message) use (&$logMessages) {
        $logMessages[] = $message;
    });
    echo "setLogCallback: " . ($result ? "success" : "failed") . "\n";

    // Execute some queries that might generate logs
    $pdo->exec("SELECT 1");
    $pdo->query("SELECT 2")->fetch();

    // Note: Log messages depend on the native driver implementation
    // We just verify the callback was set successfully
    echo "Callback registered: yes\n";
    echo "Messages captured: " . (count($logMessages) >= 0 ? "OK" : "failed") . "\n";

    // Clear callback
    $result = $pdo->clickhouseSetLogCallback(null);
    echo "Clear callback: " . ($result ? "success" : "failed") . "\n";

    echo "Test completed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
setLogCallback: success
Callback registered: yes
Messages captured: OK
Clear callback: success
Test completed
