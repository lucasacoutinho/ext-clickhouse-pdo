--TEST--
PDO ClickHouse: Basic connection
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip PDO ClickHouse extension not loaded');
?>
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'localhost';
$port = getenv('CLICKHOUSE_PORT') ?: '9000';

try {
    $pdo = new PDO("clickhouse:host=$host;port=$port;dbname=default", 'default', '');
    echo "Connection successful\n";

    // Test getAttribute
    $version = $pdo->getAttribute(PDO::ATTR_SERVER_VERSION);
    echo "Server version: " . (strlen($version) > 0 ? "OK" : "FAIL") . "\n";

    $status = $pdo->getAttribute(PDO::ATTR_CONNECTION_STATUS);
    echo "Connection status: $status\n";

    $pdo = null;
    echo "Disconnection successful\n";

} catch (PDOException $e) {
    echo "Connection failed: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Connection successful
Server version: OK
Connection status: Connected
Disconnection successful
