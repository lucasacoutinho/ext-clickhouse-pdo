--TEST--
PDO ClickHouse: clickhouseGetDatabases() method
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
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $databases = $pdo->clickhouseGetDatabases();

    echo "Databases is array: " . (is_array($databases) ? "YES" : "NO") . "\n";
    echo "Has default: " . (in_array('default', $databases) ? "YES" : "NO") . "\n";
    echo "Has system: " . (in_array('system', $databases) ? "YES" : "NO") . "\n";

    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Databases is array: YES
Has default: YES
Has system: YES
Test passed
