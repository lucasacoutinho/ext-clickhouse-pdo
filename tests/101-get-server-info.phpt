--TEST--
PDO ClickHouse: clickhouseGetServerInfo() method
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

    $info = $pdo->clickhouseGetServerInfo();

    echo "Server info is array: " . (is_array($info) ? "YES" : "NO") . "\n";
    echo "Has name: " . (isset($info['name']) ? "YES" : "NO") . "\n";
    echo "Has version_major: " . (isset($info['version_major']) ? "YES" : "NO") . "\n";
    echo "Has version_minor: " . (isset($info['version_minor']) ? "YES" : "NO") . "\n";

    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Server info is array: YES
Has name: YES
Has version_major: YES
Has version_minor: YES
Test passed
