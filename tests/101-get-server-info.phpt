--TEST--
PDO ClickHouse: getServerInfo() method
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip PDO ClickHouse extension not loaded');
?>
--FILE--
<?php
error_reporting(E_ALL & ~E_DEPRECATED);
$host = getenv('CLICKHOUSE_HOST') ?: 'localhost';
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
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $info = ch_call($pdo, 'getServerInfo');

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
