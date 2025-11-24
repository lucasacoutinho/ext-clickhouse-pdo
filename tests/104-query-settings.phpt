--TEST--
PDO ClickHouse: setQuerySetting() and clearQuerySettings()
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

    // Set query settings
    ch_call($pdo, 'setQuerySetting', 'max_execution_time', '60');
    ch_call($pdo, 'setQuerySetting', 'max_threads', '4');
    echo "Settings set\n";

    // Clear settings
    ch_call($pdo, 'clearQuerySettings');
    echo "Settings cleared\n";

    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Settings set
Settings cleared
Test passed
