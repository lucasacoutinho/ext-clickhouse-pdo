--TEST--
PDO ClickHouse: clickhouseSetQuerySetting() and clickhouseClearQuerySettings()
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

    // Set query settings
    $pdo->clickhouseSetQuerySetting('max_execution_time', '60');
    $pdo->clickhouseSetQuerySetting('max_threads', '4');
    echo "Settings set\n";

    // Clear settings
    $pdo->clickhouseClearQuerySettings();
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
