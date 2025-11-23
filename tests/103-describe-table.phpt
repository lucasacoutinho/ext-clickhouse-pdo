--TEST--
PDO ClickHouse: clickhouseDescribeTable() method
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

    // Create test table
    $pdo->exec("DROP TABLE IF EXISTS pdo_test_103");
    $pdo->exec("
        CREATE TABLE pdo_test_103 (
            id UInt32,
            name String,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY id
    ");

    $schema = $pdo->clickhouseDescribeTable('pdo_test_103');

    echo "Schema is array: " . (is_array($schema) ? "YES" : "NO") . "\n";
    echo "Column count: " . count($schema) . "\n";
    echo "First column name: " . $schema[0]['name'] . "\n";
    echo "First column has type: " . (isset($schema[0]['type']) ? "YES" : "NO") . "\n";

    $pdo->exec("DROP TABLE pdo_test_103");
    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Schema is array: YES
Column count: 3
First column name: id
First column has type: YES
Test passed
