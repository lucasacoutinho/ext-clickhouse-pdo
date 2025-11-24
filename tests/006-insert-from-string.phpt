--TEST--
PDO ClickHouse: Insert from string with different formats
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

    $pdo->exec("DROP TABLE IF EXISTS pdo_test_006");
    $pdo->exec("
        CREATE TABLE pdo_test_006 (
            id UInt32,
            name String,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY id
    ");

    // Test TabSeparated format (default)
    $data = "1\tAlice\t3.14\n2\tBob\t2.71\n";
    ch_call($pdo, 'insertFromString', 'pdo_test_006', $data, 'TabSeparated');
    echo "Inserted with TabSeparated\n";

    // Test CSV format
    $data = "3,Charlie,1.41\n4,David,1.73\n";
    ch_call($pdo, 'insertFromString', 'pdo_test_006', $data, 'CSV');
    echo "Inserted with CSV\n";

    // Test JSONEachRow format
    $data = '{"id":5,"name":"Eve","value":0.577}' . "\n" . '{"id":6,"name":"Frank","value":1.618}' . "\n";
    ch_call($pdo, 'insertFromString', 'pdo_test_006', $data, 'JSONEachRow');
    echo "Inserted with JSONEachRow\n";

    // Query to verify
    $stmt = $pdo->query("SELECT count() as cnt FROM pdo_test_006");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Total rows: " . $row['cnt'] . "\n";

    // Verify some data
    $stmt = $pdo->query("SELECT name FROM pdo_test_006 WHERE id = 1");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "First record: " . $row['name'] . "\n";

    $pdo->exec("DROP TABLE pdo_test_006");
    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Inserted with TabSeparated
Inserted with CSV
Inserted with JSONEachRow
Total rows: 6
First record: Alice
Test passed
