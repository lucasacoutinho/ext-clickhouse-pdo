--TEST--
PDO ClickHouse: CREATE TABLE and basic operations
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

    // Drop table if exists
    $pdo->exec("DROP TABLE IF EXISTS pdo_test_003");

    // Create table
    $pdo->exec("
        CREATE TABLE pdo_test_003 (
            id UInt32,
            name String,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY id
    ");
    echo "Table created\n";

    // Insert data
    $stmt = $pdo->prepare("INSERT INTO pdo_test_003 VALUES (?, ?, ?)");
    $stmt->execute([1, 'Alice', 100.5]);
    $stmt->execute([2, 'Bob', 200.75]);
    echo "Data inserted\n";

    // Query data
    $stmt = $pdo->query("SELECT * FROM pdo_test_003 ORDER BY id");
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

    echo "Row count: " . count($rows) . "\n";
    echo "First row: " . $rows[0]['id'] . ", " . $rows[0]['name'] . ", " . $rows[0]['value'] . "\n";

    // Cleanup
    $pdo->exec("DROP TABLE pdo_test_003");
    echo "Table dropped\n";

    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Table created
Data inserted
Row count: 2
First row: 1, Alice, 100.5
Table dropped
Test passed
