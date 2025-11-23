--TEST--
PDO ClickHouse: Prepared statements with parameters
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

    $pdo->exec("DROP TABLE IF EXISTS pdo_test_004");
    $pdo->exec("
        CREATE TABLE pdo_test_004 (
            id UInt32,
            name String
        ) ENGINE = MergeTree()
        ORDER BY id
    ");

    // Test positional parameters
    $stmt = $pdo->prepare("INSERT INTO pdo_test_004 VALUES (?, ?)");
    $stmt->execute([1, 'Test1']);
    $stmt->execute([2, 'Test2']);
    echo "Inserted with positional parameters\n";

    // Query with parameter
    $stmt = $pdo->prepare("SELECT * FROM pdo_test_004 WHERE id = ?");
    $stmt->execute([1]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);

    echo "Found: " . $row['name'] . "\n";

    $pdo->exec("DROP TABLE pdo_test_004");
    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Inserted with positional parameters
Found: Test1
Test passed
