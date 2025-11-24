--TEST--
PDO ClickHouse: Comprehensive prepared statement bindings
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

    $pdo->exec("DROP TABLE IF EXISTS pdo_test_008");
    $pdo->exec("
        CREATE TABLE pdo_test_008 (
            id UInt32,
            name String,
            score Float64,
            active UInt8,
            created_at String
        ) ENGINE = MergeTree()
        ORDER BY id
    ");

    // Test 1: Positional parameters with different types
    echo "Test 1: Positional parameters\n";
    $stmt = $pdo->prepare("INSERT INTO pdo_test_008 VALUES (?, ?, ?, ?, ?)");

    // Integer, string, float, boolean, string
    $stmt->execute([1, 'Alice', 95.5, 1, '2024-01-01']);
    $stmt->execute([2, 'Bob', 87.3, 0, '2024-01-02']);
    $stmt->execute([3, 'Charlie', 92.1, 1, '2024-01-03']);
    echo "Inserted 3 rows with positional params\n";

    // Test 2: Query with WHERE clause using parameters
    echo "\nTest 2: SELECT with WHERE parameters\n";
    $stmt = $pdo->prepare("SELECT name, score FROM pdo_test_008 WHERE active = ? AND score > ? ORDER BY score DESC");
    $stmt->execute([1, 90.0]);

    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        echo "- {$row['name']}: {$row['score']}\n";
    }

    // Test 3: Multiple parameters in WHERE clause
    echo "\nTest 3: Multiple WHERE conditions\n";
    $stmt = $pdo->prepare("SELECT name FROM pdo_test_008 WHERE id >= ? AND id <= ? ORDER BY id");
    $stmt->execute([1, 2]);

    $names = [];
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $names[] = $row['name'];
    }
    echo "Found: " . implode(', ', $names) . "\n";

    // Test 4: String escaping
    echo "\nTest 4: String escaping\n";
    $stmt = $pdo->prepare("INSERT INTO pdo_test_008 VALUES (?, ?, ?, ?, ?)");
    $stmt->execute([4, "O'Brien", 88.5, 1, '2024-01-04']);
    $stmt->execute([5, 'Quote: "test"', 91.0, 1, '2024-01-05']);
    echo "Inserted rows with special characters\n";

    $stmt = $pdo->prepare("SELECT name FROM pdo_test_008 WHERE id = ?");
    $stmt->execute([4]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Retrieved: {$row['name']}\n";

    // Test 5: NULL values
    echo "\nTest 5: Verify data integrity\n";
    $stmt = $pdo->query("SELECT count() as cnt FROM pdo_test_008");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Total rows: {$row['cnt']}\n";

    // Test 6: Prepared statement reuse
    echo "\nTest 6: Statement reuse\n";
    $stmt = $pdo->prepare("SELECT score FROM pdo_test_008 WHERE id = ?");
    for ($id = 1; $id <= 3; $id++) {
        $stmt->execute([$id]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        echo "ID $id score: {$row['score']}\n";
    }

    $pdo->exec("DROP TABLE pdo_test_008");
    echo "\nTest passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Test 1: Positional parameters
Inserted 3 rows with positional params

Test 2: SELECT with WHERE parameters
- Alice: 95.5
- Charlie: 92.1

Test 3: Multiple WHERE conditions
Found: Alice, Bob

Test 4: String escaping
Inserted rows with special characters
Retrieved: O'Brien

Test 5: Verify data integrity
Total rows: 5

Test 6: Statement reuse
ID 1 score: 95.5
ID 2 score: 87.3
ID 3 score: 92.1

Test passed
