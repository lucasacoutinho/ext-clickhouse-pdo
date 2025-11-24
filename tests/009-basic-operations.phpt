--TEST--
PDO ClickHouse: Basic PDO operations and features
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip PDO ClickHouse extension not loaded');
?>
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'localhost';
$port = getenv('CLICKHOUSE_PORT') ?: '9000';

try {
    // Test 1: Connection
    echo "Test 1: Connection\n";
    $pdo = new PDO("clickhouse:host=$host;port=$port;dbname=default", 'default', '');
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    echo "Connected successfully\n";

    // Test 2: Server attributes
    echo "\nTest 2: Server attributes\n";
    $version = $pdo->getAttribute(PDO::ATTR_SERVER_VERSION);
    echo "Server version: $version\n";

    $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
    echo "Driver: $driver\n";

    // Test 3: Table operations
    echo "\nTest 3: Table operations\n";
    $pdo->exec("DROP TABLE IF EXISTS pdo_test_009");
    echo "Dropped table (if exists)\n";

    $pdo->exec("
        CREATE TABLE pdo_test_009 (
            id UInt32,
            username String,
            email String,
            age UInt8
        ) ENGINE = MergeTree()
        ORDER BY id
    ");
    echo "Created table\n";

    // Test 4: Insert with exec()
    echo "\nTest 4: Insert operations\n";
    $affected = $pdo->exec("INSERT INTO pdo_test_009 VALUES (1, 'alice', 'alice@example.com', 30)");
    echo "Exec insert completed\n";

    // Test 5: Batch insert using prepare/execute
    echo "\nTest 5: Batch insert\n";
    $stmt = $pdo->prepare("INSERT INTO pdo_test_009 VALUES (?, ?, ?, ?)");
    $users = [
        [2, 'bob', 'bob@example.com', 25],
        [3, 'charlie', 'charlie@example.com', 35],
        [4, 'diana', 'diana@example.com', 28],
    ];

    foreach ($users as $user) {
        $stmt->execute($user);
    }
    echo "Inserted " . count($users) . " users\n";

    // Test 6: Query and fetch
    echo "\nTest 6: Query operations\n";
    $stmt = $pdo->query("SELECT username, age FROM pdo_test_009 ORDER BY age");

    echo "Fetch results:\n";
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        printf("- %s (age %d)\n", $row['username'], $row['age']);
    }

    // Test 7: Fetch single row
    echo "\nTest 7: Fetch single row\n";
    $stmt = $pdo->query("SELECT * FROM pdo_test_009 WHERE id = 1");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "User: {$row['username']}, Email: {$row['email']}\n";

    // Test 8: Count
    echo "\nTest 8: Aggregate query\n";
    $stmt = $pdo->query("SELECT count() as total, avg(age) as avg_age FROM pdo_test_009");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Total users: {$row['total']}, Average age: {$row['avg_age']}\n";

    // Test 9: Column metadata
    echo "\nTest 9: Column metadata\n";
    $stmt = $pdo->query("SELECT * FROM pdo_test_009 LIMIT 1");
    $meta = $stmt->getColumnMeta(0);
    echo "First column: {$meta['name']}, Type: {$meta['native_type']}\n";

    // Test 10: Quote
    echo "\nTest 10: Quote method\n";
    $unsafe = "O'Brien";
    $safe = $pdo->quote($unsafe);
    echo "Quoted: $safe\n";

    // Cleanup
    $pdo->exec("DROP TABLE pdo_test_009");
    echo "\nCleanup complete\n";
    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECTF--
Test 1: Connection
Connected successfully

Test 2: Server attributes
Server version: %s
Driver: clickhouse

Test 3: Table operations
Dropped table (if exists)
Created table

Test 4: Insert operations
Exec insert completed

Test 5: Batch insert
Inserted 3 users

Test 6: Query operations
Fetch results:
- bob (age 25)
- diana (age 28)
- alice (age 30)
- charlie (age 35)

Test 7: Fetch single row
User: alice, Email: alice@example.com

Test 8: Aggregate query
Total users: 4, Average age: %f

Test 9: Column metadata
First column: id, Type: UInt32

Test 10: Quote method
Quoted: 'O\'Brien'

Cleanup complete
Test passed
