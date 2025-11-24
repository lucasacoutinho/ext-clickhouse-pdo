--TEST--
PDO ClickHouse: Optional PDO features (statement attributes, cursors, rowsets)
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

    // Test 1: Statement attributes
    echo "Test 1: Statement Attributes\n";
    $stmt = $pdo->prepare("SELECT 1");

    // Set and get cursor type
    $stmt->setAttribute(PDO::ATTR_CURSOR, PDO::CURSOR_SCROLL);
    $cursor = $stmt->getAttribute(PDO::ATTR_CURSOR);
    echo "Cursor type: " . ($cursor == PDO::CURSOR_SCROLL ? "SCROLL" : "FWDONLY") . "\n";

    // Set and get cursor name
    $stmt->setAttribute(PDO::ATTR_CURSOR_NAME, "test_cursor");
    $name = $stmt->getAttribute(PDO::ATTR_CURSOR_NAME);
    echo "Cursor name: $name\n";

    // Set and get max column length
    $stmt->setAttribute(PDO::ATTR_MAX_COLUMN_LEN, 1024);
    $maxLen = $stmt->getAttribute(PDO::ATTR_MAX_COLUMN_LEN);
    echo "Max column length: $maxLen\n";

    // Test 2: Cursor operations
    echo "\nTest 2: Cursor Operations\n";
    $pdo->exec("DROP TABLE IF EXISTS pdo_test_010");
    $pdo->exec("
        CREATE TABLE pdo_test_010 (
            id UInt32,
            value String
        ) ENGINE = MergeTree()
        ORDER BY id
    ");

    // Insert test data
    $stmt = $pdo->prepare("INSERT INTO pdo_test_010 VALUES (?, ?)");
    for ($i = 1; $i <= 5; $i++) {
        $stmt->execute([$i, "Value$i"]);
    }
    echo "Inserted 5 rows\n";

    // Execute query with cursor
    $stmt = $pdo->prepare("SELECT * FROM pdo_test_010 ORDER BY id");
    $stmt->execute();

    echo "Cursor opened\n";
    $count = 0;
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $count++;
    }
    echo "Fetched $count rows\n";

    // Close cursor
    $stmt->closeCursor();
    echo "Cursor closed\n";

    // Test 3: Next rowset (blocks as rowsets)
    echo "\nTest 3: Next Rowset\n";
    $stmt = $pdo->query("SELECT number FROM system.numbers LIMIT 100");

    $rowsets = 0;
    $totalRows = 0;
    do {
        $rowsets++;
        $rows = 0;
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $rows++;
            $totalRows++;
        }
        if ($rows > 0) {
            echo "Rowset $rowsets: $rows rows\n";
        }
    } while ($stmt->nextRowset());

    echo "Total rowsets: $rowsets, Total rows: $totalRows\n";

    // Cleanup
    $pdo->exec("DROP TABLE pdo_test_010");
    echo "\nTest passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECTF--
Test 1: Statement Attributes
Cursor type: SCROLL
Cursor name: test_cursor
Max column length: 1024

Test 2: Cursor Operations
Inserted 5 rows
Cursor opened
Fetched 5 rows
Cursor closed

Test 3: Next Rowset
Rowset 1: 100 rows
Total rowsets: %d, Total rows: 100

Test passed
