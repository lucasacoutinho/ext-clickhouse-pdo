--TEST--
PDO ClickHouse: Positional parameter binding with ? syntax
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

    $pdo->exec("DROP TABLE IF EXISTS pdo_test_positional_params");
    $pdo->exec("
        CREATE TABLE pdo_test_positional_params (
            id UInt32,
            name String,
            value Int32,
            score Float64
        ) ENGINE = MergeTree()
        ORDER BY id
    ");

    // Test INSERT with positional parameters
    $stmt = $pdo->prepare("INSERT INTO pdo_test_positional_params VALUES (?, ?, ?, ?)");
    $stmt->execute([1, 'Alice', 100, 95.5]);
    echo "Inserted row 1\n";

    $stmt->execute([2, 'Bob', 200, 87.3]);
    echo "Inserted row 2\n";

    $stmt->execute([3, 'Charlie', 150, 92.1]);
    echo "Inserted row 3\n";

    // Test SELECT with single positional parameter
    $stmt = $pdo->prepare("SELECT * FROM pdo_test_positional_params WHERE id = ?");
    $stmt->execute([2]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Found: name=" . $row['name'] . ", value=" . $row['value'] . "\n";

    // Test SELECT with multiple positional parameters
    $stmt = $pdo->prepare("SELECT * FROM pdo_test_positional_params WHERE value > ? AND score < ?");
    $stmt->execute([120, 93]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Filtered: id=" . $row['id'] . ", name=" . $row['name'] . "\n";

    // Test different data types
    $stmt = $pdo->prepare("SELECT ? as str, ? as int, ? as float, ? as bool, ? as null_val");
    $stmt->execute(['test', 42, 3.14, true, null]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Types: str=" . $row['str'] . ", int=" . $row['int'] . ", float=" . $row['float'] . ", bool=" . $row['bool'] . ", null=" . (is_null($row['null_val']) ? 'NULL' : $row['null_val']) . "\n";

    // Test string escaping
    $stmt = $pdo->prepare("SELECT ? as escaped");
    $stmt->execute(["It's a test with 'quotes' and \\backslash"]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Escaped: " . $row['escaped'] . "\n";

    $pdo->exec("DROP TABLE pdo_test_positional_params");
    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Inserted row 1
Inserted row 2
Inserted row 3
Found: name=Bob, value=200
Filtered: id=3, name=Charlie
Types: str=test, int=42, float=3.14, bool=1, null=NULL
Escaped: It's a test with 'quotes' and \backslash
Test passed