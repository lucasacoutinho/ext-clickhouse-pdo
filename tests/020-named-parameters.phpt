--TEST--
PDO ClickHouse: Named parameter binding with :param syntax
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

    $pdo->exec("DROP TABLE IF EXISTS pdo_test_named_params");
    $pdo->exec("
        CREATE TABLE pdo_test_named_params (
            id UInt32,
            name String,
            value Int32,
            active UInt8
        ) ENGINE = MergeTree()
        ORDER BY id
    ");

    // Test INSERT with named parameters
    $stmt = $pdo->prepare("INSERT INTO pdo_test_named_params VALUES (:id, :name, :value, :active)");
    $stmt->execute([
        ':id' => 1,
        ':name' => 'Test1',
        ':value' => 100,
        ':active' => 1
    ]);
    echo "Inserted row 1 with named parameters\n";

    // Test INSERT with named parameters (without : prefix in array keys)
    $stmt = $pdo->prepare("INSERT INTO pdo_test_named_params VALUES (:id, :name, :value, :active)");
    $stmt->execute([
        'id' => 2,
        'name' => 'Test2',
        'value' => 200,
        'active' => 0
    ]);
    echo "Inserted row 2 with named parameters\n";

    // Test SELECT with named parameter in WHERE clause
    $stmt = $pdo->prepare("SELECT * FROM pdo_test_named_params WHERE id = :id");
    $stmt->execute([':id' => 1]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Found row: id=" . $row['id'] . ", name=" . $row['name'] . ", value=" . $row['value'] . "\n";

    // Test SELECT with multiple named parameters
    $stmt = $pdo->prepare("SELECT * FROM pdo_test_named_params WHERE value >= :min_value AND active = :active");
    $stmt->execute([':min_value' => 150, ':active' => 0]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Found filtered row: id=" . $row['id'] . ", value=" . $row['value'] . "\n";

    // Test named parameters with different data types
    $stmt = $pdo->prepare("SELECT :str_val as str, :int_val as int, :bool_val as bool, :null_val as null_val");
    $stmt->execute([
        ':str_val' => 'hello',
        ':int_val' => 42,
        ':bool_val' => true,
        ':null_val' => null
    ]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Types test: str=" . $row['str'] . ", int=" . $row['int'] . ", bool=" . $row['bool'] . ", null=" . (is_null($row['null_val']) ? 'NULL' : $row['null_val']) . "\n";

    // Test reusing same parameter name multiple times
    $stmt = $pdo->prepare("SELECT :val as val1, :val as val2, :val as val3");
    $stmt->execute([':val' => 'repeated']);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Repeated param: val1=" . $row['val1'] . ", val2=" . $row['val2'] . ", val3=" . $row['val3'] . "\n";

    $pdo->exec("DROP TABLE pdo_test_named_params");
    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Inserted row 1 with named parameters
Inserted row 2 with named parameters
Found row: id=1, name=Test1, value=100
Found filtered row: id=2, value=200
Types test: str=hello, int=42, bool=1, null=NULL
Repeated param: val1=repeated, val2=repeated, val3=repeated
Test passed