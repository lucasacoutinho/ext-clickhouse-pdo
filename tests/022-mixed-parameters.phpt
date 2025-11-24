--TEST--
PDO ClickHouse: Mixed positional and named parameters (should use one style consistently)
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

    // Test 1: All named parameters
    $stmt = $pdo->prepare("SELECT :a as val1, :b as val2");
    $stmt->execute([':a' => 10, ':b' => 20]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Named only: val1=" . $row['val1'] . ", val2=" . $row['val2'] . "\n";

    // Test 2: All positional parameters
    $stmt = $pdo->prepare("SELECT ? as val1, ? as val2");
    $stmt->execute([30, 40]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Positional only: val1=" . $row['val1'] . ", val2=" . $row['val2'] . "\n";

    // Test 3: Named parameter that looks like it could be positional
    $stmt = $pdo->prepare("SELECT :id as result WHERE 1 = 1");
    $stmt->execute([':id' => 999]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Named :id = " . $row['result'] . "\n";

    // Test 4: Positional in complex query
    $stmt = $pdo->prepare("SELECT * FROM (SELECT ? as x, ? as y) WHERE x > 0");
    $stmt->execute([5, 10]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Complex positional: x=" . $row['x'] . ", y=" . $row['y'] . "\n";

    // Test 5: Named in complex query
    $stmt = $pdo->prepare("SELECT * FROM (SELECT :val1 as x, :val2 as y) WHERE x > 0");
    $stmt->execute([':val1' => 7, ':val2' => 14]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Complex named: x=" . $row['x'] . ", y=" . $row['y'] . "\n";

    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Named only: val1=10, val2=20
Positional only: val1=30, val2=40
Named :id = 999
Complex positional: x=5, y=10
Complex named: x=7, y=14
Test passed