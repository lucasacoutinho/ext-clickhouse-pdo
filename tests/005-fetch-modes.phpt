--TEST--
PDO ClickHouse: Different fetch modes
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

    $stmt = $pdo->query("SELECT 1 as num, 'test' as str");

    // FETCH_ASSOC
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "FETCH_ASSOC: " . (isset($row['num']) && $row['num'] == 1 ? "OK" : "FAIL") . "\n";

    // FETCH_NUM
    $stmt = $pdo->query("SELECT 1 as num, 'test' as str");
    $row = $stmt->fetch(PDO::FETCH_NUM);
    echo "FETCH_NUM: " . ($row[0] == 1 ? "OK" : "FAIL") . "\n";

    // FETCH_BOTH
    $stmt = $pdo->query("SELECT 1 as num, 'test' as str");
    $row = $stmt->fetch(PDO::FETCH_BOTH);
    echo "FETCH_BOTH: " . (isset($row['num']) && isset($row[0]) ? "OK" : "FAIL") . "\n";

    // FETCH_OBJ
    $stmt = $pdo->query("SELECT 1 as num, 'test' as str");
    $row = $stmt->fetch(PDO::FETCH_OBJ);
    echo "FETCH_OBJ: " . (isset($row->num) && $row->num == 1 ? "OK" : "FAIL") . "\n";

    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
FETCH_ASSOC: OK
FETCH_NUM: OK
FETCH_BOTH: OK
FETCH_OBJ: OK
Test passed
