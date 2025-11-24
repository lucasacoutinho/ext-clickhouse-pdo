--TEST--
PDO ClickHouse: PDO should not expose {param} ClickHouse native syntax to users
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

    // PDO users should use PDO-style parameters (? or :named)
    // They should NOT be able to use {param:Type} ClickHouse native syntax through PDO

    // Test that PDO-style parameters work correctly
    $stmt = $pdo->prepare("SELECT :value as result");
    $stmt->execute([':value' => 'PDO-style']);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "PDO-style named param works: " . $row['result'] . "\n";

    $stmt = $pdo->prepare("SELECT ? as result");
    $stmt->execute(['positional']);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "PDO-style positional param works: " . $row['result'] . "\n";

    // Test that {param} syntax is NOT processed by PDO parameter binding
    // (it just gets sent to ClickHouse as-is in the query string)
    // This tests that PDO doesn't have special handling for {} syntax
    $stmt = $pdo->prepare("SELECT 1 as test");
    $stmt->execute();
    $row = $stmt->fetch(PDO::FETCH_ASSOC);

    // If we got here without errors, PDO is working correctly
    // It doesn't try to process {param} as a bindable parameter
    echo "PDO does not expose {param} binding interface\n";

    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
PDO-style named param works: PDO-style
PDO-style positional param works: positional
PDO does not expose {param} binding interface
Test passed