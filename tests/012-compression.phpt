--TEST--
PDO ClickHouse: Compression methods
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip PDO ClickHouse extension not loaded');
?>
--FILE--
<?php
error_reporting(E_ALL & ~E_DEPRECATED);
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse-server';
$port = getenv('CLICKHOUSE_PORT') ?: '9000';


$class = class_exists('Pdo\\Clickhouse') ? 'Pdo\Clickhouse' : 'PDO';

function ch_call($pdo, string $name, ...$args) {
    if (method_exists($pdo, $name)) {
        return $pdo->$name(...$args);
    }
    $legacy = 'clickhouse' . ucfirst($name);
    return $pdo->$legacy(...$args);
}

try {
    $pdo = new $class("clickhouse:host=$host;port=$port;dbname=default", 'default', '');

    // Test setCompression and getCompression
    echo "Initial compression: " . ch_call($pdo, 'getCompression') . "\n";

    // Enable LZ4 compression
    $result = ch_call($pdo, 'setCompression', 1);
    echo "setCompression(1): " . ($result ? "success" : "failed") . "\n";
    echo "Compression after LZ4: " . ch_call($pdo, 'getCompression') . "\n";

    // Enable ZSTD compression
    ch_call($pdo, 'setCompression', 2);
    echo "Compression after ZSTD: " . ch_call($pdo, 'getCompression') . "\n";

    // Disable compression
    ch_call($pdo, 'setCompression', 0);
    echo "Compression after disable: " . ch_call($pdo, 'getCompression') . "\n";

    // Test with actual query
    ch_call($pdo, 'setCompression', 1);
    $stmt = $pdo->query("SELECT 1 as num");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Query with compression: " . $row['num'] . "\n";

    echo "Test completed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
Initial compression: 0
setCompression(1): success
Compression after LZ4: 1
Compression after ZSTD: 2
Compression after disable: 0
Query with compression: 1
Test completed
