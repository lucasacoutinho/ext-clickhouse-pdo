--TEST--
PDO ClickHouse: Compression methods
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip PDO ClickHouse extension not loaded');
?>
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse-server';
$port = getenv('CLICKHOUSE_PORT') ?: '9000';

try {
    $pdo = new PDO("clickhouse:host=$host;port=$port;dbname=default", 'default', '');

    // Test setCompression and getCompression
    echo "Initial compression: " . $pdo->clickhouseGetCompression() . "\n";

    // Enable LZ4 compression
    $result = $pdo->clickhouseSetCompression(1);
    echo "setCompression(1): " . ($result ? "success" : "failed") . "\n";
    echo "Compression after LZ4: " . $pdo->clickhouseGetCompression() . "\n";

    // Enable ZSTD compression
    $pdo->clickhouseSetCompression(2);
    echo "Compression after ZSTD: " . $pdo->clickhouseGetCompression() . "\n";

    // Disable compression
    $pdo->clickhouseSetCompression(0);
    echo "Compression after disable: " . $pdo->clickhouseGetCompression() . "\n";

    // Test with actual query
    $pdo->clickhouseSetCompression(1);
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
