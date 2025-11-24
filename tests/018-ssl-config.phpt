--TEST--
PDO ClickHouse: SSL configuration
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

    // Check SSL availability
    $available = $pdo->clickhouseSslAvailable();
    echo "SSL available: " . ($available ? "yes" : "no") . "\n";

    // Test setSSL
    $result = $pdo->clickhouseSetSSL(true);
    echo "setSSL(true): " . ($result ? "success" : "failed") . "\n";

    // Test setSSLVerify
    $result = $pdo->clickhouseSetSSLVerify(false, false);
    echo "setSSLVerify(false, false): " . ($result ? "success" : "failed") . "\n";

    // Test setSSLCA (may not work without actual cert, but should not crash)
    try {
        $result = $pdo->clickhouseSetSSLCA('/tmp/ca.crt');
        echo "setSSLCA: " . ($result ? "success" : "failed") . "\n";
    } catch (Exception $e) {
        echo "setSSLCA: method callable\n";
    }

    // Test setSSLCert (may not work without actual certs, but should not crash)
    try {
        $result = $pdo->clickhouseSetSSLCert('/tmp/client.crt', '/tmp/client.key');
        echo "setSSLCert: " . ($result ? "success" : "failed") . "\n";
    } catch (Exception $e) {
        echo "setSSLCert: method callable\n";
    }

    // Disable SSL
    $pdo->clickhouseSetSSL(false);
    echo "setSSL(false): success\n";

    echo "Test completed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
SSL available: yes
setSSL(true): success
setSSLVerify(false, false): success
setSSLCA: success
setSSLCert: success
setSSL(false): success
Test completed
