--TEST--
PDO ClickHouse: SSL configuration
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

    // Check SSL availability
    $available = ch_call($pdo, 'sslAvailable');
    echo "SSL available: " . ($available ? "yes" : "no") . "\n";

    // Test setSSL
    $result = ch_call($pdo, 'setSSL', true);
    echo "setSSL(true): " . ($result ? "success" : "failed") . "\n";

    // Test setSSLVerify
    $result = ch_call($pdo, 'setSSLVerify', false, false);
    echo "setSSLVerify(false, false): " . ($result ? "success" : "failed") . "\n";

    // Test setSSLCA (may not work without actual cert, but should not crash)
    try {
        $result = ch_call($pdo, 'setSSLCA', '/tmp/ca.crt');
        echo "setSSLCA: " . ($result ? "success" : "failed") . "\n";
    } catch (Exception $e) {
        echo "setSSLCA: method callable\n";
    }

    // Test setSSLCert (may not work without actual certs, but should not crash)
    try {
        $result = ch_call($pdo, 'setSSLCert', '/tmp/client.crt', '/tmp/client.key');
        echo "setSSLCert: " . ($result ? "success" : "failed") . "\n";
    } catch (Exception $e) {
        echo "setSSLCert: method callable\n";
    }

    // Disable SSL
    ch_call($pdo, 'setSSL', false);
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
