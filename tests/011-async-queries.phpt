--TEST--
PDO ClickHouse: Async query support
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

    // Test 1: Basic async query
    echo "Test 1: Basic Async Query\n";
    $result = $pdo->clickhouseQueryAsync('SELECT number FROM system.numbers LIMIT 5');
    echo "Query started: " . ($result ? "yes" : "no") . "\n";

    // Check if ready (might be instant for small queries)
    $isReady = $pdo->clickhouseAsyncIsReady();
    echo "Is ready: " . ($isReady ? "yes" : "no") . "\n";

    // Wait for completion
    $data = $pdo->clickhouseAsyncWait();
    echo "Rows received: " . count($data) . "\n";

    // Test 2: Poll method
    echo "\nTest 2: Async Poll\n";
    $result = $pdo->clickhouseQueryAsync('SELECT number FROM system.numbers LIMIT 10');
    echo "Query started: " . ($result ? "yes" : "no") . "\n";

    // Poll until ready
    $pollCount = 0;
    while (!$pdo->clickhouseAsyncPoll()) {
        $pollCount++;
        if ($pollCount > 10) {
            break;
        }
        usleep(1000); // 1ms
    }
    echo "Polled $pollCount times\n";

    // Get results
    if ($pdo->clickhouseAsyncIsReady()) {
        $data = $pdo->clickhouseAsyncWait();
        echo "Rows received: " . count($data) . "\n";
    }

    // Test 3: Multiple sequential async queries
    echo "\nTest 3: Sequential Async Queries\n";
    for ($i = 0; $i < 3; $i++) {
        $pdo->clickhouseQueryAsync("SELECT $i as value");
        $data = $pdo->clickhouseAsyncWait();
        echo "Query $i completed with " . count($data) . " row(s)\n";
    }

    // Test 4: Cancel async query
    echo "\nTest 4: Cancel Async Query\n";
    $pdo->clickhouseQueryAsync('SELECT number FROM system.numbers LIMIT 10000000');
    echo "Long query started\n";

    $cancelled = $pdo->clickhouseAsyncCancel();
    echo "Query cancelled: " . ($cancelled ? "yes" : "no") . "\n";

    // After cancel, connection is automatically reconnected - no reset needed!
    $result = $pdo->clickhouseQueryAsync('SELECT 2');
    echo "New async query after cancel: " . ($result ? "yes" : "no") . "\n";
    $data = $pdo->clickhouseAsyncWait();
    echo "New query completed with " . count($data) . " row(s)\n";

    // Test 5: Error handling - poll without query
    echo "\nTest 5: Error Handling\n";

    try {
        $pdo->clickhouseAsyncPoll();
        echo "Poll without query: should have warned\n";
    } catch (Exception $e) {
        echo "Poll without query: caught exception\n";
    }

    echo "\nAll async tests passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECTF--
Test 1: Basic Async Query
Query started: yes
Is ready: %s
Rows received: 5

Test 2: Async Poll
Query started: yes
Polled %d times
Rows received: 10

Test 3: Sequential Async Queries
Query 0 completed with 1 row(s)
Query 1 completed with 1 row(s)
Query 2 completed with 1 row(s)

Test 4: Cancel Async Query
Long query started
Query cancelled: yes
New async query after cancel: yes
New query completed with 1 row(s)

Test 5: Error Handling

Warning: PDO::clickhouseAsyncPoll(): No async query in progress in %s on line %d
Poll without query: should have warned

All async tests passed
