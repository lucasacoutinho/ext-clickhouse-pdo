--TEST--
PDO ClickHouse: Progress and profile callbacks
--SKIPIF--
<?php
if (!extension_loaded('pdo_clickhouse')) die('skip PDO ClickHouse extension not loaded');
?>
--FILE--
<?php
error_reporting(E_ALL & ~E_DEPRECATED);
$host = getenv('CLICKHOUSE_HOST') ?: 'localhost';
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
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $pdo->exec("DROP TABLE IF EXISTS pdo_test_007");
    $pdo->exec("
        CREATE TABLE pdo_test_007 (
            id UInt32,
            data String
        ) ENGINE = MergeTree()
        ORDER BY id
    ");

    // Insert some data
    $stmt = $pdo->prepare("INSERT INTO pdo_test_007 VALUES (?, ?)");
    for ($i = 1; $i <= 100; $i++) {
        $stmt->execute([$i, "Data$i"]);
    }

    // Set up progress callback
    $progress_called = false;
    ch_call($pdo, 'setProgressCallback', function($rows, $bytes, $total_rows, $written_rows, $written_bytes) use (&$progress_called) {
        $progress_called = true;
        echo "Progress: rows=$rows, bytes=$bytes\n";
    });
    echo "Progress callback set\n";

    // Set up profile callback
    $profile_called = false;
    ch_call($pdo, 'setProfileCallback', function($rows, $blocks, $bytes, $applied_limit, $rows_before_limit, $calculated_rows_before_limit) use (&$profile_called) {
        $profile_called = true;
        echo "Profile: rows=$rows, blocks=$blocks, bytes=$bytes\n";
    });
    echo "Profile callback set\n";

    // Execute a query that should trigger callbacks
    $stmt = $pdo->query("SELECT * FROM pdo_test_007");
    $count = 0;
    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $count++;
    }
    echo "Fetched $count rows\n";

    if ($profile_called) {
        echo "Profile callback was called\n";
    }

    // Clear callbacks
    ch_call($pdo, 'setProgressCallback', null);
    ch_call($pdo, 'setProfileCallback', null);
    echo "Callbacks cleared\n";

    // Query without callbacks should not trigger them
    $progress_called = false;
    $profile_called = false;
    $stmt = $pdo->query("SELECT count() as cnt FROM pdo_test_007");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Count: " . $row['cnt'] . "\n";

    if (!$progress_called && !$profile_called) {
        echo "Callbacks not called after clearing\n";
    }

    $pdo->exec("DROP TABLE pdo_test_007");
    echo "Test passed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECTF--
Progress callback set
Profile callback set
%A
Fetched 100 rows
Profile callback was called
Callbacks cleared
Count: 100
Callbacks not called after clearing
Test passed
