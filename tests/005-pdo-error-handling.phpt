--TEST--
PDO ClickHouse: error handling with ERRMODE_EXCEPTION
--EXTENSIONS--
pdo_clickhouse
clickhouse
--SKIPIF--
<?php
require __DIR__ . '/pdo_clickhouse_test.inc';
pdo_clickhouse_test_skip();
?>
--FILE--
<?php
require __DIR__ . '/pdo_clickhouse_test.inc';

$pdo = pdo_clickhouse_test_pdo();

try {
    $pdo->query('SELECT * FROM _nonexistent_pdo_table_xyz');
    echo "FAIL: should have thrown\n";
} catch (PDOException $e) {
    echo "Caught PDOException\n";
    var_dump(strlen($e->getMessage()) > 0);
    var_dump($e->getCode() !== '00000');
}

$pdo->query('SELECT 1');
var_dump($pdo->errorInfo()[0]);

// Extended error data must stay attached to the statement that failed.
$first = $pdo->prepare('SELECT * FROM _nonexistent_statement_error_one');
try {
    $first->execute();
} catch (PDOException $e) {
}
$firstError = $first->errorInfo();

$pdo->query('SELECT 2');
$second = $pdo->prepare('SELECT * FROM _nonexistent_statement_error_two');
try {
    $second->execute();
} catch (PDOException $e) {
}
$preservedError = $first->errorInfo();
var_dump($preservedError[1] === $firstError[1]);
var_dump($preservedError[2] === $firstError[2]);
var_dump($second->errorInfo()[2] !== $firstError[2]);

// Quote function
$quoted = $pdo->quote("it's a test");
var_dump($quoted);
$bools = [
    $pdo->quote('no', PDO::PARAM_BOOL),
    $pdo->quote('off', PDO::PARAM_BOOL),
    $pdo->quote('0.0', PDO::PARAM_BOOL),
];
echo implode('', $bools), "\n";

// ClickHouse has no transactions, but PDO callers commonly expect these
// methods to be harmless compatibility no-ops.
var_dump($pdo->inTransaction());
var_dump($pdo->beginTransaction());
var_dump($pdo->inTransaction());
var_dump($pdo->commit());
var_dump($pdo->inTransaction());

// ssl=on/yes/True must enable TLS rather than silently falling back to
// plaintext. The default test server listens without TLS on port 9000, so a
// TLS connection should fail here.
$dsn = pdo_clickhouse_test_dsn() . ';ssl=on';
try {
    new PDO($dsn, getenv('CLICKHOUSE_USER') ?: 'default', getenv('CLICKHOUSE_PASS') ?: '', [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    ]);
    echo "FAIL: ssl=on connected over plaintext\n";
} catch (PDOException $e) {
    echo "ssl=on rejected non-TLS endpoint\n";
}

echo "OK\n";
?>
--EXPECTF--
Caught PDOException
bool(true)
bool(true)
string(5) "00000"
bool(true)
bool(true)
bool(true)
string(%d) "'it\'s a test'"
000
bool(false)
bool(true)
bool(true)
bool(true)
bool(false)
ssl=on rejected non-TLS endpoint
OK
