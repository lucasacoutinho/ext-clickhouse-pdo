--TEST--
PDO ClickHouse limits result rows and bytes without corrupting connection reuse
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

function limited_pdo(string $options, int $mode = PDO::ERRMODE_EXCEPTION): PDO {
    return new PDO(pdo_clickhouse_test_dsn() . ';' . $options,
        getenv('CLICKHOUSE_USER') ?: 'default', getenv('CLICKHOUSE_PASS') ?: '',
        [PDO::ATTR_ERRMODE => $mode]);
}

function check_reuse(PDO $pdo): bool {
    foreach ([42, 43, 44] as $expected) {
        if ((int) $pdo->query('SELECT ' . $expected)->fetchColumn() !== $expected) {
            return false;
        }
    }
    return true;
}

foreach (['none', 'lz4', 'zstd'] as $compression) {
    $pdo = limited_pdo('max_buffered_rows=2;compression=' . $compression);
    var_dump(count($pdo->query('SELECT number FROM numbers(2)')->fetchAll()) === 2);
    foreach (['SELECT number FROM numbers(3)',
              'SELECT number FROM numbers(20) SETTINGS max_block_size=1, max_threads=1'] as $sql) {
        try {
            $pdo->query($sql);
            echo "FAIL: oversized result accepted\n";
        } catch (PDOException $e) {
            var_dump(strpos($e->getMessage(), 'max_buffered_rows') !== false);
        }
        var_dump(check_reuse($pdo));
    }
}

$pdo = limited_pdo('max_buffered_rows=2');
$stmt = $pdo->prepare('SELECT number FROM numbers(toUInt64(?)) SETTINGS max_block_size=1, max_threads=1');
$stmt->execute([2]);
var_dump(count($stmt->fetchAll()) === 2);
try {
    $stmt->execute([20]);
    echo "FAIL: oversized prepared result accepted\n";
} catch (PDOException $e) {
    var_dump($stmt->rowCount() === 0);
    var_dump($stmt->fetch() === false);
}
$stmt->execute([2]);
var_dump(count($stmt->fetchAll()) === 2);

$pdo = limited_pdo('max_buffered_bytes=256');
var_dump(strlen($pdo->query("SELECT repeat('x', 64)")->fetchColumn()) === 64);
foreach (['NULL', '[NULL]', 'tuple(NULL, 1)', "map('key', NULL)"] as $value) {
    var_dump(count($pdo->query('SELECT ' . $value . ' AS value')->fetchAll()) === 1);
}
foreach (["repeat('x', 1000)", "[repeat('x', 1000)]",
          "tuple(repeat('x', 1000), 1)", "toLowCardinality(repeat('x', 1000))",
          "tuple(NULL, repeat('x', 1000))", "map(repeat('x', 1000), NULL)",
          'arrayResize([NULL], 300)'] as $value) {
    try {
        $pdo->query('SELECT ' . $value . ' AS value');
        echo "FAIL: oversized value accepted\n";
    } catch (PDOException $e) {
        var_dump(strpos($e->getMessage(), 'max_buffered_bytes') !== false);
    }
    var_dump(check_reuse($pdo));
}
$pdo = limited_pdo('max_buffered_bytes=4096');
var_dump(strlen($pdo->query("SELECT repeat('x', 1000)")->fetchColumn()) === 1000);

$pdo = limited_pdo('max_buffered_bytes=46');
var_dump(count($pdo->query("SELECT CAST([], 'Array(LowCardinality(String))') AS v")->fetchAll()) === 1);

foreach ([PDO::ERRMODE_SILENT, PDO::ERRMODE_WARNING] as $mode) {
    $pdo = limited_pdo('max_buffered_rows=2', $mode);
    set_error_handler(function () { return true; });
    var_dump($pdo->query('SELECT number FROM numbers(3)') === false);
    restore_error_handler();
    var_dump(check_reuse($pdo));
}

$pdo = pdo_clickhouse_test_pdo();
try {
    $pdo->query('SELECT number FROM numbers(1000001)');
    echo "FAIL: default row limit absent\n";
} catch (PDOException $e) {
    var_dump(strpos($e->getMessage(), 'max_buffered_rows') !== false);
}
var_dump(check_reuse($pdo));
try {
    $pdo->query("SELECT repeat(repeat('x', 8192), 8192) AS value");
    echo "FAIL: default byte limit absent\n";
} catch (PDOException $e) {
    var_dump(strpos($e->getMessage(), 'max_buffered_bytes') !== false);
}
var_dump(check_reuse($pdo));
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
