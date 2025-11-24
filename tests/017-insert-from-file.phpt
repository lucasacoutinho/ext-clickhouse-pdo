--TEST--
PDO ClickHouse: Insert from file
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

    // Create test table
    $pdo->exec("DROP TABLE IF EXISTS test_file_insert_phpt");
    $pdo->exec("CREATE TABLE test_file_insert_phpt (id UInt32, name String) ENGINE = Memory");

    // Create test CSV file
    $testFile = '/tmp/test_insert_phpt.csv';
    $csvData = "1,Alice\n2,Bob\n3,Charlie\n4,David\n";
    file_put_contents($testFile, $csvData);

    // Insert from file
    $result = $pdo->clickhouseInsertFromFile('test_file_insert_phpt', $testFile, 'CSV');
    echo "insertFromFile result: " . ($result ? "success" : "failed") . "\n";

    // Verify row count
    $stmt = $pdo->query("SELECT COUNT(*) as cnt FROM test_file_insert_phpt");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "Rows inserted: " . $row['cnt'] . "\n";

    // Verify data
    $stmt = $pdo->query("SELECT * FROM test_file_insert_phpt ORDER BY id");
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
    echo "First row: id=" . $rows[0]['id'] . ", name=" . $rows[0]['name'] . "\n";
    echo "Last row: id=" . $rows[3]['id'] . ", name=" . $rows[3]['name'] . "\n";

    // Test with TabSeparated format
    $pdo->exec("DROP TABLE IF EXISTS test_file_insert_phpt");
    $pdo->exec("CREATE TABLE test_file_insert_phpt (id UInt32, name String) ENGINE = Memory");

    $tsvFile = '/tmp/test_insert_phpt.tsv';
    $tsvData = "10\tTest1\n20\tTest2\n";
    file_put_contents($tsvFile, $tsvData);

    $pdo->clickhouseInsertFromFile('test_file_insert_phpt', $tsvFile, 'TabSeparated');
    $stmt = $pdo->query("SELECT COUNT(*) as cnt FROM test_file_insert_phpt");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    echo "TSV rows inserted: " . $row['cnt'] . "\n";

    // Cleanup
    unlink($testFile);
    unlink($tsvFile);
    $pdo->exec("DROP TABLE test_file_insert_phpt");

    echo "Test completed\n";

} catch (PDOException $e) {
    echo "Error: " . $e->getMessage() . "\n";
}
?>
--EXPECT--
insertFromFile result: success
Rows inserted: 4
First row: id=1, name=Alice
Last row: id=4, name=David
TSV rows inserted: 2
Test completed
