--TEST--
PDO ClickHouse rejects invalid result buffer limits before connecting
--EXTENSIONS--
pdo_clickhouse
clickhouse
--FILE--
<?php
foreach (['max_buffered_rows', 'max_buffered_bytes'] as $option) {
    $rejected = 0;
    foreach (['', '0', '-1', ' -1', "\t-1", '+1', '1 ', '1x', '1.5', '1e3', str_repeat('9', 100)] as $value) {
        try {
            new PDO('clickhouse:host=127.0.0.1;port=1;' . $option . '=' . $value);
            echo "FAIL: invalid limit accepted\n";
        } catch (PDOException $e) {
            if (strpos($e->getMessage(), 'Invalid ClickHouse ' . $option) !== false
                && $e->errorInfo[0] === '08001') {
                ++$rejected;
            } else {
                echo 'Wrong error: ', $e->getMessage(), "\n";
            }
        }
    }
    echo $option, ': ', $rejected, " rejected\n";
}
?>
--EXPECT--
max_buffered_rows: 11 rejected
max_buffered_bytes: 11 rejected
