<?php
function pdo_clickhouse_test_dsn(): string {
    $host = getenv('CLICKHOUSE_HOST') ?: 'localhost';
    $port = getenv('CLICKHOUSE_PORT') ?: '9000';
    $db   = getenv('CLICKHOUSE_DB')   ?: 'default';
    return "clickhouse:host={$host};port={$port};dbname={$db}";
}

function pdo_clickhouse_test_pdo(): PDO {
    $user = getenv('CLICKHOUSE_USER') ?: 'default';
    $pass = getenv('CLICKHOUSE_PASS') ?: '';
    return new PDO(pdo_clickhouse_test_dsn(), $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    ]);
}

function pdo_clickhouse_test_skip(): void {
    try {
        pdo_clickhouse_test_pdo()->query('SELECT 1');
    } catch (\Throwable $e) {
        die('skip ClickHouse not available: ' . $e->getMessage());
    }
}
