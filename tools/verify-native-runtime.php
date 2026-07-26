<?php

use ClickHouse\Driver\ClientOptions;
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Exception\ValidationException;

if (!extension_loaded('clickhouse')) {
    throw new RuntimeException('The native clickhouse extension is not loaded');
}

$constructor = new ReflectionMethod(ClientOptions::class, '__construct');
if (count($constructor->getParameters()) !== 20) {
    throw new RuntimeException(
        'The native clickhouse extension exposes obsolete ClientOptions arginfo'
    );
}

$invalidValues = [
    'Decimal precision overflow' => static function () {
        Column::create('Decimal(9,0)', ['3000000000']);
    },
    'invalid Date' => static function () {
        Column::create('Date', ['2024-02-30']);
    },
    'DateTime underflow' => static function () {
        Column::create('DateTime', [-1]);
    },
    'Date32 overflow' => static function () {
        Column::create('Date32', ['2300-01-01']);
    },
    'IPv4 overflow' => static function () {
        Column::create('IPv4', [4294967296]);
    },
    'DateTime64 coercion' => static function () {
        Column::create('DateTime64(3)', [[]]);
    },
];

foreach ($invalidValues as $label => $factory) {
    try {
        $factory();
    } catch (ValidationException $exception) {
        continue;
    }

    throw new RuntimeException("The native clickhouse extension accepted {$label}");
}
