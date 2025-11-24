/*
  +----------------------------------------------------------------------+
  | PDO ClickHouse Driver                                               |
  +----------------------------------------------------------------------+
  | Copyright (c) 2025 Lucas Coutinho                                   |
  +----------------------------------------------------------------------+
*/

#ifndef PHP_PDO_CLICKHOUSE_H
#define PHP_PDO_CLICKHOUSE_H

extern zend_module_entry pdo_clickhouse_module_entry;
#define phpext_pdo_clickhouse_ptr &pdo_clickhouse_module_entry

#define PHP_PDO_CLICKHOUSE_VERSION "0.2.0"

PHP_MINIT_FUNCTION(pdo_clickhouse);
PHP_MSHUTDOWN_FUNCTION(pdo_clickhouse);
PHP_MINFO_FUNCTION(pdo_clickhouse);

#endif /* PHP_PDO_CLICKHOUSE_H */
