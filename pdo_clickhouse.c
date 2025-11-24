/*
  +----------------------------------------------------------------------+
  | PDO ClickHouse Driver                                               |
  +----------------------------------------------------------------------+
  | Copyright (c) 2024                                                  |
  +----------------------------------------------------------------------+
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "pdo/php_pdo.h"
#include "pdo/php_pdo_driver.h"
#include "php_pdo_clickhouse.h"
#include "php_pdo_clickhouse_int.h"

/* Module entry */
zend_module_entry pdo_clickhouse_module_entry = {
    STANDARD_MODULE_HEADER,
    "pdo_clickhouse",
    NULL,
    PHP_MINIT(pdo_clickhouse),
    PHP_MSHUTDOWN(pdo_clickhouse),
    NULL,
    NULL,
    PHP_MINFO(pdo_clickhouse),
    PHP_PDO_CLICKHOUSE_VERSION,
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_PDO_CLICKHOUSE
ZEND_GET_MODULE(pdo_clickhouse)
#endif

/* Define macro for registering ClickHouse-specific constants with PHP version compatibility */
#if PHP_VERSION_ID >= 80500
    /* PHP 8.5+: Use the new deprecated alias macro for backward compatibility */
    #define REGISTER_PDO_CLICKHOUSE_CLASS_CONST_LONG(base_name, value) \
        REGISTER_PDO_CLASS_CONST_LONG_DEPRECATED_ALIAS_85(base_name, "CLICKHOUSE_", \
        "Pdo\\Clickhouse::", value)
#else
    /* PHP < 8.5: Use the old macro */
    #define REGISTER_PDO_CLICKHOUSE_CLASS_CONST_LONG(name, value) \
        REGISTER_PDO_CLASS_CONST_LONG(name, value)
#endif

PHP_MINIT_FUNCTION(pdo_clickhouse)
{
    /* Register ClickHouse-specific PDO constants */
#if PHP_VERSION_ID >= 80500
    REGISTER_PDO_CLICKHOUSE_CLASS_CONST_LONG("ATTR_COMPRESSION",
        PDO_CLICKHOUSE_ATTR_COMPRESSION);
    REGISTER_PDO_CLICKHOUSE_CLASS_CONST_LONG("ATTR_TIMEOUT",
        PDO_CLICKHOUSE_ATTR_TIMEOUT);
    REGISTER_PDO_CLICKHOUSE_CLASS_CONST_LONG("ATTR_MAX_BLOCK_SIZE",
        PDO_CLICKHOUSE_ATTR_MAX_BLOCK_SIZE);
#else
    REGISTER_PDO_CLICKHOUSE_CLASS_CONST_LONG("CLICKHOUSE_ATTR_COMPRESSION",
        PDO_CLICKHOUSE_ATTR_COMPRESSION);
    REGISTER_PDO_CLICKHOUSE_CLASS_CONST_LONG("CLICKHOUSE_ATTR_TIMEOUT",
        PDO_CLICKHOUSE_ATTR_TIMEOUT);
    REGISTER_PDO_CLICKHOUSE_CLASS_CONST_LONG("CLICKHOUSE_ATTR_MAX_BLOCK_SIZE",
        PDO_CLICKHOUSE_ATTR_MAX_BLOCK_SIZE);
#endif

    /* Register the driver */
    php_pdo_register_driver(&pdo_clickhouse_driver);

    return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(pdo_clickhouse)
{
    php_pdo_unregister_driver(&pdo_clickhouse_driver);
    return SUCCESS;
}

PHP_MINFO_FUNCTION(pdo_clickhouse)
{
    php_info_print_table_start();
    php_info_print_table_header(2, "PDO ClickHouse Driver", "enabled");
    php_info_print_table_row(2, "Version", PHP_PDO_CLICKHOUSE_VERSION);
    php_info_print_table_row(2, "Protocol", "Native (TCP/9000)");
    php_info_print_table_end();
}
