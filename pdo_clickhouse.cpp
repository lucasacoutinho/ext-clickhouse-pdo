#include "php_pdo_clickhouse.h"

extern "C" {
#include "ext/standard/info.h"
}

extern "C" {

static const zend_module_dep pdo_clickhouse_deps[] = {
    ZEND_MOD_REQUIRED("pdo") ZEND_MOD_REQUIRED("clickhouse") ZEND_MOD_END};

static PHP_MINIT_FUNCTION(pdo_clickhouse)
{
    if (php_pdo_register_driver(&pdo_clickhouse_driver) == FAILURE) {
        return FAILURE;
    }
    return SUCCESS;
}

static PHP_MSHUTDOWN_FUNCTION(pdo_clickhouse)
{
    php_pdo_unregister_driver(&pdo_clickhouse_driver);
    return SUCCESS;
}

static PHP_MINFO_FUNCTION(pdo_clickhouse)
{
    php_info_print_table_start();
    php_info_print_table_row(2, "PDO Driver for ClickHouse", "enabled");
    php_info_print_table_row(2, "PDO Driver Version", PHP_PDO_CLICKHOUSE_VERSION);
    php_info_print_table_row(2, "Protocol", "Native TCP (port 9000)");
    php_info_print_table_end();
}

zend_module_entry pdo_clickhouse_module_entry = {STANDARD_MODULE_HEADER_EX,
                                                 NULL,
                                                 pdo_clickhouse_deps,
                                                 "pdo_clickhouse",
                                                 NULL,
                                                 PHP_MINIT(pdo_clickhouse),
                                                 PHP_MSHUTDOWN(pdo_clickhouse),
                                                 NULL,
                                                 NULL,
                                                 PHP_MINFO(pdo_clickhouse),
                                                 PHP_PDO_CLICKHOUSE_VERSION,
                                                 STANDARD_MODULE_PROPERTIES};

#ifdef COMPILE_DL_PDO_CLICKHOUSE
ZEND_GET_MODULE(pdo_clickhouse)
#endif

} /* extern "C" */
