/*
  +----------------------------------------------------------------------+
  | PDO ClickHouse Driver - Driver-Specific Methods                     |
  +----------------------------------------------------------------------+
*/

#include "php.h"
#include "php_ini.h"
#include "pdo/php_pdo.h"
#include "pdo/php_pdo_driver.h"
#include "php_pdo_clickhouse_int.h"
#include "zend_exceptions.h"
#include <string.h>

/* Forward declarations */
static PHP_METHOD(PDO, clickhouseInsert);
static PHP_METHOD(PDO, clickhouseInsertFromString);
static PHP_METHOD(PDO, clickhouseGetDatabases);
static PHP_METHOD(PDO, clickhouseGetTables);
static PHP_METHOD(PDO, clickhouseDescribeTable);
static PHP_METHOD(PDO, clickhouseGetServerInfo);
static PHP_METHOD(PDO, clickhouseSetQuerySetting);
static PHP_METHOD(PDO, clickhouseClearQuerySettings);
static PHP_METHOD(PDO, clickhouseSetProgressCallback);
static PHP_METHOD(PDO, clickhouseSetProfileCallback);

/* Argument info */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_insert, 0, 3, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, columns, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, rows, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_insertfromstring, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, data, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, format, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_getdatabases, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_gettables, 0, 0, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, database, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_describetable, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, database, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_getserverinfo, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setquerysetting, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_clearquerysettings, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setprogresscallback, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, callback, IS_CALLABLE, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setprofilecallback, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, callback, IS_CALLABLE, 1)
ZEND_END_ARG_INFO()

/* Driver-specific method table */
static const zend_function_entry pdo_clickhouse_methods[] = {
    PHP_ME(PDO, clickhouseInsert, arginfo_clickhouse_insert, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseInsertFromString, arginfo_clickhouse_insertfromstring, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseGetDatabases, arginfo_clickhouse_getdatabases, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseGetTables, arginfo_clickhouse_gettables, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseDescribeTable, arginfo_clickhouse_describetable, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseGetServerInfo, arginfo_clickhouse_getserverinfo, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetQuerySetting, arginfo_clickhouse_setquerysetting, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseClearQuerySettings, arginfo_clickhouse_clearquerysettings, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetProgressCallback, arginfo_clickhouse_setprogresscallback, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetProfileCallback, arginfo_clickhouse_setprofilecallback, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

/* Get driver methods callback */
const zend_function_entry *clickhouse_get_driver_methods(pdo_dbh_t *dbh, int kind)
{
    switch (kind) {
        case PDO_DBH_DRIVER_METHOD_KIND_DBH:
            return pdo_clickhouse_methods;
        default:
            return NULL;
    }
}

/* Implementation: clickhouseGetDatabases() */
static PHP_METHOD(PDO, clickhouseGetDatabases)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    clickhouse_result *result = NULL;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Execute query to get databases */
    int status = clickhouse_connection_execute_query(H->conn,
        "SELECT name FROM system.databases ORDER BY name", &result);

    if (status != 0 || !result) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Failed to get databases");
        RETURN_FALSE;
    }

    /* Build array from result */
    array_init(return_value);

    if (result->block_count > 0) {
        for (size_t b = 0; b < result->block_count; b++) {
            clickhouse_block *block = result->blocks[b];
            if (block->column_count > 0) {
                clickhouse_column *col = block->columns[0];
                char **strings = (char **)col->data;

                for (size_t r = 0; r < block->row_count; r++) {
                    if (strings[r]) {
                        add_next_index_string(return_value, strings[r]);
                    }
                }
            }
        }
    }

    clickhouse_result_free(result);
}

/* Implementation: clickhouseGetTables() */
static PHP_METHOD(PDO, clickhouseGetTables)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    char *database = NULL;
    size_t database_len = 0;
    clickhouse_result *result = NULL;
    char query[512];

    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING_OR_NULL(database, database_len)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Build query */
    if (database && database_len > 0) {
        snprintf(query, sizeof(query),
            "SELECT name FROM system.tables WHERE database = '%s' ORDER BY name",
            database);
    } else {
        snprintf(query, sizeof(query),
            "SELECT name FROM system.tables WHERE database = 'default' ORDER BY name");
    }

    /* Execute query */
    int status = clickhouse_connection_execute_query(H->conn, query, &result);

    if (status != 0 || !result) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Failed to get tables");
        RETURN_FALSE;
    }

    /* Build array from result */
    array_init(return_value);

    if (result->block_count > 0) {
        for (size_t b = 0; b < result->block_count; b++) {
            clickhouse_block *block = result->blocks[b];
            if (block->column_count > 0) {
                clickhouse_column *col = block->columns[0];
                char **strings = (char **)col->data;

                for (size_t r = 0; r < block->row_count; r++) {
                    if (strings[r]) {
                        add_next_index_string(return_value, strings[r]);
                    }
                }
            }
        }
    }

    clickhouse_result_free(result);
}

/* Implementation: clickhouseDescribeTable() */
static PHP_METHOD(PDO, clickhouseDescribeTable)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    char *table, *database = NULL;
    size_t table_len, database_len = 0;
    clickhouse_result *result = NULL;
    char query[1024];

    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING_OR_NULL(database, database_len)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Build query */
    if (database && database_len > 0) {
        snprintf(query, sizeof(query),
            "SELECT name, type, default_expression, comment "
            "FROM system.columns "
            "WHERE database = '%s' AND table = '%s' "
            "ORDER BY position",
            database, table);
    } else {
        snprintf(query, sizeof(query),
            "SELECT name, type, default_expression, comment "
            "FROM system.columns "
            "WHERE database = 'default' AND table = '%s' "
            "ORDER BY position",
            table);
    }

    /* Execute query */
    int status = clickhouse_connection_execute_query(H->conn, query, &result);

    if (status != 0 || !result) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Failed to describe table");
        RETURN_FALSE;
    }

    /* Build array from result */
    array_init(return_value);

    if (result->block_count > 0) {
        for (size_t b = 0; b < result->block_count; b++) {
            clickhouse_block *block = result->blocks[b];

            for (size_t r = 0; r < block->row_count; r++) {
                zval row;
                array_init(&row);

                /* Get each column value */
                for (size_t c = 0; c < block->column_count && c < 4; c++) {
                    clickhouse_column *col = block->columns[c];
                    char **strings = (char **)col->data;

                    if (strings[r]) {
                        add_assoc_string(&row, col->name, strings[r]);
                    } else {
                        add_assoc_string(&row, col->name, "");
                    }
                }

                add_next_index_zval(return_value, &row);
            }
        }
    }

    clickhouse_result_free(result);
}

/* Implementation: clickhouseGetServerInfo() */
static PHP_METHOD(PDO, clickhouseGetServerInfo)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    array_init(return_value);

    if (H->conn && H->conn->server_info) {
        clickhouse_server_info *info = H->conn->server_info;

        if (info->name) {
            add_assoc_string(return_value, "name", info->name);
        }

        add_assoc_long(return_value, "version_major", info->version_major);
        add_assoc_long(return_value, "version_minor", info->version_minor);
        add_assoc_long(return_value, "revision", info->revision);

        if (info->timezone) {
            add_assoc_string(return_value, "timezone", info->timezone);
        }

        if (info->display_name) {
            add_assoc_string(return_value, "display_name", info->display_name);
        }
    }
}

/* Implementation: clickhouseSetQuerySetting() */
static PHP_METHOD(PDO, clickhouseSetQuerySetting)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    char *name, *value;
    size_t name_len, value_len;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(name, name_len)
        Z_PARAM_STRING(value, value_len)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Ensure we have capacity */
    if (H->query_settings_count >= H->query_settings_capacity) {
        size_t new_capacity = H->query_settings_capacity == 0 ? 4 : H->query_settings_capacity * 2;
        H->query_settings = perealloc(H->query_settings,
            new_capacity * sizeof(pdo_clickhouse_query_setting),
            dbh->is_persistent);
        H->query_settings_capacity = new_capacity;
    }

    /* Add new setting */
    pdo_clickhouse_query_setting *setting = &H->query_settings[H->query_settings_count];
    setting->name = pestrdup(name, dbh->is_persistent);
    setting->value = pestrdup(value, dbh->is_persistent);
    H->query_settings_count++;
}

/* Implementation: clickhouseClearQuerySettings() */
static PHP_METHOD(PDO, clickhouseClearQuerySettings)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Free all settings */
    for (size_t i = 0; i < H->query_settings_count; i++) {
        pefree(H->query_settings[i].name, dbh->is_persistent);
        pefree(H->query_settings[i].value, dbh->is_persistent);
    }

    H->query_settings_count = 0;
}

/* Implementation: clickhouseSetProgressCallback() */
static PHP_METHOD(PDO, clickhouseSetProgressCallback)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    zval *callback = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ZVAL_OR_NULL(callback)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* TODO: Store callback and wire up to progress events */

    php_error_docref(NULL, E_WARNING,
        "Progress callbacks not yet implemented in PDO driver");
}

/* Implementation: clickhouseSetProfileCallback() */
static PHP_METHOD(PDO, clickhouseSetProfileCallback)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    zval *callback = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ZVAL_OR_NULL(callback)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* TODO: Store callback and wire up to profile events */

    php_error_docref(NULL, E_WARNING,
        "Profile callbacks not yet implemented in PDO driver");
}

/* Implementation: clickhouseInsert() */
static PHP_METHOD(PDO, clickhouseInsert)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    char *table;
    size_t table_len;
    zval *columns, *rows;

    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_ARRAY(columns)
        Z_PARAM_ARRAY(rows)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Build block from PHP arrays */
    clickhouse_block *block = build_block_from_php_arrays(columns, rows);
    if (!block) {
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, "Failed to build data block");
        RETURN_FALSE;
    }

    /* Send INSERT query */
    char query[512];
    snprintf(query, sizeof(query), "INSERT INTO %s FORMAT Native", table);

    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query(H->conn, query, &result);

    if (status != 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        free_php_built_block(block);
        if (result) clickhouse_result_free(result);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "INSERT query failed");
        RETURN_FALSE;
    }

    /* Send data block */
    status = clickhouse_connection_send_data(H->conn, block);
    if (status != 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        free_php_built_block(block);
        if (result) clickhouse_result_free(result);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Failed to send data block");
        RETURN_FALSE;
    }

    /* Send empty block to signal end */
    clickhouse_connection_send_empty_block(H->conn);

    free_php_built_block(block);
    if (result) clickhouse_result_free(result);

    RETURN_TRUE;
}

/* Stub for clickhouseInsertFromString - to be fully implemented */
static PHP_METHOD(PDO, clickhouseInsertFromString)
{
    pdo_dbh_t *dbh;
    char *table, *data, *format = "TabSeparated";
    size_t table_len, data_len, format_len = 0;

    ZEND_PARSE_PARAMETERS_START(2, 3)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_STRING(data, data_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING(format, format_len)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;

    /* TODO: Implement using clickhouse_connection_insert_format_data() */

    php_error_docref(NULL, E_WARNING,
        "Insert from string not yet implemented in PDO driver");
}
