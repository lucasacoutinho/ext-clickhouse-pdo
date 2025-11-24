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
#include <unistd.h>

/* Include protocol constants for packet types */
#define CH_SERVER_EXCEPTION 2
#define CH_SERVER_END_OF_STREAM 5

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
static PHP_METHOD(PDO, clickhouseQueryAsync);
static PHP_METHOD(PDO, clickhouseAsyncPoll);
static PHP_METHOD(PDO, clickhouseAsyncWait);
static PHP_METHOD(PDO, clickhouseAsyncIsReady);
static PHP_METHOD(PDO, clickhouseAsyncCancel);
static PHP_METHOD(PDO, clickhouseInsertFromFile);
static PHP_METHOD(PDO, clickhouseSetCompression);
static PHP_METHOD(PDO, clickhouseGetCompression);
static PHP_METHOD(PDO, clickhouseSetQueryId);
static PHP_METHOD(PDO, clickhouseGetQueryId);
static PHP_METHOD(PDO, clickhouseGetLastQueryId);
static PHP_METHOD(PDO, clickhouseExecuteBatch);
static PHP_METHOD(PDO, clickhouseSetSSL);
static PHP_METHOD(PDO, clickhouseSetSSLVerify);
static PHP_METHOD(PDO, clickhouseSetSSLCA);
static PHP_METHOD(PDO, clickhouseSetSSLCert);
static PHP_METHOD(PDO, clickhouseSslAvailable);
static PHP_METHOD(PDO, clickhouseReconnect);
static PHP_METHOD(PDO, clickhouseIsConnected);
static PHP_METHOD(PDO, clickhouseSetTimeout);
static PHP_METHOD(PDO, clickhouseGetTimeout);
static PHP_METHOD(PDO, clickhouseSetLogCallback);

/* New driver-specific subclass methods */
static PHP_METHOD(Pdo_Clickhouse, insert);
static PHP_METHOD(Pdo_Clickhouse, insertFromString);
static PHP_METHOD(Pdo_Clickhouse, getDatabases);
static PHP_METHOD(Pdo_Clickhouse, getTables);
static PHP_METHOD(Pdo_Clickhouse, describeTable);
static PHP_METHOD(Pdo_Clickhouse, getServerInfo);
static PHP_METHOD(Pdo_Clickhouse, setQuerySetting);
static PHP_METHOD(Pdo_Clickhouse, clearQuerySettings);
static PHP_METHOD(Pdo_Clickhouse, setProgressCallback);
static PHP_METHOD(Pdo_Clickhouse, setProfileCallback);
static PHP_METHOD(Pdo_Clickhouse, queryAsync);
static PHP_METHOD(Pdo_Clickhouse, asyncPoll);
static PHP_METHOD(Pdo_Clickhouse, asyncWait);
static PHP_METHOD(Pdo_Clickhouse, asyncIsReady);
static PHP_METHOD(Pdo_Clickhouse, asyncCancel);
static PHP_METHOD(Pdo_Clickhouse, insertFromFile);
static PHP_METHOD(Pdo_Clickhouse, setCompression);
static PHP_METHOD(Pdo_Clickhouse, getCompression);
static PHP_METHOD(Pdo_Clickhouse, setQueryId);
static PHP_METHOD(Pdo_Clickhouse, getQueryId);
static PHP_METHOD(Pdo_Clickhouse, getLastQueryId);
static PHP_METHOD(Pdo_Clickhouse, executeBatch);
static PHP_METHOD(Pdo_Clickhouse, setSSL);
static PHP_METHOD(Pdo_Clickhouse, setSSLVerify);
static PHP_METHOD(Pdo_Clickhouse, setSSLCA);
static PHP_METHOD(Pdo_Clickhouse, setSSLCert);
static PHP_METHOD(Pdo_Clickhouse, sslAvailable);
static PHP_METHOD(Pdo_Clickhouse, reconnect);
static PHP_METHOD(Pdo_Clickhouse, isConnected);
static PHP_METHOD(Pdo_Clickhouse, setTimeout);
static PHP_METHOD(Pdo_Clickhouse, getTimeout);
static PHP_METHOD(Pdo_Clickhouse, setLogCallback);

/* Bridge function to call PHP progress callback from C */
void php_progress_callback_bridge(clickhouse_progress *progress, void *user_data) {
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)user_data;

    if (!H || !H->has_progress_callback || Z_ISUNDEF(H->progress_callback)) {
        return;
    }

    zval args[5], retval;
    ZVAL_LONG(&args[0], progress->rows);
    ZVAL_LONG(&args[1], progress->bytes);
    ZVAL_LONG(&args[2], progress->total_rows);
    ZVAL_LONG(&args[3], progress->written_rows);
    ZVAL_LONG(&args[4], progress->written_bytes);

    if (call_user_function(NULL, NULL, &H->progress_callback, &retval, 5, args) == SUCCESS) {
        zval_ptr_dtor(&retval);
    }
}

/* Bridge function to call PHP profile callback from C */
void php_profile_callback_bridge(clickhouse_profile_info *profile, void *user_data) {
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)user_data;

    if (!H || !H->has_profile_callback || Z_ISUNDEF(H->profile_callback)) {
        return;
    }

    zval args[6], retval;
    ZVAL_LONG(&args[0], profile->rows);
    ZVAL_LONG(&args[1], profile->blocks);
    ZVAL_LONG(&args[2], profile->bytes);
    ZVAL_BOOL(&args[3], profile->applied_limit);
    ZVAL_LONG(&args[4], profile->rows_before_limit);
    ZVAL_BOOL(&args[5], profile->calculated_rows_before_limit);

    if (call_user_function(NULL, NULL, &H->profile_callback, &retval, 6, args) == SUCCESS) {
        zval_ptr_dtor(&retval);
    }
}

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

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_queryasync, 0, 1, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_asyncpoll, 0, 0, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, timeout_ms, IS_LONG, 0, "0")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_asyncisready, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_asyncwait, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_asynccancel, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

/* New methods arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_insertfromfile, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, filepath, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, format, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setcompression, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, method, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_getcompression, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setqueryid, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, query_id, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_getqueryid, 0, 0, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_getlastqueryid, 0, 0, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_executebatch, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, queries, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setssl, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, enabled, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setsslverify, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, verify_peer, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, verify_host, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setsslca, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, ca_cert, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setsslcert, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, client_cert, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, client_key, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_sslavailable, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_reconnect, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_isconnected, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_settimeout, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_gettimeout, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_setlogcallback, 0, 1, IS_VOID, 0)
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
    PHP_ME(PDO, clickhouseQueryAsync, arginfo_clickhouse_queryasync, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseAsyncPoll, arginfo_clickhouse_asyncpoll, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseAsyncIsReady, arginfo_clickhouse_asyncisready, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseAsyncWait, arginfo_clickhouse_asyncwait, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseAsyncCancel, arginfo_clickhouse_asynccancel, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseInsertFromFile, arginfo_clickhouse_insertfromfile, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetCompression, arginfo_clickhouse_setcompression, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseGetCompression, arginfo_clickhouse_getcompression, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetQueryId, arginfo_clickhouse_setqueryid, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseGetQueryId, arginfo_clickhouse_getqueryid, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseGetLastQueryId, arginfo_clickhouse_getlastqueryid, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseExecuteBatch, arginfo_clickhouse_executebatch, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetSSL, arginfo_clickhouse_setssl, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetSSLVerify, arginfo_clickhouse_setsslverify, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetSSLCA, arginfo_clickhouse_setsslca, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetSSLCert, arginfo_clickhouse_setsslcert, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSslAvailable, arginfo_clickhouse_sslavailable, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseReconnect, arginfo_clickhouse_reconnect, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseIsConnected, arginfo_clickhouse_isconnected, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetTimeout, arginfo_clickhouse_settimeout, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseGetTimeout, arginfo_clickhouse_gettimeout, ZEND_ACC_PUBLIC)
    PHP_ME(PDO, clickhouseSetLogCallback, arginfo_clickhouse_setlogcallback, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

/* Driver-specific PDO subclass methods (non-deprecated API) */
const zend_function_entry pdo_clickhouse_class_methods[] = {
    PHP_ME(Pdo_Clickhouse, insert, arginfo_clickhouse_insert, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, insertFromString, arginfo_clickhouse_insertfromstring, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, getDatabases, arginfo_clickhouse_getdatabases, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, getTables, arginfo_clickhouse_gettables, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, describeTable, arginfo_clickhouse_describetable, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, getServerInfo, arginfo_clickhouse_getserverinfo, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setQuerySetting, arginfo_clickhouse_setquerysetting, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, clearQuerySettings, arginfo_clickhouse_clearquerysettings, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setProgressCallback, arginfo_clickhouse_setprogresscallback, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setProfileCallback, arginfo_clickhouse_setprofilecallback, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, queryAsync, arginfo_clickhouse_queryasync, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, asyncPoll, arginfo_clickhouse_asyncpoll, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, asyncWait, arginfo_clickhouse_asyncwait, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, asyncIsReady, arginfo_clickhouse_asyncisready, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, asyncCancel, arginfo_clickhouse_asynccancel, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, insertFromFile, arginfo_clickhouse_insertfromfile, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setCompression, arginfo_clickhouse_setcompression, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, getCompression, arginfo_clickhouse_getcompression, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setQueryId, arginfo_clickhouse_setqueryid, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, getQueryId, arginfo_clickhouse_getqueryid, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, getLastQueryId, arginfo_clickhouse_getlastqueryid, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, executeBatch, arginfo_clickhouse_executebatch, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setSSL, arginfo_clickhouse_setssl, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setSSLVerify, arginfo_clickhouse_setsslverify, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setSSLCA, arginfo_clickhouse_setsslca, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setSSLCert, arginfo_clickhouse_setsslcert, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, sslAvailable, arginfo_clickhouse_sslavailable, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, reconnect, arginfo_clickhouse_reconnect, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, isConnected, arginfo_clickhouse_isconnected, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setTimeout, arginfo_clickhouse_settimeout, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, getTimeout, arginfo_clickhouse_gettimeout, ZEND_ACC_PUBLIC)
    PHP_ME(Pdo_Clickhouse, setLogCallback, arginfo_clickhouse_setlogcallback, ZEND_ACC_PUBLIC)
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

    /* Clear existing callback */
    if (H->has_progress_callback && !Z_ISUNDEF(H->progress_callback)) {
        zval_ptr_dtor(&H->progress_callback);
        ZVAL_UNDEF(&H->progress_callback);
    }

    /* Set new callback if provided */
    if (callback && Z_TYPE_P(callback) != IS_NULL) {
        if (!zend_is_callable(callback, 0, NULL)) {
            zend_throw_exception(zend_ce_type_error, "Callback must be callable", 0);
            return;
        }
        ZVAL_COPY(&H->progress_callback, callback);
        H->has_progress_callback = 1;
    } else {
        H->has_progress_callback = 0;
    }
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

    /* Clear existing callback */
    if (H->has_profile_callback && !Z_ISUNDEF(H->profile_callback)) {
        zval_ptr_dtor(&H->profile_callback);
        ZVAL_UNDEF(&H->profile_callback);
    }

    /* Set new callback if provided */
    if (callback && Z_TYPE_P(callback) != IS_NULL) {
        if (!zend_is_callable(callback, 0, NULL)) {
            zend_throw_exception(zend_ce_type_error, "Callback must be callable", 0);
            return;
        }
        ZVAL_COPY(&H->profile_callback, callback);
        H->has_profile_callback = 1;
    } else {
        H->has_profile_callback = 0;
    }
}

/* Implementation: clickhouseQueryAsync() */
static PHP_METHOD(PDO, clickhouseQueryAsync)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    char *query;
    size_t query_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(query, query_len)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Free any existing async query */
    if (H->async_query) {
        clickhouse_async_query_free(H->async_query);
        H->async_query = NULL;
        H->has_async_query = 0;
    }

    /* Start async query */
    int status = clickhouse_connection_query_async(H->conn, query, NULL, &H->async_query);

    if (status != 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Failed to start async query");
        RETURN_FALSE;
    }

    H->has_async_query = 1;
    RETURN_TRUE;
}

/* Implementation: clickhouseAsyncPoll() */
static PHP_METHOD(PDO, clickhouseAsyncPoll)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    zend_long timeout_ms = 0;

    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_LONG(timeout_ms)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (!H->has_async_query || !H->async_query) {
        php_error_docref(NULL, E_WARNING, "No async query in progress");
        RETURN_FALSE;
    }

    int result = clickhouse_async_poll(H->conn, H->async_query);

    if (result < 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Async poll failed");
        RETURN_FALSE;
    }

    RETURN_BOOL(result == 1);
}

/* Implementation: clickhouseAsyncIsReady() */
static PHP_METHOD(PDO, clickhouseAsyncIsReady)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (!H->has_async_query || !H->async_query) {
        RETURN_FALSE;
    }

    /* Check if complete without blocking */
    int result = clickhouse_async_poll(H->conn, H->async_query);
    RETURN_BOOL(result == 1);
}

/* Implementation: clickhouseAsyncWait() */
static PHP_METHOD(PDO, clickhouseAsyncWait)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (!H->has_async_query || !H->async_query) {
        php_error_docref(NULL, E_WARNING, "No async query in progress");
        RETURN_FALSE;
    }

    /* Poll until complete */
    while (1) {
        int result = clickhouse_async_poll(H->conn, H->async_query);
        if (result < 0) {
            const char *err = clickhouse_connection_get_error(H->conn);
            pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
                "HY000", 1, err ? err : "Async wait failed");
            RETURN_FALSE;
        }
        if (result == 1) {
            break;  /* Complete */
        }
        /* Sleep briefly to avoid busy-waiting */
        usleep(1000);  /* 1ms */
    }

    /* Get result */
    clickhouse_result *result = H->async_query->result;
    if (!result) {
        RETURN_EMPTY_ARRAY();
    }

    /* Convert result to PHP array */
    array_init(return_value);
    for (size_t b = 0; b < result->block_count; b++) {
        clickhouse_block *block = result->blocks[b];
        for (size_t row = 0; row < block->row_count; row++) {
            zval row_array;
            array_init(&row_array);
            for (size_t c = 0; c < block->column_count; c++) {
                clickhouse_column *col = block->columns[c];
                zval cell;
                /* Convert column data to PHP zval using shared helper */
                pdo_clickhouse_column_to_zval(col, row, &cell);
                add_assoc_zval(&row_array, col->name, &cell);
            }
            add_next_index_zval(return_value, &row_array);
        }
    }

    /* Clean up async query */
    clickhouse_async_query_free(H->async_query);
    H->async_query = NULL;
    H->has_async_query = 0;

    /* return_value was already set by array_init above */
}

/* Implementation: clickhouseAsyncCancel() */
static PHP_METHOD(PDO, clickhouseAsyncCancel)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (!H->has_async_query || !H->async_query) {
        php_error_docref(NULL, E_WARNING, "No async query in progress");
        RETURN_FALSE;
    }

    /* Send cancel packet to server */
    clickhouse_connection_cancel(H->conn);

    /* Free the async query */
    clickhouse_async_query_free(H->async_query);
    H->async_query = NULL;
    H->has_async_query = 0;

    /* The cleanest way to handle cancel: close and reconnect the TCP connection.
     * This ensures all buffers and state are completely cleared. */
    clickhouse_connection_close(H->conn);

    /* Reconnect */
    if (clickhouse_connection_connect(H->conn) != 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "08001", 1, err ? err : "Failed to reconnect after cancel");
        RETURN_FALSE;
    }

    /* Restore timeouts after reconnect */
    clickhouse_connection_set_connect_timeout(H->conn, H->connect_timeout);
    clickhouse_connection_set_read_timeout(H->conn, H->read_timeout);

    RETURN_TRUE;
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

/* Implementation: clickhouseInsertFromString() */
static PHP_METHOD(PDO, clickhouseInsertFromString)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
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
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Insert formatted data directly */
    int status = clickhouse_connection_insert_format_data(H->conn, table, format, data, data_len);

    if (status != 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Failed to insert data");
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

/* Implementation: clickhouseInsertFromFile() */
static PHP_METHOD(PDO, clickhouseInsertFromFile)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    char *table, *filepath, *format = "CSV";
    size_t table_len, filepath_len, format_len = 3;

    ZEND_PARSE_PARAMETERS_START(2, 3)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_STRING(filepath, filepath_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING(format, format_len)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Read file */
    php_stream *stream = php_stream_open_wrapper(filepath, "rb", REPORT_ERRORS, NULL);
    if (!stream) {
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, "Failed to open file");
        RETURN_FALSE;
    }

    zend_string *contents = php_stream_copy_to_mem(stream, PHP_STREAM_COPY_ALL, 0);
    php_stream_close(stream);

    if (!contents || ZSTR_LEN(contents) == 0) {
        if (contents) zend_string_release(contents);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, "File is empty or unreadable");
        RETURN_FALSE;
    }

    /* Insert */
    int status = clickhouse_connection_insert_format_data(H->conn, table, format, 
                                                          ZSTR_VAL(contents), ZSTR_LEN(contents));
    zend_string_release(contents);

    if (status != 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Failed to insert from file");
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

/* Implementation: clickhouseSetCompression() */
static PHP_METHOD(PDO, clickhouseSetCompression)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    zend_long method;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(method)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (method < 0 || method > 2) {
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, "Invalid compression method (0=none, 1=LZ4, 2=ZSTD)");
        RETURN_FALSE;
    }

    H->conn->compression = (uint8_t)method;
    RETURN_TRUE;
}

/* Implementation: clickhouseGetCompression() */
static PHP_METHOD(PDO, clickhouseGetCompression)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    RETURN_LONG(H->conn->compression);
}

/* Implementation: clickhouseSetQueryId() */
static PHP_METHOD(PDO, clickhouseSetQueryId)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    char *query_id = NULL;
    size_t query_id_len = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING_OR_NULL(query_id, query_id_len)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Free existing query ID */
    if (H->default_query_id) {
        efree(H->default_query_id);
        H->default_query_id = NULL;
    }

    /* Set new query ID */
    if (query_id && query_id_len > 0) {
        H->default_query_id = estrndup(query_id, query_id_len);
    }
    RETURN_TRUE;
}

/* Implementation: clickhouseGetQueryId() */
static PHP_METHOD(PDO, clickhouseGetQueryId)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (H->default_query_id) {
        RETURN_STRING(H->default_query_id);
    }
    RETURN_NULL();
}

/* Implementation: clickhouseGetLastQueryId() */
static PHP_METHOD(PDO, clickhouseGetLastQueryId)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (H->last_query_id) {
        RETURN_STRING(H->last_query_id);
    }
    RETURN_NULL();
}

/* Implementation: clickhouseExecuteBatch() */
static PHP_METHOD(PDO, clickhouseExecuteBatch)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    zval *queries;
    HashTable *queries_ht;
    zval *entry;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ARRAY(queries)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    queries_ht = Z_ARRVAL_P(queries);
    array_init(return_value);

    ZEND_HASH_FOREACH_VAL(queries_ht, entry) {
        if (Z_TYPE_P(entry) != IS_STRING) {
            add_next_index_bool(return_value, 0);
            continue;
        }

        clickhouse_result *result = NULL;
        int status = clickhouse_connection_execute_query(H->conn, Z_STRVAL_P(entry), &result);
        if (result) {
            clickhouse_result_free(result);
        }
        add_next_index_bool(return_value, status == 0);
    } ZEND_HASH_FOREACH_END();

    return;
}

/* Implementation: clickhouseSetSSL() */
static PHP_METHOD(PDO, clickhouseSetSSL)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    zend_bool enabled;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_BOOL(enabled)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    clickhouse_connection_set_ssl_enabled(H->conn, enabled);
    RETURN_TRUE;
}

/* Implementation: clickhouseSetSSLVerify() */
static PHP_METHOD(PDO, clickhouseSetSSLVerify)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    zend_bool verify_peer, verify_host;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_BOOL(verify_peer)
        Z_PARAM_BOOL(verify_host)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    clickhouse_connection_set_ssl_verify(H->conn, verify_peer, verify_host);
    RETURN_TRUE;
}

/* Implementation: clickhouseSetSSLCA() */
static PHP_METHOD(PDO, clickhouseSetSSLCA)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    char *ca_cert;
    size_t ca_cert_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(ca_cert, ca_cert_len)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    clickhouse_connection_set_ssl_ca_cert(H->conn, ca_cert);
    RETURN_TRUE;
}

/* Implementation: clickhouseSetSSLCert() */
static PHP_METHOD(PDO, clickhouseSetSSLCert)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    char *client_cert, *client_key;
    size_t cert_len, key_len;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(client_cert, cert_len)
        Z_PARAM_STRING(client_key, key_len)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    clickhouse_connection_set_ssl_client_cert(H->conn, client_cert, client_key);
    RETURN_TRUE;
}

/* Implementation: clickhouseSslAvailable() */
static PHP_METHOD(PDO, clickhouseSslAvailable)
{
    ZEND_PARSE_PARAMETERS_NONE();
    RETURN_BOOL(clickhouse_ssl_available());
}

/* Implementation: clickhouseReconnect() */
static PHP_METHOD(PDO, clickhouseReconnect)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    clickhouse_connection_close(H->conn);

    if (clickhouse_connection_connect(H->conn) != 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "08001", 1, err ? err : "Failed to reconnect");
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

/* Implementation: clickhouseIsConnected() */
static PHP_METHOD(PDO, clickhouseIsConnected)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    RETURN_BOOL(H->conn->state == CONN_STATE_AUTHENTICATED);
}

/* Implementation: clickhouseSetTimeout() */
static PHP_METHOD(PDO, clickhouseSetTimeout)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;
    zend_long timeout_ms;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(timeout_ms)
    ZEND_PARSE_PARAMETERS_END();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    clickhouse_connection_set_query_timeout_ms(H->conn, (int)timeout_ms);
    H->read_timeout = (unsigned int)(timeout_ms / 1000);
    RETURN_TRUE;
}

/* Implementation: clickhouseGetTimeout() */
static PHP_METHOD(PDO, clickhouseGetTimeout)
{
    pdo_dbh_t *dbh;
    pdo_clickhouse_db_handle *H;

    ZEND_PARSE_PARAMETERS_NONE();

    dbh = Z_PDO_DBH_P(ZEND_THIS);
    PDO_CONSTRUCT_CHECK;
    H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    RETURN_LONG(H->read_timeout * 1000);
}

/* Implementation: clickhouseSetLogCallback() */
static PHP_METHOD(PDO, clickhouseSetLogCallback)
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

    /* Clear existing callback */
    if (H->has_log_callback && !Z_ISUNDEF(H->log_callback)) {
        zval_ptr_dtor(&H->log_callback);
        ZVAL_UNDEF(&H->log_callback);
        H->has_log_callback = 0;
    }

    /* Set new callback */
    if (callback && Z_TYPE_P(callback) != IS_NULL) {
        ZVAL_COPY(&H->log_callback, callback);
        H->has_log_callback = 1;
    }
    RETURN_TRUE;
}

/* Wrapper methods for the non-deprecated Pdo\\Clickhouse subclass.
 * They simply forward to the legacy driver-specific handlers. */
static PHP_METHOD(Pdo_Clickhouse, insert)
{
    zim_PDO_clickhouseInsert(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, insertFromString)
{
    zim_PDO_clickhouseInsertFromString(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, getDatabases)
{
    zim_PDO_clickhouseGetDatabases(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, getTables)
{
    zim_PDO_clickhouseGetTables(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, describeTable)
{
    zim_PDO_clickhouseDescribeTable(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, getServerInfo)
{
    zim_PDO_clickhouseGetServerInfo(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setQuerySetting)
{
    zim_PDO_clickhouseSetQuerySetting(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, clearQuerySettings)
{
    zim_PDO_clickhouseClearQuerySettings(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setProgressCallback)
{
    zim_PDO_clickhouseSetProgressCallback(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setProfileCallback)
{
    zim_PDO_clickhouseSetProfileCallback(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, queryAsync)
{
    zim_PDO_clickhouseQueryAsync(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, asyncPoll)
{
    zim_PDO_clickhouseAsyncPoll(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, asyncWait)
{
    zim_PDO_clickhouseAsyncWait(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, asyncIsReady)
{
    zim_PDO_clickhouseAsyncIsReady(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, asyncCancel)
{
    zim_PDO_clickhouseAsyncCancel(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, insertFromFile)
{
    zim_PDO_clickhouseInsertFromFile(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setCompression)
{
    zim_PDO_clickhouseSetCompression(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, getCompression)
{
    zim_PDO_clickhouseGetCompression(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setQueryId)
{
    zim_PDO_clickhouseSetQueryId(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, getQueryId)
{
    zim_PDO_clickhouseGetQueryId(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, getLastQueryId)
{
    zim_PDO_clickhouseGetLastQueryId(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, executeBatch)
{
    zim_PDO_clickhouseExecuteBatch(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setSSL)
{
    zim_PDO_clickhouseSetSSL(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setSSLVerify)
{
    zim_PDO_clickhouseSetSSLVerify(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setSSLCA)
{
    zim_PDO_clickhouseSetSSLCA(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setSSLCert)
{
    zim_PDO_clickhouseSetSSLCert(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, sslAvailable)
{
    zim_PDO_clickhouseSslAvailable(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, reconnect)
{
    zim_PDO_clickhouseReconnect(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, isConnected)
{
    zim_PDO_clickhouseIsConnected(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setTimeout)
{
    zim_PDO_clickhouseSetTimeout(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, getTimeout)
{
    zim_PDO_clickhouseGetTimeout(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}

static PHP_METHOD(Pdo_Clickhouse, setLogCallback)
{
    zim_PDO_clickhouseSetLogCallback(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}
