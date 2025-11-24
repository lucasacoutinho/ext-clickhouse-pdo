/*
  +----------------------------------------------------------------------+
  | PDO ClickHouse Driver - Driver Implementation                       |
  +----------------------------------------------------------------------+
*/

#include "php.h"
#include "php_ini.h"
#include "pdo/php_pdo.h"
#include "pdo/php_pdo_driver.h"
#include "php_pdo_clickhouse.h"
#include "php_pdo_clickhouse_int.h"
#include "zend_exceptions.h"
#include <string.h>

/* Forward declarations */
static void clickhouse_handle_closer(pdo_dbh_t *dbh);
static bool clickhouse_handle_preparer(pdo_dbh_t *dbh, zend_string *sql,
                                       pdo_stmt_t *stmt, zval *driver_options);
static zend_long clickhouse_handle_doer(pdo_dbh_t *dbh, const zend_string *sql);
static zend_string *clickhouse_handle_quoter(pdo_dbh_t *dbh, const zend_string *unquoted,
                                             enum pdo_param_type paramtype);
static bool clickhouse_handle_begin(pdo_dbh_t *dbh);
static bool clickhouse_handle_commit(pdo_dbh_t *dbh);
static bool clickhouse_handle_rollback(pdo_dbh_t *dbh);
static bool clickhouse_set_attribute(pdo_dbh_t *dbh, zend_long attr, zval *val);
static int clickhouse_get_attribute(pdo_dbh_t *dbh, zend_long attr, zval *val);
static void clickhouse_fetch_error(pdo_dbh_t *dbh, pdo_stmt_t *stmt, zval *info);
static zend_result clickhouse_check_liveness(pdo_dbh_t *dbh);
static bool clickhouse_in_transaction(pdo_dbh_t *dbh);

/* Error handling */
int pdo_clickhouse_error(pdo_dbh_t *dbh, pdo_stmt_t *stmt,
                         const char *file, int line,
                         const char *sqlstate, int errcode,
                         const char *message)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;
    pdo_clickhouse_error_info *einfo;

    if (stmt) {
        pdo_clickhouse_stmt *S = (pdo_clickhouse_stmt *)stmt->driver_data;
        einfo = &S->einfo;
    } else {
        einfo = &H->einfo;
    }

    einfo->file = file;
    einfo->line = line;
    einfo->errcode = errcode;

    if (einfo->errmsg) {
        pefree(einfo->errmsg, dbh->is_persistent);
    }
    einfo->errmsg = pestrdup(message, dbh->is_persistent);

    if (sqlstate && sqlstate[0]) {
        memcpy(einfo->sqlstate, sqlstate, 5);
        einfo->sqlstate[5] = '\0';
    } else {
        memcpy(einfo->sqlstate, "HY000", 6);
    }

    if (stmt) {
        strcpy(stmt->error_code, einfo->sqlstate);
    }
    strcpy(dbh->error_code, einfo->sqlstate);

    return errcode;
}

/* Parse DSN: clickhouse:host=localhost;port=9000;dbname=default */
static int parse_dsn(pdo_dbh_t *dbh, char **host, int *port,
                     char **database, char **user, char **password)
{
    struct pdo_data_src_parser vars[] = {
        { "host", "localhost", 0 },
        { "port", "9000", 0 },
        { "dbname", "default", 0 },
        { "user", "default", 0 },
        { "password", "", 0 },
    };
    int nvars = sizeof(vars) / sizeof(vars[0]);

    php_pdo_parse_data_source(dbh->data_source, dbh->data_source_len, vars, nvars);

    *host = vars[0].optval;
    *port = atoi(vars[1].optval);
    *database = vars[2].optval;
    *user = vars[3].optval;
    *password = vars[4].optval;

    return 1;
}

/* Connection factory - called when new PDO('clickhouse:...') */
static int pdo_clickhouse_handle_factory(pdo_dbh_t *dbh, zval *driver_options)
{
    pdo_clickhouse_db_handle *H;
    char *host, *database, *user, *password;
    int port;
    int ret = 0;

    H = pecalloc(1, sizeof(pdo_clickhouse_db_handle), dbh->is_persistent);
    dbh->driver_data = H;

    /* Parse DSN */
    if (!parse_dsn(dbh, &host, &port, &database, &user, &password)) {
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, "Failed to parse DSN");
        goto cleanup;
    }

    /* Override with username/password if provided */
    if (dbh->username && dbh->username[0]) {
        user = dbh->username;
    }
    if (dbh->password && dbh->password[0]) {
        password = dbh->password;
    }

    /* Apply driver options */
    H->connect_timeout = 10;
    H->read_timeout = 30;
    H->max_block_size = 65536;
    H->compression = 0;

    /* Initialize callbacks */
    ZVAL_UNDEF(&H->progress_callback);
    ZVAL_UNDEF(&H->profile_callback);
    ZVAL_UNDEF(&H->log_callback);
    H->has_progress_callback = 0;
    H->has_profile_callback = 0;
    H->has_log_callback = 0;

    /* Initialize query IDs */
    H->default_query_id = NULL;
    H->last_query_id = NULL;

    /* Initialize async query */
    H->async_query = NULL;
    H->has_async_query = 0;

    if (driver_options) {
        zval *val;
        if ((val = zend_hash_index_find(Z_ARRVAL_P(driver_options),
                                        PDO_ATTR_TIMEOUT)) != NULL) {
            H->connect_timeout = (unsigned int)zval_get_long(val);
        }
        if ((val = zend_hash_index_find(Z_ARRVAL_P(driver_options),
                                        PDO_CLICKHOUSE_ATTR_COMPRESSION)) != NULL) {
            H->compression = zval_is_true(val);
        }
    }

    /* Create connection */
    H->conn = clickhouse_connection_create(host, (uint16_t)port,
                                           user, password, database);
    if (!H->conn) {
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "08001", 1, "Failed to create connection");
        goto cleanup;
    }

    /* Set timeouts */
    clickhouse_connection_set_connect_timeout(H->conn, H->connect_timeout);
    clickhouse_connection_set_read_timeout(H->conn, H->read_timeout);

    /* Connect */
    if (clickhouse_connection_connect(H->conn) != 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "08001", 1, err ? err : "Connection failed");
        goto cleanup;
    }

    /* Set driver methods */
    dbh->methods = &clickhouse_methods;
    dbh->alloc_own_columns = 1;

    ret = 1;

cleanup:
    if (!ret) {
        if (H->conn) {
            clickhouse_connection_free(H->conn);
            H->conn = NULL;
        }
    }
    return ret;
}

/* Close connection */
static void clickhouse_handle_closer(pdo_dbh_t *dbh)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (H) {
        if (H->conn) {
            clickhouse_connection_close(H->conn);
            clickhouse_connection_free(H->conn);
        }
        if (H->einfo.errmsg) {
            pefree(H->einfo.errmsg, dbh->is_persistent);
        }
        /* Clean up query settings */
        if (H->query_settings) {
            for (size_t i = 0; i < H->query_settings_count; i++) {
                pefree(H->query_settings[i].name, dbh->is_persistent);
                pefree(H->query_settings[i].value, dbh->is_persistent);
            }
            pefree(H->query_settings, dbh->is_persistent);
        }
        /* Clean up callbacks */
        if (H->has_progress_callback && !Z_ISUNDEF(H->progress_callback)) {
            zval_ptr_dtor(&H->progress_callback);
        }
        if (H->has_profile_callback && !Z_ISUNDEF(H->profile_callback)) {
            zval_ptr_dtor(&H->profile_callback);
        }
        if (H->has_log_callback && !Z_ISUNDEF(H->log_callback)) {
            zval_ptr_dtor(&H->log_callback);
        }
        /* Clean up query IDs */
        if (H->default_query_id) {
            efree(H->default_query_id);
        }
        if (H->last_query_id) {
            efree(H->last_query_id);
        }
        /* Clean up async query */
        if (H->async_query) {
            clickhouse_async_query_free(H->async_query);
            H->async_query = NULL;
        }
        pefree(H, dbh->is_persistent);
        dbh->driver_data = NULL;
    }
}

/* Prepare statement */
static bool clickhouse_handle_preparer(pdo_dbh_t *dbh, zend_string *sql,
                                       pdo_stmt_t *stmt, zval *driver_options)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;
    pdo_clickhouse_stmt *S;

    S = ecalloc(1, sizeof(pdo_clickhouse_stmt));
    S->H = H;

    /* Initialize statement attributes with defaults */
    S->cursor_type = PDO_CURSOR_FWDONLY;
    S->max_column_len = 0;  /* 0 = unlimited */
    S->cursor_name = NULL;
    S->cursor_open = 0;

    stmt->driver_data = S;
    stmt->methods = &clickhouse_stmt_methods;
    stmt->supports_placeholders = PDO_PLACEHOLDER_NONE;

    return true;
}

/* Execute non-SELECT query directly */
static zend_long clickhouse_handle_doer(pdo_dbh_t *dbh, const zend_string *sql)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;
    clickhouse_result *result = NULL;

    int status = clickhouse_connection_execute_query(H->conn, ZSTR_VAL(sql), &result);

    if (result) {
        clickhouse_result_free(result);
    }

    if (status != 0) {
        const char *err = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Query failed");
        return -1;
    }

    /* ClickHouse doesn't return affected row count for most operations */
    return 0;
}

/* Quote string */
static zend_string *clickhouse_handle_quoter(pdo_dbh_t *dbh, const zend_string *unquoted,
                                             enum pdo_param_type paramtype)
{
    const char *str = ZSTR_VAL(unquoted);
    size_t len = ZSTR_LEN(unquoted);

    /* Count characters that need escaping */
    size_t extra = 0;
    for (size_t i = 0; i < len; i++) {
        if (str[i] == '\'' || str[i] == '\\') {
            extra++;
        }
    }

    /* Allocate result: original + escapes + 2 quotes + null */
    zend_string *result = zend_string_alloc(len + extra + 2, 0);
    char *out = ZSTR_VAL(result);

    *out++ = '\'';
    for (size_t i = 0; i < len; i++) {
        if (str[i] == '\'' || str[i] == '\\') {
            *out++ = '\\';
        }
        *out++ = str[i];
    }
    *out++ = '\'';
    *out = '\0';

    return result;
}

/* Begin transaction (limited support in ClickHouse) */
static bool clickhouse_handle_begin(pdo_dbh_t *dbh)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    /* Execute BEGIN TRANSACTION via ClickHouse connection
     * Note: Requires ClickHouse 21.11+ with Atomic database engine
     * This feature is EXPERIMENTAL */
    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query(H->conn, "BEGIN TRANSACTION", &result);

    if (result) {
        clickhouse_result_free(result);
    }

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__, "HY000", status,
            error ? error : "Failed to begin transaction. Note: Transactions require ClickHouse 21.11+ with Atomic database engine. This feature is EXPERIMENTAL.");
        return false;
    }

    H->in_transaction = 1;
    return true;
}

/* Commit transaction */
static bool clickhouse_handle_commit(pdo_dbh_t *dbh)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (!H->in_transaction) {
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__, "HY000", 0,
            "No active transaction to commit");
        return false;
    }

    /* Execute COMMIT via ClickHouse connection */
    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query(H->conn, "COMMIT", &result);

    if (result) {
        clickhouse_result_free(result);
    }

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__, "HY000", status,
            error ? error : "Failed to commit transaction");
        return false;
    }

    H->in_transaction = 0;
    return true;
}

/* Rollback transaction */
static bool clickhouse_handle_rollback(pdo_dbh_t *dbh)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (!H->in_transaction) {
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__, "HY000", 0,
            "No active transaction to rollback");
        return false;
    }

    /* Execute ROLLBACK via ClickHouse connection */
    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query(H->conn, "ROLLBACK", &result);

    if (result) {
        clickhouse_result_free(result);
    }

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(H->conn);
        pdo_clickhouse_error(dbh, NULL, __FILE__, __LINE__, "HY000", status,
            error ? error : "Failed to rollback transaction");
        return false;
    }

    H->in_transaction = 0;
    return true;
}

/* Set attribute */
static bool clickhouse_set_attribute(pdo_dbh_t *dbh, zend_long attr, zval *val)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    switch (attr) {
        case PDO_CLICKHOUSE_ATTR_COMPRESSION:
            H->compression = zval_is_true(val);
            return true;

        case PDO_CLICKHOUSE_ATTR_TIMEOUT:
            H->read_timeout = (unsigned int)zval_get_long(val);
            clickhouse_connection_set_read_timeout(H->conn, H->read_timeout);
            return true;

        case PDO_CLICKHOUSE_ATTR_MAX_BLOCK_SIZE:
            H->max_block_size = (unsigned int)zval_get_long(val);
            return true;

        default:
            return false;
    }
}

/* Get attribute */
static int clickhouse_get_attribute(pdo_dbh_t *dbh, zend_long attr, zval *val)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    switch (attr) {
        case PDO_ATTR_SERVER_VERSION:
            if (H->conn && H->conn->server_info) {
                char version[64];
                snprintf(version, sizeof(version), "%lu.%lu.%lu",
                    (unsigned long)H->conn->server_info->version_major,
                    (unsigned long)H->conn->server_info->version_minor,
                    (unsigned long)H->conn->server_info->revision);
                ZVAL_STRING(val, version);
                return 1;
            }
            return 0;

        case PDO_ATTR_SERVER_INFO:
            if (H->conn && H->conn->server_info && H->conn->server_info->name) {
                ZVAL_STRING(val, H->conn->server_info->name);
                return 1;
            }
            return 0;

        case PDO_ATTR_CLIENT_VERSION:
            ZVAL_STRING(val, PHP_PDO_CLICKHOUSE_VERSION);
            return 1;

        case PDO_ATTR_CONNECTION_STATUS:
            if (H->conn && clickhouse_connection_ping(H->conn) == 0) {
                ZVAL_STRING(val, "Connected");
            } else {
                ZVAL_STRING(val, "Disconnected");
            }
            return 1;

        case PDO_CLICKHOUSE_ATTR_COMPRESSION:
            ZVAL_BOOL(val, H->compression);
            return 1;

        default:
            return 0;
    }
}

/* Fetch error info */
static void clickhouse_fetch_error(pdo_dbh_t *dbh, pdo_stmt_t *stmt, zval *info)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;
    pdo_clickhouse_error_info *einfo;

    if (stmt) {
        pdo_clickhouse_stmt *S = (pdo_clickhouse_stmt *)stmt->driver_data;
        einfo = &S->einfo;
    } else {
        einfo = &H->einfo;
    }

    if (einfo->errcode) {
        add_next_index_long(info, einfo->errcode);
        add_next_index_string(info, einfo->errmsg ? einfo->errmsg : "");
    }
}

/* Check if connection is alive */
static zend_result clickhouse_check_liveness(pdo_dbh_t *dbh)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;

    if (H->conn && clickhouse_connection_ping(H->conn) == 0) {
        return SUCCESS;
    }
    return FAILURE;
}

/* Check if in transaction */
static bool clickhouse_in_transaction(pdo_dbh_t *dbh)
{
    pdo_clickhouse_db_handle *H = (pdo_clickhouse_db_handle *)dbh->driver_data;
    return H->in_transaction;
}

/* Driver methods structure */
const struct pdo_dbh_methods clickhouse_methods = {
    clickhouse_handle_closer,
    clickhouse_handle_preparer,
    clickhouse_handle_doer,
    clickhouse_handle_quoter,
    clickhouse_handle_begin,
    clickhouse_handle_commit,
    clickhouse_handle_rollback,
    clickhouse_set_attribute,
    NULL, /* last_insert_id - ClickHouse doesn't have auto-increment IDs */
    clickhouse_fetch_error,
    clickhouse_get_attribute,
    clickhouse_check_liveness,
    clickhouse_get_driver_methods, /* get_driver_methods */
    NULL, /* request_shutdown */
    clickhouse_in_transaction,
    NULL  /* get_gc */
};

/* Driver entry */
const pdo_driver_t pdo_clickhouse_driver = {
    PDO_DRIVER_HEADER(clickhouse),
    pdo_clickhouse_handle_factory
};
