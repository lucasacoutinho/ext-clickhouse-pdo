#include "php_pdo_clickhouse.h"

#include <cstring>
#include <chrono>
#include <string>
#include <new>

void pdo_clickhouse_error(pdo_dbh_t *dbh, pdo_stmt_t *stmt,
                          int errcode, const char *errmsg, const char *sqlstate)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    pdo_error_type *pdo_err = stmt ? &stmt->error_code : &dbh->error_code;

    H->errcode = errcode;
    H->errmsg = errmsg ? errmsg : "";

    if (sqlstate) {
        strncpy(*pdo_err, sqlstate, sizeof(*pdo_err));
        (*pdo_err)[5] = '\0';
    } else {
        strncpy(*pdo_err, "HY000", sizeof(*pdo_err));
    }
}

static void clickhouse_handle_closer(pdo_dbh_t *dbh)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    if (H) {
        H->client.~unique_ptr();
        H->options.~unique_ptr();
        H->errmsg.~basic_string();
        pefree(H, dbh->is_persistent);
        dbh->driver_data = nullptr;
    }
}

static bool clickhouse_handle_preparer(pdo_dbh_t *dbh, zend_string *sql,
                                       pdo_stmt_t *stmt, zval *driver_options)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);

    auto *S = static_cast<pdo_clickhouse_stmt *>(
        ecalloc(1, sizeof(pdo_clickhouse_stmt)));

    /* Placement-new for ALL C++ members */
    new (&S->query) std::string(ZSTR_VAL(sql), ZSTR_LEN(sql));
    new (&S->rewritten_query) std::string();
    new (&S->blocks) std::vector<clickhouse::Block>();
    new (&S->col_names) std::vector<std::string>();
    new (&S->col_type_names) std::vector<std::string>();
    new (&S->row_map) std::vector<std::pair<size_t, size_t>>();

    S->H = H;
    S->total_rows = 0;
    S->current_row = 0;
    S->executed = false;
    S->affected_rows = 0;

    stmt->driver_data = S;
    stmt->methods = &clickhouse_stmt_methods;

    /* All queries use SQL text path via PDO emulated prepares.
     * ClickHouse's server-side VALUES parser handles all types including
     * Map, Array, Tuple correctly. */
    stmt->supports_placeholders = PDO_PLACEHOLDER_NONE;

    return true;
}

static zend_long clickhouse_handle_doer(pdo_dbh_t *dbh, const zend_string *sql)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);

    try {
        clickhouse::Query q(std::string(ZSTR_VAL(sql), ZSTR_LEN(sql)));
        H->client->Execute(q);
        return 0; /* ClickHouse doesn't report affected rows for DDL */
    } catch (const clickhouse::ServerException &e) {
        pdo_clickhouse_error(dbh, nullptr, e.GetCode(), e.what(), "HY000");
        return -1;
    } catch (const std::exception &e) {
        pdo_clickhouse_error(dbh, nullptr, -1, e.what(), "HY000");
        return -1;
    }
}

static zend_string *clickhouse_handle_quoter(pdo_dbh_t *dbh,
                                             const zend_string *unquoted,
                                             enum pdo_param_type paramtype)
{
    const char *src = ZSTR_VAL(unquoted);
    size_t src_len = ZSTR_LEN(unquoted);

    /* Worst case: every char is escaped + 2 quotes */
    zend_string *quoted = zend_string_alloc(src_len * 2 + 2, 0);
    char *dst = ZSTR_VAL(quoted);

    *dst++ = '\'';
    for (size_t i = 0; i < src_len; i++) {
        switch (src[i]) {
            case '\'': *dst++ = '\\'; *dst++ = '\''; break;
            case '\\': *dst++ = '\\'; *dst++ = '\\'; break;
            case '\0': *dst++ = '\\'; *dst++ = '0'; break;
            default:   *dst++ = src[i]; break;
        }
    }
    *dst++ = '\'';
    *dst = '\0';

    ZSTR_LEN(quoted) = dst - ZSTR_VAL(quoted);
    quoted = zend_string_truncate(quoted, ZSTR_LEN(quoted), 0);

    return quoted;
}

static bool clickhouse_handle_begin(pdo_dbh_t *dbh)
{
    /* ClickHouse has no transactions; silently succeed */
    return true;
}

static bool clickhouse_handle_commit(pdo_dbh_t *dbh)
{
    return true;
}

static bool clickhouse_handle_rollback(pdo_dbh_t *dbh)
{
    /* Can't rollback — ClickHouse is append-only */
    pdo_clickhouse_error(dbh, nullptr, -1,
        "ClickHouse does not support transactions/rollback", "IM001");
    return false;
}

static bool clickhouse_handle_in_transaction(pdo_dbh_t *dbh)
{
    return false;
}

static void clickhouse_fetch_error_func(pdo_dbh_t *dbh, pdo_stmt_t *stmt, zval *info)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    if (H->errcode) {
        add_next_index_long(info, static_cast<zend_long>(H->errcode));
        add_next_index_string(info, H->errmsg.c_str());
    }
}

static int clickhouse_handle_get_attribute(pdo_dbh_t *dbh, zend_long attr, zval *val)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);

    switch (attr) {
        case PDO_ATTR_SERVER_VERSION: {
            try {
                const auto &info = H->client->GetServerInfo();
                std::string ver = std::to_string(info.version_major) + "." +
                                  std::to_string(info.version_minor) + "." +
                                  std::to_string(info.version_patch);
                ZVAL_STRING(val, ver.c_str());
                return 1;
            } catch (...) {
                return 0;
            }
        }
        case PDO_ATTR_CLIENT_VERSION:
            ZVAL_STRING(val, PHP_PDO_CLICKHOUSE_VERSION);
            return 1;
        case PDO_ATTR_SERVER_INFO: {
            try {
                const auto &info = H->client->GetServerInfo();
                ZVAL_STRING(val, info.name.c_str());
                return 1;
            } catch (...) {
                return 0;
            }
        }
        case PDO_ATTR_CONNECTION_STATUS:
            ZVAL_STRING(val, "Connected via native TCP");
            return 1;
        default:
            return 0;
    }
}

static bool clickhouse_handle_set_attribute(pdo_dbh_t *dbh, zend_long attr, zval *val)
{
    return false; /* No settable attributes yet */
}

static zend_result clickhouse_handle_check_liveness(pdo_dbh_t *dbh)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    try {
        H->client->Ping();
        return SUCCESS;
    } catch (...) {
        return FAILURE;
    }
}

static zend_string *clickhouse_handle_last_id(pdo_dbh_t *dbh, const zend_string *name)
{
    return zend_string_init("0", 1, 0);
}

const struct pdo_dbh_methods clickhouse_dbh_methods = {
    clickhouse_handle_closer,          /* closer */
    clickhouse_handle_preparer,        /* preparer */
    clickhouse_handle_doer,            /* doer */
    clickhouse_handle_quoter,          /* quoter */
    clickhouse_handle_begin,           /* begin */
    clickhouse_handle_commit,          /* commit */
    clickhouse_handle_rollback,        /* rollback */
    clickhouse_handle_set_attribute,   /* set_attribute */
    clickhouse_handle_last_id,         /* last_id */
    clickhouse_fetch_error_func,       /* fetch_err */
    clickhouse_handle_get_attribute,   /* get_attribute */
    clickhouse_handle_check_liveness,  /* check_liveness */
    NULL,                              /* get_driver_methods */
    NULL,                              /* persistent_shutdown */
    clickhouse_handle_in_transaction,  /* in_transaction */
    NULL,                              /* get_gc */
};

static int pdo_clickhouse_handle_factory(pdo_dbh_t *dbh, zval *driver_options)
{
    struct pdo_data_src_parser parsed[] = {
        { "host",     NULL, 0 },
        { "port",     NULL, 0 },
        { "dbname",   NULL, 0 },
    };

    php_pdo_parse_data_source(dbh->data_source, dbh->data_source_len,
                              parsed, sizeof(parsed) / sizeof(parsed[0]));

    const char *host   = parsed[0].optval ? parsed[0].optval : "localhost";
    int port           = parsed[1].optval ? atoi(parsed[1].optval) : 9000;
    const char *dbname = parsed[2].optval ? parsed[2].optval : "default";
    const char *user   = dbh->username ? dbh->username : "default";
    const char *pass   = dbh->password ? dbh->password : "";

    /* Allocate driver handle — respecting is_persistent */
    auto *H = static_cast<pdo_clickhouse_db_handle *>(
        pecalloc(1, sizeof(pdo_clickhouse_db_handle), dbh->is_persistent));

    /* Placement-new for C++ members */
    new (&H->client) std::unique_ptr<clickhouse::Client>();
    new (&H->options) std::unique_ptr<clickhouse::ClientOptions>();
    new (&H->errmsg) std::string();
    H->errcode = 0;

    dbh->driver_data = H;

    try {
        auto opts = std::make_unique<clickhouse::ClientOptions>();
        opts->SetHost(host);
        opts->SetPort(static_cast<uint16_t>(port));
        opts->SetDefaultDatabase(dbname);
        opts->SetUser(user);
        opts->SetPassword(pass);

        /* Parse timeout from driver_options if provided */
        zend_long timeout = pdo_attr_lval(driver_options, PDO_ATTR_TIMEOUT, 5);
        opts->SetConnectionConnectTimeout(std::chrono::seconds(timeout));

        H->options = std::move(opts);
        H->client = std::make_unique<clickhouse::Client>(*H->options);
    } catch (const clickhouse::ServerException &e) {
        pdo_clickhouse_error(dbh, nullptr, e.GetCode(), e.what(), "08001");
        goto cleanup;
    } catch (const std::exception &e) {
        pdo_clickhouse_error(dbh, nullptr, -1, e.what(), "08001");
        goto cleanup;
    }

    dbh->methods = &clickhouse_dbh_methods;
    dbh->alloc_own_columns = 1;
    dbh->max_escaped_char_length = 2;

    /* Free parsed DSN values */
    for (size_t i = 0; i < sizeof(parsed) / sizeof(parsed[0]); i++) {
        if (parsed[i].freeme) {
            efree(parsed[i].optval);
        }
    }

    return 1; /* success */

cleanup:
    for (size_t i = 0; i < sizeof(parsed) / sizeof(parsed[0]); i++) {
        if (parsed[i].freeme) {
            efree(parsed[i].optval);
        }
    }
    clickhouse_handle_closer(dbh);
    return 0; /* failure */
}

const pdo_driver_t pdo_clickhouse_driver = {
    PDO_DRIVER_HEADER(clickhouse),
    pdo_clickhouse_handle_factory
};
