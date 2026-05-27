#include "php_pdo_clickhouse.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <string>
#include <new>
#include <vector>

static std::string clickhouse_normalize_option_value(const char *src, size_t src_len)
{
    size_t begin = 0;
    size_t end = src_len;

    while (begin < end && std::isspace(static_cast<unsigned char>(src[begin]))) {
        begin++;
    }
    while (end > begin && std::isspace(static_cast<unsigned char>(src[end - 1]))) {
        end--;
    }

    std::string value(src + begin, end - begin);
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

static bool clickhouse_parse_dsn_bool(const char *src, bool *out)
{
    if (!src) {
        return false;
    }

    std::string value = clickhouse_normalize_option_value(src, strlen(src));
    if (value == "1" || value == "true" || value == "yes" || value == "on") {
        *out = true;
        return true;
    }
    if (value == "0" || value == "false" || value == "no" || value == "off") {
        *out = false;
        return true;
    }

    return false;
}

static bool clickhouse_is_zero_number_string(const std::string &value)
{
    size_t pos = 0;
    bool seen_digit = false;
    bool seen_dot = false;

    if (value.empty()) {
        return false;
    }

    if (value[pos] == '+' || value[pos] == '-') {
        pos++;
        if (pos == value.size()) {
            return false;
        }
    }

    for (; pos < value.size(); ++pos) {
        char c = value[pos];
        if (c == '.') {
            if (seen_dot) {
                return false;
            }
            seen_dot = true;
            continue;
        }
        if (c < '0' || c > '9') {
            return false;
        }
        seen_digit = true;
        if (c != '0') {
            return false;
        }
    }

    return seen_digit;
}

static bool clickhouse_param_bool_is_truthy(const char *src, size_t src_len)
{
    std::string value = clickhouse_normalize_option_value(src, src_len);

    if (value.empty() || value == "false" || value == "no" || value == "off" ||
        clickhouse_is_zero_number_string(value)) {
        return false;
    }

    return true;
}

void pdo_clickhouse_error(pdo_dbh_t *dbh, pdo_stmt_t *stmt, int errcode, const char *errmsg,
                          const char *sqlstate)
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

    /* During factory (dbh->methods not yet set), PDO core cannot fetch
     * driver error info — throw a PDOException directly so the real
     * message reaches userland instead of generic "Constructor failed". */
    if (!dbh->methods) {
        pdo_throw_exception(static_cast<unsigned int>(errcode),
                            const_cast<char *>(H->errmsg.c_str()), pdo_err);
    }
}

void pdo_clickhouse_clear_error(pdo_dbh_t *dbh, pdo_stmt_t *stmt)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    pdo_error_type *pdo_err = stmt ? &stmt->error_code : &dbh->error_code;

    if (H) {
        H->errcode = 0;
        H->errmsg.clear();
    }
    memcpy(*pdo_err, "00000", sizeof("00000"));
}

#if PHP_VERSION_ID < 80100
static int clickhouse_handle_closer(pdo_dbh_t *dbh)
#else
static void clickhouse_handle_closer(pdo_dbh_t *dbh)
#endif
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    if (H) {
        H->client.~unique_ptr();
        H->options.~unique_ptr();
        H->errmsg.~basic_string();
        pefree(H, dbh->is_persistent);
        dbh->driver_data = nullptr;
    }
#if PHP_VERSION_ID < 80100
    return 0;
#endif
}

static bool clickhouse_handle_preparer_impl(pdo_dbh_t *dbh, const char *sql, size_t sql_len,
                                            pdo_stmt_t *stmt, zval *driver_options)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);

    auto *S = static_cast<pdo_clickhouse_stmt *>(ecalloc(1, sizeof(pdo_clickhouse_stmt)));

    /* Placement-new for ALL C++ members */
    new (&S->query) std::string(sql, sql_len);
    new (&S->rewritten_query) std::string();
    new (&S->blocks) std::vector<clickhouse::Block>();
    new (&S->col_names) std::vector<std::string>();
    new (&S->col_type_names) std::vector<std::string>();
    new (&S->block_row_offsets) std::vector<size_t>();

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

#if PHP_VERSION_ID < 80100
static int clickhouse_handle_preparer(pdo_dbh_t *dbh, const char *sql, size_t sql_len,
                                      pdo_stmt_t *stmt, zval *driver_options)
{
    return clickhouse_handle_preparer_impl(dbh, sql, sql_len, stmt, driver_options) ? 1 : 0;
}
#else
static bool clickhouse_handle_preparer(pdo_dbh_t *dbh, zend_string *sql, pdo_stmt_t *stmt,
                                       zval *driver_options)
{
    return clickhouse_handle_preparer_impl(dbh, ZSTR_VAL(sql), ZSTR_LEN(sql), stmt, driver_options);
}
#endif

static zend_long clickhouse_handle_doer_impl(pdo_dbh_t *dbh, const char *sql, size_t sql_len)
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    pdo_clickhouse_clear_error(dbh, nullptr);

    try {
        clickhouse::Query q(std::string(sql, sql_len));
        H->client->Execute(q);
        pdo_clickhouse_clear_error(dbh, nullptr);
        return 0; /* ClickHouse doesn't report affected rows for DDL */
    } catch (const clickhouse::ServerException &e) {
        pdo_clickhouse_error(dbh, nullptr, e.GetCode(), e.what(), "HY000");
        return -1;
    } catch (const std::exception &e) {
        pdo_clickhouse_error(dbh, nullptr, -1, e.what(), "HY000");
        return -1;
    }
}

#if PHP_VERSION_ID < 80100
static zend_long clickhouse_handle_doer(pdo_dbh_t *dbh, const char *sql, size_t sql_len)
{
    return clickhouse_handle_doer_impl(dbh, sql, sql_len);
}
#else
static zend_long clickhouse_handle_doer(pdo_dbh_t *dbh, const zend_string *sql)
{
    return clickhouse_handle_doer_impl(dbh, ZSTR_VAL(sql), ZSTR_LEN(sql));
}
#endif

static zend_string *clickhouse_quote_impl(pdo_dbh_t *dbh, const char *src, size_t src_len,
                                          enum pdo_param_type paramtype)
{
    switch (PDO_PARAM_TYPE(paramtype)) {
    case PDO_PARAM_NULL:
        return zend_string_init("NULL", sizeof("NULL") - 1, 0);

    case PDO_PARAM_BOOL: {
        bool truthy = clickhouse_param_bool_is_truthy(src, src_len);
        return zend_string_init(truthy ? "1" : "0", 1, 0);
    }

    case PDO_PARAM_INT: {
        size_t pos = 0;
        if (src_len == 0) {
            pdo_clickhouse_error(dbh, nullptr, -1, "Invalid integer parameter: empty string",
                                 "HY000");
            return nullptr;
        }
        if (src[pos] == '+' || src[pos] == '-') {
            pos++;
            if (pos == src_len) {
                pdo_clickhouse_error(dbh, nullptr, -1, "Invalid integer parameter", "HY000");
                return nullptr;
            }
        }
        for (; pos < src_len; ++pos) {
            if (src[pos] < '0' || src[pos] > '9') {
                pdo_clickhouse_error(dbh, nullptr, -1, "Invalid integer parameter", "HY000");
                return nullptr;
            }
        }
        return zend_string_init(src, src_len, 0);
    }
    }

    /* Worst case: every char is escaped + 2 quotes */
    zend_string *quoted = zend_string_alloc(src_len * 2 + 2, 0);
    char *dst = ZSTR_VAL(quoted);

    *dst++ = '\'';
    for (size_t i = 0; i < src_len; i++) {
        switch (src[i]) {
        case '\'':
            *dst++ = '\\';
            *dst++ = '\'';
            break;
        case '\\':
            *dst++ = '\\';
            *dst++ = '\\';
            break;
        case '\0':
            *dst++ = '\\';
            *dst++ = '0';
            break;
        case '\b':
            *dst++ = '\\';
            *dst++ = 'b';
            break;
        case '\f':
            *dst++ = '\\';
            *dst++ = 'f';
            break;
        case '\n':
            *dst++ = '\\';
            *dst++ = 'n';
            break;
        case '\r':
            *dst++ = '\\';
            *dst++ = 'r';
            break;
        case '\t':
            *dst++ = '\\';
            *dst++ = 't';
            break;
        default:
            *dst++ = src[i];
            break;
        }
    }
    *dst++ = '\'';
    *dst = '\0';

    ZSTR_LEN(quoted) = dst - ZSTR_VAL(quoted);
    quoted = zend_string_truncate(quoted, ZSTR_LEN(quoted), 0);

    return quoted;
}

#if PHP_VERSION_ID < 80100
static int clickhouse_handle_quoter(pdo_dbh_t *dbh, const char *unquoted, size_t unquotedlen,
                                    char **quoted, size_t *quotedlen, enum pdo_param_type paramtype)
{
    zend_string *quoted_str = clickhouse_quote_impl(dbh, unquoted, unquotedlen, paramtype);
    if (!quoted_str) {
        return 0;
    }

    *quotedlen = ZSTR_LEN(quoted_str);
    *quoted = estrndup(ZSTR_VAL(quoted_str), *quotedlen);
    zend_string_release(quoted_str);
    return 1;
}
#else
static zend_string *clickhouse_handle_quoter(pdo_dbh_t *dbh, const zend_string *unquoted,
                                             enum pdo_param_type paramtype)
{
    return clickhouse_quote_impl(dbh, ZSTR_VAL(unquoted), ZSTR_LEN(unquoted), paramtype);
}
#endif

#if PHP_VERSION_ID < 80100
static int clickhouse_handle_begin(pdo_dbh_t *dbh)
#else
static bool clickhouse_handle_begin(pdo_dbh_t *dbh)
#endif
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    H->transaction_open = true;
    pdo_clickhouse_clear_error(dbh, nullptr);
    return true;
}

#if PHP_VERSION_ID < 80100
static int clickhouse_handle_commit(pdo_dbh_t *dbh)
#else
static bool clickhouse_handle_commit(pdo_dbh_t *dbh)
#endif
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    H->transaction_open = false;
    pdo_clickhouse_clear_error(dbh, nullptr);
    return true;
}

#if PHP_VERSION_ID < 80100
static int clickhouse_handle_rollback(pdo_dbh_t *dbh)
#else
static bool clickhouse_handle_rollback(pdo_dbh_t *dbh)
#endif
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    H->transaction_open = false;
    pdo_clickhouse_clear_error(dbh, nullptr);
    return true;
}

#if PHP_VERSION_ID < 80100
static int clickhouse_handle_in_transaction(pdo_dbh_t *dbh)
#else
static bool clickhouse_handle_in_transaction(pdo_dbh_t *dbh)
#endif
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    return H && H->transaction_open;
}

#if PHP_VERSION_ID < 80100
static int clickhouse_fetch_error_func(pdo_dbh_t *dbh, pdo_stmt_t *stmt, zval *info)
#else
static void clickhouse_fetch_error_func(pdo_dbh_t *dbh, pdo_stmt_t *stmt, zval *info)
#endif
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    if (H->errcode) {
        add_next_index_long(info, static_cast<zend_long>(H->errcode));
        add_next_index_string(info, H->errmsg.c_str());
#if PHP_VERSION_ID < 80100
        return 1;
#endif
    }
#if PHP_VERSION_ID < 80100
    return 0;
#endif
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
        ZVAL_STRING(val,
                    H->ssl_enabled ? "Connected via native TCP (TLS)" : "Connected via native TCP");
        return 1;
    default:
        return 0;
    }
}

#if PHP_VERSION_ID < 80100
static int clickhouse_handle_set_attribute(pdo_dbh_t *dbh, zend_long attr, zval *val)
#else
static bool clickhouse_handle_set_attribute(pdo_dbh_t *dbh, zend_long attr, zval *val)
#endif
{
    return false; /* No settable attributes yet */
}

#if PHP_VERSION_ID < 80100
static int clickhouse_handle_check_liveness(pdo_dbh_t *dbh)
#else
static zend_result clickhouse_handle_check_liveness(pdo_dbh_t *dbh)
#endif
{
    auto *H = static_cast<pdo_clickhouse_db_handle *>(dbh->driver_data);
    try {
        H->client->Ping();
        return SUCCESS;
    } catch (...) {
        return FAILURE;
    }
}

#if PHP_VERSION_ID < 80100
static char *clickhouse_handle_last_id(pdo_dbh_t *dbh, const char *name, size_t *len)
{
    *len = 1;
    return estrndup("0", 1);
}
#else
static zend_string *clickhouse_handle_last_id(pdo_dbh_t *dbh, const zend_string *name)
{
    return zend_string_init("0", 1, 0);
}
#endif

const struct pdo_dbh_methods clickhouse_dbh_methods = {
    clickhouse_handle_closer,         /* closer */
    clickhouse_handle_preparer,       /* preparer */
    clickhouse_handle_doer,           /* doer */
    clickhouse_handle_quoter,         /* quoter */
    clickhouse_handle_begin,          /* begin */
    clickhouse_handle_commit,         /* commit */
    clickhouse_handle_rollback,       /* rollback */
    clickhouse_handle_set_attribute,  /* set_attribute */
    clickhouse_handle_last_id,        /* last_id */
    clickhouse_fetch_error_func,      /* fetch_err */
    clickhouse_handle_get_attribute,  /* get_attribute */
    clickhouse_handle_check_liveness, /* check_liveness */
    NULL,                             /* get_driver_methods */
    NULL,                             /* persistent_shutdown */
    clickhouse_handle_in_transaction, /* in_transaction */
#if PHP_VERSION_ID >= 80100
    NULL, /* get_gc */
#endif
};

static int pdo_clickhouse_handle_factory(pdo_dbh_t *dbh, zval *driver_options)
{
    struct pdo_data_src_parser parsed[] = {
        {"host", NULL, 0},        {"port", NULL, 0},    {"dbname", NULL, 0},
        {"compression", NULL, 0}, {"ssl", NULL, 0},     {"skip_verify", NULL, 0},
        {"ca_path", NULL, 0},     {"ca_file", NULL, 0}, {"client_cert", NULL, 0},
        {"client_key", NULL, 0},
    };

    php_pdo_parse_data_source(dbh->data_source, dbh->data_source_len, parsed,
                              sizeof(parsed) / sizeof(parsed[0]));

    const char *host = parsed[0].optval ? parsed[0].optval : "localhost";
    const char *port_str = parsed[1].optval;
    const char *dbname = parsed[2].optval ? parsed[2].optval : "default";
    const char *compression_str = parsed[3].optval ? parsed[3].optval : nullptr;
    const char *ssl_str = parsed[4].optval ? parsed[4].optval : nullptr;
    const char *skip_verify_str = parsed[5].optval ? parsed[5].optval : nullptr;
    const char *ca_path_str = parsed[6].optval ? parsed[6].optval : nullptr;
    const char *ca_file_str = parsed[7].optval ? parsed[7].optval : nullptr;
    const char *client_cert_str = parsed[8].optval ? parsed[8].optval : nullptr;
    const char *client_key_str = parsed[9].optval ? parsed[9].optval : nullptr;
    const char *user = dbh->username ? dbh->username : "default";
    const char *pass = dbh->password ? dbh->password : "";

    /* Allocate driver handle — respecting is_persistent */
    auto *H = static_cast<pdo_clickhouse_db_handle *>(
        pecalloc(1, sizeof(pdo_clickhouse_db_handle), dbh->is_persistent));

    /* Placement-new for C++ members */
    new (&H->client) std::unique_ptr<clickhouse::Client>();
    new (&H->options) std::unique_ptr<clickhouse::ClientOptions>();
    new (&H->errmsg) std::string();
    H->ssl_enabled = false;
    H->transaction_open = false;
    H->errcode = 0;

    dbh->driver_data = H;

    long port = 9000;
    if (port_str) {
        char *end = nullptr;
        port = strtol(port_str, &end, 10);
        if (!end || *end != '\0' || port < 1 || port > 65535) {
            pdo_clickhouse_error(dbh, nullptr, -1, "Invalid ClickHouse port in DSN", "08001");
            goto cleanup;
        }
    }

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

        /* Parse compression from DSN parameter */
        if (compression_str) {
            std::string comp(compression_str);
            std::transform(comp.begin(), comp.end(), comp.begin(), ::tolower);
            if (comp == "lz4") {
                opts->SetCompressionMethod(clickhouse::CompressionMethod::LZ4);
            } else if (comp == "zstd") {
                opts->SetCompressionMethod(clickhouse::CompressionMethod::ZSTD);
            } else if (comp == "none") {
                opts->SetCompressionMethod(clickhouse::CompressionMethod::None);
            } else {
                pdo_clickhouse_error(dbh, nullptr, -1, "Invalid ClickHouse compression in DSN",
                                     "08001");
                goto cleanup;
            }
        }

        bool ssl_enabled = false;
        if (ssl_str && !clickhouse_parse_dsn_bool(ssl_str, &ssl_enabled)) {
            pdo_clickhouse_error(dbh, nullptr, -1, "Invalid ClickHouse ssl option in DSN", "08001");
            goto cleanup;
        }

        bool skip_verify = false;
        if (skip_verify_str && !clickhouse_parse_dsn_bool(skip_verify_str, &skip_verify)) {
            pdo_clickhouse_error(dbh, nullptr, -1, "Invalid ClickHouse skip_verify option in DSN",
                                 "08001");
            goto cleanup;
        }

        /* Parse SSL/TLS options from DSN (required for ClickHouse Cloud) */
        if (ssl_enabled) {
            clickhouse::ClientOptions::SSLOptions ssl_opts;
            ssl_opts.SetUseDefaultCALocations(true);
            ssl_opts.SetUseSNI(true);

            if (skip_verify) {
                ssl_opts.SetSkipVerification(true);
            }
            if (ca_path_str) {
                ssl_opts.SetPathToCADirectory(ca_path_str);
            }
            if (ca_file_str) {
                ssl_opts.SetPathToCAFiles(std::vector<std::string>{ca_file_str});
            }
            if (client_cert_str || client_key_str) {
                std::vector<clickhouse::ClientOptions::SSLOptions::CommandAndValue> config;
                if (client_cert_str) {
                    config.push_back({"Certificate", std::string(client_cert_str)});
                }
                if (client_key_str) {
                    config.push_back({"PrivateKey", std::string(client_key_str)});
                }
                ssl_opts.SetConfiguration(config);
            }

            opts->SetSSLOptions(std::move(ssl_opts));
            H->ssl_enabled = true;
        }

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

const pdo_driver_t pdo_clickhouse_driver = {PDO_DRIVER_HEADER(clickhouse),
                                            pdo_clickhouse_handle_factory};
