#ifndef PHP_PDO_CLICKHOUSE_H
#define PHP_PDO_CLICKHOUSE_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

extern "C" {
#include "php.h"
#include "ext/pdo/php_pdo.h"
#include "ext/pdo/php_pdo_driver.h"
}

#define PHP_PDO_CLICKHOUSE_VERSION "1.0.1"

#if PHP_VERSION_ID < 80000
typedef int zend_result;
#endif

#ifndef PDO_PARAM_TYPE
#define PDO_PARAM_TYPE(param_type) ((param_type) & ~PDO_PARAM_INPUT_OUTPUT)
#endif

extern const pdo_driver_t pdo_clickhouse_driver;

#include "clickhouse/client.h"
#include "clickhouse/block.h"

#include <memory>
#include <string>
#include <vector>

/* Per-connection driver data — stored in pdo_dbh_t->driver_data */
struct pdo_clickhouse_db_handle
{
    std::unique_ptr<clickhouse::Client> client;
    std::unique_ptr<clickhouse::ClientOptions> options;

    /* Connection state */
    bool ssl_enabled;
    bool transaction_open;

    /* Error state */
    int errcode;
    std::string errmsg;
};

/* Per-statement driver data — stored in pdo_stmt_t->driver_data */
struct pdo_clickhouse_stmt
{
    pdo_clickhouse_db_handle *H; /* back-reference to connection */

    std::string query;           /* original SQL */
    std::string rewritten_query; /* after placeholder rewriting */

    /* SELECT results: all blocks collected into rows */
    std::vector<clickhouse::Block> blocks;
    std::vector<std::string> col_names;
    std::vector<std::string> col_type_names;
    size_t total_rows;
    size_t current_row;

    /* Starting flat row index for each buffered block. */
    std::vector<size_t> block_row_offsets;

    bool executed;
    zend_long affected_rows;
};

/* Method tables */
extern const struct pdo_dbh_methods clickhouse_dbh_methods;
extern const struct pdo_stmt_methods clickhouse_stmt_methods;

/* Error helper */
void pdo_clickhouse_error(pdo_dbh_t *dbh, pdo_stmt_t *stmt, int errcode, const char *errmsg,
                          const char *sqlstate);
void pdo_clickhouse_clear_error(pdo_dbh_t *dbh, pdo_stmt_t *stmt);

#endif /* PHP_PDO_CLICKHOUSE_H */
