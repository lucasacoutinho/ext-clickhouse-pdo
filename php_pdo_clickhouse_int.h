/*
  +----------------------------------------------------------------------+
  | PDO ClickHouse Driver - Internal Definitions                        |
  +----------------------------------------------------------------------+
*/

#ifndef PHP_PDO_CLICKHOUSE_INT_H
#define PHP_PDO_CLICKHOUSE_INT_H

#include "php.h"
#include "pdo/php_pdo_driver.h"

/* Include the native clickhouse extension headers */
#include "../clickhouse/src/buffer.h"
#include "../clickhouse/src/protocol.h"
#include "../clickhouse/src/connection.h"
#include "../clickhouse/src/column.h"

/* ClickHouse-specific PDO attributes */
enum {
    PDO_CLICKHOUSE_ATTR_COMPRESSION = PDO_ATTR_DRIVER_SPECIFIC,
    PDO_CLICKHOUSE_ATTR_TIMEOUT,
    PDO_CLICKHOUSE_ATTR_MAX_BLOCK_SIZE,
};

/* Error information */
typedef struct {
    const char *file;
    int line;
    int errcode;
    char *errmsg;
    char sqlstate[6];
} pdo_clickhouse_error_info;

/* Query setting entry */
typedef struct {
    char *name;
    char *value;
} pdo_clickhouse_query_setting;

/* Connection handle */
typedef struct {
    clickhouse_connection *conn;

    /* Connection settings */
    unsigned int connect_timeout;
    unsigned int read_timeout;
    unsigned int max_block_size;
    unsigned int compression:1;

    /* Transaction state (ClickHouse has limited support) */
    unsigned int in_transaction:1;

    /* Query settings */
    pdo_clickhouse_query_setting *query_settings;
    size_t query_settings_count;
    size_t query_settings_capacity;

    /* Callbacks */
    zval progress_callback;
    zval profile_callback;
    unsigned int has_progress_callback:1;
    unsigned int has_profile_callback:1;

    /* Error info */
    pdo_clickhouse_error_info einfo;
} pdo_clickhouse_db_handle;

/* Statement handle */
typedef struct {
    pdo_clickhouse_db_handle *H;

    /* Query result */
    clickhouse_result *result;

    /* Current block being read */
    size_t current_block;
    size_t current_row;

    /* Column metadata */
    size_t column_count;
    char **column_names;
    char **column_types;

    /* Bound parameters */
    size_t param_count;
    zval *bound_params;

    /* Error info */
    pdo_clickhouse_error_info einfo;

    /* State flags */
    unsigned int done:1;
} pdo_clickhouse_stmt;

/* Driver methods */
extern const pdo_driver_t pdo_clickhouse_driver;
extern const struct pdo_dbh_methods clickhouse_methods;
extern const struct pdo_stmt_methods clickhouse_stmt_methods;

/* Driver-specific methods */
const zend_function_entry *clickhouse_get_driver_methods(pdo_dbh_t *dbh, int kind);

/* Error handling */
int pdo_clickhouse_error(pdo_dbh_t *dbh, pdo_stmt_t *stmt,
                         const char *file, int line,
                         const char *sqlstate, int errcode,
                         const char *message);

/* Statement functions */
pdo_stmt_t *pdo_clickhouse_create_statement(pdo_dbh_t *dbh);

/* Helper functions */
clickhouse_block *build_block_from_php_arrays(zval *columns_array, zval *rows_array);
void free_php_built_block(clickhouse_block *block);

#endif /* PHP_PDO_CLICKHOUSE_INT_H */
