/*
  +----------------------------------------------------------------------+
  | PDO ClickHouse Driver - Statement Implementation                    |
  +----------------------------------------------------------------------+
*/

#include "php.h"
#include "pdo/php_pdo.h"
#include "pdo/php_pdo_driver.h"
#include "zend_smart_str.h"
#include "php_pdo_clickhouse_int.h"
#include <string.h>
#include <time.h>
#include <stdio.h>

/* Forward declarations */
static int clickhouse_stmt_dtor(pdo_stmt_t *stmt);
static int clickhouse_stmt_execute(pdo_stmt_t *stmt);
static int clickhouse_stmt_fetch(pdo_stmt_t *stmt, enum pdo_fetch_orientation ori,
                                 zend_long offset);
static int clickhouse_stmt_describe(pdo_stmt_t *stmt, int colno);
static int clickhouse_stmt_get_col(pdo_stmt_t *stmt, int colno, zval *result,
                                   enum pdo_param_type *type);
static int clickhouse_stmt_param_hook(pdo_stmt_t *stmt, struct pdo_bound_param_data *param,
                                      enum pdo_param_event event_type);
static int clickhouse_stmt_col_meta(pdo_stmt_t *stmt, zend_long colno, zval *return_value);

/* Helper: Get current block from result */
static clickhouse_block *get_current_block(pdo_clickhouse_stmt *S)
{
    if (!S->result || S->current_block >= S->result->block_count) {
        return NULL;
    }
    return S->result->blocks[S->current_block];
}

/* Helper: Convert ClickHouse value to zval */
static void column_to_zval(clickhouse_column *col, size_t row, zval *zv)
{
    clickhouse_type_info *type = col->type;

    /* Handle Nullable */
    if (type->type_id == CH_TYPE_NULLABLE) {
        if (col->nulls && col->nulls[row]) {
            ZVAL_NULL(zv);
            return;
        }
        type = type->nested;
    }

    switch (type->type_id) {
        case CH_TYPE_INT8: {
            int8_t *data = (int8_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_INT16: {
            int16_t *data = (int16_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_INT32: {
            int32_t *data = (int32_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_INT64: {
            int64_t *data = (int64_t *)col->data;
            ZVAL_LONG(zv, (zend_long)data[row]);
            break;
        }
        case CH_TYPE_UINT8:
        case CH_TYPE_BOOL: {
            uint8_t *data = (uint8_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_UINT16: {
            uint16_t *data = (uint16_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_UINT32: {
            uint32_t *data = (uint32_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_UINT64: {
            uint64_t *data = (uint64_t *)col->data;
            /* Convert to string if too large for zend_long */
            if (data[row] > ZEND_LONG_MAX) {
                char buf[32];
                snprintf(buf, sizeof(buf), "%llu", (unsigned long long)data[row]);
                ZVAL_STRING(zv, buf);
            } else {
                ZVAL_LONG(zv, (zend_long)data[row]);
            }
            break;
        }
        case CH_TYPE_FLOAT32: {
            float *data = (float *)col->data;
            ZVAL_DOUBLE(zv, (double)data[row]);
            break;
        }
        case CH_TYPE_FLOAT64: {
            double *data = (double *)col->data;
            ZVAL_DOUBLE(zv, data[row]);
            break;
        }
        case CH_TYPE_STRING: {
            char **strings = (char **)col->data;
            if (strings[row]) {
                ZVAL_STRING(zv, strings[row]);
            } else {
                ZVAL_EMPTY_STRING(zv);
            }
            break;
        }
        case CH_TYPE_FIXED_STRING: {
            size_t fixed_len = type->fixed_size;
            char *data = (char *)col->data + (row * fixed_len);
            ZVAL_STRINGL(zv, data, fixed_len);
            break;
        }
        case CH_TYPE_DATE: {
            uint16_t *data = (uint16_t *)col->data;
            /* Convert days since epoch to date string */
            time_t ts = (time_t)data[row] * 86400;
            struct tm *tm = gmtime(&ts);
            char buf[16];
            strftime(buf, sizeof(buf), "%Y-%m-%d", tm);
            ZVAL_STRING(zv, buf);
            break;
        }
        case CH_TYPE_DATETIME: {
            uint32_t *data = (uint32_t *)col->data;
            /* Convert Unix timestamp to datetime string */
            time_t ts = (time_t)data[row];
            struct tm *tm = gmtime(&ts);
            char buf[32];
            strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
            ZVAL_STRING(zv, buf);
            break;
        }
        case CH_TYPE_UUID: {
            uint8_t *data = (uint8_t *)col->data + (row * 16);
            char uuid_str[37];
            snprintf(uuid_str, sizeof(uuid_str),
                "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                data[0], data[1], data[2], data[3],
                data[4], data[5], data[6], data[7],
                data[8], data[9], data[10], data[11],
                data[12], data[13], data[14], data[15]);
            ZVAL_STRING(zv, uuid_str);
            break;
        }
        default:
            ZVAL_NULL(zv);
            break;
    }
}

/* Destructor */
static int clickhouse_stmt_dtor(pdo_stmt_t *stmt)
{
    pdo_clickhouse_stmt *S = (pdo_clickhouse_stmt *)stmt->driver_data;

    if (S) {
        if (S->result) {
            clickhouse_result_free(S->result);
        }
        if (S->column_names) {
            for (size_t i = 0; i < S->column_count; i++) {
                efree(S->column_names[i]);
            }
            efree(S->column_names);
        }
        if (S->column_types) {
            for (size_t i = 0; i < S->column_count; i++) {
                efree(S->column_types[i]);
            }
            efree(S->column_types);
        }
        if (S->bound_params) {
            for (size_t i = 0; i < S->param_count; i++) {
                zval_ptr_dtor(&S->bound_params[i]);
            }
            efree(S->bound_params);
        }
        if (S->einfo.errmsg) {
            efree(S->einfo.errmsg);
        }
        efree(S);
        stmt->driver_data = NULL;
    }

    return 1;
}

/* Build query with parameters substituted */
static char *build_query_with_params(pdo_stmt_t *stmt)
{
    if (!stmt->bound_params || zend_hash_num_elements(stmt->bound_params) == 0) {
        return estrdup(ZSTR_VAL(stmt->query_string));
    }

    /* Simple parameter substitution */
    smart_str result = {0};
    const char *sql = ZSTR_VAL(stmt->query_string);
    size_t sql_len = ZSTR_LEN(stmt->query_string);
    size_t i = 0;
    size_t param_idx = 0;

    while (i < sql_len) {
        if (sql[i] == '?') {
            /* Positional parameter */
            zval *param;
            zend_string *key = NULL;
            zend_ulong num_key;

            ZEND_HASH_FOREACH_KEY_VAL(stmt->bound_params, num_key, key, param) {
                struct pdo_bound_param_data *p = (struct pdo_bound_param_data *)Z_PTR_P(param);
                if (!key && num_key == param_idx) {
                    zval *value = &p->parameter;
                    switch (Z_TYPE_P(value)) {
                        case IS_NULL:
                            smart_str_appends(&result, "NULL");
                            break;
                        case IS_LONG:
                            smart_str_append_long(&result, Z_LVAL_P(value));
                            break;
                        case IS_DOUBLE: {
                            char buf[64];
                            snprintf(buf, sizeof(buf), "%g", Z_DVAL_P(value));
                            smart_str_appends(&result, buf);
                            break;
                        }
                        case IS_TRUE:
                            smart_str_appends(&result, "1");
                            break;
                        case IS_FALSE:
                            smart_str_appends(&result, "0");
                            break;
                        default: {
                            zend_string *str = zval_get_string(value);
                            smart_str_appendc(&result, '\'');
                            /* Escape string */
                            for (size_t j = 0; j < ZSTR_LEN(str); j++) {
                                char c = ZSTR_VAL(str)[j];
                                if (c == '\'' || c == '\\') {
                                    smart_str_appendc(&result, '\\');
                                }
                                smart_str_appendc(&result, c);
                            }
                            smart_str_appendc(&result, '\'');
                            zend_string_release(str);
                            break;
                        }
                    }
                    break;
                }
            } ZEND_HASH_FOREACH_END();
            param_idx++;
            i++;
        } else {
            smart_str_appendc(&result, sql[i]);
            i++;
        }
    }

    smart_str_0(&result);
    char *query = estrdup(ZSTR_VAL(result.s));
    smart_str_free(&result);
    return query;
}

/* Execute statement */
static int clickhouse_stmt_execute(pdo_stmt_t *stmt)
{
    pdo_clickhouse_stmt *S = (pdo_clickhouse_stmt *)stmt->driver_data;

    /* Free previous result */
    if (S->result) {
        clickhouse_result_free(S->result);
        S->result = NULL;
    }
    S->current_block = 0;
    S->current_row = 0;
    S->done = 0;

    /* Build query with parameters */
    char *query = build_query_with_params(stmt);

    /* Execute query */
    int status = clickhouse_connection_execute_query(S->H->conn, query, &S->result);
    efree(query);

    if (status != 0) {
        const char *err = clickhouse_connection_get_error(S->H->conn);
        pdo_clickhouse_error(stmt->dbh, stmt, __FILE__, __LINE__,
            "HY000", 1, err ? err : "Query execution failed");
        return 0;
    }

    /* Setup column info from result */
    if (S->result && S->result->block_count > 0) {
        clickhouse_block *first_block = S->result->blocks[0];
        S->column_count = first_block->column_count;
        stmt->column_count = S->column_count;

        /* Free old column names/types */
        if (S->column_names) {
            for (size_t i = 0; i < S->column_count; i++) {
                efree(S->column_names[i]);
            }
            efree(S->column_names);
        }
        if (S->column_types) {
            for (size_t i = 0; i < S->column_count; i++) {
                efree(S->column_types[i]);
            }
            efree(S->column_types);
        }

        S->column_names = ecalloc(S->column_count, sizeof(char *));
        S->column_types = ecalloc(S->column_count, sizeof(char *));

        for (size_t i = 0; i < S->column_count; i++) {
            S->column_names[i] = estrdup(first_block->columns[i]->name);
            S->column_types[i] = estrdup(first_block->columns[i]->type->type_name);
        }
    } else {
        S->column_count = 0;
        stmt->column_count = 0;
    }

    return 1;
}

/* Fetch next row */
static int clickhouse_stmt_fetch(pdo_stmt_t *stmt, enum pdo_fetch_orientation ori,
                                 zend_long offset)
{
    pdo_clickhouse_stmt *S = (pdo_clickhouse_stmt *)stmt->driver_data;

    if (S->done || !S->result) {
        return 0;
    }

    clickhouse_block *block = get_current_block(S);
    if (!block) {
        S->done = 1;
        return 0;
    }

    /* Check if we need to move to next block */
    if (S->current_row >= block->row_count) {
        S->current_block++;
        S->current_row = 0;
        block = get_current_block(S);
        if (!block) {
            S->done = 1;
            return 0;
        }
    }

    /* Move to next row (current_row is the row we're about to read) */
    S->current_row++;
    return 1;
}

/* Describe column */
static int clickhouse_stmt_describe(pdo_stmt_t *stmt, int colno)
{
    pdo_clickhouse_stmt *S = (pdo_clickhouse_stmt *)stmt->driver_data;

    if (colno < 0 || (size_t)colno >= S->column_count) {
        return 0;
    }

    struct pdo_column_data *col = &stmt->columns[colno];
    col->name = zend_string_init(S->column_names[colno], strlen(S->column_names[colno]), 0);
    col->maxlen = 0;
    col->precision = 0;

    return 1;
}

/* Get column value */
static int clickhouse_stmt_get_col(pdo_stmt_t *stmt, int colno, zval *result,
                                   enum pdo_param_type *type)
{
    pdo_clickhouse_stmt *S = (pdo_clickhouse_stmt *)stmt->driver_data;

    if (colno < 0 || (size_t)colno >= S->column_count) {
        return 0;
    }

    clickhouse_block *block = get_current_block(S);
    if (!block || S->current_row == 0) {
        return 0;
    }

    /* current_row was incremented after fetch, so actual row is current_row - 1 */
    size_t row = S->current_row - 1;

    if ((size_t)colno >= block->column_count || row >= block->row_count) {
        return 0;
    }

    clickhouse_column *col = block->columns[colno];
    column_to_zval(col, row, result);

    return 1;
}

/* Parameter binding hook */
static int clickhouse_stmt_param_hook(pdo_stmt_t *stmt, struct pdo_bound_param_data *param,
                                      enum pdo_param_event event_type)
{
    /* ClickHouse uses emulated prepared statements */
    switch (event_type) {
        case PDO_PARAM_EVT_ALLOC:
        case PDO_PARAM_EVT_FREE:
        case PDO_PARAM_EVT_EXEC_PRE:
        case PDO_PARAM_EVT_EXEC_POST:
        case PDO_PARAM_EVT_FETCH_PRE:
        case PDO_PARAM_EVT_FETCH_POST:
        case PDO_PARAM_EVT_NORMALIZE:
            break;
    }
    return 1;
}

/* Get column metadata */
static int clickhouse_stmt_col_meta(pdo_stmt_t *stmt, zend_long colno, zval *return_value)
{
    pdo_clickhouse_stmt *S = (pdo_clickhouse_stmt *)stmt->driver_data;

    if (colno < 0 || (size_t)colno >= S->column_count) {
        return FAILURE;
    }

    array_init(return_value);

    add_assoc_string(return_value, "native_type", S->column_types[colno]);
    add_assoc_string(return_value, "driver:decl_type", S->column_types[colno]);

    /* Determine PDO type */
    const char *type = S->column_types[colno];
    zend_long pdo_type = PDO_PARAM_STR;

    if (strstr(type, "Int") || strstr(type, "UInt")) {
        pdo_type = PDO_PARAM_INT;
    } else if (strstr(type, "Float") || strstr(type, "Decimal")) {
        pdo_type = PDO_PARAM_STR; /* Use string for precision */
    } else if (strstr(type, "Bool")) {
        pdo_type = PDO_PARAM_BOOL;
    }

    add_assoc_long(return_value, "pdo_type", pdo_type);

    return SUCCESS;
}

/* Statement methods structure */
const struct pdo_stmt_methods clickhouse_stmt_methods = {
    clickhouse_stmt_dtor,
    clickhouse_stmt_execute,
    clickhouse_stmt_fetch,
    clickhouse_stmt_describe,
    clickhouse_stmt_get_col,
    clickhouse_stmt_param_hook,
    NULL, /* set_attr */
    NULL, /* get_attr */
    clickhouse_stmt_col_meta,
    NULL, /* next_rowset */
    NULL  /* cursor_closer */
};
