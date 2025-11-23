/*
  +----------------------------------------------------------------------+
  | PDO ClickHouse Driver - Helper Functions                            |
  +----------------------------------------------------------------------+
*/

#include "php.h"
#include "php_pdo_clickhouse_int.h"
#include "zend_exceptions.h"
#include <string.h>

/* Helper: Build clickhouse_block from PHP arrays */
clickhouse_block *build_block_from_php_arrays(zval *columns_array, zval *rows_array)
{
    HashTable *columns_ht = Z_ARRVAL_P(columns_array);
    HashTable *rows_ht = Z_ARRVAL_P(rows_array);

    size_t column_count = zend_hash_num_elements(columns_ht);
    size_t row_count = zend_hash_num_elements(rows_ht);

    if (column_count == 0 || row_count == 0) {
        return NULL;
    }

    /* Create block */
    clickhouse_block *block = (clickhouse_block *)emalloc(sizeof(clickhouse_block));
    block->column_count = column_count;
    block->row_count = row_count;
    block->columns = (clickhouse_column **)ecalloc(column_count, sizeof(clickhouse_column *));

    /* Extract column names */
    char **column_names = (char **)ecalloc(column_count, sizeof(char *));
    size_t col_idx = 0;
    zval *col_name;

    ZEND_HASH_FOREACH_VAL(columns_ht, col_name) {
        convert_to_string(col_name);
        column_names[col_idx] = estrdup(Z_STRVAL_P(col_name));
        col_idx++;
    } ZEND_HASH_FOREACH_END();

    /* Analyze first row to determine types */
    zval *first_row = zend_hash_index_find(rows_ht, 0);
    if (!first_row || Z_TYPE_P(first_row) != IS_ARRAY) {
        /* Cleanup */
        for (size_t i = 0; i < column_count; i++) {
            efree(column_names[i]);
        }
        efree(column_names);
        efree(block->columns);
        efree(block);
        return NULL;
    }

    HashTable *first_row_ht = Z_ARRVAL_P(first_row);
    if (zend_hash_num_elements(first_row_ht) != column_count) {
        /* Row column count mismatch */
        for (size_t i = 0; i < column_count; i++) {
            efree(column_names[i]);
        }
        efree(column_names);
        efree(block->columns);
        efree(block);
        return NULL;
    }

    /* Create columns based on PHP types */
    col_idx = 0;
    zval *cell;
    ZEND_HASH_FOREACH_VAL(first_row_ht, cell) {
        clickhouse_column *col = (clickhouse_column *)ecalloc(1, sizeof(clickhouse_column));
        col->name = column_names[col_idx];
        col->row_count = row_count;

        /* Determine type from PHP value */
        clickhouse_type_info *type = (clickhouse_type_info *)ecalloc(1, sizeof(clickhouse_type_info));

        switch (Z_TYPE_P(cell)) {
            case IS_NULL:
                /* Nullable String by default */
                type->type_id = CH_TYPE_NULLABLE;
                type->type_name = estrdup("Nullable(String)");
                type->nested = (clickhouse_type_info *)ecalloc(1, sizeof(clickhouse_type_info));
                type->nested->type_id = CH_TYPE_STRING;
                type->nested->type_name = estrdup("String");
                break;

            case IS_LONG:
                type->type_id = CH_TYPE_INT64;
                type->type_name = estrdup("Int64");
                break;

            case IS_DOUBLE:
                type->type_id = CH_TYPE_FLOAT64;
                type->type_name = estrdup("Float64");
                break;

            case IS_TRUE:
            case IS_FALSE:
                type->type_id = CH_TYPE_BOOL;
                type->type_name = estrdup("Bool");
                break;

            case IS_STRING:
            default:
                type->type_id = CH_TYPE_STRING;
                type->type_name = estrdup("String");
                break;
        }

        col->type = type;

        /* Allocate data array based on type */
        if (type->type_id == CH_TYPE_STRING ||
            (type->type_id == CH_TYPE_NULLABLE && type->nested->type_id == CH_TYPE_STRING)) {
            col->data = ecalloc(row_count, sizeof(char *));
            if (type->type_id == CH_TYPE_NULLABLE) {
                col->nulls = ecalloc(row_count, sizeof(uint8_t));
            }
        } else if (type->type_id == CH_TYPE_INT64) {
            col->data = ecalloc(row_count, sizeof(int64_t));
        } else if (type->type_id == CH_TYPE_FLOAT64) {
            col->data = ecalloc(row_count, sizeof(double));
        } else if (type->type_id == CH_TYPE_BOOL) {
            col->data = ecalloc(row_count, sizeof(uint8_t));
        }

        block->columns[col_idx] = col;
        col_idx++;
    } ZEND_HASH_FOREACH_END();

    /* Fill data from all rows */
    size_t row_idx = 0;
    zval *row;

    ZEND_HASH_FOREACH_VAL(rows_ht, row) {
        if (Z_TYPE_P(row) != IS_ARRAY) {
            continue;
        }

        HashTable *row_ht = Z_ARRVAL_P(row);
        col_idx = 0;

        ZEND_HASH_FOREACH_VAL(row_ht, cell) {
            if (col_idx >= column_count) break;

            clickhouse_column *col = block->columns[col_idx];
            clickhouse_type_info *type = col->type;

            if (type->type_id == CH_TYPE_NULLABLE && Z_TYPE_P(cell) == IS_NULL) {
                col->nulls[row_idx] = 1;
            } else if (type->type_id == CH_TYPE_STRING ||
                       (type->type_id == CH_TYPE_NULLABLE && type->nested->type_id == CH_TYPE_STRING)) {
                convert_to_string(cell);
                char **strings = (char **)col->data;
                strings[row_idx] = estrdup(Z_STRVAL_P(cell));
                if (type->type_id == CH_TYPE_NULLABLE) {
                    col->nulls[row_idx] = 0;
                }
            } else if (type->type_id == CH_TYPE_INT64) {
                convert_to_long(cell);
                int64_t *ints = (int64_t *)col->data;
                ints[row_idx] = Z_LVAL_P(cell);
            } else if (type->type_id == CH_TYPE_FLOAT64) {
                convert_to_double(cell);
                double *floats = (double *)col->data;
                floats[row_idx] = Z_DVAL_P(cell);
            } else if (type->type_id == CH_TYPE_BOOL) {
                uint8_t *bools = (uint8_t *)col->data;
                bools[row_idx] = zval_is_true(cell) ? 1 : 0;
            }

            col_idx++;
        } ZEND_HASH_FOREACH_END();

        row_idx++;
    } ZEND_HASH_FOREACH_END();

    efree(column_names);
    return block;
}

/* Helper: Free clickhouse_block built from PHP arrays */
void free_php_built_block(clickhouse_block *block)
{
    if (!block) return;

    for (size_t i = 0; i < block->column_count; i++) {
        clickhouse_column *col = block->columns[i];
        if (!col) continue;

        if (col->name) {
            efree(col->name);
        }

        if (col->type) {
            if (col->type->type_name) efree(col->type->type_name);
            if (col->type->nested) {
                if (col->type->nested->type_name) efree(col->type->nested->type_name);
                efree(col->type->nested);
            }
            efree(col->type);
        }

        if (col->data) {
            if (col->type && (col->type->type_id == CH_TYPE_STRING ||
                             (col->type->type_id == CH_TYPE_NULLABLE &&
                              col->type->nested && col->type->nested->type_id == CH_TYPE_STRING))) {
                char **strings = (char **)col->data;
                for (size_t r = 0; r < col->row_count; r++) {
                    if (strings[r]) {
                        efree(strings[r]);
                    }
                }
            }
            efree(col->data);
        }

        if (col->nulls) {
            efree(col->nulls);
        }

        efree(col);
    }

    efree(block->columns);
    efree(block);
}
