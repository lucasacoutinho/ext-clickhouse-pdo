#include "php_pdo_clickhouse.h"

/* ext-clickhouse type conversion functions */
#include "src/column_convert.h"

#include <new>
#include <string>

static int clickhouse_stmt_dtor(pdo_stmt_t *stmt)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);
    if (S) {
        S->query.~basic_string();
        S->rewritten_query.~basic_string();
        S->blocks.~vector();
        S->col_names.~vector();
        S->col_type_names.~vector();
        S->row_map.~vector();
        efree(S);
        stmt->driver_data = nullptr;
    }
    return 1;
}

static int clickhouse_stmt_execute(pdo_stmt_t *stmt)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);
    auto *H = S->H;

    /* Clear previous results */
    S->blocks.clear();
    S->col_names.clear();
    S->col_type_names.clear();
    S->row_map.clear();
    S->total_rows = 0;
    S->current_row = 0;
    S->affected_rows = 0;
    S->executed = true;

    /* Use the active query string (PDO may have rewritten placeholders) */
    const char *sql = stmt->active_query_string
        ? ZSTR_VAL(stmt->active_query_string)
        : ZSTR_VAL(stmt->query_string);
    size_t sql_len = stmt->active_query_string
        ? ZSTR_LEN(stmt->active_query_string)
        : ZSTR_LEN(stmt->query_string);

    std::string query_str(sql, sql_len);

    /* Detect SELECT-like queries */
    std::string upper = query_str;
    for (auto &c : upper) c = toupper(c);
    size_t start = upper.find_first_not_of(" \t\n\r");
    /* Skip leading parentheses for union queries: (SELECT ...) UNION ALL ... */
    size_t check = start;
    if (check != std::string::npos) {
        while (check < upper.size() && upper[check] == '(') check++;
        while (check < upper.size() && (upper[check] == ' ' || upper[check] == '\t')) check++;
    }
    bool is_select = (check != std::string::npos && check < upper.size()) &&
        (upper.compare(check, 6, "SELECT") == 0 ||
         upper.compare(check, 4, "SHOW") == 0 ||
         upper.compare(check, 8, "DESCRIBE") == 0 ||
         upper.compare(check, 7, "EXPLAIN") == 0 ||
         upper.compare(check, 6, "EXISTS") == 0 ||
         upper.compare(check, 4, "WITH") == 0);

    try {
        if (is_select) {
            /* SELECT: buffer all blocks */
            bool first_block = true;

            clickhouse::Query q(query_str);
            q.OnData([&](const clickhouse::Block &block) {
                if (block.GetRowCount() == 0) return;

                if (first_block) {
                    first_block = false;
                    for (size_t c = 0; c < block.GetColumnCount(); ++c) {
                        S->col_names.push_back(block.GetColumnName(c));
                        S->col_type_names.push_back(block[c]->Type()->GetName());
                    }
                    php_pdo_stmt_set_column_count(stmt,
                        static_cast<int>(block.GetColumnCount()));
                }

                size_t block_idx = S->blocks.size();
                size_t rows = block.GetRowCount();
                for (size_t r = 0; r < rows; ++r) {
                    S->row_map.emplace_back(block_idx, r);
                }
                S->total_rows += rows;

                clickhouse::Block stored;
                for (size_t c = 0; c < block.GetColumnCount(); ++c) {
                    stored.AppendColumn(block.GetColumnName(c), block[c]);
                }
                S->blocks.push_back(std::move(stored));
            });

            H->client->Execute(q);

            if (first_block) {
                php_pdo_stmt_set_column_count(stmt, 0);
            }
            stmt->row_count = static_cast<zend_long>(S->total_rows);

        } else {
            /* Non-SELECT: execute as text */
            clickhouse::Query q(query_str);
            q.OnProgress([&](const clickhouse::Progress &p) {
                S->affected_rows = static_cast<zend_long>(p.written_rows);
            });
            H->client->Execute(q);

            php_pdo_stmt_set_column_count(stmt, 0);
            stmt->row_count = S->affected_rows;
        }

        return 1;

    } catch (const clickhouse::ServerException &e) {
        pdo_clickhouse_error(stmt->dbh, stmt, e.GetCode(), e.what(), "HY000");
        return 0;
    } catch (const std::exception &e) {
        pdo_clickhouse_error(stmt->dbh, stmt, -1, e.what(), "HY000");
        return 0;
    }
}

static int clickhouse_stmt_fetch(pdo_stmt_t *stmt,
                                 enum pdo_fetch_orientation ori, zend_long offset)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);

    if (ori != PDO_FETCH_ORI_NEXT) return 0;
    if (S->current_row >= S->total_rows) return 0;

    S->current_row++;
    return 1;
}

static int clickhouse_stmt_describe(pdo_stmt_t *stmt, int colno)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);

    if (colno < 0 || static_cast<size_t>(colno) >= S->col_names.size()) return 0;

    stmt->columns[colno].name = zend_string_init(
        S->col_names[colno].c_str(), S->col_names[colno].size(), 0);
    stmt->columns[colno].maxlen = SIZE_MAX;
    stmt->columns[colno].precision = 0;

    return 1;
}

static int clickhouse_stmt_get_col(pdo_stmt_t *stmt, int colno,
                                   zval *result, enum pdo_param_type *type)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);

    size_t row_idx = S->current_row - 1;
    if (row_idx >= S->total_rows || colno < 0) return 0;

    auto &[block_idx, block_row] = S->row_map[row_idx];
    const auto &block = S->blocks[block_idx];

    if (static_cast<size_t>(colno) >= block.GetColumnCount()) return 0;

    php_clickhouse_column_to_zval(block[static_cast<size_t>(colno)], block_row, result);
    return 1;
}

static int clickhouse_stmt_param_hook(pdo_stmt_t *stmt,
                                      struct pdo_bound_param_data *param,
                                      enum pdo_param_event event_type)
{
    return 1;
}

static int clickhouse_stmt_get_column_meta(pdo_stmt_t *stmt, zend_long colno, zval *return_value)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);

    if (colno < 0 || static_cast<size_t>(colno) >= S->col_type_names.size()) return FAILURE;

    array_init(return_value);
    add_assoc_string(return_value, "native_type",
        const_cast<char *>(S->col_type_names[colno].c_str()));
    add_assoc_string(return_value, "driver:decl_type",
        const_cast<char *>(S->col_type_names[colno].c_str()));
    add_assoc_string(return_value, "name",
        const_cast<char *>(S->col_names[colno].c_str()));

    return SUCCESS;
}

static int clickhouse_stmt_cursor_closer(pdo_stmt_t *stmt)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);
    S->blocks.clear();
    S->row_map.clear();
    S->col_names.clear();
    S->col_type_names.clear();
    S->total_rows = 0;
    S->current_row = 0;
    return 1;
}

const struct pdo_stmt_methods clickhouse_stmt_methods = {
    clickhouse_stmt_dtor,
    clickhouse_stmt_execute,
    clickhouse_stmt_fetch,
    clickhouse_stmt_describe,
    clickhouse_stmt_get_col,
    clickhouse_stmt_param_hook,
    NULL,                              /* set_attribute */
    NULL,                              /* get_attribute */
    clickhouse_stmt_get_column_meta,
    NULL,                              /* next_rowset */
    clickhouse_stmt_cursor_closer,
};
