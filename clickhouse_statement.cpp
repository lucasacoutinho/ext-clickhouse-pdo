#include "php_pdo_clickhouse.h"

/* ext-clickhouse type conversion functions */
#include "src/column_convert.h"

#include <new>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <string>

struct clickhouse_bound_param_state
{
    enum pdo_param_type original_type;
    zval original_parameter;
    bool has_original;
    bool has_original_parameter;
};

static clickhouse_bound_param_state *clickhouse_param_state(struct pdo_bound_param_data *param)
{
    auto *state = static_cast<clickhouse_bound_param_state *>(param->driver_data);
    if (!state) {
        state = static_cast<clickhouse_bound_param_state *>(
            ecalloc(1, sizeof(clickhouse_bound_param_state)));
        param->driver_data = state;
    }
    return state;
}

static enum pdo_param_type clickhouse_param_type_with_base(enum pdo_param_type original,
                                                           enum pdo_param_type base)
{
    return static_cast<enum pdo_param_type>((original & PDO_PARAM_INPUT_OUTPUT) | base);
}

static std::string clickhouse_normalize_param_string(zval *value)
{
    std::string normalized(Z_STRVAL_P(value), Z_STRLEN_P(value));
    size_t begin = 0;
    size_t end = normalized.size();

    while (begin < end && std::isspace(static_cast<unsigned char>(normalized[begin]))) {
        begin++;
    }
    while (end > begin && std::isspace(static_cast<unsigned char>(normalized[end - 1]))) {
        end--;
    }

    normalized = normalized.substr(begin, end - begin);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return normalized;
}

static bool clickhouse_zero_number_param_string(const std::string &value)
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

static bool clickhouse_bool_param_string_truthy(zval *value)
{
    std::string normalized = clickhouse_normalize_param_string(value);
    return !(normalized.empty() || normalized == "false" || normalized == "no" ||
             normalized == "off" || clickhouse_zero_number_param_string(normalized));
}

static void clickhouse_restore_param_state(struct pdo_bound_param_data *param,
                                           clickhouse_bound_param_state *state)
{
    if (!state) {
        return;
    }
    if (state->has_original) {
        param->param_type = state->original_type;
        state->has_original = false;
    }
    if (state->has_original_parameter) {
        zval_ptr_dtor(&param->parameter);
        ZVAL_COPY(&param->parameter, &state->original_parameter);
        zval_ptr_dtor(&state->original_parameter);
        state->has_original_parameter = false;
    }
}

static void clickhouse_stmt_set_column_count(pdo_stmt_t *stmt, int column_count)
{
#if PHP_VERSION_ID < 80000
    stmt->column_count = column_count;
#else
    php_pdo_stmt_set_column_count(stmt, column_count);
#endif
}

static int clickhouse_stmt_dtor(pdo_stmt_t *stmt)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);
    if (S) {
        S->query.~basic_string();
        S->rewritten_query.~basic_string();
        S->blocks.~vector();
        S->col_names.~vector();
        S->col_type_names.~vector();
        S->block_row_offsets.~vector();
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
    S->block_row_offsets.clear();
    S->total_rows = 0;
    S->current_row = 0;
    S->affected_rows = 0;
    S->executed = true;
    pdo_clickhouse_clear_error(stmt->dbh, stmt);

    /* Use the active query string (PDO may have rewritten placeholders) */
#if PHP_VERSION_ID < 80100
    const char *sql = stmt->active_query_string ? stmt->active_query_string : stmt->query_string;
    size_t sql_len =
        stmt->active_query_string ? stmt->active_query_stringlen : stmt->query_stringlen;
#else
    const char *sql = stmt->active_query_string ? ZSTR_VAL(stmt->active_query_string)
                                                : ZSTR_VAL(stmt->query_string);
    size_t sql_len = stmt->active_query_string ? ZSTR_LEN(stmt->active_query_string)
                                               : ZSTR_LEN(stmt->query_string);
#endif

    std::string query_str(sql, sql_len);

    /* Detect SELECT-like queries */
    std::string upper = query_str;
    for (auto &c : upper)
        c = toupper(c);
    size_t start = upper.find_first_not_of(" \t\n\r");
    /* Skip leading parentheses for union queries: (SELECT ...) UNION ALL ... */
    size_t check = start;
    if (check != std::string::npos) {
        while (check < upper.size() && upper[check] == '(')
            check++;
        while (check < upper.size() && (upper[check] == ' ' || upper[check] == '\t'))
            check++;
    }
    bool is_select =
        (check != std::string::npos && check < upper.size()) &&
        (upper.compare(check, 6, "SELECT") == 0 || upper.compare(check, 4, "SHOW") == 0 ||
         upper.compare(check, 8, "DESCRIBE") == 0 || upper.compare(check, 7, "EXPLAIN") == 0 ||
         upper.compare(check, 6, "EXISTS") == 0 || upper.compare(check, 4, "WITH") == 0);

    try {
        if (is_select) {
            /* SELECT: buffer all blocks */
            bool first_block = true;

            clickhouse::Query q(query_str);
            q.OnData([&](const clickhouse::Block &block) {
                if (block.GetRowCount() == 0)
                    return;

                if (first_block) {
                    first_block = false;
                    for (size_t c = 0; c < block.GetColumnCount(); ++c) {
                        S->col_names.push_back(block.GetColumnName(c));
                        S->col_type_names.push_back(block[c]->Type()->GetName());
                    }
                    clickhouse_stmt_set_column_count(stmt,
                                                     static_cast<int>(block.GetColumnCount()));
                }

                size_t block_idx = S->blocks.size();
                size_t rows = block.GetRowCount();
                S->block_row_offsets.push_back(S->total_rows);
                S->total_rows += rows;

                clickhouse::Block stored;
                for (size_t c = 0; c < block.GetColumnCount(); ++c) {
                    stored.AppendColumn(block.GetColumnName(c), block[c]);
                }
                S->blocks.push_back(std::move(stored));
            });

            H->client->Execute(q);

            if (first_block) {
                clickhouse_stmt_set_column_count(stmt, 0);
            }
            stmt->row_count = static_cast<zend_long>(S->total_rows);
            pdo_clickhouse_clear_error(stmt->dbh, stmt);

        } else {
            /* Non-SELECT: execute as text */
            clickhouse::Query q(query_str);
            q.OnProgress([&](const clickhouse::Progress &p) {
                S->affected_rows = static_cast<zend_long>(p.written_rows);
            });
            H->client->Execute(q);

            clickhouse_stmt_set_column_count(stmt, 0);
            stmt->row_count = S->affected_rows;
            pdo_clickhouse_clear_error(stmt->dbh, stmt);
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

static int clickhouse_stmt_fetch(pdo_stmt_t *stmt, enum pdo_fetch_orientation ori, zend_long offset)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);

    if (ori != PDO_FETCH_ORI_NEXT)
        return 0;
    if (S->current_row >= S->total_rows)
        return 0;

    S->current_row++;
    return 1;
}

static int clickhouse_stmt_describe(pdo_stmt_t *stmt, int colno)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);

    if (colno < 0 || static_cast<size_t>(colno) >= S->col_names.size())
        return 0;

    stmt->columns[colno].name =
        zend_string_init(S->col_names[colno].c_str(), S->col_names[colno].size(), 0);
    stmt->columns[colno].maxlen = SIZE_MAX;
    stmt->columns[colno].precision = 0;
#if PHP_VERSION_ID < 80100
    stmt->columns[colno].param_type = PDO_PARAM_STR;
#endif

    return 1;
}

static int clickhouse_stmt_read_col_zval(pdo_stmt_t *stmt, int colno, zval *result)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);

    size_t row_idx = S->current_row - 1;
    if (row_idx >= S->total_rows || colno < 0)
        return 0;

    auto it = std::upper_bound(S->block_row_offsets.begin(), S->block_row_offsets.end(), row_idx);
    if (it == S->block_row_offsets.begin())
        return 0;

    size_t block_idx = static_cast<size_t>(std::distance(S->block_row_offsets.begin(), it - 1));
    size_t block_row = row_idx - S->block_row_offsets[block_idx];
    const auto &block = S->blocks[block_idx];

    if (static_cast<size_t>(colno) >= block.GetColumnCount())
        return 0;

    php_clickhouse_column_to_zval(block[static_cast<size_t>(colno)], block_row, result);
    return 1;
}

#if PHP_VERSION_ID < 80100
static int clickhouse_stmt_get_col(pdo_stmt_t *stmt, int colno, char **ptr, size_t *len,
                                   int *caller_frees)
{
    zval result;
    ZVAL_UNDEF(&result);

    if (!clickhouse_stmt_read_col_zval(stmt, colno, &result)) {
        return 0;
    }

    if (Z_TYPE(result) == IS_NULL) {
        *ptr = nullptr;
        *len = 0;
        *caller_frees = 0;
        return 1;
    }

    if (Z_TYPE(result) == IS_ARRAY) {
        zval_ptr_dtor(&result);
        pdo_clickhouse_error(stmt->dbh, stmt, -1,
                             "Complex ClickHouse values require PHP 8.1 or newer PDO fetch "
                             "support; PHP 7.4 and 8.0 cannot return arrays from PDO drivers",
                             "HY000");
        return 0;
    }

    convert_to_string(&result);
    *len = Z_STRLEN(result);
    *ptr = static_cast<char *>(emalloc(*len + 1));
    memcpy(*ptr, Z_STRVAL(result), *len);
    (*ptr)[*len] = '\0';
    *caller_frees = 1;
    zval_ptr_dtor(&result);
    return 1;
}
#else
static int clickhouse_stmt_get_col(pdo_stmt_t *stmt, int colno, zval *result,
                                   enum pdo_param_type *type)
{
    return clickhouse_stmt_read_col_zval(stmt, colno, result);
}
#endif

static int clickhouse_stmt_param_hook(pdo_stmt_t *stmt, struct pdo_bound_param_data *param,
                                      enum pdo_param_event event_type)
{
    if (event_type == PDO_PARAM_EVT_ALLOC) {
        clickhouse_param_state(param);
        return 1;
    }

    if (event_type == PDO_PARAM_EVT_FREE) {
        if (param->driver_data) {
            clickhouse_restore_param_state(
                param, static_cast<clickhouse_bound_param_state *>(param->driver_data));
            efree(param->driver_data);
            param->driver_data = nullptr;
        }
        return 1;
    }

    if (event_type == PDO_PARAM_EVT_EXEC_PRE) {
        auto *state = clickhouse_param_state(param);
        clickhouse_restore_param_state(param, state);
        state->original_type = param->param_type;
        state->has_original = true;

        zval *parameter = &param->parameter;
        ZVAL_DEREF(parameter);

        if (Z_TYPE_P(parameter) == IS_NULL) {
            param->param_type = clickhouse_param_type_with_base(param->param_type, PDO_PARAM_NULL);
            return 1;
        }

        if (PDO_PARAM_TYPE(param->param_type) == PDO_PARAM_BOOL &&
            Z_TYPE_P(parameter) == IS_STRING) {
            bool truthy = clickhouse_bool_param_string_truthy(parameter);
            ZVAL_COPY(&state->original_parameter, &param->parameter);
            state->has_original_parameter = true;
            zval_ptr_dtor(&param->parameter);
            ZVAL_BOOL(&param->parameter, truthy);
            return 1;
        }

        if (PDO_PARAM_TYPE(param->param_type) == PDO_PARAM_STR) {
            switch (Z_TYPE_P(parameter)) {
            case IS_TRUE:
            case IS_FALSE:
                param->param_type =
                    clickhouse_param_type_with_base(param->param_type, PDO_PARAM_BOOL);
                break;
            case IS_LONG:
                param->param_type =
                    clickhouse_param_type_with_base(param->param_type, PDO_PARAM_INT);
                break;
            default:
                break;
            }
        }
    } else if (event_type == PDO_PARAM_EVT_EXEC_POST) {
        clickhouse_restore_param_state(
            param, static_cast<clickhouse_bound_param_state *>(param->driver_data));
    }

    return 1;
}

static int clickhouse_stmt_get_column_meta(pdo_stmt_t *stmt, zend_long colno, zval *return_value)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);

    if (colno < 0 || static_cast<size_t>(colno) >= S->col_type_names.size())
        return FAILURE;

    array_init(return_value);
    add_assoc_string(return_value, "native_type",
                     const_cast<char *>(S->col_type_names[colno].c_str()));
    add_assoc_string(return_value, "driver:decl_type",
                     const_cast<char *>(S->col_type_names[colno].c_str()));
    add_assoc_string(return_value, "name", const_cast<char *>(S->col_names[colno].c_str()));

    return SUCCESS;
}

static int clickhouse_stmt_cursor_closer(pdo_stmt_t *stmt)
{
    auto *S = static_cast<pdo_clickhouse_stmt *>(stmt->driver_data);
    S->blocks.clear();
    S->block_row_offsets.clear();
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
    NULL, /* set_attribute */
    NULL, /* get_attribute */
    clickhouse_stmt_get_column_meta,
    NULL, /* next_rowset */
    clickhouse_stmt_cursor_closer,
};
