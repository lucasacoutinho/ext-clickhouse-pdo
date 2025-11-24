dnl config.m4 for extension pdo_clickhouse

PHP_ARG_WITH([pdo-clickhouse],
  [for ClickHouse support for PDO],
  [AS_HELP_STRING([--with-pdo-clickhouse],
    [Include PDO ClickHouse support])],
  [no])

if test "$PHP_PDO_CLICKHOUSE" != "no"; then
  dnl Check for PDO extension
  if test "$PHP_PDO" = "no" && test "$ext_shared" = "no"; then
    AC_MSG_ERROR([PDO is not enabled. Please enable PDO before enabling pdo_clickhouse.])
  fi

  dnl Check if the clickhouse extension is available
  PHP_ADD_INCLUDE([$ext_srcdir/../clickhouse/src])

  dnl Source files
  PHP_NEW_EXTENSION(pdo_clickhouse,
    pdo_clickhouse.c \
    clickhouse_driver.c \
    clickhouse_driver_methods.c \
    clickhouse_helpers.c \
    clickhouse_statement.c \
    ../clickhouse/src/buffer.c \
    ../clickhouse/src/protocol.c \
    ../clickhouse/src/connection.c \
    ../clickhouse/src/column.c,
    $ext_shared,, -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1 -std=c11)

  dnl Add dependency on PDO
  PHP_ADD_EXTENSION_DEP(pdo_clickhouse, pdo)
fi
