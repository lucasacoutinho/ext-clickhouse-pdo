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

  dnl Locate ClickHouse native extension sources
  CLICKHOUSE_SRC_DIR=""
  if test "$PHP_PDO_CLICKHOUSE" = "yes" || test -z "$PHP_PDO_CLICKHOUSE"; then
    if test -d "$ext_srcdir/../clickhouse/src"; then
      CLICKHOUSE_SRC_DIR="$ext_srcdir/../clickhouse/src"
    elif test -d "$ext_srcdir/vendor/clickhouse/src"; then
      CLICKHOUSE_SRC_DIR="$ext_srcdir/vendor/clickhouse/src"
    fi
  else
    CLICKHOUSE_SRC_DIR="$PHP_PDO_CLICKHOUSE"
  fi

  dnl Allow passing the root of the clickhouse repo instead of src/
  if test -d "$CLICKHOUSE_SRC_DIR/src"; then
    CLICKHOUSE_SRC_DIR="$CLICKHOUSE_SRC_DIR/src"
  fi

  if test ! -f "$CLICKHOUSE_SRC_DIR/buffer.c"; then
    AC_MSG_ERROR([Could not find ClickHouse sources. Clone https://github.com/lucasacoutinho/ext-clickhouse next to this directory or pass --with-pdo-clickhouse=/path/to/ext-clickhouse/src])
  fi

  PHP_ADD_INCLUDE([$CLICKHOUSE_SRC_DIR])

  dnl Source files - only PDO-specific files
  PHP_NEW_EXTENSION(pdo_clickhouse,
    pdo_clickhouse.c \
    clickhouse_driver.c \
    clickhouse_driver_methods.c \
    clickhouse_helpers.c \
    clickhouse_statement.c,
    $ext_shared,, -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1 -std=c11)

  dnl Add object files from clickhouse extension
  PHP_ADD_SOURCES_X($CLICKHOUSE_SRC_DIR,
    buffer.c protocol.c connection.c column.c cityhash.c,
    , shared_objects_pdo_clickhouse, -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1 -std=c11)

  dnl Add dependency on PDO and clickhouse
  PHP_ADD_EXTENSION_DEP(pdo_clickhouse, pdo)
  PHP_ADD_EXTENSION_DEP(pdo_clickhouse, clickhouse)
fi
