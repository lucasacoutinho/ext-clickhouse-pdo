dnl config.m4 for pdo_clickhouse

PHP_ARG_ENABLE([pdo-clickhouse],
  [for ClickHouse PDO driver],
  [AS_HELP_STRING([--enable-pdo-clickhouse],
    [Enable ClickHouse PDO driver (requires ext-clickhouse)])],
  [yes])

if test "$PHP_PDO_CLICKHOUSE" != "no"; then

  PHP_CHECK_PDO_INCLUDES

  dnl ext_srcdir can be empty when building in-tree; default to "."
  if test -z "$ext_srcdir"; then
    ext_srcdir="."
  fi

  dnl Prefer the sibling checkout for development, otherwise use headers
  dnl installed by ext-clickhouse through PHP_INSTALL_HEADERS.
  CLICKHOUSE_EXT_DIR="$ext_srcdir/../ext-clickhouse"
  if test -f "$CLICKHOUSE_EXT_DIR/php_clickhouse.h"; then
    CLICKHOUSE_CPP_DIR="$CLICKHOUSE_EXT_DIR/clickhouse-cpp"
  elif test -f "$phpincludedir/ext/clickhouse/php_clickhouse.h"; then
    CLICKHOUSE_EXT_DIR="$phpincludedir/ext/clickhouse"
    CLICKHOUSE_CPP_DIR="$CLICKHOUSE_EXT_DIR/clickhouse-cpp"
  else
    AC_MSG_ERROR([ext-clickhouse headers not found. Install ext-clickhouse first or keep ../ext-clickhouse as a sibling checkout])
  fi

  PHP_ADD_INCLUDE([$CLICKHOUSE_EXT_DIR])
  PHP_ADD_INCLUDE([$CLICKHOUSE_CPP_DIR])
  PHP_ADD_INCLUDE([$CLICKHOUSE_CPP_DIR/contrib/absl])

  PHP_REQUIRE_CXX()
  PHP_CXX_COMPILE_STDCXX([17], [mandatory], [PHP_PDO_CLICKHOUSE_STDCXX])

  dnl Match the BigNum ABI used by the paired ext-clickhouse 1.3.x release.
  PDO_CLICKHOUSE_BIGNUM_FLAGS="-DCH_USE_ABSEIL_FOR_BIGNUM=1"

  dnl Detect OpenSSL (must match ext-clickhouse build)
  PKG_CHECK_MODULES([OPENSSL], [openssl >= 1.1.0], [
    PDO_CLICKHOUSE_OPENSSL_FLAGS="-DWITH_OPENSSL=1"
  ], [
    PDO_CLICKHOUSE_OPENSSL_FLAGS=""
  ])

  PHP_NEW_EXTENSION([pdo_clickhouse],
    [pdo_clickhouse.cpp clickhouse_driver.cpp clickhouse_statement.cpp],
    [$ext_shared],,
    [$PHP_PDO_CLICKHOUSE_STDCXX -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1 $PDO_CLICKHOUSE_BIGNUM_FLAGS $PDO_CLICKHOUSE_OPENSSL_FLAGS],
    [cxx])

  PHP_ADD_LIBRARY(stdc++, 1, PDO_CLICKHOUSE_SHARED_LIBADD)
  PHP_SUBST([PDO_CLICKHOUSE_SHARED_LIBADD])
  PHP_ADD_EXTENSION_DEP(pdo_clickhouse, pdo)
  PHP_ADD_EXTENSION_DEP(pdo_clickhouse, clickhouse)
fi
