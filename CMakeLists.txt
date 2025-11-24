cmake_minimum_required(VERSION 3.15)
project(pdo-clickhouse C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Version
set(PDO_CLICKHOUSE_VERSION "0.1.1")

# Options
option(BUILD_SHARED_LIBS "Build shared library" ON)
option(BUILD_TESTS "Build and run tests" ON)
option(WITH_CLICKHOUSE_EXT "Use system ext-clickhouse" OFF)

# Find PHP
find_program(PHP_CONFIG php-config REQUIRED)
if(NOT PHP_CONFIG)
    message(FATAL_ERROR "php-config not found. Install php-dev package.")
endif()

# Get PHP configuration
execute_process(COMMAND ${PHP_CONFIG} --includes OUTPUT_VARIABLE PHP_INCLUDES OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PHP_CONFIG} --extension-dir OUTPUT_VARIABLE PHP_EXTENSION_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PHP_CONFIG} --php-binary OUTPUT_VARIABLE PHP_BINARY OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PHP_CONFIG} --include-dir OUTPUT_VARIABLE PHP_INCLUDE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)

string(REPLACE "-I" "" PHP_INCLUDES "${PHP_INCLUDES}")
string(REPLACE " " ";" PHP_INCLUDE_LIST "${PHP_INCLUDES}")

message(STATUS "PHP includes: ${PHP_INCLUDES}")
message(STATUS "PHP extension dir: ${PHP_EXTENSION_DIR}")
message(STATUS "PHP binary: ${PHP_BINARY}")

# Include directories
include_directories(${PHP_INCLUDE_LIST})

set(PDO_CLICKHOUSE_SOURCES
    pdo_clickhouse.c
    clickhouse_driver.c
    clickhouse_driver_methods.c
    clickhouse_helpers.c
    clickhouse_statement.c
)

if(WITH_CLICKHOUSE_EXT)
    message(STATUS "Using system ext-clickhouse")
else()
    message(STATUS "Bundling ext-clickhouse sources")

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../clickhouse/src")
        set(CLICKHOUSE_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../clickhouse/src")
    elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/vendor/clickhouse/src")
        set(CLICKHOUSE_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/vendor/clickhouse/src")
    else()
        message(FATAL_ERROR
            "ClickHouse extension sources not found. Either:\n"
            "  1. Clone from https://github.com/lucasacoutinho/ext-clickhouse to ../clickhouse/ OR\n"
            "  2. Place in vendor/clickhouse/ OR\n"
            "  3. Use -DWITH_CLICKHOUSE_EXT=ON to use system installation")
    endif()

    message(STATUS "Using ClickHouse sources from: ${CLICKHOUSE_SRC_DIR}")

    list(APPEND PDO_CLICKHOUSE_SOURCES
        ${CLICKHOUSE_SRC_DIR}/buffer.c
        ${CLICKHOUSE_SRC_DIR}/protocol.c
        ${CLICKHOUSE_SRC_DIR}/connection.c
        ${CLICKHOUSE_SRC_DIR}/column.c
        ${CLICKHOUSE_SRC_DIR}/cityhash.c
    )

    include_directories(${CLICKHOUSE_SRC_DIR})
endif()

add_library(pdo_clickhouse MODULE ${PDO_CLICKHOUSE_SOURCES})

target_include_directories(pdo_clickhouse PRIVATE
    ${PHP_INCLUDE_LIST}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Define necessary compilation flags for PHP module
target_compile_definitions(pdo_clickhouse PRIVATE
    COMPILE_DL_PDO_CLICKHOUSE=1
    ZEND_ENABLE_STATIC_TSRMLS_CACHE=1
)

# Compression support (optional)
find_package(PkgConfig)
if(PkgConfig_FOUND)
    pkg_check_modules(LZ4 liblz4)
    pkg_check_modules(ZSTD libzstd)

    if(LZ4_FOUND)
        target_link_libraries(pdo_clickhouse ${LZ4_LIBRARIES})
        target_include_directories(pdo_clickhouse PRIVATE ${LZ4_INCLUDE_DIRS})
        target_compile_definitions(pdo_clickhouse PRIVATE HAVE_LZ4=1)
        message(STATUS "LZ4 compression: enabled")
    else()
        message(STATUS "LZ4 compression: disabled (not found)")
    endif()

    if(ZSTD_FOUND)
        target_link_libraries(pdo_clickhouse ${ZSTD_LIBRARIES})
        target_include_directories(pdo_clickhouse PRIVATE ${ZSTD_INCLUDE_DIRS})
        target_compile_definitions(pdo_clickhouse PRIVATE HAVE_ZSTD=1)
        message(STATUS "ZSTD compression: enabled")
    else()
        message(STATUS "ZSTD compression: disabled (not found)")
    endif()
endif()

# SSL support (optional)
find_package(OpenSSL)
if(OPENSSL_FOUND)
    target_link_libraries(pdo_clickhouse OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(pdo_clickhouse PRIVATE HAVE_OPENSSL=1)
    message(STATUS "SSL/TLS support: enabled")
else()
    message(STATUS "SSL/TLS support: disabled (OpenSSL not found)")
endif()

set_target_properties(pdo_clickhouse PROPERTIES
    PREFIX ""
    SUFFIX ".so"
    OUTPUT_NAME "pdo_clickhouse"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/modules"
    VERSION ${PDO_CLICKHOUSE_VERSION}
)

install(TARGETS pdo_clickhouse
    LIBRARY DESTINATION ${PHP_EXTENSION_DIR}
)

if(BUILD_TESTS)
    enable_testing()

    # Get run-tests.php from PHP source
    if(NOT EXISTS "${CMAKE_BINARY_DIR}/run-tests.php")
        # Try to find it in common locations
        set(RUN_TESTS_CANDIDATES
            "${PHP_INCLUDE_DIR}/../run-tests.php"
            "/usr/share/php/run-tests.php"
            "/usr/local/share/php/run-tests.php"
        )

        set(RUN_TESTS_FOUND FALSE)
        foreach(CANDIDATE ${RUN_TESTS_CANDIDATES})
            if(EXISTS "${CANDIDATE}")
                file(COPY ${CANDIDATE} DESTINATION ${CMAKE_BINARY_DIR})
                set(RUN_TESTS_FOUND TRUE)
                message(STATUS "Found run-tests.php at: ${CANDIDATE}")
                break()
            endif()
        endforeach()

        # If not found locally, download from PHP source
        if(NOT RUN_TESTS_FOUND)
            message(STATUS "Downloading run-tests.php from PHP source...")
            execute_process(COMMAND ${PHP_CONFIG} --version OUTPUT_VARIABLE PHP_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)
            string(REGEX REPLACE "^([0-9]+\\.[0-9]+).*" "\\1" PHP_MAJOR_MINOR "${PHP_VERSION}")

            file(DOWNLOAD
                "https://raw.githubusercontent.com/php/php-src/PHP-${PHP_MAJOR_MINOR}/run-tests.php"
                "${CMAKE_BINARY_DIR}/run-tests.php"
                STATUS DOWNLOAD_STATUS
                TIMEOUT 30
            )

            list(GET DOWNLOAD_STATUS 0 DOWNLOAD_CODE)
            if(NOT DOWNLOAD_CODE EQUAL 0)
                message(WARNING "Could not download run-tests.php, trying master branch...")
                file(DOWNLOAD
                    "https://raw.githubusercontent.com/php/php-src/master/run-tests.php"
                    "${CMAKE_BINARY_DIR}/run-tests.php"
                    TIMEOUT 30
                )
            endif()
        endif()
    endif()

    # Find all test files
    file(GLOB PHPT_TESTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.phpt")

    foreach(PHPT_TEST ${PHPT_TESTS})
        get_filename_component(TEST_NAME ${PHPT_TEST} NAME_WE)
        add_test(
            NAME ${TEST_NAME}
            COMMAND ${PHP_BINARY}
                ${CMAKE_BINARY_DIR}/run-tests.php
                -d extension=${CMAKE_BINARY_DIR}/modules/pdo_clickhouse.so
                ${PHPT_TEST}
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        )
        set_tests_properties(${TEST_NAME} PROPERTIES
            ENVIRONMENT "CLICKHOUSE_HOST=localhost;CLICKHOUSE_PORT=9000"
        )
    endforeach()
endif()

# ============================================================================
# Examples
# ============================================================================

# Copy examples if they exist
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples")
    file(COPY examples DESTINATION ${CMAKE_BINARY_DIR})
endif()

# ============================================================================
# Package / Distribution
# ============================================================================

set(CPACK_PACKAGE_NAME "pdo_clickhouse")
set(CPACK_PACKAGE_VERSION ${PDO_CLICKHOUSE_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "PDO driver for ClickHouse database")
set(CPACK_PACKAGE_VENDOR "ClickHouse Native PHP")
set(CPACK_GENERATOR "TGZ")

include(CPack)


message(STATUS "")
message(STATUS "=== PDO ClickHouse Configuration ===")
message(STATUS "Version: ${PDO_CLICKHOUSE_VERSION}")
message(STATUS "Build tests: ${BUILD_TESTS}")
message(STATUS "Use system ext-clickhouse: ${WITH_CLICKHOUSE_EXT}")
if(NOT WITH_CLICKHOUSE_EXT)
    message(STATUS "ClickHouse sources: ${CLICKHOUSE_SRC_DIR}")
endif()
message(STATUS "PHP binary: ${PHP_BINARY}")
message(STATUS "Extension dir: ${PHP_EXTENSION_DIR}")
message(STATUS "Output: ${CMAKE_BINARY_DIR}/modules/pdo_clickhouse.so")
message(STATUS "")
