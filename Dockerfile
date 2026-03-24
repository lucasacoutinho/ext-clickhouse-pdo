ARG PHP_VERSION=8.4

# --- Stage 1: Build ---
FROM php:${PHP_VERSION}-cli AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf g++ make git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Build ext-clickhouse first (pdo_clickhouse depends on it)
RUN git clone --recursive https://github.com/lightprofco/ext-clickhouse.git ext-clickhouse \
    && cd ext-clickhouse \
    && phpize \
    && ./configure --enable-clickhouse \
    && make -j$(nproc) \
    && make install

# Build pdo_clickhouse
COPY . ext-pdo-clickhouse/
RUN cd ext-pdo-clickhouse \
    && phpize \
    && ./configure --enable-pdo-clickhouse \
    && make -j$(nproc)

# --- Stage 2: Runtime ---
FROM php:${PHP_VERSION}-cli

# Install ext-clickhouse
COPY --from=builder /build/ext-clickhouse/modules/clickhouse.so /tmp/clickhouse.so
RUN cp /tmp/clickhouse.so $(php -r 'echo ini_get("extension_dir");')/ \
    && rm /tmp/clickhouse.so \
    && docker-php-ext-enable clickhouse

# Install pdo_clickhouse
COPY --from=builder /build/ext-pdo-clickhouse/modules/pdo_clickhouse.so /tmp/pdo_clickhouse.so
RUN cp /tmp/pdo_clickhouse.so $(php -r 'echo ini_get("extension_dir");')/ \
    && rm /tmp/pdo_clickhouse.so \
    && docker-php-ext-enable pdo_clickhouse

RUN php -m | grep -E "clickhouse|pdo_clickhouse"
