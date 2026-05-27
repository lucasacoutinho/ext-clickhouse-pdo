ARG PHP_VERSION=8.5

# --- Stage 1: Build ---
FROM php:${PHP_VERSION}-cli AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf g++ make git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
ARG EXT_CLICKHOUSE_REPOSITORY=https://github.com/lucasacoutinho/ext-clickhouse.git
ARG EXT_CLICKHOUSE_REF=v1.0.3

# Build ext-clickhouse first (pdo_clickhouse depends on it)
RUN git clone "${EXT_CLICKHOUSE_REPOSITORY}" ext-clickhouse \
    && cd ext-clickhouse \
    && git checkout "${EXT_CLICKHOUSE_REF}" \
    && git submodule sync --recursive \
    && for attempt in 1 2 3; do \
        git submodule update --init --force --depth=1 --recursive && break; \
        if [ "$attempt" = "3" ]; then exit 1; fi; \
        sleep $((attempt * 10)); \
    done \
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
