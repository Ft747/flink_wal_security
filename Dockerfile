# syntax=docker/dockerfile:1
FROM apache/flink:2.1.0-java17 AS runtime

USER root

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    FLINK_CONF_DIR=${FLINK_HOME}/conf \
    PYFLINK_CLIENT_EXECUTABLE=python3

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libsnappy-dev \
        librocksdb-dev \
        libbz2-dev \
        liblz4-dev \
        libzstd-dev \
        libgflags-dev \
        ca-certificates \
        curl


COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml ./
COPY uv.lock ./
RUN uv sync --locked
RUN mkdir -p ${FLINK_CONF_DIR} \
    && mkdir -p /tmp/flink-savepoints \
    && mkdir -p /tmp/rocksdb
COPY config.yaml ${FLINK_CONF_DIR}/config.yaml


COPY . .
RUN chmod +x run.sh \
    && chown -R flink:flink /app \
    && chown -R flink:flink /tmp/flink-savepoints \
    && chown -R flink:flink /tmp/rocksdb

USER flink

CMD ["/app/run.sh", "/app/.venv/bin/python"]
