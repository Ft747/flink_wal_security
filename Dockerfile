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
        python3 \
        python3-pip \
        python3-venv \
        python3-dev \
        build-essential \
        libsnappy-dev \
        librocksdb-dev \
        libbz2-dev \
        liblz4-dev \
        libzstd-dev \
        libgflags-dev \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv \
    && rm -rf /root/.local \
    && ln -sf /usr/bin/python3 /usr/bin/python

COPY pyproject.toml ./
COPY uv.lock ./
RUN uv pip install --python python3 --system .

RUN mkdir -p ${FLINK_CONF_DIR} \
    && mkdir -p /tmp/flink-savepoints \
    && mkdir -p /tmp/rocksdb
COPY config.yaml ${FLINK_CONF_DIR}/config.yaml


COPY . .
RUN chmod +x run_flink_job.sh \
    && chown -R flink:flink /app \
    && chown -R flink:flink /tmp/flink-savepoints \
    && chown -R flink:flink /tmp/rocksdb

USER flink

CMD ["/app/run_flink_job.sh", "/usr/bin/python3"]
