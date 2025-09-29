# syntax=docker/dockerfile:1
FROM apache/flink:2.1.0-python3.11 AS runtime

USER root

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    FLINK_CONF_DIR=${FLINK_HOME}/conf

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
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh -s -- --install-dir /usr/local/bin

COPY pyproject.toml ./
COPY uv.lock ./
RUN uv pip install --system --no-editable .

RUN mkdir -p ${FLINK_CONF_DIR}
COPY config.yaml ${FLINK_CONF_DIR}/flink-conf.yaml

COPY . .
RUN chown -R flink:flink /app

USER flink

CMD ["flink", "run", "-py", "job.py"]
