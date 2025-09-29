# syntax=docker/dockerfile:1
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    FLINK_CONF_DIR=/app/conf
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
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

CMD ["python", "job.py"]
