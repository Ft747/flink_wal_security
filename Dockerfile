FROM flink:latest

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    FLINK_CONF_DIR=${FLINK_HOME}/conf \
    PYFLINK_CLIENT_EXECUTABLE=python3 \
    XDG_DATA_HOME=/opt/uv/data

WORKDIR /app

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libsnappy-dev \
        libsnappy1v5 \
        librocksdb-dev \
        libbz2-dev \
        liblz4-dev \
        libzstd-dev \
        libgflags-dev \
        ca-certificates \
        curl \
        jq \
        zlib1g-dev \
        gosu \
    && rm -rf /var/lib/apt/lists/*

# Install uv and setup Python environment
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
RUN uv python pin 3.9.23 \
    && uv sync --locked \
    && chmod -R a+rx /opt/uv /app/.venv
COPY . .

RUN chmod +x run.sh swap_sst_last5 \
    && chmod 755 /opt/flink/bin/flink
RUN useradd -m -s /bin/bash swapuser

# Setup Flink directories with root ownership AND write permissions
RUN mkdir -p ${FLINK_CONF_DIR} \ 
    && mkdir -p /tmp/flink-savepoints \
    && mkdir -p /tmp/flink-checkpoints \
    && mkdir -p /tmp/rocksdb \
    && chown -R flink:flink /tmp/flink-savepoints /tmp/flink-checkpoints /tmp/rocksdb 

COPY config.yaml ${FLINK_CONF_DIR}/config.yaml

# Compile C++ tool
RUN chown swapuser:swapuser /app/swap_sst_last5 \
    && chmod 700 /app/swap_sst_last5
#RUN g++ -std=c++17 swap_sst_last5.cpp \
#    -o swap_sst_last5 \
#    -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -pthread -ldl -lrt

# Copy application code

# Set permissions for executables


# Run as root
# Create a new non-root user called "safeuser"


CMD ["/app/run.sh", "/app/.venv/bin/python"]