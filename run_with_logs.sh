#!/bin/bash

# Create logs directory on host
mkdir -p ./logs

echo "Building Docker image..."
docker build -t flink-wal-test .

echo "Running Flink job with log mounting..."
docker run --rm \
    -v "$(pwd)/logs:/opt/flink/log" \
    -p 8081:8081 \
    flink-wal-test

echo ""
echo "=== Flink Logs ==="
echo "Logs are available in: $(pwd)/logs"
echo ""
echo "JobManager log:"
if [[ -f "./logs/flink-flink-standalonesession-0-*.log" ]]; then
    tail -20 ./logs/flink-flink-standalonesession-0-*.log
fi

echo ""
echo "TaskManager log:"
if [[ -f "./logs/flink-flink-taskexecutor-0-*.log" ]]; then
    tail -20 ./logs/flink-flink-taskexecutor-0-*.log
fi
