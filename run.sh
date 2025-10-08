#!/usr/bin/env bash
set -uo pipefail

usage() {
    cat <<'EOF'
Usage: run_flink_job.sh <path-to-python-exec>

Starts a local Flink cluster, submits the Python job defined in job.py via uv,
then waits until interrupted. On exit, the script stops the submitted job and
shuts down the cluster.

Arguments:
  <path-to-python-exec>  Absolute path to the Python interpreter inside the
                         desired virtual environment (e.g. from uv venv).
EOF
}

# Function to extract job ID from flink run output
extract_job_id() {
    local output="$1"
    echo "$output" | grep -o "JobID [a-f0-9]\{32\}" | cut -d' ' -f2
}

# Function to check job status
check_job_status() {
    local job_id="$1"
    local target="$2"
    curl -s "http://${target}/jobs/${job_id}" | jq -r '.state' 2>/dev/null || echo "UNKNOWN"
}

if [[ ${#} -lt 1 ]]; then
    usage
    exit 1
fi

PY_EXECUTABLE=$1

FLINK_HOME=${FLINK_HOME:-/opt/flink}
FLINK_BIN_DIR="${FLINK_HOME}/bin"
FLINK_REST_URL=${FLINK_REST_URL:-http://127.0.0.1:8081}
FLINK_JOBMANAGER_TARGET=${FLINK_JOBMANAGER_TARGET:-127.0.0.1:8081}

echo "Starting Flink cluster..."
"${FLINK_BIN_DIR}/start-cluster.sh"

echo "Waiting for cluster to be ready..."
sleep 5
echo "Submitting job..."
JOB_OUTPUT=$(uv run -- "${FLINK_BIN_DIR}/flink" run -m "${FLINK_JOBMANAGER_TARGET}" -d -py job.py -pyexec "${PY_EXECUTABLE}" "$@" 2>&1)
echo "$JOB_OUTPUT"

JOB_ID=$(extract_job_id "$JOB_OUTPUT")
echo "DEBUG: Extracted Job ID: '$JOB_ID'"

if [ -z "$JOB_ID" ]; then
    echo "Error: Could not extract job ID from output"
    exit 1
fi

sleep 4

# Check job status before creating savepoint
echo "Checking job status and waiting for full initialization..."
max_wait=30
wait_time=0
while [ $wait_time -lt $max_wait ]; do
    job_status=$(check_job_status "$JOB_ID" "$FLINK_JOBMANAGER_TARGET")
    echo "Job status: $job_status (waited ${wait_time}s)"
    
    if [ "$job_status" = "RUNNING" ]; then
        # Job is running, but let's wait a bit more for all tasks to be fully initialized
        echo "Job is running, waiting additional 10 seconds for task initialization..."
        sleep 10
        break
    fi
    
    sleep 2
    wait_time=$((wait_time + 2))
done

if [ "$job_status" != "RUNNING" ]; then
    echo "Error: Job failed to reach RUNNING state within ${max_wait} seconds. Current status: $job_status"
    exit 1
fi

# Stop job with savepoint (default savepoint directory)
echo "Stopping job and creating savepoint..."
savepoint_output=$(flink savepoint $JOB_ID --type native 2>&1)

# Extract savepoint path - look for the line with "Path: file:"
savepoint_path=$(echo "$savepoint_output" | grep "Path: file:" | sed 's/.*Path: file:\([^[:space:]]*\).*/\1/')

if [ -z "$savepoint_path" ]; then
    echo "Error: Could not find savepoint path"
    echo "Full output was:"
    echo "$savepoint_output"
    exit 1
fi

echo "Savepoint created at: $savepoint_path"

# Pick the largest state file (exclude _metadata) - this should be the main savepoint
TARGET_FILE=""
MAX_SIZE=0
for file in "$savepoint_path"/*; do
    if [[ -f "$file" && "$(basename "$file")" != "_metadata" ]]; then
        size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file" 2>/dev/null)
        if (( size > MAX_SIZE )); then
            MAX_SIZE=$size
            TARGET_FILE="$file"
        fi
    fi
done

if [ -z "$TARGET_FILE" ]; then
    echo "Error: No state file found in savepoint"
    exit 1
fi

echo "Found state file: $TARGET_FILE"

# Rename to .sst
mv "$TARGET_FILE" "${TARGET_FILE}.sst"

# Run your custom swap tool
./swap_sst_last5 "${TARGET_FILE}.sst" 

# Rename back to original name
mv "${TARGET_FILE}.sst" "$TARGET_FILE"

# Resume job from modified savepoint
uv run -- flink run -s "$savepoint_path" -py job.py -pyexec "${PY_EXECUTABLE}" "$@"

echo "Flink logs location: ${FLINK_HOME}/log/"
echo "To view logs: docker run -v \$(pwd)/logs:/opt/flink/log flink-wal-test"
