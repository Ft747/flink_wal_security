#!/usr/bin/env bash
set -euo pipefail

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
if [[ -n "$JOB_ID" ]]; then
    echo "Job submitted with ID: $JOB_ID"
    echo "Monitoring job status..."
    
    while true; do
        STATUS=$(check_job_status "$JOB_ID" "$FLINK_JOBMANAGER_TARGET")
        echo "Job status: $STATUS"
        
        case "$STATUS" in
            "FINISHED"|"CANCELED"|"FAILED")
                echo "Job completed with status: $STATUS"
                break
                ;;
            "UNKNOWN")
                echo "Unable to check job status. Job may have completed."
                break
                ;;
        esac
        sleep 2
    done
else
    echo "Could not extract job ID from output"
fi

echo "Flink logs location: ${FLINK_HOME}/log/"
echo "To view logs: docker run -v \$(pwd)/logs:/opt/flink/log flink-wal-test"
