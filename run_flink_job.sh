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

if [[ ${#} -lt 1 ]]; then
    usage
    exit 1
fi

PY_EXECUTABLE=$1
shift || true

if [[ ! -x "${PY_EXECUTABLE}" ]]; then
    echo "Error: '${PY_EXECUTABLE}' is not an executable Python interpreter." >&2
    exit 1
fi

FLINK_HOME=${FLINK_HOME:-/opt/flink}
FLINK_BIN_DIR="${FLINK_HOME}/bin"
STOP_DELAY_SECONDS=${STOP_DELAY_SECONDS:-3}
STARTUP_TIMEOUT_SECONDS=${STARTUP_TIMEOUT_SECONDS:-60}
STARTUP_POLL_INTERVAL_SECONDS=${STARTUP_POLL_INTERVAL_SECONDS:-2}
FLINK_REST_URL=${FLINK_REST_URL:-http://127.0.0.1:8081}
FLINK_JOBMANAGER_TARGET=${FLINK_JOBMANAGER_TARGET:-127.0.0.1:8081}

for flink_bin in start-cluster.sh stop-cluster.sh flink; do
    if [[ ! -x "${FLINK_BIN_DIR}/${flink_bin}" ]]; then
        echo "Error: '${FLINK_BIN_DIR}/${flink_bin}' was not found or is not executable." >&2
        exit 1
    fi
done

CLUSTER_STARTED=0
JOB_ID=""
MONITOR_PID=0

cleanup() {
    local exit_code=$?

    if [[ ${MONITOR_PID} -ne 0 ]]; then
        echo "Stopping monitor_and_swap.py (PID ${MONITOR_PID})..."
        kill "${MONITOR_PID}" 2>/dev/null || true
    fi

    if [[ -n "${JOB_ID}" ]]; then
        echo "Flink job ${JOB_ID} left running; skipping automatic stop."
    fi

    if [[ ${CLUSTER_STARTED} -eq 1 ]]; then
        echo "Flink cluster left running; skipping automatic shutdown."
    fi

    exit ${exit_code}
}

trap cleanup EXIT INT TERM

echo "Starting Flink cluster..."
"${FLINK_BIN_DIR}/start-cluster.sh"
CLUSTER_STARTED=1

attempts=$(( (STARTUP_TIMEOUT_SECONDS + STARTUP_POLL_INTERVAL_SECONDS - 1) / STARTUP_POLL_INTERVAL_SECONDS ))
echo "Waiting up to ${STARTUP_TIMEOUT_SECONDS}s for Flink REST endpoint at ${FLINK_REST_URL}..."
for ((i=1; i<=attempts; i++)); do
    if curl --silent --fail "${FLINK_REST_URL}/taskmanagers" >/dev/null; then
        echo "Flink REST endpoint is reachable."
        break
    fi

    if [[ $i -eq attempts ]]; then
        echo "Error: Flink REST endpoint did not become ready within ${STARTUP_TIMEOUT_SECONDS}s." >&2
        exit 1
    fi

    sleep "${STARTUP_POLL_INTERVAL_SECONDS}"
done

echo "Submitting Flink job via uv to ${FLINK_JOBMANAGER_TARGET}..."
uv run monitor_and_swap.py &
MONITOR_PID=$!
echo "monitor_and_swap.py running with PID ${MONITOR_PID}."

set +e
submission_output=$(uv run -- "${FLINK_BIN_DIR}/flink" run -m "${FLINK_JOBMANAGER_TARGET}" -d -py job.py -pyexec "${PY_EXECUTABLE}" "$@" 2>&1)
submit_status=$?
set -e

printf '%s\n' "${submission_output}"

if [[ ${submit_status} -ne 0 ]]; then
    echo "Error: Flink submission command failed with exit code ${submit_status}." >&2
    exit ${submit_status}
fi

JOB_ID=$(printf '%s\n' "${submission_output}" | sed -n 's/.*Job has been submitted with JobID \([0-9a-fA-F-]\{1,\}\).*/\1/p' | head -n1)

if [[ -z "${JOB_ID}" ]]; then
    echo "Error: Unable to determine JobID from Flink submission output." >&2
    exit 1
fi

echo "Flink job submitted with JobID ${JOB_ID}."
echo "monitor_and_swap.py is watching RocksDB SST files."

wait "${MONITOR_PID}"
