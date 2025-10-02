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
FLINK_REST_URL=${FLINK_REST_URL:-http://localhost:8081}
FLINK_JOBMANAGER_TARGET=${FLINK_JOBMANAGER_TARGET:-localhost:8081}

for flink_bin in start-cluster.sh stop-cluster.sh flink; do
    if [[ ! -x "${FLINK_BIN_DIR}/${flink_bin}" ]]; then
        echo "Error: '${FLINK_BIN_DIR}/${flink_bin}' was not found or is not executable." >&2
        exit 1
    fi
done

CLUSTER_STARTED=0
JOB_ID=""

cleanup() {
    local exit_code=$?

    if [[ -n "${JOB_ID}" ]]; then
        echo "Stopping Flink job ${JOB_ID}..."
        "${FLINK_BIN_DIR}/flink" stop "${JOB_ID}" || true
    fi

    if [[ ${CLUSTER_STARTED} -eq 1 ]]; then
        echo "Stopping Flink cluster..."
        "${FLINK_BIN_DIR}/stop-cluster.sh" || true
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
submission_output=$(uv run -- "${FLINK_BIN_DIR}/flink" run -m "${FLINK_JOBMANAGER_TARGET}" -d -py job.py -pyexec "${PY_EXECUTABLE}" "$@" 2>&1)

printf '%s\n' "${submission_output}"

JOB_ID=$(printf '%s\n' "${submission_output}" | sed -n 's/.*Job has been submitted with JobID \([0-9a-fA-F-]\{1,\}\).*/\1/p' | head -n1)

if [[ -z "${JOB_ID}" ]]; then
    echo "Error: Unable to determine JobID from Flink submission output." >&2
    exit 1
fi

echo "Flink job submitted with JobID ${JOB_ID}."
echo "Waiting ${STOP_DELAY_SECONDS} seconds before stopping the job..."

sleep "${STOP_DELAY_SECONDS}"

echo "Stop delay elapsed; exiting to trigger cleanup."
