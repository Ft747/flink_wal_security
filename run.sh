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
printf '%s\n' "${submission_output}"

JOB_ID=$(printf '%s\n' "${submission_output}" | sed -n 's/.*Job has been submitted with JobID \([0-9a-fA-F-]\{1,\}\).*/\1/p' | head -n1)

if [[ ! -x "${PY_EXECUTABLE}" ]]; then
    echo "Error: '${PY_EXECUTABLE}' is not an executable Python interpreter." >&2
    exit 1
fi

FLINK_HOME=${FLINK_HOME:-/opt/flink}
FLINK_BIN_DIR="${FLINK_HOME}/bin"
FLINK_REST_URL=${FLINK_REST_URL:-http://127.0.0.1:8081}
FLINK_JOBMANAGER_TARGET=${FLINK_JOBMANAGER_TARGET:-127.0.0.1:8081}



"${FLINK_BIN_DIR}/start-cluster.sh"

# uv run monitor_and_swap.py &
# MONITOR_PID=$!
# echo "monitor_and_swap.py running with PID ${MONITOR_PID}."

uv run -- "${FLINK_BIN_DIR}/flink" run -m "${FLINK_JOBMANAGER_TARGET}" -d -py job.py -pyexec "${PY_EXECUTABLE}" "$@"
