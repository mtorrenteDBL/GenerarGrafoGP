#!/usr/bin/env bash
# main.sh – Fetch all NiFi flows and then run main.py for each one.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLOWS_DIR="${SCRIPT_DIR}/flows"

# ---------------------------------------------------------------------------
# 1. Fetch flows from remote clusters
# ---------------------------------------------------------------------------
echo "=== Fetching flows ==="
uv run "${SCRIPT_DIR}/fetch_flows.py"

# ---------------------------------------------------------------------------
# 2. Run main.py for every flow file (*.json and *.xml) in the flows dir
# ---------------------------------------------------------------------------
echo ""
echo "=== Processing flows ==="

shopt -s nullglob
flow_files=("${FLOWS_DIR}"/*.json "${FLOWS_DIR}"/*.xml)

if [[ ${#flow_files[@]} -eq 0 ]]; then
    echo "No flow files found in ${FLOWS_DIR}" >&2
    exit 1
fi

for flow_file in "${flow_files[@]}"; do
    flow_name="$(basename "${flow_file}")"
    echo ""
    echo "--- Processing: ${flow_name} ---"
    uv run "${SCRIPT_DIR}/main.py" --flow "${flow_file}" --root-name "${flow_name%.*}"
done

echo ""
echo "=== Done ==="
echo "(Logs written to ${SCRIPT_DIR}/log/)"
