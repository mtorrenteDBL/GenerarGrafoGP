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

failed_flows=()
for flow_file in "${flow_files[@]}"; do
    flow_name="$(basename "${flow_file}")"

    # Skip empty files left over from failed downloads
    if [[ ! -s "${flow_file}" ]]; then
        echo ""
        echo "--- Skipping: ${flow_name} (empty file) ---"
        failed_flows+=("${flow_name}")
        continue
    fi

    echo ""
    echo "--- Processing: ${flow_name} ---"
    if ! uv run "${SCRIPT_DIR}/main.py" --flow "${flow_file}" --root-name "${flow_name%.*}"; then
        echo "  ✘ ${flow_name} failed" >&2
        failed_flows+=("${flow_name}")
    fi
done

echo ""
if [[ ${#failed_flows[@]} -gt 0 ]]; then
    echo "=== Warning: ${#failed_flows[@]} flow(s) failed: ${failed_flows[*]} ==="
else
    echo "=== All flows processed successfully ==="
fi
echo "(Logs written to ${SCRIPT_DIR}/log/)"
