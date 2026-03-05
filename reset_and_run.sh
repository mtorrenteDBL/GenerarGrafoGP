#!/usr/bin/env bash
# reset_and_run.sh – Backward compatibility wrapper for main.sh
# This script simply delegates to the new Python orchestrator.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Execute main.sh which runs the Python orchestrator
exec bash "$SCRIPT_DIR/main.sh" "$@"
