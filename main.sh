#!/usr/bin/env bash
# main.sh – Launch the GenerarGrafoPetersen orchestrator with uv
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Run the orchestrator with uv
exec uv run python main.py "$@"
