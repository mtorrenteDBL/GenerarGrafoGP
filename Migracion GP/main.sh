#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/log"
LOG_FILE="$LOG_DIR/run_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$LOG_DIR"

echo "Starting pipeline — logging to $LOG_FILE"

exec > >(tee -a "$LOG_FILE") 2>&1

cd "$SCRIPT_DIR"

uv run main.py "$@"
