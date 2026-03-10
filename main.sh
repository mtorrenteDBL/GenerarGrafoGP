#!/usr/bin/env bash
# main.sh – Launch the GenerarGrafoPetersen orchestrator with uv
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Git pull from data/git/gdc_terms before running the pipeline
GIT_TERMS_DIR="$SCRIPT_DIR/data/git/gdc_terms"
if [ -d "$GIT_TERMS_DIR" ]; then
    echo "Pulling latest terms from $GIT_TERMS_DIR..."
    cd "$GIT_TERMS_DIR"
    git pull || echo "Warning: git pull failed in $GIT_TERMS_DIR"
    cd "$SCRIPT_DIR"
else
    echo "Warning: $GIT_TERMS_DIR not found. Skipping git pull."
fi

# Run the orchestrator with uv
exec uv run python main.py "$@"
