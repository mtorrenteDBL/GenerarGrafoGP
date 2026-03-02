#!/usr/bin/env bash
# reset_and_run.sh – Wipe the Neo4j database and run both pipeline scripts.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# ---------------------------------------------------------------------------
# 1. Load credentials from .env
# ---------------------------------------------------------------------------
if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: .env file not found at $ENV_FILE" >&2
    exit 1
fi

set -a
# shellcheck source=.env
source "$ENV_FILE"
set +a

echo "=== Neo4j target: $NEO4J_HOST ==="

# ---------------------------------------------------------------------------
# 2. Wipe Neo4j database
# ---------------------------------------------------------------------------
echo ""
echo "=== Wiping Neo4j database ==="

# Use cypher-shell if available, otherwise fall back to uv run
if command -v cypher-shell &>/dev/null; then
    cypher-shell -a "$NEO4J_HOST" -u "$NEO4J_USER" -p "$NEO4J_PASS" \
        "MATCH (n) DETACH DELETE n;"
    echo "Database wiped via cypher-shell."
else
    uv run --project "$SCRIPT_DIR/Migracion GP" - <<EOF
from neo4j import GraphDatabase

driver = GraphDatabase.driver("${NEO4J_HOST}", auth=("${NEO4J_USER}", "${NEO4J_PASS}"))
with driver.session() as session:
    result = session.run("MATCH (n) DETACH DELETE n")
    summary = result.consume()
    print(
        f"Database wiped via Python driver — "
        f"{summary.counters.nodes_deleted} nodes deleted, "
        f"{summary.counters.relationships_deleted} relationships deleted."
    )
driver.close()
EOF
fi

# ---------------------------------------------------------------------------
# 3. Run FlowToGraph pipeline
# ---------------------------------------------------------------------------
echo ""
echo "=== Running FlowToGraph/main.sh ==="
bash "$SCRIPT_DIR/FlowToGraph/main.sh"

# ---------------------------------------------------------------------------
# 4. Run Migracion GP pipeline
# ---------------------------------------------------------------------------
echo ""
echo "=== Running 'Migracion GP/main.sh' ==="
bash "$SCRIPT_DIR/Migracion GP/main.sh"

echo ""
echo "=== All done ==="
