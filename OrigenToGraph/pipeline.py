#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OrigenToGraph pipeline.

Classifies every Tabla node in Neo4j as one of:
  - "Microservicios"      → found in Elasticsearch cron_microservicios_logs
  - "DIS"                 → found in Hive DIS audit tables
  - "Proceso no estándar" → not found in either source

Steps
-----
1. Fetch all Tabla nodes from Neo4j.
2. Build the Microservicios set via Elasticsearch composite aggregations.
3. Classify Neo4j tables against the Microservicios set.
4. For unresolved tables, query Hive once (UNION ALL) to get the DIS set.
5. Classify the remaining tables against the DIS set.
6. Mark all others as "Proceso no estándar".
7. Batch-update Neo4j (one UNWIND transaction per 5 000 rows).

Returns
-------
dict with keys:
  microservicios  – count of nodes tagged "Microservicios"
  dis             – count of nodes tagged "DIS"
  no_estandar     – count of nodes tagged "Proceso no estándar"
  total           – total Tabla nodes processed
  errors          – list of error messages (empty on full success)
"""

import logging
from typing import Set

from .config import get_neo4j_config, get_elastic_config, get_hive_config, get_impala_config
from .neo4j_client import Neo4jClient
from .elastic_client import ElasticClient
from .hive_client import HiveClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Matching helpers
# ---------------------------------------------------------------------------

def _candidate_keys(nombre: str | None, database: str | None) -> list[str]:
    """
    Return the set of normalised lookup keys for a Neo4j Tabla node.

    - If `database` is set and non-empty → primary key is "db.table" (lowercase).
    - Secondary key is always just "table" (lowercase) for bare-name matches.

    This dual lookup handles cases where the external source stores the full
    qualified name (db.table) while the node only has the table name, or
    vice-versa.
    """
    nombre = (nombre or "").strip().lower()
    if not nombre:
        return []

    database = (database or "").strip().lower()
    if database:
        return [f"{database}.{nombre}", nombre]
    return [nombre]


def _is_in_set(nombre: str | None, database: str | None, lookup: Set[str]) -> bool:
    """Return True if any candidate key for this node exists in `lookup`."""
    candidates = _candidate_keys(nombre, database)
    for c in candidates:
        if c in lookup:
            logger.debug(
                "Match found: nombre=%s, database=%s → key=%s",
                nombre,
                database,
                c,
            )
            return True
    return False


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_origen_pipeline() -> dict:
    """
    Run the full OrigenToGraph pipeline.

    All connection parameters are sourced from environment variables.

    Returns a result dict (see module docstring).
    """
    result = {
        "microservicios": 0,
        "dis": 0,
        "no_estandar": 0,
        "total": 0,
        "errors": [],
    }

    # ── 1. Fetch all Tabla nodes from Neo4j ───────────────────────────── #
    neo4j_cfg = get_neo4j_config()
    logger.info("Connecting to Neo4j at %s …", neo4j_cfg["uri"])
    logger.debug("Neo4j config: uri=%s, user=%s", neo4j_cfg["uri"], neo4j_cfg["user"])

    try:
        neo4j = Neo4jClient(**neo4j_cfg)
    except Exception as exc:
        msg = f"Cannot connect to Neo4j: {exc}"
        logger.error(msg, exc_info=True)
        result["errors"].append(msg)
        return result

    try:
        tablas = neo4j.get_all_tablas()
    except Exception as exc:
        msg = f"Failed to fetch Tabla nodes from Neo4j: {exc}"
        logger.error(msg, exc_info=True)
        result["errors"].append(msg)
        neo4j.close()
        return result

    if not tablas:
        logger.warning("No Tabla nodes found in Neo4j – nothing to classify.")
        neo4j.close()
        return result

    result["total"] = len(tablas)
    logger.info("Classifying %d Tabla nodes …", len(tablas))

    # ── 2. Build Microservicios set from Elasticsearch ────────────────── #
    elastic_set: Set[str] = set()
    elastic_cfg = get_elastic_config()
    logger.info(
        "Connecting to Elasticsearch at %s:%s …",
        elastic_cfg["host"],
        elastic_cfg["port"],
    )
    logger.debug(
        "Elasticsearch config: host=%s, port=%s, lookback_days=%s",
        elastic_cfg["host"],
        elastic_cfg["port"],
        elastic_cfg.get("lookback_days", 60),
    )
    try:
        with ElasticClient(**elastic_cfg) as es:
            elastic_set = es.get_microservicios_tables()
        logger.info(
            "Microservicios set built: %d distinct table identifiers",
            len(elastic_set),
        )
    except Exception as exc:
        msg = f"Elasticsearch query failed: {exc}"
        logger.error(msg, exc_info=True)
        result["errors"].append(msg)
        # We continue: all tables will be checked against DIS, then no-estándar.

    # ── 3. First-pass classification: Microservicios ──────────────────── #
    update_rows: list[dict] = []
    unresolved: list[dict] = []

    logger.debug("First-pass: classifying %d nodes against Microservicios set...", len(tablas))
    for row in tablas:
        if _is_in_set(row.get("nombre"), row.get("database"), elastic_set):
            update_rows.append({"eid": row["eid"], "origen": "Microservicios"})
        else:
            unresolved.append(row)

    logger.info(
        "After Elasticsearch pass: %d Microservicios, %d unresolved",
        len(update_rows),
        len(unresolved),
    )

    # ── 4. Build DIS set from Hive (only if there are unresolved tables) ─ #
    dis_set: Set[str] = set()
    if unresolved:
        hive_cfg = get_impala_config()
        logger.info("Connecting to Impala at %s:%s …", hive_cfg["host"], hive_cfg["port"])
        logger.debug(
            "Impala config: host=%s, port=%s, user=%s",
            hive_cfg["host"],
            hive_cfg["port"],
            hive_cfg["user"],
        )
        try:
            with HiveClient(**hive_cfg) as hive:
                dis_set = hive.get_dis_tables()
            logger.info("DIS set built: %d distinct table identifiers", len(dis_set))
        except Exception as exc:
            msg = f"Hive query failed: {exc}"
            logger.error(msg, exc_info=True)
            result["errors"].append(msg)
            # All unresolved will fall to "Proceso no estándar".
    else:
        logger.info("No unresolved tables – skipping Hive query")

    # ── 5 & 6. Second-pass classification: DIS or no-estándar ────────── #
    logger.debug("Second-pass: classifying %d unresolved nodes against DIS set...", len(unresolved))
    for row in unresolved:
        if _is_in_set(row.get("nombre"), row.get("database"), dis_set):
            update_rows.append({"eid": row["eid"], "origen": "DIS"})
        else:
            update_rows.append({"eid": row["eid"], "origen": "Proceso no estándar"})

    # Tally results
    result["microservicios"] = sum(
        1 for r in update_rows if r["origen"] == "Microservicios"
    )
    result["dis"]         = sum(1 for r in update_rows if r["origen"] == "DIS")
    result["no_estandar"] = sum(
        1 for r in update_rows if r["origen"] == "Proceso no estándar"
    )

    logger.info(
        "Classification complete — Microservicios: %d | DIS: %d | Proceso no estándar: %d",
        result["microservicios"],
        result["dis"],
        result["no_estandar"],
    )

    # ── 7. Batch-update Neo4j ─────────────────────────────────────────── #
    try:
        neo4j.update_origen_batch(update_rows)
        logger.info("Neo4j update complete.")
    except Exception as exc:
        msg = f"Failed to update Neo4j: {exc}"
        logger.error(msg, exc_info=True)
        result["errors"].append(msg)
    finally:
        neo4j.close()

    # Final summary
    logger.info("")
    logger.info("=== OrigenToGraph Pipeline Summary ===")
    logger.info(
        "Classified %d / %d Tabla nodes (errors: %d)",
        result["microservicios"] + result["dis"] + result["no_estandar"],
        result["total"],
        len(result["errors"]),
    )

    return result
