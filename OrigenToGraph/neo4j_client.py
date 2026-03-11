#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Neo4j client for OrigenToGraph.

Responsibilities:
- Fetch all Tabla nodes from Neo4j (with their identity, nombre, database).
- Batch-update the `origen` attribute on those nodes.
"""

import logging
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class Neo4jClient:

    # ------------------------------------------------------------------ #
    #  Cypher queries                                                       #
    # ------------------------------------------------------------------ #

    _GET_ALL_TABLAS = """
        MATCH (t:Tabla)
        RETURN
            elementId(t) AS eid,
            t.nombre     AS nombre,
            t.database   AS database
    """

    # UNWIND a list of {eid, origen} maps and SET the property in one TX.
    _UPDATE_ORIGEN_BATCH = """
        UNWIND $rows AS row
        MATCH (t:Tabla) WHERE elementId(t) = row.eid
        SET t.origen = row.origen
    """

    def __init__(self, uri: str, user: str, password: str) -> None:
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.debug("Neo4j driver created for %s", uri)

    def close(self) -> None:
        logger.debug("Closing Neo4j connection...")
        self._driver.close()
        logger.debug("Neo4j connection closed")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------ #
    #  Public methods                                                       #
    # ------------------------------------------------------------------ #

    def get_all_tablas(self) -> list[dict]:
        """
        Return all Tabla nodes as a list of dicts with keys:
          - eid      : internal Neo4j element id (str)
          - nombre   : table name (str)
          - database : database/schema name (str | None)
        """
        logger.debug("Fetching all Tabla nodes from Neo4j...")
        with self._driver.session() as session:
            result = session.run(self._GET_ALL_TABLAS)
            rows = [dict(r) for r in result]
        logger.info(
            "Fetched %d Tabla nodes from Neo4j (with %d having database defined)",
            len(rows),
            sum(1 for r in rows if r.get("database")),
        )
        return rows

    def update_origen_batch(self, rows: list[dict]) -> None:
        """
        Batch-set t.origen for each Tabla node.

        Args:
            rows: list of dicts with keys `eid` and `origen`.
        """
        if not rows:
            logger.warning("update_origen_batch called with empty list – nothing to do")
            return

        # Neo4j recommends chunks of ~10 000 rows per transaction for large sets.
        chunk_size = 5_000
        total_updated = 0
        ms_count = sum(1 for r in rows if r["origen"] == "Microservicios")
        dis_count = sum(1 for r in rows if r["origen"] == "DIS")
        ne_count = sum(1 for r in rows if r["origen"] == "Proceso no estándar")

        logger.debug(
            "Batching %d rows into chunks of %d (MS=%d, DIS=%d, NE=%d)",
            len(rows),
            chunk_size,
            ms_count,
            dis_count,
            ne_count,
        )

        with self._driver.session() as session:
            for start in range(0, len(rows), chunk_size):
                chunk = rows[start : start + chunk_size]
                logger.debug(
                    "Updating chunk [%d:%d] (%d rows)...",
                    start,
                    start + len(chunk),
                    len(chunk),
                )
                session.run(self._UPDATE_ORIGEN_BATCH, rows=chunk)
                total_updated += len(chunk)

        logger.info(
            "Updated `origen` on %d Tabla nodes (Microservicios=%d, DIS=%d, Proceso no estándar=%d)",
            total_updated,
            ms_count,
            dis_count,
            ne_count,
        )
