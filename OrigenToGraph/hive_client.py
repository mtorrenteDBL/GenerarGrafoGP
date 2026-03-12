#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hive client for OrigenToGraph.

Queries the DIS audit tables (Kudu / HiveServer2) and returns the set of all
table identifiers confirmed as DIS-loaded within the last 2 months.

Audit tables queried:
  - test.log_drs_confirmados_snap
  - test.log_drs_confirmados_kudu
  - test.log_drs_confirmados_refinados

The `tabla` field in each table holds the "db.table" identifier.
We issue a single UNION ALL query to minimise round-trips (Hive is slow).

Connection uses impyla (HiveServer2 / Thrift).
"""

import logging
from typing import Set

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 60  # ≈ 2 months

# DIS audit tables and their column names for tabla
_DIS_SOURCES = [
    ("test.log_drs_confirmados", "tabla_raw"),
    ("test.log_drs_confirmados", "tabla_cur"),
    ("test.log_drs_confirmados_snap", "tabla"),
    ("test.log_drs_confirmados_kudu", "tabla"),
    ("test.log_drs_confirmados_refinados", "tabla"),
]

# Single optimised query: one UNION ALL → one Hive job instead of 5.
# Notes:
# - IS NOT NULL is pushed INTO each branch (Hive rejects aliases starting with '_')
# - Outer subquery uses a plain alias `t`
# - fecha_proceso is stored as a varchar in yyyyMMdd format; comparison is done directly.
_UNION_PART = (
    "SELECT CAST({col} AS STRING) AS tabla "
    "FROM {table} "
    "WHERE {col} IS NOT NULL "
    "AND fecha_proceso > '20260213'"
)

_DIS_QUERY = """
SELECT DISTINCT tabla
FROM (
    {union_parts}
) t
""".strip()


def _build_query() -> str:
    """Build the UNION ALL query across all DIS audit sources."""
    parts = [
        _UNION_PART.format(table=table, col=col)
        for table, col in _DIS_SOURCES
    ]
    return _DIS_QUERY.format(union_parts="\n    UNION ALL\n    ".join(parts))


def _build_single_query(table: str, col: str) -> str:
    """Build a query for a single DIS audit source."""
    return (
        f"SELECT DISTINCT CAST({col} AS STRING) AS tabla "
        f"FROM {table} "
        f"WHERE {col} IS NOT NULL "
        f"AND fecha_proceso > '20260213'"
    )


def _normalise(value: str | None) -> str | None:
    """Lowercase and strip; return None if blank."""
    if not value:
        return None
    v = value.strip().lower()
    return v if v else None


class HiveClient:
    """
    Thin wrapper around an impyla HiveServer2 connection.

    Opens ONE connection per pipeline run, executes the UNION ALL query
    across all DIS audit tables, and returns a set of normalised table keys.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int = 21050,
        timeout: int = 600,
    ) -> None:
        try:
            from impala.dbapi import connect  # local import – optional dep
        except ImportError as exc:
            raise ImportError(
                "The 'impyla' package is required. "
                "Install it with: uv add impyla"
            ) from exc

        logger.debug("Connecting to Impala via impyla at %s:%s ...", host, port)
        self._conn = connect(
            host=host,
            port=port,
            user=user,
            password=password,
            timeout=timeout,
        )
        logger.debug("Impala connection established")

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            logger.debug("HiveClient.close() raised (ignored)", exc_info=True)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def get_dis_tables(self) -> Set[str]:
        """
        Execute queries across all DIS audit tables and return a set of
        normalised table identifiers (lowercased).

        Strategy:
        1. Try a single UNION ALL query (fastest).
        2. If compilation fails (e.g. Hive Metastore varchar SerDe issue),
           fall back to querying each table individually, skipping any that fail.
        3. Log a warning if all tables fail; return an empty set rather than
           crashing the pipeline (all unresolved nodes will be classified as
           "Proceso no estándar").
        """
        query = _build_query()
        logger.info("Executing DIS UNION ALL query (date filter: fecha_proceso > '20260213'):")
        logger.debug("Full query:\n%s", query)

        rows = self._try_execute(query)
        if rows is None:
            # UNION ALL failed – query each source individually
            logger.warning(
                "UNION ALL query failed (likely a Hive Metastore varchar SerDe issue). "
                "Falling back to per-source queries."
            )
            rows = []
            successful = 0
            for table, col in _DIS_SOURCES:
                single_query = _build_single_query(table, col)
                logger.debug("Trying individual query for: %s.%s", table, col)
                table_rows = self._try_execute(single_query, table_name=f"{table}.{col}")
                if table_rows is not None:
                    rows.extend(table_rows)
                    successful += 1
                    logger.info("  %s.%s → %d rows", table, col, len(table_rows))
                else:
                    logger.warning("  %s.%s → skipped (query failed)", table, col)
            if successful == 0:
                logger.error(
                    "All %d DIS audit sources failed to query. "
                    "This is a server-side Hive Metastore / SerDe issue "
                    "(varchar column type not supported). "
                    "All unresolved nodes will be classified as 'Proceso no estándar'.",
                    len(_DIS_SOURCES),
                )
                return set()
        else:
            logger.info("UNION ALL query returned %d rows", len(rows))

        results: Set[str] = set()
        skipped = 0
        for (tabla,) in rows:
            norm = _normalise(tabla)
            if norm:
                results.add(norm)
            else:
                skipped += 1

        logger.info(
            "Fetched %d distinct DIS table identifiers from Hive (skipped %d null/blank)",
            len(results),
            skipped,
        )
        return results

    def _try_execute(
        self, query: str, table_name: str = "UNION ALL"
    ):
        """
        Execute *query* and return the fetched rows, or None if it fails.
        Logs the error at WARNING level so the caller can decide how to proceed.
        """
        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as exc:
            logger.warning(
                "Hive query failed for %s: %s",
                table_name,
                exc,
            )
            return None
        finally:
            cursor.close()
