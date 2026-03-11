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

Connection uses pyhive (thrift / HiveServer2).
"""

import logging
from typing import Set

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 60  # ≈ 2 months

# DIS audit tables (all share the same schema: must have `tabla` + `fecha_proceso`)
_DIS_AUDIT_TABLES = [
    "test.log_drs_confirmados_snap",
    "test.log_drs_confirmados_kudu",
    "test.log_drs_confirmados_refinados",
]

# Single optimised query: one UNION ALL → one Hive job instead of 3.
_DIS_QUERY = """
SELECT DISTINCT tabla
FROM (
    {union_parts}
) _dis_union
WHERE tabla IS NOT NULL
""".strip()

_UNION_PART = (
    "SELECT tabla "
    "FROM {table} "
    "WHERE fecha_proceso >= date_sub(current_date(), {days})"
)


def _build_query() -> str:
    """Build the UNION ALL query across all DIS audit tables."""
    parts = [
        _UNION_PART.format(table=t, days=LOOKBACK_DAYS)
        for t in _DIS_AUDIT_TABLES
    ]
    return _DIS_QUERY.format(union_parts="\n    UNION ALL\n    ".join(parts))


def _normalise(value: str | None) -> str | None:
    """Lowercase and strip; return None if blank."""
    if not value:
        return None
    v = value.strip().lower()
    return v if v else None


class HiveClient:
    """
    Thin wrapper around a pyhive HiveServer2 connection.

    Opens ONE connection per pipeline run, executes the UNION ALL query
    across all DIS audit tables, and returns a set of normalised table keys.
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 10000,
        database: str = "default",
        auth: str = "CUSTOM",
    ) -> None:
        try:
            from impala.dbapi import connect  # local import – optional dep
        except ImportError as exc:
            raise ImportError(
                "The 'impyla' package is required. "
                "Install it with: uv add impyla"
            ) from exc

        logger.debug("Connecting to HiveServer2 via impyla at %s:%s ...", host, port)
        self._conn = connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database,
            auth_mechanism=auth,
        )
        logger.debug("HiveServer2 connection established")

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
        Execute a single UNION ALL query across all DIS audit tables and
        return a set of normalised table identifiers (lowercased).

        The normalised form is "db.table" or "table" depending on the value
        stored in the `tabla` column.
        """
        query = _build_query()
        logger.info("Executing DIS UNION ALL query (lookback=%d days):", LOOKBACK_DAYS)
        logger.debug("Query:\n%s", query)

        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        finally:
            cursor.close()

        results: Set[str] = set()
        for (tabla,) in rows:
            norm = _normalise(tabla)
            if norm:
                results.add(norm)

        logger.info(
            "Fetched %d distinct DIS table identifiers from Hive", len(results)
        )
        return results
