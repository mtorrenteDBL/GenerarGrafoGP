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

# DIS audit tables (all share the same schema: must have `tabla` + `fecha_proceso`)
_DIS_AUDIT_TABLES = [
    "test.log_drs_confirmados_snap",
    "test.log_drs_confirmados_kudu",
    "test.log_drs_confirmados_refinados",
]

# Single optimised query: one UNION ALL → one Hive job instead of 3.
# Notes:
# - IS NOT NULL is pushed INTO each branch (Hive rejects aliases starting with '_')
# - Outer subquery uses a plain alias `t`
# - fecha_proceso is stored as a varchar in yyyyMMdd format; explicit cast avoids
#   implicit varchar↔DATE coercion which triggers a SerDe error in some Hive versions.
_UNION_PART = (
    "SELECT CAST(tabla AS STRING) AS tabla "
    "FROM {table} "
    "WHERE CAST(from_unixtime(unix_timestamp(fecha_proceso, 'yyyyMMdd')) AS DATE) "
    "    >= date_sub(current_date(), {days}) "
    "AND tabla IS NOT NULL"
)

_DIS_QUERY = """
SELECT DISTINCT tabla
FROM (
    {union_parts}
) t
""".strip()


def _build_query() -> str:
    """Build the UNION ALL query across all DIS audit tables."""
    parts = [
        _UNION_PART.format(table=tbl, days=LOOKBACK_DAYS)
        for tbl in _DIS_AUDIT_TABLES
    ]
    return _DIS_QUERY.format(union_parts="\n    UNION ALL\n    ".join(parts))


def _build_single_query(table: str) -> str:
    """Build a query for a single DIS audit table."""
    return (
        "SELECT DISTINCT CAST(tabla AS STRING) AS tabla "
        f"FROM {table} "
        f"WHERE CAST(from_unixtime(unix_timestamp(fecha_proceso, 'yyyyMMdd')) AS DATE) "
        f"    >= date_sub(current_date(), {LOOKBACK_DAYS}) "
        "AND tabla IS NOT NULL"
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
        username: str,
        password: str,
        port: int = 10000,
        database: str = "default",
        auth_mechanism: str = "PLAIN",
        timeout: int = 600,
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
            auth_mechanism=auth_mechanism,
            timeout=timeout,
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
        logger.info("Executing DIS UNION ALL query (lookback=%d days):", LOOKBACK_DAYS)
        logger.debug("Full query:\n%s", query)

        rows = self._try_execute(query)
        if rows is None:
            # UNION ALL failed – query each table individually
            logger.warning(
                "UNION ALL query failed (likely a Hive Metastore varchar SerDe issue). "
                "Falling back to per-table queries."
            )
            rows = []
            successful = 0
            for table in _DIS_AUDIT_TABLES:
                single_query = _build_single_query(table)
                logger.debug("Trying individual query for: %s", table)
                table_rows = self._try_execute(single_query, table_name=table)
                if table_rows is not None:
                    rows.extend(table_rows)
                    successful += 1
                    logger.info("  %s → %d rows", table, len(table_rows))
                else:
                    logger.warning("  %s → skipped (query failed)", table)
            if successful == 0:
                logger.error(
                    "All %d DIS audit tables failed to query. "
                    "This is a server-side Hive Metastore / SerDe issue "
                    "(varchar column type not supported). "
                    "All unresolved nodes will be classified as 'Proceso no estándar'.",
                    len(_DIS_AUDIT_TABLES),
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
