#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Elasticsearch client for OrigenToGraph.

Queries the `cron_microservicios_logs` index and returns the set of all
table identifiers (normalised as "db.table", lowercase) that belong to
the Microservicios origin, restricted to the last 2 months.

Each rule maps an alias_proceso / condition to the field(s) that hold the
bd.tabla value:

  Layer  | alias_proceso | extra condition                          | field(s)
  -------|---------------|------------------------------------------|----------------------------
  01 RAW | MS02          | log_message = "Ingesta RAW exitosa."     | table_raw
  02 CUR | MS02          | log_message = "Ingesta en CUR exitosa."  | table_cur
  03 REF | MS05          | nivel_de_log = "FIN"                     | spark_table
  03 REF | MS08          | nivel_de_log = "FIN"                     | database + inserthbase_tabla_mapping_refinado
  04 CUR | MS06          | nivel_de_log = "FIN"                     | table
  05 DM  | MS07          | nivel_de_log = "FIN"                     | database + table

For multi-field rules the composite key is built as "database.table" before
comparison.
"""

import logging
from typing import Set

logger = logging.getLogger(__name__)

INDEX = "cron_microservicios_logs"
DATE_FIELD = "fecha_evento"
# Maximum buckets per composite aggregation page
_PAGE_SIZE = 1_000


def _normalise(value: str | None) -> str | None:
    """Lowercase and strip a table identifier; return None if blank."""
    if not value:
        return None
    v = value.strip().lower()
    return v if v else None


def _combine(db: str | None, table: str | None) -> str | None:
    """Combine database + table → "db.table", or just table if db is None."""
    table = _normalise(table)
    if not table:
        return None
    db = _normalise(db)
    return f"{db}.{table}" if db else table


# ---------------------------------------------------------------------------
# Rule definitions
# ---------------------------------------------------------------------------

_RULES: list[dict] = [
    # ── single-field rules ────────────────────────────────────────────
    {
        "alias_proceso": "MS02",
        "extra_filter": {"term": {"log_message.keyword": "Ingesta RAW exitosa."}},
        "fields":         ("table_raw",),
        "keyword_fields": ("table_raw.keyword",),
    },
    {
        "alias_proceso": "MS02",
        "extra_filter": {"term": {"log_message.keyword": "Ingesta en CUR exitosa."}},
        "fields":         ("table_cur",),
        "keyword_fields": ("table_cur.keyword",),
    },
    {
        "alias_proceso": "MS05",
        "extra_filter": {"term": {"nivel_de_log.keyword": "FIN"}},
        "fields":         ("spark_table",),
        "keyword_fields": ("spark_table.keyword",),
    },
    {
        "alias_proceso": "MS06",
        "extra_filter": {"term": {"nivel_de_log.keyword": "FIN"}},
        "fields":         ("table",),
        "keyword_fields": ("table.keyword",),
    },
    # ── multi-field rules (database + table name) ──────────────────────
    {
        "alias_proceso": "MS08",
        "extra_filter": {"term": {"nivel_de_log.keyword": "FIN"}},
        "fields":         ("database", "inserthbase_tabla_mapping_refinado"),
        "keyword_fields": ("database.keyword", "inserthbase_tabla_mapping_refinado.keyword"),
    },
    {
        "alias_proceso": "MS07",
        "extra_filter": {"term": {"nivel_de_log.keyword": "FIN"}},
        "fields":         ("database", "table"),
        "keyword_fields": ("database.keyword", "table.keyword"),
    },
]


class ElasticClient:
    """
    Thin wrapper around the `elasticsearch` Python client that fetches all
    distinct Microservicios table names via composite aggregations.
    """

    def __init__(self, host: str, port: int, username: str, password: str, lookback_days: int = 60) -> None:
        try:
            from elasticsearch import Elasticsearch  # local import – optional dep
        except ImportError as exc:
            raise ImportError(
                "The 'elasticsearch' package is required. "
                "Install it with: uv add elasticsearch"
            ) from exc

        self._es = Elasticsearch(
            f"http://{host}:{port}",
            basic_auth=(username, password),
            request_timeout=60,
        )
        # ES date math string, e.g. "now-60d/d"
        self._date_filter = {"range": {DATE_FIELD: {"gte": f"now-{lookback_days}d/d"}}}
        logger.debug("Elasticsearch client created for %s:%s (lookback=%dd)", host, port, lookback_days)

    def close(self) -> None:
        self._es.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _composite_agg_pages(
        self, query: dict, sources: list[dict]
    ) -> list[dict]:
        """
        Run a composite aggregation and page through ALL buckets.

        Returns the full list of bucket dicts.
        """
        buckets: list[dict] = []
        after_key = None
        pages = 0

        while True:
            agg_body: dict = {
                "composite": {"size": _PAGE_SIZE, "sources": sources}
            }
            if after_key is not None:
                agg_body["composite"]["after"] = after_key

            logger.debug(
                "Fetching composite agg page (after_key=%s, fields=%s)",
                after_key,
                [s for s in sources],
            )
            resp = self._es.search(
                index=INDEX,
                body={
                    "size": 0,
                    "query": query,
                    "aggs": {"values": agg_body},
                },
            )

            page = resp["aggregations"]["values"]["buckets"]
            buckets.extend(page)
            pages += 1
            logger.debug("Page %d: got %d buckets", pages, len(page))

            if len(page) < _PAGE_SIZE:
                break  # last page
            after_key = page[-1]["key"]

        logger.debug(
            "Composite agg complete: %d pages, %d total buckets", pages, len(buckets)
        )
        return buckets

    def _fetch_single_field(self, rule: dict, date_filter: dict) -> Set[str]:
        """Fetch distinct values for a single-field rule."""
        field = rule["fields"][0]
        keyword_field = rule["keyword_fields"][0]

        logger.debug(
            "Fetching single-field rule: alias_proceso=%s, field=%s, keyword_field=%s",
            rule["alias_proceso"],
            field,
            keyword_field,
        )

        query = {
            "bool": {
                "filter": [
                    date_filter,
                    {"term": {"alias_proceso.keyword": rule["alias_proceso"]}},
                    rule["extra_filter"],
                    {"exists": {"field": field}},
                ]
            }
        }
        sources = [{"val": {"terms": {"field": keyword_field}}}]

        results: Set[str] = set()
        try:
            for bucket in self._composite_agg_pages(query, sources):
                raw = bucket["key"].get("val")
                norm = _normalise(raw)
                if norm:
                    results.add(norm)
        except Exception as exc:
            logger.error(
                "Error fetching single-field rule alias_proceso=%s, field=%s: %s",
                rule["alias_proceso"],
                field,
                exc,
                exc_info=True,
            )

        logger.info(
            "Rule %s/%s single-field '%s': %d distinct values",
            rule["alias_proceso"],
            rule["extra_filter"],
            field,
            len(results),
        )
        return results

    def _fetch_multi_field(self, rule: dict, date_filter: dict) -> Set[str]:
        """Fetch combined 'database.table' for a dual-field rule."""
        db_field, tbl_field = rule["fields"]
        db_keyword, tbl_keyword = rule["keyword_fields"]

        query = {
            "bool": {
                "filter": [
                    date_filter,
                    {"term": {"alias_proceso.keyword": rule["alias_proceso"]}},
                    rule["extra_filter"],
                    {"exists": {"field": tbl_field}},
                ]
            }
        }
        sources = [
            {"db":  {"terms": {"field": db_keyword}}},
            {"tbl": {"terms": {"field": tbl_keyword}}},
        ]

        results: Set[str] = set()
        for bucket in self._composite_agg_pages(query, sources):
            db_val  = bucket["key"].get("db")
            tbl_val = bucket["key"].get("tbl")
            combined = _combine(db_val, tbl_val)
            if combined:
                results.add(combined)

        logger.debug(
            "Rule %s multi-field '%s'+'%s': %d distinct values",
            rule["alias_proceso"],
            db_field,
            tbl_field,
            len(results),
        )
        return results

    # ------------------------------------------------------------------ #
    #  Public interface                                                     #
    # ------------------------------------------------------------------ #

    def get_microservicios_tables(self) -> Set[str]:
        """
        Return a set of normalised table identifiers ("db.table" or "table",
        all lowercase) that appear in Microservicios logs within the lookback window.
        """
        date_filter = self._date_filter
        all_tables: Set[str] = set()

        for rule in _RULES:
            try:
                if len(rule["fields"]) == 1:
                    tables = self._fetch_single_field(rule, date_filter)
                else:
                    tables = self._fetch_multi_field(rule, date_filter)
                all_tables |= tables
            except Exception:
                logger.warning(
                    "Failed to fetch rule for alias_proceso=%s, extra=%s",
                    rule["alias_proceso"],
                    rule["extra_filter"],
                    exc_info=True,
                )

        logger.info(
            "Total distinct Microservicios table identifiers from Elasticsearch: %d",
            len(all_tables),
        )
        return all_tables
