from __future__ import annotations

import re
from dataclasses import dataclass
from sql_utils import SQLUtils

SQL_STATEMENT_REGEX = re.compile(
    r"^\s*(select|insert|update|delete|merge|with)\s+.+\b(from|into|set|where)\b",
    re.IGNORECASE | re.DOTALL
)


@dataclass(frozen=True, eq=True)
class Table:
    """Represents a database table with its name, database schema, and zone."""
    name: str
    database: str  # may be empty string
    zone: str

    @property
    def clave(self) -> str:
        return f"{self.zone}::{self.name}"

def infer_from_text(s: str | None) -> str | None:
    """Heuristic to guess zone from a raw string (table name or attribute key)."""
    if not s:
        return None

    txt = str(s).lower()

    # Specific overrides (substring-based on purpose)
    if "kudu" in txt or "insertkudu" in txt:
        return "Consumo"
    if "data_analytics" in txt:
        return "data_analytics"
    if "sftp" in txt or "ftp" in txt:
        return "SFTP"
    if "archivo" in txt:
        return "Landing"

    # Regex checks
    checks = [
        # datamart_<name> → return full token
        (
            r"(^|[^a-z0-9])(datamart_[a-z0-9_]+)($|[^a-z0-9])",
            lambda m: m.group(2),
        ),

        # Zone identifiers
        (r"(^|[^a-z0-9])(raw|1raw)($|[^a-z0-9])", "Raw"),
        (r"(^|[^a-z0-9])(ref|refinado|3ref)($|[^a-z0-9])", "Refinado"),
        (r"(^|[^a-z0-9])(cur|curado|2cur)($|[^a-z0-9])", "Curado"),
        (r"(^|[^a-z0-9])(land|lnd|landing|1land|archivo)($|[^a-z0-9])", "Landing"),
        (r"(^|[^a-z0-9])(con|consumo|4con)($|[^a-z0-9])", "Consumo"),

        # Generic semantic hints
        (r"(^|[^a-z0-9])(dm|mart|datamart)($|[^a-z0-9])", "Datamart"),
        (r"(^|[^a-z0-9])(archivo|file|path|directorio|carpeta|folder)($|[^a-z0-9])", "file"),
    ]

    for pattern, result in checks:
        m = re.search(pattern, txt)
        if m:
            return result(m) if callable(result) else result

    return "Origen"

def _get_processor_properties(processor: dict) -> dict:
    """Extract properties dict from a processor, handling both flat and config-nested formats."""
    p = processor.get("component", processor)
    return (
        ((p.get("config") or {}).get("properties") or {})
        or (p.get("properties") or {})
        or {}
    )


def _split_db_table(t: str) -> tuple[str, str]:
    """Split 'db.table' into (name, database). Returns (name, '') if no dot."""
    parts = t.split('.', 1)
    if len(parts) == 2:
        return parts[1], parts[0]
    return parts[0], ''


def _is_valid_table(t: str) -> bool:
    """Returns False for variable placeholders and file-like paths."""
    if t.startswith("var_") or t.startswith("pr_var_"):
        return False
    # After _restore_name, NiFi variables appear as $VAR inside the name
    if '$' in t:
        return False
    if infer_from_text(t) == "file":
        return False
    return True


def extract_sql_from_processor(processor: dict) -> list[str]:
    """Extract SQL statements from a processor's properties."""
    sql_statements = []
    props = _get_processor_properties(processor)
    for value in props.values():
        if not isinstance(value, str):
            continue
        if SQL_STATEMENT_REGEX.search(value):
            sql_statements.append(value)
    return sql_statements


def extract_tables_from_processors(processors: list[dict]) -> tuple[set[Table], Table | None]:
    """
    Scan all processors in a process group for SQL statements.
    Returns (sources, destination) as Table objects, filtering out invalid entries.
    """
    all_sources: set[Table] = set()
    destination: Table | None = None

    for processor in processors:
        for sql in extract_sql_from_processor(processor):
            raw_sources, raw_dest = SQLUtils.extract_tables_and_dest(sql)

            for t in raw_sources:
                if _is_valid_table(t):
                    name, db = _split_db_table(t)
                    zone = infer_from_text(t) or "Origen"
                    all_sources.add(Table(name=name, database=db, zone=zone))

            if raw_dest and _is_valid_table(raw_dest):
                name, db = _split_db_table(raw_dest)
                zone = infer_from_text(raw_dest) or "Origen"
                destination = Table(name=name, database=db, zone=zone)

    return all_sources, destination
