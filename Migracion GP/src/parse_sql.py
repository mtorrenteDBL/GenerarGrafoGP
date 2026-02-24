import re
import sqlparse

from typing import Optional, List

from .logger import setup_logger
logger = setup_logger(name='parse_sql')


def clean_table_name(raw: str) -> str:
    if not raw:
        return ""
    name = raw.strip()
    name = name.split()[0]
    name = name.replace("`", "").replace("[", "").replace("]", "")
    if name.startswith('"') and name.endswith('"') and len(name) >= 2:
        name = name[1:-1]
    return name


def looks_like_sql(v: str) -> bool:
    s = (v or "").lower()
    return (" select " in f" {s} ") or (" from " in f" {s} ") or (" join " in f" {s} ") or (" insert " in f" {s} ")
