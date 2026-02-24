#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Pattern

# =========================
# Tabla & Relationships for SQL Mapping
# =========================

MERGE_TABLA = """
MERGE (t:Tabla {clave: $clave})
ON CREATE SET t.nombre = $nombre, t.zona = $zona, t.database = $database
ON MATCH  SET t.nombre = $nombre, t.zona = $zona, t.database = $database
RETURN t
"""

MERGE_ALIMENTA_A = """
MATCH (t_from:Tabla {clave: $from_clave})
MATCH (t_into:Tabla {clave: $into_clave})
MERGE (t_from)-[:ALIMENTA_A]->(t_into)
"""

MERGE_LEE_DE = """
MATCH (pg:ProcessGroup {nifi_id: $nifi_id, flow_name: $flow_name})
MATCH (t:Tabla {clave: $clave})
MERGE (pg)-[:LEE_DE]->(t)
"""

MERGE_ESCRIBE_EN = """
MATCH (pg:ProcessGroup {nifi_id: $nifi_id, flow_name: $flow_name})
MATCH (t:Tabla {clave: $clave})
MERGE (pg)-[:ESCRIBE_EN]->(t)
"""

# =========================
# Script & Relationships
# =========================

MERGE_SCRIPT = """
MERGE (s:Script {clave: $clave})
ON CREATE SET s.value = $value, s.ref_type = $ref_type, s.prop_name = $prop_name, s.extra = $extra
ON MATCH  SET s.value = $value, s.ref_type = $ref_type, s.prop_name = $prop_name, s.extra = $extra
RETURN s
"""

MERGE_EXECUTES = """
MATCH (pg:ProcessGroup {nifi_id: $nifi_id, flow_name: $flow_name})
MATCH (s:Script {clave: $clave})
MERGE (pg)-[:EXECUTES {processor_name: $processor_name, processor_type: $processor_type, categoria: $categoria}]->(s)
"""

# =========================
# Neo4j Connection
# =========================
NEO4J_URI = "bolt://172.30.213.52:10000"
NEO4J_USER = "neo4j"
NEO4J_PASS = "Dblandit2025"

# NEO4J_URI = "bolt://localhost:7687"
# NEO4J_USER = "neo4j"
# NEO4J_PASS = "12345678"

# =========================
# Defaults (whitelist ENFORCADA por defecto)
# =========================
DEFAULT_NOT_ALLOWED = {
    "failure", "retry", "no retry", "no_retry",
    "backpressure-full", "back pressure full",
    "penalize", "expired", "timeout",
    "dropped", "dead", "invalid", "requeue"
}
DEFAULT_ALLOWED = {
    "success", "matched", "unmatched",
    "original", "split", "merged"
}
DEFAULT_LOGGING_NAME_REGEX = r"\b(log|logger|logging)\b"
DEFAULT_LOGGING_TYPES = {"LogAttribute"}

# =========================
# Kafka Processor Hints
# =========================
KAFKA_PUBLISH_HINTS = ("PublishKafka", "PublishKafkaRecord")
KAFKA_CONSUME_HINTS = ("ConsumeKafka", "ConsumeKafkaRecord")

# =========================
# Cypher Queries
# =========================

MERGE_PG = """
MERGE (pg:ProcessGroup {nifi_id: $nifi_id, flow_name: $flow_name})
ON CREATE SET pg.name = $name, pg.is_root = $is_root
ON MATCH  SET pg.name = $name, pg.is_root = $is_root
RETURN pg
"""

MERGE_REL_PG_CHILD = """
MATCH (parent:ProcessGroup {nifi_id: $parent_id, flow_name: $flow_name})
MATCH (child:ProcessGroup  {nifi_id: $child_id, flow_name: $flow_name})
MERGE (parent)-[:CONTIENE]->(child)
"""

MERGE_ATLAS_TERM = """
MERGE (at:`Atlas Term` {nombre: $nombre})
RETURN at
"""

# PG -> Atlas Term  (PREPARA)
MERGE_REL_PG_ATLAS = """
MATCH (pg:ProcessGroup {nifi_id: $pg_id, flow_name: $flow_name})
MATCH (at:`Atlas Term` {nombre: $nombre})
MERGE (pg)-[:PREPARA]->(at)
"""

# PG -> Kafka (PRODUCE)  | Nodo Kafka por (topic, role, pg_id, flow_name)
MERGE_KAFKA_AND_LINK_PRODUCE = """
MATCH (pg:ProcessGroup {nifi_id: $pg_id, flow_name: $flow_name})
MERGE (k:Kafka {topic: $topic, role: $role, pg_id: $pg_id, flow_name: $flow_name})
  ON CREATE SET k.topic = $topic, k.role = $role, k.pg_id = $pg_id, k.flow_name = $flow_name,
               k.group_id = coalesce($group_id, null)
  ON MATCH  SET k.group_id = coalesce($group_id, k.group_id)
MERGE (pg)-[:PRODUCE]->(k)
RETURN k
"""

# PG -> Kafka (CONSUME)  | Nodo Kafka por (topic, role, pg_id, flow_name)
MERGE_KAFKA_AND_LINK_CONSUME = """
MATCH (pg:ProcessGroup {nifi_id: $pg_id, flow_name: $flow_name})
MERGE (k:Kafka {topic: $topic, role: $role, pg_id: $pg_id, flow_name: $flow_name})
  ON CREATE SET k.topic = $topic, k.role = $role, k.pg_id = $pg_id, k.flow_name = $flow_name,
               k.group_id = coalesce($group_id, null)
  ON MATCH  SET k.group_id = coalesce($group_id, k.group_id)
MERGE (pg)-[:CONSUME]->(k)
RETURN k
"""

# Kafka PRODUCE -> CONSUME por mismo topic (puede cruzar flows)
MERGE_REL_ENVIA_A = """
MATCH (src:Kafka {topic: $topic, role: 'PRODUCE', pg_id: $src_pg, flow_name: $src_flow})
MATCH (dst:Kafka {topic: $topic, role: 'CONSUME', pg_id: $dst_pg, flow_name: $dst_flow})
MERGE (src)-[:ENVIA_A {topic: $topic}]->(dst)
"""
MERGE_ARCHIVO = '''
MERGE (a:Archivo {nombre: $path, zona: "Landing"})
RETURN a
'''

# PG -> Archivo (ESCRIBE_EN)
MERGE_REL_PG_ESCRIBE_ARCHIVO = """
MATCH (pg:ProcessGroup {nifi_id: $pg_id, flow_name: $flow_name})
MATCH (a:Archivo {nombre: $path, zona: "Landing"})
MERGE (pg)-[:ESCRIBE_EN]->(a)
"""

# Archivo -> PG (LEE_DE)
MERGE_REL_PG_LEE_ARCHIVO = """
MATCH (pg:ProcessGroup {nifi_id: $pg_id, flow_name: $flow_name})
MATCH (a:Archivo {nombre: $path, zona: "Landing"})
MERGE (pg)-[:LEE_DE]->(a)
"""


@dataclass
class Config:
    """Runtime configuration for graph building."""
    enforce_allowed: bool = True
    allowed_set: set[str] = field(default_factory=set)
    not_allowed_set: set[str] = field(default_factory=set)
    logging_name_regex: Pattern[str] = field(default_factory=lambda: re.compile(DEFAULT_LOGGING_NAME_REGEX, re.IGNORECASE))
    logging_type_set: set[str] = field(default_factory=set)
    stop_on_ua_override: bool = False

    @classmethod
    def from_args(cls, args: Any) -> Config:
        """Create a Config from parsed command-line arguments."""
        return cls(
            enforce_allowed=not bool(getattr(args, 'no_enforce_allowed', False)),
            allowed_set={s.strip().lower() for s in args.allowed_rels.split(",") if s.strip()},
            not_allowed_set={s.strip().lower() for s in args.not_allowed_rels.split(",") if s.strip()},
            logging_name_regex=re.compile(args.logging_name_regex, re.IGNORECASE),
            logging_type_set={s.strip() for s in args.logging_types.split(",") if s.strip()},
            stop_on_ua_override=bool(getattr(args, 'stop_on_ua_override', False)),
        )


# Keep backward compatibility with old constructor
def _config_compat_init(self: Config, args: Any) -> None:
    """Backward-compatible init from args."""
    cfg = Config.from_args(args)
    self.enforce_allowed = cfg.enforce_allowed
    self.allowed_set = cfg.allowed_set
    self.not_allowed_set = cfg.not_allowed_set
    self.logging_name_regex = cfg.logging_name_regex
    self.logging_type_set = cfg.logging_type_set
    self.stop_on_ua_override = cfg.stop_on_ua_override