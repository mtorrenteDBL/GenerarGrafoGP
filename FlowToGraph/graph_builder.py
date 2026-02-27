#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import logging
from pathlib import Path
from collections import defaultdict
from typing import TYPE_CHECKING

from neo4j import GraphDatabase
from config import (
    NEO4J_URI, NEO4J_USER, NEO4J_PASS,
    MERGE_PG, MERGE_REL_PG_CHILD, MERGE_ATLAS_TERM, MERGE_REL_PG_ATLAS,
    MERGE_KAFKA_AND_LINK_PRODUCE, MERGE_KAFKA_AND_LINK_CONSUME,
    MERGE_REL_ENVIA_A,
    MERGE_ARCHIVO, MERGE_REL_PG_ESCRIBE_ARCHIVO, MERGE_REL_PG_LEE_ARCHIVO,
    MERGE_TABLA, MERGE_ALIMENTA_A, MERGE_LEE_DE, MERGE_ESCRIBE_EN,
    MERGE_SCRIPT, MERGE_EXECUTES
)

if TYPE_CHECKING:
    from nifi_parser import FlowIndex

log = logging.getLogger(__name__)

# =========================
# Neo4j Setup
# =========================

def ensure_constraints(session):
    # Neo4j 4.x/5.x
    stmts_modern = [
        "CREATE CONSTRAINT process_group_id_flow IF NOT EXISTS FOR (pg:ProcessGroup) REQUIRE (pg.nifi_id, pg.flow_name) IS UNIQUE",
        "CREATE CONSTRAINT atlas_term_nombre IF NOT EXISTS FOR (at:`Atlas Term`) REQUIRE at.nombre IS UNIQUE",
        "CREATE CONSTRAINT kafka_topic_role_pg_flow IF NOT EXISTS FOR (k:Kafka) REQUIRE (k.topic, k.role, k.pg_id, k.flow_name) IS UNIQUE",
        "CREATE CONSTRAINT archivo_nombre_zona IF NOT EXISTS FOR (a:Archivo) REQUIRE (a.nombre, a.zona) IS UNIQUE",
        "CREATE INDEX kafka_topic_idx IF NOT EXISTS FOR (k:Kafka) ON (k.topic)",
        "CREATE INDEX archivo_zona_idx IF NOT EXISTS FOR (a:Archivo) ON (a.zona)"
    ]
    for q in stmts_modern:
        try:
            session.run(q)
        except Exception:
            pass

    # Fallback Neo4j 3.x (simplified constraints)
    try:
        session.run("CREATE INDEX ON :ProcessGroup(nifi_id)")
    except Exception:
        pass
    try:
        session.run("CREATE INDEX ON :ProcessGroup(flow_name)")
    except Exception:
        pass
    try:
        session.run("CREATE CONSTRAINT ON (at:`Atlas Term`) ASSERT at.nombre IS UNIQUE")
    except Exception:
        pass
    try:
        session.run("CREATE CONSTRAINT ON (a:Archivo) ASSERT a.path IS UNIQUE")
    except Exception:
        pass
    try:
        session.run("CREATE INDEX ON :Kafka(topic)")
    except Exception:
        pass


def _extract_zona_from_path(path: str) -> str | None:
    """
    Extract 'zona' from a file path based on common patterns.
    
    Patterns recognized:
    - /user/admin/prod/bsf/00-landing/... -> "landing"
    - /user/admin/prod/bsf/01-raw/... -> "raw"
    - /user/admin/prod/bsf/02-trusted/... -> "trusted"
    - /user/admin/prod/bsf/03-ref/... -> "ref"
    - Other paths -> None
    """
    import re
    
    # Pattern for numbered zones: 00-landing, 01-raw, 02-trusted, 03-ref
    match = re.search(r'/\d{2}-([a-zA-Z_]+)/', path)
    if match:
        return match.group(1).lower()
    
    # Fallback patterns
    if '/landing/' in path.lower():
        return 'landing'
    if '/raw/' in path.lower():
        return 'raw'
    if '/trusted/' in path.lower():
        return 'trusted'
    if '/ref/' in path.lower():
        return 'ref'
    
    return None


def build_graph(root_id: str, root_name: str, flow_index: FlowIndex, script_dir: Path) -> dict[str, int]:
    """
    Build the graph in Neo4j from parsed NiFi flow data.
    
    Args:
        root_id: The NiFi ID of the root process group
        root_name: The name of the root process group
        flow_index: FlowIndex dataclass with all indexed flow data
        script_dir: Directory where to save the Atlas terms CSV
    
    Returns:
        Dictionary with counters of created elements
    """
    # Aggregated by topic -> {pgs}
    producers_by_topic: dict[str, set[str]] = defaultdict(set)
    consumers_by_topic: dict[str, set[str]] = defaultdict(set)
    for pid, topics in flow_index.publish_topics_by_proc.items():
        pg = flow_index.proc_to_pg.get(pid)
        if pg:
            for t in topics:
                producers_by_topic[t].add(pg)
    for pid, topics in flow_index.consume_topics_by_proc.items():
        pg = flow_index.proc_to_pg.get(pid)
        if pg:
            for t in topics:
                consumers_by_topic[t].add(pg)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    log.debug("Connected to Neo4j at %s", NEO4J_URI)
    counters = {
        "pg_nodes": 0, "pg_rels": 0,
        "atlas_nodes": 0, "atlas_rels": 0,
        "kafka_nodes": 0, "kafka_rels": 0,
        "envia_a_rels": 0,
        "archivo_nodes": 0, "escribe_rels": 0, "lee_rels": 0,
        "tabla_nodes": 0, "alimenta_rels": 0, "lee_tabla_rels": 0, "escribe_tabla_rels": 0,
        "script_nodes": 0, "executes_rels": 0
    }

    def _run(session, query, context: str = "", **params) -> bool:
        """Run a Cypher query, consume the result, return *True* on success."""
        try:
            result = session.run(query, **params)
            result.consume()          # ensures auto-commit tx is finalised
            return True
        except Exception as e:
            log.warning("Neo4j error [%s]: %s | params: %s", context, e, params)
            return False

    with driver.session() as session:
        ensure_constraints(session)
        log.debug("Neo4j constraints ensured")

        # 1) PGs y jerarquía
        pg_created = set()
        if _run(session, MERGE_PG, "MERGE_PG/root", nifi_id=root_id, name=root_name, is_root=True, flow_name=root_name):
            counters["pg_nodes"] += 1
            pg_created.add(root_id)

        for pg_node in flow_index.pg_hierarchy:
            if pg_node.id not in pg_created:
                if _run(session, MERGE_PG, f"MERGE_PG/{pg_node.name}",
                        nifi_id=pg_node.id, name=pg_node.name, is_root=False, flow_name=root_name):
                    counters["pg_nodes"] += 1
                    pg_created.add(pg_node.id)
            if pg_node.parent_id:
                if _run(session, MERGE_REL_PG_CHILD, f"CONTIENE/{pg_node.name}",
                        parent_id=pg_node.parent_id, child_id=pg_node.id, flow_name=root_name):
                    counters["pg_rels"] += 1

        # 2) Atlas (nodos) y PREPARA (PG->Atlas)
        for pid, terms in flow_index.terms_by_proc.items():
            pg = flow_index.proc_to_pg.get(pid)
            if not pg:
                continue
            for t in terms:
                if _run(session, MERGE_ATLAS_TERM, f"ATLAS/{t}", nombre=t):
                    counters["atlas_nodes"] += 1
                if _run(session, MERGE_REL_PG_ATLAS, f"PREPARA/{t}", pg_id=pg, nombre=t, flow_name=root_name):
                    counters["atlas_rels"] += 1

        # 3) Nodos Kafka y PG->Kafka (PRODUCE / CONSUME)
        for pid, topics in flow_index.publish_topics_by_proc.items():
            pg = flow_index.proc_to_pg.get(pid)
            if not pg:
                continue
            gid = flow_index.kafka_group_id_by_proc.get(pid)
            for t in topics:
                # Always pass group_id (None becomes null in Neo4j, coalesce handles it)
                if _run(session, MERGE_KAFKA_AND_LINK_PRODUCE, f"PRODUCE/{t}",
                        pg_id=pg, topic=t, role="PRODUCE", group_id=gid, flow_name=root_name):
                    counters["kafka_nodes"] += 1
                    counters["kafka_rels"] += 1

        for pid, topics in flow_index.consume_topics_by_proc.items():
            pg = flow_index.proc_to_pg.get(pid)
            if not pg:
                continue
            gid = flow_index.kafka_group_id_by_proc.get(pid)
            for t in topics:
                if _run(session, MERGE_KAFKA_AND_LINK_CONSUME, f"CONSUME/{t}",
                        pg_id=pg, topic=t, role="CONSUME", group_id=gid, flow_name=root_name):
                    counters["kafka_nodes"] += 1
                    counters["kafka_rels"] += 1

        # 4) Kafka PRODUCE -> CONSUME por topic (mismo o diferentes flows)
        for topic, src_pgs in producers_by_topic.items():
            dst_pgs = consumers_by_topic.get(topic, set())
            for src in src_pgs:
                for dst in dst_pgs:
                    if src == dst:
                        continue
                    if _run(session, MERGE_REL_ENVIA_A, f"ENVIA_A/{topic}",
                            src_pg=src, dst_pg=dst, topic=topic,
                            src_flow=root_name, dst_flow=root_name):
                        counters["envia_a_rels"] += 1

        # 5) Archivos: nodos y relaciones ESCRIBE_EN / LEE_DE
        for pid, paths in flow_index.write_paths_by_proc.items():
            pg = flow_index.proc_to_pg.get(pid)
            if not pg:
                continue
            for path in paths:
                if _run(session, MERGE_ARCHIVO, f"ARCHIVO/{path}", path=path):
                    counters["archivo_nodes"] += 1
                if _run(session, MERGE_REL_PG_ESCRIBE_ARCHIVO, f"ESCRIBE_ARCHIVO/{path}",
                        pg_id=pg, flow_name=root_name, path=path):
                    counters["escribe_rels"] += 1

        for pid, paths in flow_index.read_paths_by_proc.items():
            pg = flow_index.proc_to_pg.get(pid)
            if not pg:
                continue
            for path in paths:
                if _run(session, MERGE_ARCHIVO, f"ARCHIVO/{path}", path=path):
                    counters["archivo_nodes"] += 1
                if _run(session, MERGE_REL_PG_LEE_ARCHIVO, f"LEE_ARCHIVO/{path}",
                        pg_id=pg, flow_name=root_name, path=path):
                    counters["lee_rels"] += 1

        # 6) Tablas from SQL: nodos Tabla y relaciones LEE_DE / ESCRIBE_EN / ALIMENTA_A
        all_pg_ids = set(flow_index.sql_sources_by_pg) | set(flow_index.sql_destination_by_pg)
        for pg_id in all_pg_ids:
            sources = flow_index.sql_sources_by_pg.get(pg_id, set())
            dest = flow_index.sql_destination_by_pg.get(pg_id)

            for table in sources:
                if _run(session, MERGE_TABLA, f"TABLA/{table.clave}",
                        nombre=table.name, zona=table.zone, clave=table.clave, database=table.database):
                    counters["tabla_nodes"] += 1
                if _run(session, MERGE_LEE_DE, f"LEE_DE/{table.clave}",
                        nifi_id=pg_id, flow_name=root_name, clave=table.clave):
                    counters["lee_tabla_rels"] += 1

            if dest:
                if _run(session, MERGE_TABLA, f"TABLA/{dest.clave}",
                        nombre=dest.name, zona=dest.zone, clave=dest.clave, database=dest.database):
                    counters["tabla_nodes"] += 1
                if _run(session, MERGE_ESCRIBE_EN, f"ESCRIBE_EN/{dest.clave}",
                        nifi_id=pg_id, flow_name=root_name, clave=dest.clave):
                    counters["escribe_tabla_rels"] += 1

                for table in sources:
                    if _run(session, MERGE_ALIMENTA_A, f"ALIMENTA_A/{table.clave}->{dest.clave}",
                                        from_clave=table.clave,
                                        into_clave=dest.clave):
                        counters["alimenta_rels"] += 1

        # 7) Scripts: nodos Script y relaciones EXECUTES (PG->Script)
        for pg_id, refs in flow_index.script_refs_by_pg.items():
            for ref in refs:
                clave = f"{ref.ref_type}::{ref.value[:200]}"
                if _run(session, MERGE_SCRIPT, f"SCRIPT/{clave}",
                        clave=clave, value=ref.value, ref_type=ref.ref_type,
                        prop_name=ref.prop_name, extra=ref.extra):
                    counters["script_nodes"] += 1
                if _run(session, MERGE_EXECUTES, f"EXECUTES/{pg_id}/{clave}",
                        nifi_id=pg_id, flow_name=root_name, clave=clave,
                        processor_name=ref.processor_name, processor_type=ref.processor_type,
                        categoria=ref.categoria):
                    counters["executes_rels"] += 1

    driver.close()

    # === CSV de Atlas Terms encontrados ===
    try:
        unique_terms = sorted({t for term_set in flow_index.terms_by_proc.values() for t in term_set})
        csv_path = script_dir / "atlas_terms.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["atlas_term"])
            for term in unique_terms:
                writer.writerow([term])
        log.info("CSV de Atlas Terms generado: %s (%d terms)", csv_path, len(unique_terms))
    except Exception as e:
        log.error("Error al generar CSV de Atlas Terms: %s", e)

    log.info("Graph build complete")
    return counters