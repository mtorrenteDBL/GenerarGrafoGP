#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import logging
from collections import defaultdict, deque
from pathlib import Path
from typing import TYPE_CHECKING

from neo4j import GraphDatabase
from config import (
    NEO4J_URI, NEO4J_USER, NEO4J_PASS,
    Config,
    MERGE_PG, MERGE_REL_PG_CHILD, MERGE_ATLAS_TERM, MERGE_REL_PG_ATLAS,
    MERGE_REL_ATLAS_PUBLICA_EN_KAFKA,
    MERGE_KAFKA_AND_LINK_PRODUCE, MERGE_KAFKA_AND_LINK_CONSUME,
    MERGE_REL_ENVIA_A,
    MERGE_ARCHIVO, MERGE_REL_PG_ESCRIBE_ARCHIVO, MERGE_REL_PG_LEE_ARCHIVO,
    MERGE_TABLA, MERGE_ALIMENTA_A, MERGE_LEE_DE, MERGE_ESCRIBE_EN,
    MERGE_SCRIPT, MERGE_EXECUTES
)

if TYPE_CHECKING:
    from nifi_parser import FlowIndex

log = logging.getLogger(__name__)

logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)

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


def build_graph(root_id: str, root_name: str, flow_index: FlowIndex, script_dir: Path, cfg: Config | None = None) -> dict[str, int]:
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

    # Pre-calculate node counts before attempting Neo4j insertion
    estimated_pg_nodes = len(flow_index.pg_hierarchy) + 1  # +1 for root
    estimated_pg_rels = sum(1 for pg in flow_index.pg_hierarchy if pg.parent_id)
    
    # Atlas terms
    all_atlas_terms = set()
    for terms in flow_index.terms_by_proc.values():
        all_atlas_terms.update(terms)
    estimated_atlas_nodes = len(all_atlas_terms)
    estimated_atlas_rels = sum(len(terms) for terms in flow_index.terms_by_proc.values())
    
    # Kafka nodes (unique topic x role x pg x group_id combinations)
    kafka_node_keys = set()
    for pid, topics in flow_index.publish_topics_by_proc.items():
        pg = flow_index.proc_to_pg.get(pid)
        gid = flow_index.kafka_group_id_by_proc.get(pid)
        if pg:
            for t in topics:
                kafka_node_keys.add((t, "PRODUCE", pg, gid))
    for pid, topics in flow_index.consume_topics_by_proc.items():
        pg = flow_index.proc_to_pg.get(pid)
        gid = flow_index.kafka_group_id_by_proc.get(pid)
        if pg:
            for t in topics:
                kafka_node_keys.add((t, "CONSUME", pg, gid))
    estimated_kafka_nodes = len(kafka_node_keys)
    estimated_kafka_rels = estimated_kafka_nodes  # one rel per node
    
    # Kafka ENVIA_A relations
    estimated_envia_a_rels = sum(
        len(consumers_by_topic.get(topic, set())) * (len(src_pgs) - 1) if len(src_pgs) > 1 else 0
        for topic, src_pgs in producers_by_topic.items()
        if topic in consumers_by_topic
    )
    
    # Files (Archivos)
    all_write_paths = set()
    for paths in flow_index.write_paths_by_proc.values():
        all_write_paths.update(paths)
    all_read_paths = set()
    for paths in flow_index.read_paths_by_proc.values():
        all_read_paths.update(paths)
    all_files = all_write_paths | all_read_paths
    estimated_archivo_nodes = len(all_files)
    estimated_escribe_rels = sum(len(paths) for paths in flow_index.write_paths_by_proc.values())
    estimated_lee_rels = sum(len(paths) for paths in flow_index.read_paths_by_proc.values())
    
    # Tables
    all_tables = set()
    for tables in flow_index.sql_sources_by_pg.values():
        all_tables.update(tables)
    for table in flow_index.sql_destination_by_pg.values():
        if table:
            all_tables.add(table)
    estimated_tabla_nodes = len(all_tables)
    estimated_lee_tabla_rels = sum(len(sources) for sources in flow_index.sql_sources_by_pg.values())
    estimated_escribe_tabla_rels = sum(1 for dest in flow_index.sql_destination_by_pg.values() if dest)
    estimated_alimenta_rels = sum(
        len(flow_index.sql_sources_by_pg.get(pg, set())) if dest else 0
        for pg, dest in flow_index.sql_destination_by_pg.items()
    )
    
    # Scripts
    estimated_script_nodes = sum(len(refs) for refs in flow_index.script_refs_by_pg.values())
    estimated_executes_rels = estimated_script_nodes
    
    # Atlas->Kafka relations (estimated)
    estimated_atlas_kafka_rels = sum(
        len(reachable_topics) for terms in flow_index.terms_by_proc.values() for reachable_topics in [
            set(flow_index.publish_topics_by_proc.get(pid, []))
            for pid in flow_index.publish_ids
        ]
    )
    
    log.info("=== Node Type Summary (Before Neo4j Insertion) ===")
    log.info("  ProcessGroup nodes: %d", estimated_pg_nodes)
    log.info("  ProcessGroup relations: %d", estimated_pg_rels)
    log.info("  Atlas Term nodes: %d", estimated_atlas_nodes)
    log.info("  Atlas relations (PG->Atlas): %d", estimated_atlas_rels)
    log.info("  Kafka nodes: %d", estimated_kafka_nodes)
    log.info("  Kafka relations (PG->Kafka): %d", estimated_kafka_rels)
    log.info("  Kafka->Kafka relations (ENVIA_A): %d", estimated_envia_a_rels)
    log.info("  Atlas->Kafka relations (PUBLICA_EN): %d (estimated)", estimated_atlas_kafka_rels)
    log.info("  File nodes: %d", estimated_archivo_nodes)
    log.info("  File write relations (PG->File): %d", estimated_escribe_rels)
    log.info("  File read relations (PG->File): %d", estimated_lee_rels)
    log.info("  Table nodes: %d", estimated_tabla_nodes)
    log.info("  Table feed relations (Table->Table): %d (estimated)", estimated_alimenta_rels)
    log.info("  Table read relations (PG->Table): %d", estimated_lee_tabla_rels)
    log.info("  Table write relations (PG->Table): %d", estimated_escribe_tabla_rels)
    log.info("  Script nodes: %d", estimated_script_nodes)
    log.info("  Script execution relations (PG->Script): %d", estimated_executes_rels)
    log.info("==================================================")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    log.debug("Connected to Neo4j at %s", NEO4J_URI)
    _cfg = cfg or Config()
    counters = {
        "pg_nodes": 0, "pg_rels": 0,
        "atlas_nodes": 0, "atlas_rels": 0,
        "kafka_nodes": 0, "kafka_rels": 0,
        "envia_a_rels": 0,
        "atlas_kafka_rels": 0,
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

        # 5) Atlas -> Kafka (PUBLICA_EN) con camino dirigido válido
        for ua_proc, terms in flow_index.terms_by_proc.items():
            visited: set[str] = {ua_proc}
            q: deque[str] = deque([ua_proc])
            reachable_publishers: set[str] = set()

            while q:
                cur = q.popleft()

                if cur in flow_index.publish_ids:
                    reachable_publishers.add(cur)

                for nxt in flow_index.fwd_adj.get(cur, []):
                    if nxt in visited:
                        continue
                    if nxt in flow_index.consume_ids:      # no transitar consumidores
                        continue
                    if nxt in flow_index.logging_ids:       # evitar logging
                        continue
                    if nxt in flow_index.deletes_atlas_ids:  # si borra atlas_term, cortamos
                        continue
                    if _cfg.stop_on_ua_override and nxt in flow_index.ua_ids and nxt != ua_proc:
                        continue
                    visited.add(nxt)
                    q.append(nxt)

            if not reachable_publishers:
                continue

            for pub_proc in reachable_publishers:
                pub_pg = flow_index.proc_to_pg.get(pub_proc)
                if not pub_pg:
                    continue
                gid = flow_index.kafka_group_id_by_proc.get(pub_proc)
                for topic in flow_index.publish_topics_by_proc.get(pub_proc, []):
                    # Ensure the Kafka node exists before linking
                    _run(session, MERGE_KAFKA_AND_LINK_PRODUCE,
                         f"PRODUCE/{topic}",
                         pg_id=pub_pg, topic=topic, role="PRODUCE",
                         group_id=gid, flow_name=root_name)
                    for term in terms:
                        if _run(session, MERGE_REL_ATLAS_PUBLICA_EN_KAFKA,
                                f"PUBLICA_EN/{term}/{topic}",
                                nombre=term, topic=topic, pg_id=pub_pg,
                                flow_name=root_name):
                            counters["atlas_kafka_rels"] += 1

        # 6) Archivos: nodos y relaciones ESCRIBE_EN / LEE_DE
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

        # 7) Tablas from SQL: nodos Tabla y relaciones LEE_DE / ESCRIBE_EN / ALIMENTA_A
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

        # 8) Scripts: nodos Script y relaciones EXECUTES (PG->Script)
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


    log.info("Graph build complete")
    return counters