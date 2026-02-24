#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nifi_parser.py
==============
Core NiFi flow parser.  Builds a :class:`FlowIndex` by walking the process-group
tree of a loaded flow and delegating to specialised sub-modules:

  * nifi_models     – domain dataclasses and type aliases
  * nifi_loader     – JSON / XML file loading and dict-navigation helpers
  * nifi_extractors – Kafka / Atlas / connection-filter extractors

Public names are re-exported here so that existing callers continue to work
without modification.
"""
from __future__ import annotations

import logging
from typing import Any

from config import Config
from find_files import extract_file_paths_from_processors
from find_scripts import extract_script_refs_from_processor, ScriptRef  # noqa: F401
from find_sql import extract_tables_from_processors, Table  # noqa: F401

# -- sub-module re-exports (backward-compat) ----------------------------------
from nifi_models import (  # noqa: F401
    ProcessorId,
    ProcessGroupId,
    TopicName,
    ProcessorInfo,
    ConnectionInfo,
    ProcessGroupNode,
    FlowIndex,
)
from nifi_loader import (  # noqa: F401
    as_component,
    dotget,
    load_flow,
    load_flow_json,
    load_flow_xml,
)
from nifi_extractors import (  # noqa: F401
    _is_publish,
    _is_consume,
    _props_dict,
    _deletes_atlas_term_for_props,
    extract_atlas_terms_from_processors_map,
    extract_kafka_group_id_by_processor,
    extract_kafka_topics_by_processor,
    normalize_rels,
    conn_allowed,
)

log = logging.getLogger(__name__)

# =========================
# Process-group field helpers
# =========================

def pg_fields_with_connections(
    pg: dict[str, Any]
) -> tuple[ProcessGroupId | None, str, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Extract key fields from a process group including its connections."""
    pg = as_component(pg)
    contents = pg.get("contents") or {}
    g_id: ProcessGroupId | None = pg.get("instanceIdentifier") or pg.get("id")
    g_name: str = pg.get("name") or "SIN_NOMBRE"
    child_groups: list[dict[str, Any]] = contents.get("processGroups") or pg.get("processGroups") or []
    processors: list[dict[str, Any]] = contents.get("processors") or pg.get("processors") or []
    connections: list[dict[str, Any]] = contents.get("connections") or pg.get("connections") or []
    return g_id, g_name, child_groups, processors, connections

# =========================
# Root resolver
# =========================

def resolve_nifi_root(
    flow: dict[str, Any], override_name: str | None = None
) -> tuple[dict[str, Any], str, str]:
    """Resolve the root contents, ID, and name from a NiFi flow structure."""
    candidates_contents = []
    fc = flow.get("flowContents")
    if isinstance(fc, dict):
        candidates_contents.append(fc)
    fl = flow.get("flow")
    if isinstance(fl, dict):
        comp = fl.get("component")
        if isinstance(comp, dict):
            if isinstance(comp.get("contents"), dict):
                candidates_contents.append(comp["contents"])
            candidates_contents.append(comp)
        if isinstance(fl.get("contents"), dict):
            candidates_contents.append(fl["contents"])
        candidates_contents.append(fl)
    pgf_flow = dotget(flow, "processGroupFlow", "flow", default=None)
    if isinstance(pgf_flow, dict):
        candidates_contents.append(pgf_flow)
    rg = flow.get("rootGroup")
    if isinstance(rg, dict):
        if isinstance(rg.get("contents"), dict):
            candidates_contents.append(rg["contents"])
        candidates_contents.append(rg)
    candidates_contents.append(flow)

    root_contents = None
    for cand in candidates_contents:
        if not isinstance(cand, dict):
            continue
        if cand.get("processGroups") or cand.get("processors"):
            root_contents = cand
            break

    # resolver id/name
    meta_nodes = []
    if isinstance(fl, dict):
        meta_nodes.append(fl)
        comp = fl.get("component")
        if isinstance(comp, dict):
            meta_nodes.append(comp)
    crumb = dotget(flow, "processGroupFlow", "breadcrumb", "breadcrumb", default=None)
    if isinstance(crumb, dict):
        meta_nodes.append(crumb)
    if isinstance(rg, dict):
        meta_nodes.append(rg)
    meta_nodes.append(flow)

    root_id = None
    root_name = None
    for node in meta_nodes:
        if not isinstance(node, dict):
            continue
        if not root_id:
            root_id = node.get("instanceIdentifier") or node.get("id")
        if not root_name:
            root_name = node.get("name")
    if override_name:
        root_name = override_name
    if not root_id:
        root_id = dotget(flow, "processGroupFlow", "id", default=None) or "ROOT"
    if not root_name:
        root_name = flow.get("name") or "ROOT"

    return root_contents or {}, root_id, root_name

def index_tree(root_contents: dict[str, Any], cfg: Config) -> FlowIndex:
    """
    Build a FlowIndex from the root contents of a NiFi flow.
    
    Indexes:
      - Forward adjacency graph (filtered by connection relationship blacklist/whitelist)
      - Processor metadata (type, name, parent process group)
      - Kafka topics (publish/consume)
      - Atlas terms by processor
      - Processor category sets (publish, consume, logging, etc.)
      - Process group hierarchy
    """
    result = FlowIndex()

    def _is_logging_processor(ptype: str, pname: str) -> bool:
        if any(t in (ptype or "") for t in cfg.logging_type_set):
            return True
        if pname and cfg.logging_name_regex.search(pname):
            return True
        return False

    def walk(pg_node: dict[str, Any], parent_id: ProcessGroupId | None, inherited_vars: dict[str, str] | None = None) -> None:
        g_id, g_name, child_groups, processors, connections = pg_fields_with_connections(pg_node)
        if not g_id:
            return
        
        # Variables: inherit from parent, override with local
        local_vars = pg_node.get("variables") or {}
        current_vars = {**(inherited_vars or {}), **local_vars}
        
        result.pg_hierarchy.append(ProcessGroupNode(
            id=g_id,
            name=g_name,
            parent_id=parent_id,
            child_groups=child_groups
        ))

        # processors
        for raw_p in (processors or []):
            p = as_component(raw_p)
            pid: ProcessorId | None = p.get("instanceIdentifier") or p.get("id")
            ptype = str(p.get("type") or "")
            pname = str(p.get("name") or "")
            if not pid:
                continue

            result.proc_to_pg[pid] = g_id
            result.proc_type_by_id[pid] = ptype
            result.proc_name_by_id[pid] = pname

            if "UpdateAttribute" in ptype:
                result.ua_ids.add(pid)
            if _is_publish(ptype):
                result.publish_ids.add(pid)
            if _is_consume(ptype):
                result.consume_ids.add(pid)
            if _is_logging_processor(ptype, pname):
                result.logging_ids.add(pid)

            # detecta borrado atlas_term
            props = _props_dict(p)
            if "DeleteAttribute" in ptype and _deletes_atlas_term_for_props(props):
                result.deletes_atlas_ids.add(pid)
            if "UpdateAttribute" in ptype and _deletes_atlas_term_for_props(props):
                result.deletes_atlas_ids.add(pid)

        # términos por UA
        terms_here = extract_atlas_terms_from_processors_map(processors or [])
        for pid, terms in terms_here.items():
            result.terms_by_proc.setdefault(pid, set()).update(terms)

        # Kafka topics
        pub_by_proc, con_by_proc = extract_kafka_topics_by_processor(processors or [])
        for pid, topics in pub_by_proc.items():
            result.publish_topics_by_proc.setdefault(pid, set()).update(topics)
        for pid, topics in con_by_proc.items():
            result.consume_topics_by_proc.setdefault(pid, set()).update(topics)

        gid_by_proc = extract_kafka_group_id_by_processor(processors or [])
        for pid, gid in gid_by_proc.items():
            result.kafka_group_id_by_proc[pid] = gid

        # File paths (GetFile, PutFile, SFTP, HDFS, etc.)
        read_paths, write_paths = extract_file_paths_from_processors(processors or [], current_vars)
        for pid, paths in read_paths.items():
            result.read_paths_by_proc.setdefault(pid, set()).update(paths)
        for pid, paths in write_paths.items():
            result.write_paths_by_proc.setdefault(pid, set()).update(paths)

        # SQL tables (hardcoded queries in processors)
        sql_sources, sql_dest = extract_tables_from_processors(processors or [])
        if sql_sources:
            result.sql_sources_by_pg.setdefault(g_id, set()).update(sql_sources)
        if sql_dest:
            result.sql_destination_by_pg[g_id] = sql_dest

        # Script / command references
        for raw_p in (processors or []):
            refs = extract_script_refs_from_processor(raw_p, g_id, g_name, g_name)
            if refs:
                result.script_refs_by_pg.setdefault(g_id, []).extend(refs)

        # conexiones dirigidas filtradas
        for raw_c in (connections or []):
            c = as_component(raw_c)
            if not conn_allowed(c, cfg.not_allowed_set, cfg.enforce_allowed, cfg.allowed_set):
                continue
            src = c.get("source") or {}
            dst = c.get("destination") or {}
            sid: ProcessorId | None = src.get("id") or src.get("instanceIdentifier")
            did: ProcessorId | None = dst.get("id") or dst.get("instanceIdentifier")
            if sid and did and sid != did:
                result.fwd_adj[sid].add(did)

        # hijos
        for ch in (child_groups or []):
            walk(ch, g_id, current_vars)

    walk(root_contents, parent_id=None, inherited_vars=None)

    return result