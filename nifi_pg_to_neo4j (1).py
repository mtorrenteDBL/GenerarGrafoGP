#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
import re
import csv  # <<< agregado
from fnmatch import fnmatch
from pathlib import Path
from collections import deque, defaultdict
from neo4j import GraphDatabase

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "neo4jneo4j"

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
# Cypher
# =========================

MERGE_PG = """
MERGE (pg:ProcessGroup {nifi_id: $nifi_id})
ON CREATE SET pg.name = $name
ON MATCH  SET pg.name = $name
RETURN pg
"""

MERGE_REL_PG_CHILD = """
MATCH (parent:ProcessGroup {nifi_id: $parent_id})
MATCH (child:ProcessGroup  {nifi_id: $child_id})
MERGE (parent)-[:CONTIENE]->(child)
"""

MERGE_ATLAS_TERM = """
MERGE (at:`Atlas Term` {nombre: $nombre})
RETURN at
"""

# PG -> Atlas Term  (PREPARA)
MERGE_REL_PG_ATLAS = """
MATCH (pg:ProcessGroup {nifi_id: $pg_id})
MATCH (at:`Atlas Term` {nombre: $nombre})
MERGE (pg)-[:PREPARA]->(at)
"""

# PG -> Kafka (PRODUCE)  | Nodo Kafka por (topic, role, pg_id)
MERGE_KAFKA_AND_LINK = """
MATCH (pg:ProcessGroup {nifi_id: $pg_id})
MERGE (k:Kafka {topic: $topic, role: $role, pg_id: $pg_id})
  ON CREATE SET k.topic = $topic, k.role = $role, k.pg_id = $pg_id, k.group_id = $group_id
  ON MATCH  SET k.group_id = coalesce($group_id, k.group_id)
MERGE (pg)-[:PRODUCE]->(k)
RETURN k
"""

# Kafka PRODUCE -> CONSUME por mismo topic
MERGE_REL_ENVIA_A = """
MATCH (src:Kafka {topic: $topic, role: 'PRODUCE', pg_id: $src_pg})
MATCH (dst:Kafka {topic: $topic, role: 'CONSUME', pg_id: $dst_pg})
MERGE (src)-[:ENVIA_A {topic: $topic}]->(dst)
"""

# Atlas Term -> Kafka (si hay camino dirigido válido)
MERGE_REL_ATLAS_PUBLICA_EN_KAFKA = """
MATCH (at:`Atlas Term` {nombre: $nombre})
MATCH (k:Kafka {topic: $topic, role: 'PRODUCE', pg_id: $pg_id})
MERGE (at)-[:PUBLICA_EN]->(k)
"""

# =========================
# Utils
# =========================

# === Helpers para NiFi Expression Language (ifElse) ===
_IFELSE_RE = re.compile(
    r":ifElse\(\s*(['\"])(.*?)\1\s*,\s*(['\"])(.*?)\3\s*\)"
)

def _expand_nifi_terms(val: str) -> set[str]:
    """
    Encuentra todas las ocurrencias de :ifElse('termA','termB') o con comillas dobles,
    y devuelve ambos términos. No evalúa la condición; solo expande ramas.
    """
    if not val or not isinstance(val, str):
        return set()
    terms = set()
    for m in _IFELSE_RE.finditer(val):
        # grupos: 1 = quoteA, 2 = termA, 3 = quoteB, 4 = termB
        a = (m.group(2) or "").strip()
        b = (m.group(4) or "").strip()
        if a:
            terms.add(a)
        if b:
            terms.add(b)
    return terms

def _strip_quotes(s: str) -> str:
    if not s:
        return s
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s

def load_flow(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def ensure_constraints(session):
    # Neo4j 4.x/5.x
    stmts_modern = [
        "CREATE CONSTRAINT process_group_nifi_id IF NOT EXISTS FOR (pg:ProcessGroup) REQUIRE pg.nifi_id IS UNIQUE",
        "CREATE CONSTRAINT atlas_term_nombre IF NOT EXISTS FOR (at:`Atlas Term`) REQUIRE at.nombre IS UNIQUE",
        "CREATE CONSTRAINT kafka_topic_role_pg IF NOT EXISTS FOR (k:Kafka) REQUIRE (k.topic, k.role, k.pg_id) IS UNIQUE",
        "CREATE INDEX kafka_topic_idx IF NOT EXISTS FOR (k:Kafka) ON (k.topic)"
    ]
    for q in stmts_modern:
        try:
            session.run(q)
        except Exception:
            pass

    # Fallback Neo4j 3.x
    try:
        session.run("CREATE CONSTRAINT ON (pg:ProcessGroup) ASSERT pg.nifi_id IS UNIQUE")
    except Exception:
        pass
    try:
        session.run("CREATE CONSTRAINT ON (at:`Atlas Term`) ASSERT at.nombre IS UNIQUE")
    except Exception:
        pass
    try:
        session.run("CREATE INDEX ON :Kafka(topic)")
    except Exception:
        pass

def as_component(obj: dict) -> dict:
    if not isinstance(obj, dict):
        return {}
    return obj.get("component", obj)

def dotget(d, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default

# =========================
# Detectores / Extractores
# =========================

KAFKA_PUBLISH_HINTS = ("PublishKafka", "PublishKafkaRecord")
KAFKA_CONSUME_HINTS = ("ConsumeKafka", "ConsumeKafkaRecord")

def _is_publish(ptype: str) -> bool:
    return any(h in ptype for h in KAFKA_PUBLISH_HINTS)

def _is_consume(ptype: str) -> bool:
    return any(h in ptype for h in KAFKA_CONSUME_HINTS)

def _props_dict(p: dict) -> dict:
    return ((p.get("config") or {}).get("properties") or {}) or (p.get("properties") or {}) or {}

def _split_multi(s: str) -> list:
    if not s:
        return []
    parts = re.split(r'[,\n;]+', s)
    return [x.strip() for x in parts if x and x.strip()]

def _pattern_kills_atlas_term(token: str) -> bool:
    if not token:
        return False
    tok = token.strip()
    if tok.lower() == "atlas_term":
        return True
    try:
        if fnmatch("atlas_term", tok):  # wildcards estilo shell
            return True
    except Exception:
        pass
    try:
        if re.search(tok, "atlas_term"):  # regex
            return True
    except re.error:
        pass
    return False

def _deletes_atlas_term_for_props(props: dict) -> bool:
    if not props:
        return False
    candidate_keys = []
    for k in props.keys():
        kl = str(k).lower()
        if ("delete" in kl and "attribute" in kl) or ("attributes to delete" in kl) or ("delete attributes" in kl):
            candidate_keys.append(k)
    base_keys = ["attributes", "attributes.to.delete", "attributes-to-delete"]
    candidate_keys.extend([k for k in base_keys if k in props])
    for key in candidate_keys:
        val = props.get(key)
        if val is None:
            continue
        if isinstance(val, (list, dict)):
            val = json.dumps(val, ensure_ascii=False)
        tokens = _split_multi(str(val))
        for t in tokens:
            if _pattern_kills_atlas_term(t):
                return True
    return False

def extract_atlas_terms_from_processors_map(processors: list) -> dict:
    out = {}
    if not processors:
        return out
    for raw_p in processors:
        p = as_component(raw_p)
        ptype = str(p.get("type") or "")
        if "UpdateAttribute" not in ptype:
            continue
        props = _props_dict(p)

        terms = set()
        for k, v in props.items():
            if not (k and isinstance(k, str) and k.lower() == "atlas_term"):
                continue

            # Tomo el valor COMPLETO (antes del split) para capturar ifElse(...) aunque esté "cortado" por comas
            val_full = "" if v is None else str(v)

            # 1) Expando todas las ramas de ifElse('A','B') que aparezcan en el string completo
            terms.update(_expand_nifi_terms(val_full))

            # 2) Además, mantengo tu split original para casos simples
            for piece in _split_multi(val_full):
                piece = piece.strip()
                if not piece:
                    continue
                # Ignoro expresiones puras ${...} (ya capturadas si tenían ifElse)
                if piece.startswith("${") and piece.endswith("}"):
                    continue
                # Si quedó una mitad tipo "'can_...')" o "${...:ifElse('A'"
                # intento limpiar comillas y paréntesis residuales
                cleaned = piece.strip()
                # saco paréntesis/residuos comunes de mitades
                cleaned = cleaned.rstrip(")}").lstrip("${")
                cleaned = _strip_quotes(cleaned).strip()
                # si sigue teniendo un ifElse pendiente, ya lo cubrió _expand_nifi_terms
                if cleaned and ":ifElse(" not in cleaned and "}" not in cleaned and "{" not in cleaned:
                    terms.add(cleaned)

        if terms:
            pid = p.get("instanceIdentifier") or p.get("id")
            if pid:
                out[pid] = terms
    return out


def extract_kafka_group_id_by_processor(processors: list) -> dict:
    """
    Devuelve {processor_id: group_id} para procesadores Kafka (Publish/Consume) cuando corresponda.
    En Kafka, el group.id aplica típicamente a consumidores.
    """
    out = {}
    if not processors:
        return out
    for raw_p in processors:
        p = as_component(raw_p)
        ptype = str(p.get("type") or "")
        if not (_is_publish(ptype) or _is_consume(ptype)):
            continue
        props = _props_dict(p)
        gid = None
        # Claves típicas según NiFi
        for key in ("group.id", "group.id.value", "kafka.group.id"):
            if key in props and props[key] is not None:
                gid = str(props[key]).strip()
                if gid:
                    break
        if not gid:
            continue
        pid = p.get("instanceIdentifier") or p.get("id")
        if pid:
            out[pid] = gid
    return out


def extract_kafka_topics_by_processor(processors: list):
    pub, con = {}, {}
    if not processors:
        return pub, con
    for raw_p in processors:
        p = as_component(raw_p)
        ptype = str(p.get("type") or "")
        if not (_is_publish(ptype) or _is_consume(ptype)):
            continue
        props = _props_dict(p)
        topic_val = None
        for key in ("topic", "topic.name", "topics"):
            if key in props and props[key] is not None:
                topic_val = str(props[key])
                if topic_val:
                    break
        topics = {t.strip() for t in (topic_val or "").replace(";", ",").split(",") if t.strip()}
        topics = {t for t in topics if "l2elastic" not in t.lower()}  # filtro pedido
        if not topics:
            continue
        pid = p.get("instanceIdentifier") or p.get("id")
        if not pid:
            continue
        if _is_publish(ptype):
            pub[pid] = topics
        if _is_consume(ptype):
            con[pid] = topics
    return pub, con

# =========================
# Lectura de PGs con conexiones
# =========================

def pg_fields_with_connections(pg: dict):
    pg = as_component(pg)
    contents = pg.get("contents") or {}
    g_id = pg.get("instanceIdentifier") or pg.get("id")
    g_name = pg.get("name") or "SIN_NOMBRE"
    child_groups = contents.get("processGroups") or pg.get("processGroups") or []
    processors   = contents.get("processors")    or pg.get("processors")    or []
    connections  = contents.get("connections")   or pg.get("connections")   or []
    return g_id, g_name, child_groups, processors, connections

# =========================
# Resolver raíz
# =========================

def resolve_nifi_root(flow: dict, override_name: str | None = None):
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

# =========================
# Config + Grafo
# =========================

def normalize_rels(conn_dict):
    rels = (conn_dict or {}).get("selectedRelationships")
    if not rels:
        return set()
    return {str(r).strip().lower() for r in rels if r}

def conn_allowed(conn_dict, not_allowed_set, enforce_allowed, allowed_set) -> bool:
    rels_norm = normalize_rels(conn_dict)
    if not rels_norm:
        # sin relaciones declaradas → permitido
        return True
    if rels_norm & not_allowed_set:
        return False
    if enforce_allowed:
        return bool(rels_norm & allowed_set)
    # modo flexible (no usado por defecto)
    return True

def index_tree(root_contents: dict, cfg):
    """
    Construye:
      - fwd_adj: {src_id: set(dst_ids)} filtrando por blacklist y, por defecto, whitelist
      - proc_to_pg, proc_type_by_id, proc_name_by_id
      - publish_topics_by_proc, consume_topics_by_proc
      - terms_by_proc
      - publish_ids, consume_ids, logging_ids, deletes_atlas_ids, ua_ids
      - pg_hierarchy
    """
    fwd_adj = defaultdict(set)
    proc_to_pg = {}
    proc_type_by_id = {}
    proc_name_by_id = {}
    terms_by_proc = {}
    publish_topics_by_proc = {}
    consume_topics_by_proc = {}
    kafka_group_id_by_proc = {}
    publish_ids, consume_ids, logging_ids, deletes_atlas_ids, ua_ids = set(), set(), set(), set(), set()
    pg_hierarchy = []

    def _is_logging_processor(ptype: str, pname: str) -> bool:
        if any(t in (ptype or "") for t in cfg.logging_type_set):
            return True
        if pname and cfg.logging_name_regex.search(pname):
            return True
        return False

    def walk(pg_node, parent_id):
        g_id, g_name, child_groups, processors, connections = pg_fields_with_connections(pg_node)
        if not g_id:
            return
        pg_hierarchy.append((g_id, g_name, parent_id, child_groups))

        # processors
        for raw_p in (processors or []):
            p = as_component(raw_p)
            pid = p.get("instanceIdentifier") or p.get("id")
            ptype = str(p.get("type") or "")
            pname = str(p.get("name") or "")
            if not pid:
                continue

            proc_to_pg[pid] = g_id
            proc_type_by_id[pid] = ptype
            proc_name_by_id[pid] = pname

            if "UpdateAttribute" in ptype:
                ua_ids.add(pid)
            if _is_publish(ptype):
                publish_ids.add(pid)
            if _is_consume(ptype):
                consume_ids.add(pid)
            if _is_logging_processor(ptype, pname):
                logging_ids.add(pid)

            # detecta borrado atlas_term
            props = _props_dict(p)
            if "DeleteAttribute" in ptype and _deletes_atlas_term_for_props(props):
                deletes_atlas_ids.add(pid)
            if "UpdateAttribute" in ptype and _deletes_atlas_term_for_props(props):
                deletes_atlas_ids.add(pid)

        # términos por UA
        terms_here = extract_atlas_terms_from_processors_map(processors or [])
        for pid, terms in terms_here.items():
            terms_by_proc.setdefault(pid, set()).update(terms)

        # Kafka topics
        pub_by_proc, con_by_proc = extract_kafka_topics_by_processor(processors or [])
        for pid, topics in pub_by_proc.items():
            publish_topics_by_proc.setdefault(pid, set()).update(topics)
        for pid, topics in con_by_proc.items():
            consume_topics_by_proc.setdefault(pid, set()).update(topics)

        gid_by_proc = extract_kafka_group_id_by_processor(processors or [])
        for pid, gid in gid_by_proc.items():
            kafka_group_id_by_proc[pid] = gid

        # conexiones dirigidas filtradas
        for raw_c in (connections or []):
            c = as_component(raw_c)
            if not conn_allowed(c, cfg.not_allowed_set, cfg.enforce_allowed, cfg.allowed_set):
                continue
            src = c.get("source") or {}
            dst = c.get("destination") or {}
            sid = src.get("id") or src.get("instanceIdentifier")
            did = dst.get("id") or dst.get("instanceIdentifier")
            if sid and did and sid != did:
                fwd_adj[sid].add(did)

        # hijos
        for ch in (child_groups or []):
            walk(ch, g_id)

    walk(root_contents, parent_id=None)

    return (fwd_adj, proc_to_pg, proc_type_by_id, proc_name_by_id, terms_by_proc,
            publish_topics_by_proc, consume_topics_by_proc,
            publish_ids, consume_ids, logging_ids, deletes_atlas_ids, ua_ids, pg_hierarchy, kafka_group_id_by_proc)

# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Importa PGs (CONTIENE), Atlas (PREPARA), Kafka (PRODUCE/ENVIA_A) y Atlas->Kafka (PUBLICA_EN) con caminos dirigidos. Whitelist positiva ENFORCADA por defecto."
    )
    parser.add_argument("--flow", type=Path, default=Path("flow.json"))
    parser.add_argument("--root-name", type=str, default=None)

    # Listas de relaciones (editables)
    parser.add_argument("--allowed-rels", type=str, default=",".join(sorted(DEFAULT_ALLOWED)),
                        help="Lista separada por comas de relaciones permitidas (whitelist).")
    parser.add_argument("--not-allowed-rels", type=str, default=",".join(sorted(DEFAULT_NOT_ALLOWED)),
                        help="Lista separada por comas de relaciones prohibidas (blacklist).")

    # Logging
    parser.add_argument("--logging-name-regex", type=str, default=DEFAULT_LOGGING_NAME_REGEX,
                        help="Regex (case-insensitive) para nombres de procesador de logging.")
    parser.add_argument("--logging-types", type=str, default=",".join(sorted(DEFAULT_LOGGING_TYPES)),
                        help="Tipos que se consideran de logging (substrings).")

    # Opcional: desactivar whitelist (no recomendado)
    parser.add_argument("--no-enforce-allowed", action="store_true",
                        help="Desactiva la whitelist positiva (quedan solo relaciones en blacklist).")

    # Opcional: cortar si aparece otro UpdateAttribute (override)
    parser.add_argument("--stop-on-ua-override", action="store_true",
                        help="Si se pasa, el BFS corta al encontrar otro UpdateAttribute (posible override de atlas_term).")

    # Opcional: dry-run mode (solo CSV, sin Neo4j)
    parser.add_argument("--dry-run", action="store_true",
                        help="Modo dry-run: solo extrae y exporta CSV de Atlas Terms, sin conectar a Neo4j.")

    args = parser.parse_args()

    # Config derivada
    class Cfg:
        pass
    cfg = Cfg()
    # 🔧 FIX: usar el nombre correcto con underscore
    cfg.enforce_allowed = not bool(args.no_enforce_allowed)  # ENFORCADA por defecto
    cfg.allowed_set = {s.strip().lower() for s in args.allowed_rels.split(",") if s.strip()}
    cfg.not_allowed_set = {s.strip().lower() for s in args.not_allowed_rels.split(",") if s.strip()}
    cfg.logging_name_regex = re.compile(args.logging_name_regex, re.IGNORECASE)
    cfg.logging_type_set = {s.strip() for s in args.logging_types.split(",") if s.strip()}
    cfg.stop_on_ua_override = bool(args.stop_on_ua_override)

    flow = load_flow(args.flow)
    root_contents, root_id, root_name = resolve_nifi_root(flow, args.root_name)

    (fwd_adj, proc_to_pg, proc_type_by_id, proc_name_by_id, terms_by_proc,
     publish_topics_by_proc, consume_topics_by_proc,
     publish_ids, consume_ids, logging_ids, deletes_atlas_ids, ua_ids, pg_hierarchy, kafka_group_id_by_proc) = index_tree(root_contents, cfg)

    # Agregados por topic -> {pgs}
    producers_by_topic = defaultdict(set)
    consumers_by_topic = defaultdict(set)
    for pid, topics in publish_topics_by_proc.items():
        pg = proc_to_pg.get(pid)
        if pg:
            for t in topics:
                producers_by_topic[t].add(pg)
    for pid, topics in consume_topics_by_proc.items():
        pg = proc_to_pg.get(pid)
        if pg:
            for t in topics:
                consumers_by_topic[t].add(pg)

    counters = {
        "pg_nodes": 0, "pg_rels": 0,
        "atlas_nodes": 0, "atlas_rels": 0,
        "kafka_nodes": 0, "kafka_rels": 0,
        "envia_a_rels": 0,
        "atlas_kafka_rels": 0
    }

    # Dry-run mode: skip Neo4j entirely
    if args.dry_run:
        print("🏃 MODO DRY-RUN: Saltando Neo4j, solo generando CSV...")
    else:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

        with driver.session() as session:
            ensure_constraints(session)

            # 1) PGs y jerarquía
            pg_created = set()
            session.run(MERGE_PG, nifi_id=root_id, name=root_name)
            counters["pg_nodes"] += 1
            pg_created.add(root_id)

            for g_id, g_name, parent_id, _children in pg_hierarchy:
                if g_id not in pg_created:
                    session.run(MERGE_PG, nifi_id=g_id, name=g_name)
                    counters["pg_nodes"] += 1
                    pg_created.add(g_id)
                if parent_id:
                    session.run(MERGE_REL_PG_CHILD, parent_id=parent_id, child_id=g_id)
                    counters["pg_rels"] += 1

            # 2) Atlas (nodos) y PREPARA (PG->Atlas)
            for pid, terms in terms_by_proc.items():
                pg = proc_to_pg.get(pid)
                if not pg:
                    continue
                for t in terms:
                    session.run(MERGE_ATLAS_TERM, nombre=t)
                    session.run(MERGE_REL_PG_ATLAS, pg_id=pg, nombre=t)
                    counters["atlas_nodes"] += 1
                    counters["atlas_rels"] += 1

            # 3) Nodos Kafka y PG->Kafka (PRODUCE / CONSUME)
            for pid, topics in publish_topics_by_proc.items():
                pg = proc_to_pg.get(pid)
                if not pg:
                    continue
                gid = kafka_group_id_by_proc.get(pid)
                for t in topics:
                    session.run(MERGE_KAFKA_AND_LINK, pg_id=pg, topic=t, role="PRODUCE", group_id=gid)
                    counters["kafka_nodes"] += 1
                    counters["kafka_rels"] += 1

            for pid, topics in consume_topics_by_proc.items():
                pg = proc_to_pg.get(pid)
                if not pg:
                    continue
                gid = kafka_group_id_by_proc.get(pid)
                for t in topics:
                    session.run(MERGE_KAFKA_AND_LINK, pg_id=pg, topic=t, role="CONSUME", group_id=gid)
                    counters["kafka_nodes"] += 1
                    counters["kafka_rels"] += 1

            # 4) Kafka PRODUCE -> CONSUME por topic
            for topic, src_pgs in producers_by_topic.items():
                dst_pgs = consumers_by_topic.get(topic, set())
                for src in src_pgs:
                    for dst in dst_pgs:
                        if src == dst:
                            continue
                        session.run(MERGE_REL_ENVIA_A, src_pg=src, dst_pg=dst, topic=topic)
                        counters["envia_a_rels"] += 1

            # 5) Atlas -> Kafka (PUBLICA_EN) con camino dirigido válido
            for ua_proc, terms in terms_by_proc.items():
                visited = {ua_proc}
                q = deque([ua_proc])
                reachable_publishers = set()

                while q:
                    cur = q.popleft()

                    if cur in publish_ids:
                        reachable_publishers.add(cur)

                    for nxt in fwd_adj.get(cur, []):
                        if nxt in visited:
                            continue
                        if nxt in consume_ids:      # no transitar consumidores
                            continue
                        if nxt in logging_ids:      # evitar logging
                            continue
                        if nxt in deletes_atlas_ids:  # si borra atlas_term, cortamos
                            continue

                        visited.add(nxt)
                        q.append(nxt)

                if not reachable_publishers:
                    continue

                for pub_proc in reachable_publishers:
                    pub_pg = proc_to_pg.get(pub_proc)
                    if not pub_pg:
                        continue
                    gid = kafka_group_id_by_proc.get(pub_proc)
                    for topic in publish_topics_by_proc.get(pub_proc, []):
                        session.run(MERGE_KAFKA_AND_LINK, pg_id=pub_pg, topic=topic, role="PRODUCE", group_id=gid)
                        for term in terms:
                            session.run(MERGE_REL_ATLAS_PUBLICA_EN_KAFKA, nombre=term, topic=topic, pg_id=pub_pg)
                            counters["atlas_kafka_rels"] += 1

        driver.close()

    # === CSV de Atlas Terms encontrados (agregado, sin modificar lógica existente) ===
    try:
        unique_terms = sorted({t for term_set in terms_by_proc.values() for t in term_set})
        script_dir = Path(__file__).resolve().parent
        csv_path = script_dir / "atlas_terms.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["atlas_term"])
            for term in unique_terms:
                writer.writerow([term])
        print(f"✅ CSV de Atlas Terms generado: {csv_path}")
    except Exception as e:
        print(f"❌ ERROR al generar CSV de Atlas Terms: {e}")

    print("\n==== RESUMEN ====")
    if args.dry_run:
        print("📋 MODO DRY-RUN (sin cambios en Neo4j)")
        print(f"Atlas Terms extraídos: {len({t for term_set in terms_by_proc.values() for t in term_set})}")
    else:
        print(f"ProcessGroups mergeados: {counters['pg_nodes']}")
        print(f"Relaciones CONTIENE (PG->PG): {counters['pg_rels']}")
        print(f"Atlas Terms mergeados: {counters['atlas_nodes']}")
        print(f"Relaciones PREPARA (PG->Atlas): {counters['atlas_rels']}")
        print(f"Kafka nodos (PG x rol x topic): {counters['kafka_nodes']}")
        print(f"Relaciones PRODUCE (PG->Kafka): {counters['kafka_rels']}")
        print(f"Relaciones ENVIA_A (Kafka->Kafka): {counters['envia_a_rels']}")
        print(f"Relaciones PUBLICA_EN (Atlas->Kafka): {counters['atlas_kafka_rels']}")
        print("OK: Whitelist positiva aplicada por defecto; usar --no-enforce-allowed para desactivarla si hace falta.")

if __name__ == "__main__":
    main()
