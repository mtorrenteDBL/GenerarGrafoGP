#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nifi_extractors.py
==================
Detector and extractor functions for NiFi processor properties:
  - Kafka publish/consume topic detection
  - Atlas term extraction from UpdateAttribute processors
  - DeleteAttribute / UpdateAttribute "deletes atlas_term" detection
  - Connection relationship filtering helpers
"""
from __future__ import annotations

import json
import logging
import re
from fnmatch import fnmatch
from typing import Any

from config import KAFKA_CONSUME_HINTS, KAFKA_PUBLISH_HINTS
from nifi_models import ProcessorId, TopicName
from nifi_loader import as_component

log = logging.getLogger(__name__)


# =========================
# NiFi Expression Language helpers (ifElse)
# =========================

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


# =========================
# Kafka type detection
# =========================

def _is_publish(ptype: str) -> bool:
    return any(h in ptype for h in KAFKA_PUBLISH_HINTS)


def _is_consume(ptype: str) -> bool:
    return any(h in ptype for h in KAFKA_CONSUME_HINTS)


# =========================
# Property helpers
# =========================

def _props_dict(p: dict[str, Any]) -> dict[str, Any]:
    """Extract properties dictionary from a processor."""
    return ((p.get("config") or {}).get("properties") or {}) or (p.get("properties") or {}) or {}


def _split_multi(s: str) -> list[str]:
    """Split a string by commas, newlines, or semicolons."""
    if not s:
        return []
    parts = re.split(r'[,\n;]+', s)
    return [x.strip() for x in parts if x and x.strip()]


# =========================
# Atlas term deletion detection
# =========================

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


def _deletes_atlas_term_for_props(props: dict[str, Any]) -> bool:
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


# =========================
# Extractors
# =========================

def extract_atlas_terms_from_processors_map(processors: list[dict[str, Any]]) -> dict[ProcessorId, set[str]]:
    """Extract atlas_term values from UpdateAttribute processors."""
    out: dict[ProcessorId, set[str]] = {}
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


def extract_kafka_group_id_by_processor(processors: list[dict[str, Any]]) -> dict[ProcessorId, str]:
    """
    Return {processor_id: group_id} for Kafka processors when applicable.
    In Kafka, group.id typically applies to consumers.
    """
    out: dict[ProcessorId, str] = {}
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


def extract_kafka_topics_by_processor(
    processors: list[dict[str, Any]],
) -> tuple[dict[ProcessorId, set[TopicName]], dict[ProcessorId, set[TopicName]]]:
    """Extract publish and consume Kafka topics by processor ID."""
    pub: dict[ProcessorId, set[TopicName]] = {}
    con: dict[ProcessorId, set[TopicName]] = {}
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
# Connection relationship filtering
# =========================

def normalize_rels(conn_dict: dict[str, Any] | None) -> set[str]:
    """Normalize connection relationships to lowercase set."""
    rels = (conn_dict or {}).get("selectedRelationships")
    if not rels:
        return set()
    return {str(r).strip().lower() for r in rels if r}


def conn_allowed(
    conn_dict: dict[str, Any],
    not_allowed_set: set[str],
    enforce_allowed: bool,
    allowed_set: set[str],
) -> bool:
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
