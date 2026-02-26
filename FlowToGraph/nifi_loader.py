#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nifi_loader.py
==============
Functions for loading NiFi flow files (JSON and XML) and low-level
dict-navigation utilities used throughout the pipeline.
"""
from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# =========================
# Dict-navigation helpers
# =========================

def as_component(obj: dict[str, Any]) -> dict[str, Any]:
    """Extract the 'component' sub-dict if present, otherwise return the dict itself."""
    if not isinstance(obj, dict):
        return {}
    return obj.get("component", obj)


def dotget(d: dict[str, Any], *path: str, default: Any = None) -> Any:
    """Navigate nested dictionaries using a sequence of keys."""
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default


# =========================
# Flow file loading
# =========================

def load_flow(path: Path) -> dict[str, Any]:
    """Load a NiFi flow from JSON or XML file."""
    if not path.exists():
        raise FileNotFoundError(f"Flow file not found: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"Flow file is empty (0 bytes): {path}")
    suffix = path.suffix.lower()
    log.debug("Loading flow file: %s (format: %s)", path.name, suffix)
    if suffix == ".xml":
        return load_flow_xml(path)
    else:
        return load_flow_json(path)


def load_flow_json(path: Path) -> dict[str, Any]:
    """Load a NiFi flow from JSON file."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_flow_xml(path: Path) -> dict[str, Any]:
    """
    Load a NiFi flow from XML file and convert to JSON-like structure.
    This handles the flow.xml format used by NiFi.
    """
    tree = ET.parse(path)
    root = tree.getroot()

    # Find the rootGroup element
    root_group_elem = root.find("rootGroup")
    if root_group_elem is None:
        # Fallback: maybe the root itself is the process group
        root_group_elem = root

    # Convert XML to JSON-like structure
    flow_data = _xml_process_group_to_dict(root_group_elem)
    return {"rootGroup": flow_data}


# =========================
# XML conversion helpers
# =========================

def _xml_get_text(elem: ET.Element, tag: str, default: str = "") -> str:
    """Get text content of a child element."""
    child = elem.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return default


def _xml_processor_to_dict(proc_elem: ET.Element) -> dict[str, Any]:
    """Convert an XML processor element to JSON-like dict."""
    proc_id = _xml_get_text(proc_elem, "id")
    proc_name = _xml_get_text(proc_elem, "name")
    proc_class = _xml_get_text(proc_elem, "class")  # XML uses <class> instead of <type>

    # Extract properties
    properties = {}
    for prop_elem in proc_elem.findall("property"):
        prop_name = _xml_get_text(prop_elem, "name")
        prop_value = _xml_get_text(prop_elem, "value")
        if prop_name:
            properties[prop_name] = prop_value

    return {
        "id": proc_id,
        "instanceIdentifier": proc_id,  # For compatibility with JSON format
        "name": proc_name,
        "type": proc_class,  # Map <class> to type for consistency
        "config": {"properties": properties},
        "properties": properties,
    }


def _xml_connection_to_dict(conn_elem: ET.Element) -> dict[str, Any]:
    """Convert an XML connection element to JSON-like dict."""
    conn_id = _xml_get_text(conn_elem, "id")
    source_id = _xml_get_text(conn_elem, "sourceId")
    dest_id = _xml_get_text(conn_elem, "destinationId")
    relationship = _xml_get_text(conn_elem, "relationship")

    # XML uses single <relationship>, JSON uses array selectedRelationships
    selected_rels = [relationship] if relationship else []

    return {
        "id": conn_id,
        "instanceIdentifier": conn_id,
        "source": {"id": source_id, "instanceIdentifier": source_id},
        "destination": {"id": dest_id, "instanceIdentifier": dest_id},
        "selectedRelationships": selected_rels,
    }


def _xml_extract_variables(pg_elem: ET.Element) -> dict[str, str]:
    """Extract variable definitions from a process group element."""
    variables: dict[str, str] = {}
    for var_elem in pg_elem.findall("variable"):
        name = var_elem.get("name", "")
        value = var_elem.get("value", "")
        if name:
            variables[name] = value
    return variables


def _xml_process_group_to_dict(pg_elem: ET.Element) -> dict[str, Any]:
    """Convert an XML processGroup element to JSON-like dict recursively."""
    pg_id = _xml_get_text(pg_elem, "id")
    pg_name = _xml_get_text(pg_elem, "name")

    # Extract variables defined at this process group level
    variables = _xml_extract_variables(pg_elem)

    # Extract processors
    processors = []
    for proc_elem in pg_elem.findall("processor"):
        processors.append(_xml_processor_to_dict(proc_elem))

    # Extract connections
    connections = []
    for conn_elem in pg_elem.findall("connection"):
        connections.append(_xml_connection_to_dict(conn_elem))

    # Extract child process groups (recursive)
    process_groups = []
    for child_pg_elem in pg_elem.findall("processGroup"):
        process_groups.append(_xml_process_group_to_dict(child_pg_elem))

    return {
        "id": pg_id,
        "instanceIdentifier": pg_id,
        "name": pg_name,
        "variables": variables,
        "processors": processors,
        "connections": connections,
        "processGroups": process_groups,
    }
