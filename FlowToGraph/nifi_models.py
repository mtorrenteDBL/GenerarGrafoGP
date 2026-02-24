#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nifi_models.py
==============
Type aliases and dataclasses that represent the NiFi domain model used
throughout the FlowToGraph pipeline.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from find_sql import Table
from find_scripts import ScriptRef


# =========================
# Type Aliases
# =========================
ProcessorId = str
ProcessGroupId = str
TopicName = str


# =========================
# Dataclasses
# =========================

@dataclass
class ProcessorInfo:
    """Information about a single NiFi processor."""
    id: ProcessorId
    name: str
    type: str
    process_group_id: ProcessGroupId
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class ConnectionInfo:
    """Information about a NiFi connection between components."""
    id: str
    source_id: ProcessorId
    destination_id: ProcessorId
    relationships: set[str] = field(default_factory=set)


@dataclass
class ProcessGroupNode:
    """Represents a node in the process group hierarchy."""
    id: ProcessGroupId
    name: str
    parent_id: ProcessGroupId | None
    child_groups: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class FlowIndex:
    """
    Complete indexed representation of a NiFi flow.
    Replaces the large tuple returned by index_tree().
    """
    # Adjacency graph
    fwd_adj: dict[ProcessorId, set[ProcessorId]] = field(default_factory=lambda: defaultdict(set))

    # Processor mappings
    proc_to_pg: dict[ProcessorId, ProcessGroupId] = field(default_factory=dict)
    proc_type_by_id: dict[ProcessorId, str] = field(default_factory=dict)
    proc_name_by_id: dict[ProcessorId, str] = field(default_factory=dict)

    # Atlas terms by processor
    terms_by_proc: dict[ProcessorId, set[str]] = field(default_factory=dict)

    # Kafka topics by processor
    publish_topics_by_proc: dict[ProcessorId, set[TopicName]] = field(default_factory=dict)
    consume_topics_by_proc: dict[ProcessorId, set[TopicName]] = field(default_factory=dict)
    kafka_group_id_by_proc: dict[ProcessorId, str] = field(default_factory=dict)

    # File paths by processor
    read_paths_by_proc: dict[ProcessorId, set[str]] = field(default_factory=dict)
    write_paths_by_proc: dict[ProcessorId, set[str]] = field(default_factory=dict)

    # Processor ID sets by category
    publish_ids: set[ProcessorId] = field(default_factory=set)
    consume_ids: set[ProcessorId] = field(default_factory=set)
    logging_ids: set[ProcessorId] = field(default_factory=set)
    deletes_atlas_ids: set[ProcessorId] = field(default_factory=set)
    ua_ids: set[ProcessorId] = field(default_factory=set)

    # Process group hierarchy
    pg_hierarchy: list[ProcessGroupNode] = field(default_factory=list)

    # SQL-derived tables by process group
    sql_sources_by_pg: dict[ProcessGroupId, set[Table]] = field(default_factory=dict)
    sql_destination_by_pg: dict[ProcessGroupId, Table] = field(default_factory=dict)

    # Script references by process group
    script_refs_by_pg: dict[ProcessGroupId, list[ScriptRef]] = field(default_factory=dict)
