#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FlowToGraph Pipeline - Fetch and process NiFi flows into Neo4j graph.

This module can be run directly to fetch and process all flows,
or imported to use its functions programmatically.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from config import (
    DEFAULT_NOT_ALLOWED, DEFAULT_ALLOWED, 
    DEFAULT_LOGGING_NAME_REGEX, DEFAULT_LOGGING_TYPES,
    Config
)
from nifi_parser import load_flow, resolve_nifi_root, index_tree
from graph_builder import build_graph

log = logging.getLogger(__name__)


# Export for programmatic use
__all__ = ['process_single_flow', 'fetch_and_process_all_flows', 'main']


def process_single_flow(flow_path: Path, root_name: str | None, cfg: Config) -> dict:
    """Process a single flow file and return counters."""
    log.info("Loading flow from %s", flow_path)
    flow = load_flow(flow_path)
    root_contents, root_id, root_name_resolved = resolve_nifi_root(flow, root_name)
    log.info("Root process group: %s (id=%s)", root_name_resolved, root_id[:8] + "..." if len(root_id) > 12 else root_id)

    # Indexar árbol de procesos
    log.info("Indexing process tree...")
    flow_index = index_tree(root_contents, cfg)
    log.info("Indexed %d process groups, %d processors", len(flow_index.pg_hierarchy), len(flow_index.proc_to_pg))

    # Construir grafo en Neo4j
    log.info("Building graph in Neo4j...")
    script_dir = Path(__file__).resolve().parent
    counters = build_graph(root_id, root_name_resolved, flow_index, script_dir, cfg)
    
    return counters


def fetch_and_process_all_flows(cfg: Config, verbose: bool = False) -> dict:
    """
    Fetch flows from remote clusters and process all of them.

    Returns a dict with four keys:
        fetch_ok    – cluster names successfully retrieved this run
        fetch_fail  – cluster names that could not be retrieved this run
        process_ok  – flow file names processed successfully
        process_fail – flow file names that failed processing

    Only flow files that were **both** fetched successfully in this run
    **and** processed successfully are deleted from disk; all others are kept
    so they can be inspected or retried later.
    """
    from fetch_flows import main as fetch_flows_main

    _empty = {"fetch_ok": [], "fetch_fail": [], "process_ok": [], "process_fail": []}

    log.info("=== Fetching flows from remote clusters ===")
    try:
        fetch_results = fetch_flows_main()
    except Exception as e:
        log.error(f"Failed to fetch flows: {e}", exc_info=verbose)
        return _empty

    log.info("")
    log.info("=== Processing all flows ===")

    project_root = Path(__file__).resolve().parent.parent
    flows_dir = project_root / "data" / "flows"
    flow_files = list(flows_dir.glob("*.json")) + list(flows_dir.glob("*.xml"))

    if not flow_files:
        log.error(f"No flow files found in {flows_dir}")
        return {
            "fetch_ok": fetch_results.get("ok", []),
            "fetch_fail": fetch_results.get("fail", []),
            "process_ok": [],
            "process_fail": [],
        }

    process_ok: list[str] = []
    process_fail: list[str] = []

    for flow_file in flow_files:
        flow_name = flow_file.name

        # Skip empty files
        if flow_file.stat().st_size == 0:
            log.warning(f"--- Skipping: {flow_name} (empty file) ---")
            process_fail.append(flow_name)
            continue

        log.info("")
        log.info(f"--- Processing: {flow_name} ---")

        try:
            counters = process_single_flow(flow_file, flow_file.stem, cfg)
            log.info(f"✔ {flow_name} processed successfully")
            log.info("  ProcessGroups: %s, Atlas Terms: %s, Kafka nodes: %s, Atlas->Kafka: %s, Tables: %s",
                     counters['pg_nodes'], counters['atlas_nodes'],
                     counters['kafka_nodes'], counters['atlas_kafka_rels'], counters['tabla_nodes'])
            process_ok.append(flow_name)

        except Exception as e:
            log.error(f"✘ {flow_name} failed: {e}", exc_info=verbose)
            process_fail.append(flow_name)

    log.info("")
    if process_fail:
        log.warning(f"=== Warning: {len(process_fail)} flow(s) failed processing: {', '.join(process_fail)} ===")
    else:
        log.info("=== All flows processed successfully ===")

    return {
        "fetch_ok": fetch_results.get("ok", []),
        "fetch_fail": fetch_results.get("fail", []),
        "process_ok": process_ok,
        "process_fail": process_fail,
    }


def main():
    parser = argparse.ArgumentParser(
        description="FlowToGraph: Fetch and process NiFi flows into Neo4j graph. By default, fetches all flows from remote clusters and processes them."
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--fetch-all", action="store_true", default=False,
                           help="Fetch all flows from remote clusters and process them (default mode if no --flow specified)")
    mode_group.add_argument("--flow", type=Path, default=None,
                           help="Process a single flow file (legacy mode)")
    
    parser.add_argument("--root-name", type=str, default=None,
                       help="Root process group name (only used with --flow)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

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

    args = parser.parse_args()

    # Crear configuración
    cfg = Config.from_args(args)
    log.debug("Config: enforce_allowed=%s, stop_on_ua_override=%s", cfg.enforce_allowed, cfg.stop_on_ua_override)

    # Determine mode: fetch-all (default) or single flow
    if args.flow:
        # Legacy mode: process a single flow file
        log.info("=== Single flow mode ===")
        counters = process_single_flow(args.flow, args.root_name, cfg)
        
        # Imprimir resumen
        log.info("==== RESUMEN ====")
        log.info("ProcessGroups mergeados: %s", counters['pg_nodes'])
        log.info("Relaciones CONTIENE (PG->PG): %s", counters['pg_rels'])
        log.info("Atlas Terms mergeados: %s", counters['atlas_nodes'])
        log.info("Relaciones PREPARA (PG->Atlas): %s", counters['atlas_rels'])
        log.info("Kafka nodos (PG x rol x topic): %s", counters['kafka_nodes'])
        log.info("Relaciones PRODUCE/CONSUME (PG->Kafka): %s", counters['kafka_rels'])
        log.info("Relaciones ENVIA_A (Kafka->Kafka): %s", counters['envia_a_rels'])
        log.info("Relaciones PUBLICA_EN (Atlas->Kafka): %s", counters['atlas_kafka_rels'])
        log.info("Archivos mergeados: %s", counters['archivo_nodes'])
        log.info("Relaciones ESCRIBE_EN (PG->Archivo): %s", counters['escribe_rels'])
        log.info("Relaciones LEE_DE (PG->Archivo): %s", counters['lee_rels'])
        log.info("Tablas SQL mergeadas: %s", counters['tabla_nodes'])
        log.info("Relaciones LEE_DE (PG->Tabla): %s", counters['lee_tabla_rels'])
        log.info("Relaciones ESCRIBE_EN (PG->Tabla): %s", counters['escribe_tabla_rels'])
        log.info("Relaciones ALIMENTA_A (Tabla->Tabla): %s", counters['alimenta_rels'])
        log.info("Scripts mergeados: %s", counters['script_nodes'])
        log.info("Relaciones EXECUTES (PG->Script): %s", counters['executes_rels'])
        log.info("OK: Whitelist positiva aplicada por defecto; usar --no-enforce-allowed para desactivarla si hace falta.")
    else:
        # Default mode: fetch all flows and process them
        log.info("=== Fetch-all mode (default) ===")
        result = fetch_and_process_all_flows(cfg, args.verbose)
        fetch_ok = result["fetch_ok"]
        fetch_fail = result["fetch_fail"]
        process_ok = result["process_ok"]
        process_fail = result["process_fail"]

        log.info("")
        log.info("==== SUMMARY ====")
        log.info("Fetched successfully  (%d): %s", len(fetch_ok), ", ".join(fetch_ok) or "—")
        log.info("Fetch failed          (%d): %s", len(fetch_fail), ", ".join(fetch_fail) or "—")
        log.info("Processed successfully(%d): %s", len(process_ok), ", ".join(process_ok) or "—")
        log.info("Processing failed     (%d): %s", len(process_fail), ", ".join(process_fail) or "—")

        if fetch_fail or process_fail:
            log.error("Pipeline completed with errors!")
            sys.exit(1)
        log.info("Pipeline completed successfully!")


if __name__ == "__main__":
    from log_setup import setup_logging
    setup_logging(name="flow_pipeline")
    main()