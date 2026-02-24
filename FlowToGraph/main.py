#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from config import (
    DEFAULT_NOT_ALLOWED, DEFAULT_ALLOWED, 
    DEFAULT_LOGGING_NAME_REGEX, DEFAULT_LOGGING_TYPES,
    Config
)
from nifi_parser import load_flow, resolve_nifi_root, index_tree
from graph_builder import build_graph

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Importa PGs (CONTIENE), Atlas (PREPARA), Kafka (PRODUCE/ENVIA_A) con caminos dirigidos. Whitelist positiva ENFORCADA por defecto."
    )
    parser.add_argument("--flow", type=Path, default=Path("flow.json"))
    parser.add_argument("--root-name", type=str, default=None)
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

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S"
    )

    # Crear configuración
    cfg = Config.from_args(args)
    log.debug("Config: enforce_allowed=%s, stop_on_ua_override=%s", cfg.enforce_allowed, cfg.stop_on_ua_override)

    # Parsear flujo NiFi
    log.info("Loading flow from %s", args.flow)
    flow = load_flow(args.flow)
    root_contents, root_id, root_name = resolve_nifi_root(flow, args.root_name)
    log.info("Root process group: %s (id=%s)", root_name, root_id[:8] + "..." if len(root_id) > 12 else root_id)

    # Indexar árbol de procesos
    log.info("Indexing process tree...")
    flow_index = index_tree(root_contents, cfg)
    log.info("Indexed %d process groups, %d processors", len(flow_index.pg_hierarchy), len(flow_index.proc_to_pg))

    # Construir grafo en Neo4j
    log.info("Building graph in Neo4j...")
    script_dir = Path(__file__).resolve().parent
    counters = build_graph(root_id, root_name, flow_index, script_dir)

    # Imprimir resumen
    log.info("==== RESUMEN ====")
    log.info("ProcessGroups mergeados: %s", counters['pg_nodes'])
    log.info("Relaciones CONTIENE (PG->PG): %s", counters['pg_rels'])
    log.info("Atlas Terms mergeados: %s", counters['atlas_nodes'])
    log.info("Relaciones PREPARA (PG->Atlas): %s", counters['atlas_rels'])
    log.info("Kafka nodos (PG x rol x topic): %s", counters['kafka_nodes'])
    log.info("Relaciones PRODUCE/CONSUME (PG->Kafka): %s", counters['kafka_rels'])
    log.info("Relaciones ENVIA_A (Kafka->Kafka): %s", counters['envia_a_rels'])
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


if __name__ == "__main__":
    main()