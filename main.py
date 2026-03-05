#!/usr/bin/env python3
"""
Main orchestration script for GenerarGrafoPetersen project.

This script:
1. Loads environment configuration
2. Wipes the Neo4j database
3. Runs the FlowToGraph pipeline
4. Runs the Migracion GP pipeline

Designed to be run via: bash /opt/GenerarGrafoGP/main.sh
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Add subprojects to path
sys.path.insert(0, str(Path(__file__).parent / "FlowToGraph"))
sys.path.insert(0, str(Path(__file__).parent / "Migracion GP"))

# Import subproject modules (static analysis warnings are expected - imports work at runtime)
from flow_pipeline import fetch_and_process_all_flows  # type: ignore
from config import Config  # type: ignore
from src.pipeline import PipelineRunner  # type: ignore


# Configure logging
LOG_DIR = Path(__file__).parent / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"orchestrator_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)


log = logging.getLogger(__name__)


def load_env_config() -> dict:
    """Load and validate environment configuration from .env file."""
    env_file = Path(__file__).parent / ".env"
    
    if not env_file.exists():
        log.error(f"ERROR: .env file not found at {env_file}")
        sys.exit(1)
    
    load_dotenv(env_file, override=True)
    
    config = {
        "neo4j_host": os.getenv("NEO4J_HOST"),
        "neo4j_user": os.getenv("NEO4J_USER"),
        "neo4j_pass": os.getenv("NEO4J_PASS"),
    }
    
    if not all(config.values()):
        log.error("ERROR: Missing required environment variables (NEO4J_HOST, NEO4J_USER, NEO4J_PASS)")
        sys.exit(1)
    
    log.info(f"=== Neo4j target: {config['neo4j_host']} ===")
    return config


def wipe_neo4j_database(config: dict) -> None:
    """Wipe all nodes and relationships from the Neo4j database."""
    log.info("")
    log.info("=== Wiping Neo4j database ===")
    
    try:
        
        driver = GraphDatabase.driver(
            config["neo4j_host"],
            auth=(config["neo4j_user"], config["neo4j_pass"])
        )
        
        with driver.session() as session:
            result = session.run("MATCH (n) DETACH DELETE n")
            summary = result.consume()
            
            nodes_deleted = summary.counters.nodes_deleted
            rels_deleted = summary.counters.relationships_deleted
            
            log.info(
                f"Database wiped — {nodes_deleted} nodes deleted, "
                f"{rels_deleted} relationships deleted."
            )
        
        driver.close()
        
    except Exception as e:
        log.error(f"Failed to wipe Neo4j database: {e}")
        sys.exit(1)


def run_flow_to_graph_pipeline() -> bool:
    """
    Run the FlowToGraph pipeline by calling its main function.
    The FlowToGraph pipeline handles fetching and processing all flows.
    
    Returns True if all flows processed successfully.
    """
    log.info("")
    log.info("=== Running FlowToGraph Pipeline ===")
    
    try:
        # Create default configuration
        class Args:
            def __init__(self):
                self.allowed_rels = None  # Will use defaults
                self.not_allowed_rels = None
                self.logging_name_regex = None
                self.logging_types = None
                self.no_enforce_allowed = False
                self.stop_on_ua_override = False
        
        args = Args()
        cfg = Config.from_args(args)
        
        # Fetch and process all flows
        success = fetch_and_process_all_flows(cfg, verbose=False)
        
        if success:
            log.info("=== FlowToGraph pipeline completed successfully ===")
        else:
            log.error("=== FlowToGraph pipeline failed ===")
        
        return success
        
    except Exception as e:
        log.error(f"FlowToGraph pipeline failed: {e}", exc_info=True)
        return False


def run_migracion_gp_pipeline() -> bool:
    """
    Run the Migracion GP pipeline to migrate Atlas terms to Neo4j.
    
    Returns True if successful.
    """
    log.info("")
    log.info("=== Running Migracion GP Pipeline ===")
    
    try:
        # CSV file is now in data directory
        csv_file = Path(__file__).parent / "data" / "atlas_terms.csv"
        
        if not csv_file.exists():
            log.error(f"atlas_terms.csv not found at {csv_file}")
            return False
        
        runner = PipelineRunner(str(csv_file))
        terms = runner.get_terms()
        
        log.info(f"Processing {len(terms)} Atlas terms")
        
        config = {
            "uri": os.getenv('NEO4J_HOST'),
            "user": os.getenv('NEO4J_USER'),
            "password": os.getenv('NEO4J_PASS'),
        }
        
        runner.run_load_mode(terms, config, delete_all=False)
        
        log.info("=== Migracion GP Pipeline completed successfully ===")
        return True
        
    except Exception as e:
        log.error(f"Migracion GP pipeline failed: {e}", exc_info=True)
        return False


def main():
    """Main orchestration function."""
    log.info("=" * 70)
    log.info("=== GenerarGrafoPetersen Orchestrator Started ===")
    log.info(f"=== Log file: {log_file} ===")
    log.info("=" * 70)
    
    # 1. Load environment configuration
    config = load_env_config()
    
    # 2. Wipe Neo4j database
    wipe_neo4j_database(config)
    
    # 3. Run FlowToGraph pipeline
    flow_success = run_flow_to_graph_pipeline()
    
    # 4. Run Migracion GP pipeline
    migration_success = run_migracion_gp_pipeline()
    
    # 5. Summary
    log.info("")
    log.info("=" * 70)
    log.info("=== Pipeline Summary ===")
    log.info(f"FlowToGraph: {'✔ SUCCESS' if flow_success else '✘ FAILED'}")
    log.info(f"Migracion GP: {'✔ SUCCESS' if migration_success else '✘ FAILED'}")
    log.info("=" * 70)
    log.info("=== All done ===")
    
    # Exit with error code if any pipeline failed
    if not (flow_success and migration_success):
        sys.exit(1)


if __name__ == "__main__":
    main()
