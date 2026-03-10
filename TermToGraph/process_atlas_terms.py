#!/usr/bin/env python3
"""
Atlas Migration Pipeline - Migrate Atlas terms to Neo4j.

This module processes Atlas lineage metadata and loads it into Neo4j.
Note: Environment variables are loaded by the main orchestrator.
"""
import argparse
from src.pipeline import PipelineRunner

from os import getenv


def main():
    
    parser = argparse.ArgumentParser()
    # Optional CSV path (if not provided, will query Neo4j by default)
    parser.add_argument("--csv", default=None, help="CSV file with Atlas terms (if not provided, will query Neo4j)")
    
    parser.add_argument("--no-constraints", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--out-csv", default="./plan.csv")

    args = parser.parse_args()
    
    # Load Neo4j config from environment variables
    neo4j_config = {
        "uri": getenv('NEO4J_HOST'),
        "user": getenv('NEO4J_USER'),
        "password": getenv('NEO4J_PASS'),
    }
    
    # Create runner with Neo4j config by default, or CSV if explicitly provided
    if args.csv:
        runner = PipelineRunner(csv_path=args.csv, neo4j_config=neo4j_config)
    else:
        runner = PipelineRunner(neo4j_config=neo4j_config)
    
    terms = runner.get_terms()
    
    # terms = ["veraz_modelo_03"]

    if args.plan_only:
        runner.run_plan_mode(terms, args.out_csv)
    
    else:
        return runner.run_load_mode(terms, neo4j_config, delete_all=False)

if __name__ == "__main__":
    main()