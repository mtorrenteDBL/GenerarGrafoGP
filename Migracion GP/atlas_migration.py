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
    # Default path is now relative to project root
    parser.add_argument("--csv", default="../data/atlas_terms.csv")
    
    parser.add_argument("--no-constraints", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--out-csv", default="./plan.csv")

    args = parser.parse_args()
    
    runner = PipelineRunner(args.csv)
    terms = runner.get_terms()
    
    # terms = ["veraz_modelo_03"]

    if args.plan_only:
        runner.run_plan_mode(terms, args.out_csv)
    
    else:
        config = {
            "uri": getenv('NEO4J_HOST'),
            "user": getenv('NEO4J_USER'),
            "password": getenv('NEO4J_PASS'),
        }
        return runner.run_load_mode(terms, config, delete_all=False)

if __name__ == "__main__":
    main()