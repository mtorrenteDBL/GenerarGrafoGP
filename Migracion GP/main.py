import argparse
from src.pipeline import PipelineRunner

from os import getenv
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="./atlas_terms.csv")
    
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
        runner.run_load_mode(terms, config, delete_all=True)

if __name__ == "__main__":
    main()