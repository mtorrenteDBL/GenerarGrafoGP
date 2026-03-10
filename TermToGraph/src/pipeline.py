import csv
import logging
import json
import os

from glob import glob
from pathlib import Path
from typing import List, Optional

from .extractors.extractor import Extractor
from .neo4j import Neo4jLoader
from .apache_atlas import Atlas
from .search import find_atlas_term_json

from .logger import setup_logger
logger = setup_logger('pipeline')


class PipelineRunner:
    
    def __init__(self, csv_path=None, neo4j_config=None):
        self.csv_path = Path(csv_path) if csv_path else None
        self.neo4j_config = neo4j_config
        self.extractor = Extractor()
        self.atlas = Atlas()

    def search_term(self, term: str, disable_gitlab = False) -> tuple[Optional[str], Optional[dict], Optional[str]]:

        display_name, entity, guid = None, None, None
        origin = None
        
        # Busca la entidad en GitLab (simulado usando la carpeta DRS_ATLAS_PROD por ahora)
        try:
            logger.debug('Searching for term JSON in GitLab...')
            filepath = find_atlas_term_json(base_dir=Path('./DRS_ATLAS_PROD'), term_name=term)

            if filepath is None:

                # Busca la entidad en Atlas si no se encuentra en GitLab
                try:
                    logger.debug('Searching for term JSON in Atlas...')
                    display_name, entity, guid = self.atlas.get_entity(term)
                    origin = 'Apache Atlas'

                except Exception:
                    logger.error(f"Error fetching term from Atlas: {term}", exc_info=True)
                    return None, None, None, None
            
            # Se encontró el archivo en GitLab correctamente
            else:           
                with open(filepath) as f:
                    display_name = str(filepath)
                    entity = json.load(f)
                    origin = 'GitLab'

        # Si falla por acá es porque falló procesando el JSON, no buscandolo
        except json.JSONDecodeError:
            logger.error(f"Error searching for term JSON: {term}")
            return None, None, None, None
        
        except Exception as e:
            logger.error(f"Unexpected error processing term {term}: {e}")
            return None, None, None, None

        return display_name, entity, guid, origin

    def get_terms(self) -> List[str]:
        all_terms = []
        
        # 1. Try Neo4j if configured
        if self.neo4j_config:
            logger.info('Querying Atlas Term nodes from Neo4j...')
            loader = Neo4jLoader(**self.neo4j_config)
            try:
                neo4j_terms = loader.get_all_atlas_terms()
                logger.info(f'Found {len(neo4j_terms)} Atlas Term nodes in Neo4j')
                all_terms.extend(neo4j_terms)
            except Exception as e:
                logger.warning(f"Neo4j query failed, falling back to CSV and data/git/: {e}")
            finally:
                try:
                    loader.close()
                except Exception as e:
                    logger.warning(f"Failed to close Neo4j connection: {e}")
        
        # 2. Try CSV if provided
        csv_terms = []
        if self.csv_path and self.csv_path.exists():
            try:
                with self.csv_path.open("r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    if "atlas_term" in (reader.fieldnames or []):
                        # Eliminar duplicados
                        csv_terms = list(
                            dict.fromkeys(row["atlas_term"].strip() 
                            for row in reader if row.get("atlas_term"))
                        )
                        logger.info(f'Found {len(csv_terms)} terms in CSV file')
                        all_terms.extend(csv_terms)
            except Exception as e:
                logger.warning(f"Error reading from CSV: {e}")
        
        # 3. Always scan data/git/ for JSON files (procesamos TODOS asumiendo 
        # que si están en data/git/ están productivos)
        git_terms = []
        try:
            git_files = glob("data/git/**/*.json", recursive=True)
            git_terms = [Path(f).name.replace(".json", "") for f in git_files]
            if git_terms:
                logger.info(f'Found {len(git_terms)} terms in data/git/')
                all_terms.extend(git_terms)
        except Exception as e:
            logger.warning(f"Error scanning data/git/: {e}")
        
        # 4. Remove duplicates and return
        terms = list(dict.fromkeys(all_terms))
        
        logger.info(f'Total unique terms: {len(terms)}')
        if len(terms) == 0:
            logger.warning('No terms found from any source (Neo4j, CSV, or data/git/)')
        
        return terms

    def run_plan_mode(
            self, 
            terms: List[str], 
            out_csv: str, 
        ):
        """Generates a CSV of what WOULD be inserted."""
        logger.info(f"Running PLAN ONLY mode. output={out_csv}")
        all_rows = []
        total = len(terms)
        logger.debug(f"Total terms to process: {total}")

        for i, term in enumerate(terms, start=1):
                        
            display_name, entity, guid, origin = self.search_term(term)
        
            # Procesa la entidad si fue encontrada
            if not entity:
                continue
            
            # Procesa la entidad
            extraction = self.extractor.process_atlas_term(display_name, entity)

            # Si se pudo procesar correctamente se guarda la info
            if extraction.status == 'ok':
                logger.info(f"Loaded {len(extraction.rows)} rows.")

                # Agrega las filas a la lista (convertir a dict para CSV)
                all_rows.extend([row.to_dict() for row in extraction.rows])

                # Guarda el JSON de clasificaciones para referencia
                clasifications = entity.get('classifications', [])
                if clasifications:
                    with open(Path('DONWLOADED_ATLAS_TERMS') / f'{term}.json', 'w', encoding='utf-8') as f:
                        json.dump(clasifications, f, indent=2)

        if all_rows:
            keys = all_rows[0].keys()
            with open(out_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(all_rows)
            logger.info(f"Plan saved to {out_csv}")

        else:
            logger.warning("No lineage found for any term.")

    def run_load_mode(self, terms: List[str], neo4j_config: dict, delete_all: bool = False) -> dict:
        """Extracts and Loads directly to Neo4j.

        Returns a dict with three keys:
            ok        – terms loaded successfully
            not_found – terms not found in Atlas or GitLab
            failed    – terms that raised an exception during processing
        """
        loader = Neo4jLoader(**neo4j_config)

        if delete_all:
            loader.delete_all()

        if not neo4j_config.get('no_constraints'):
            loader.ensure_constraints()

        results = {"ok": [], "not_found": [], "failed": []}
        total = len(terms)
        for i, term in enumerate(terms, 1):

            logger.info(f'Processing term {i}/{total}: {term}')

            try:

                # Buscar la entidad
                display_name, entity, guid, origin = self.search_term(term)
                if not entity:
                    logger.warning('Term not found neither in Atlas or GitLab.')
                    results["not_found"].append(term)
                    continue

                # Realizar la extracción
                extraction = self.extractor.process_atlas_term(display_name, entity)

                term_data = {
                    "term": term,
                    "guid": guid,
                    "glossary": None,
                    'origin': origin
                }

                # Load (convert to dict for backward compatibility)
                loader.load_term_lineage(term_data, extraction.rows)

                if len(extraction.rows) == 0:
                    logger.warning(f"No loaded rows.")
                else:
                    logger.info(f"Loaded {len(extraction.rows)} rows.")

                results["ok"].append(term)

            except Exception as e:
                logger.error(f"[{i}/{total}] Error processing {term}: {e}")
                results["failed"].append(term)

        loader.close()
        return results


def run_migration(csv_path=None, neo4j_config=None) -> dict:
    """High-level entry point: load terms from Neo4j by default, or from CSV if provided.

    By default, queries Atlas Term nodes from Neo4j using the provided config.
    If csv_path is provided, reads from CSV instead.

    Args:
        csv_path: Optional path to CSV file. If not provided, will query Neo4j.
        neo4j_config: Dictionary with uri, user, password for Neo4j connection.
                     If not provided, will attempt to read from environment variables.

    Returns:
        Dictionary with results::

            {
                "ok":        [...],   # terms loaded successfully
                "not_found": [...],   # terms not found in Atlas or GitLab
                "failed":    [...],   # terms that raised an exception
            }
    """
    # If neo4j_config not provided, read from env vars
    if neo4j_config is None:
        neo4j_config = {
            "uri":      os.getenv("NEO4J_HOST"),
            "user":     os.getenv("NEO4J_USER"),
            "password": os.getenv("NEO4J_PASS"),
        }
    
    # Create runner with Neo4j config by default, or CSV if explicitly provided
    if csv_path:
        runner = PipelineRunner(csv_path=csv_path, neo4j_config=neo4j_config)
    else:
        runner = PipelineRunner(neo4j_config=neo4j_config)
    
    terms = runner.get_terms()

    logger.info(f"Processing {len(terms)} Atlas terms")

    return runner.run_load_mode(terms, neo4j_config, delete_all=False)