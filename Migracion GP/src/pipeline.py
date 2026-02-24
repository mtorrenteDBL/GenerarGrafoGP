import csv
import logging
import json

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
    
    def __init__(self, csv_path):
        self.csv_path = Path(csv_path)
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
        
        except Exception:
            logger.error(f"Unexpected error processing term {term}.")
            return None, None, None, None

        return display_name, entity, guid, origin

    def get_terms(self) -> List[str]:
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV not found: {self.csv_path}")

        # Leer CSV y extraer las entidades
        with self.csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if "atlas_term" not in (reader.fieldnames or []):
                 raise ValueError("CSV missing 'atlas_term' column")
            
            # Eliminar duplicados
            csv_terms = list(
                dict.fromkeys(row["atlas_term"].strip() 
                for row in reader if row.get("atlas_term"))
            )
        
        # Leer archivos de GitLab (procesamos TODOS asumiendo 
        # que si están en GitLab están productivos)
        drs_atlas_files = glob("DRS_ATLAS_PROD/Prod/*/*.json")
        drs_atlas_files = [Path(f).name.replace(".json", "") for f in drs_atlas_files] 

        logging.info(f'Found {len(csv_terms)} terms in CSV file')
        logging.info(f'Found {len(drs_atlas_files)} terms in DRS folder')

        # Nos quedamos con las entidades de GitLab 
        # y las del CSV sin duplicados
        terms = list(set(csv_terms + drs_atlas_files))

        logging.info(f'Total terms: {len(terms)}')

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

    def run_load_mode(self, terms: List[str], neo4j_config: dict, delete_all: bool = False):
        """Extracts and Loads directly to Neo4j."""
        loader = Neo4jLoader(**neo4j_config)

        if delete_all:
            loader.delete_all()
        
        if not neo4j_config.get('no_constraints'):
            loader.ensure_constraints()

        total = len(terms)
        for i, term in enumerate(terms, 1):

            logger.info(f'Processing term {i}/{total}: {term}')

            try:

                # Buscar la entidad
                display_name, entity, guid, origin = self.search_term(term)
                if not entity:
                    logger.warning('Term not found neither in Atlas or GitLab.')
                    continue
                
                # Realizar la extracción
                extraction = self.extractor.process_atlas_term(display_name, entity)

                term_data = {
                    "term": term, 
                    "guid": guid, 
                    "glossary": None,
                    'origin': origin
                }

                # 3. Load (convert to dict for backward compatibility)
                loader.load_term_lineage(term_data, extraction.rows)

                if (rows_amount := len(extraction.rows)) == 0:
                    logger.warning(f"No loaded rows.")
                else:
                    logger.info(f"Loaded {len(extraction.rows)} rows.")

            except Exception as e:
                logger.error(f"[{i}/{total}] Error processing {term}: {e}")
        
        loader.close()