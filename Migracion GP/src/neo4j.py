from typing import List, Dict
from neo4j import GraphDatabase

from .extractors.dataclasses_extractor import LineageRow
from .logger import setup_logger
logger = setup_logger(name='upload_to_neo4j')

class GraphQueries:
    """Centralizes Cypher queries to keep logic clean."""

    DELETE_ALL = """
    MATCH (n) DETACH DELETE n
    """
    
    CONSTRAINTS = [
        "CREATE CONSTRAINT tabla_node_key IF NOT EXISTS FOR (t:Tabla) REQUIRE (t.nombre, t.zona) IS NODE KEY",
        "CREATE CONSTRAINT archivo_node_key IF NOT EXISTS FOR (a:Archivo) REQUIRE (a.nombre, a.zona) IS NODE KEY",
        "CREATE CONSTRAINT clasif_clave IF NOT EXISTS FOR (c:Clasificacion) REQUIRE c.clave IS UNIQUE",
        "CREATE CONSTRAINT term_clave IF NOT EXISTS FOR (t:`Atlas Term`) REQUIRE t.clave IS UNIQUE",
        "CREATE INDEX tabla_nombre_idx IF NOT EXISTS FOR (t:Tabla) ON (t.nombre)",
    ]

    MERGE_ATLAS_TERM = """
    MERGE (at:`Atlas Term` {nombre: $nombre})
    ON CREATE SET
        at.clave    = $clave,
        at.guid     = $guid,
        at.glossary = $glossary,
        at.origin   = $origin
    ON MATCH SET
        at.clave    = coalesce($clave, at.clave),
        at.guid     = coalesce($guid, at.guid),
        at.origin   = coalesce($origin, at.origin),
        at.glossary = coalesce($glossary, at.glossary)
    RETURN at
    """

    MERGE_CLASIFICACION = """
    MERGE (c:Clasificacion {clave: $clave})
    ON CREATE SET
        c.nombre     = $nombre,
        c.tipo       = $tipo,
        c.term_clave = $term_clave,
        c.attrs_json = $attrs_json
    ON MATCH SET
        c.attrs_json = $attrs_json
    RETURN c
    """

    LINK_TERM_CLASIF = """
    MATCH (at:`Atlas Term` {clave: $term_clave})
    MATCH (c:Clasificacion {clave: $clasif_clave})
    MERGE (at)-[:TIENE_CLASIFICACION]->(c)
    """

    # Note: We use MERGE on properties defined in the Node Key constraint
    MERGE_TABLA = """
    MERGE (t:Tabla {nombre: $nombre, zona: $zona})
    ON CREATE SET t.clave = $clave
    ON MATCH SET t.clave = $clave
    """

    MERGE_ARCHIVO = '''
    MERGE (a:Archivo {nombre: $nombre, zona: $zona})
    ON CREATE SET a.clave = $clave
    ON MATCH SET a.clave = $clave
    '''

    LINK_CLASIF_SRC_TABLA = """
    MATCH (c:Clasificacion {clave: $clasif_clave})
    MATCH (t:Tabla {nombre: $nombre, zona: $zona})
    MERGE (c)-[:LEE_DE]->(t)
    """

    LINK_CLASIF_SRC_ARCHIVO = """
    MATCH (c:Clasificacion {clave: $clasif_clave})
    MATCH (t:Archivo {nombre: $nombre, zona: $zona})
    MERGE (c)-[:LEE_DE]->(t)
    """

    LINK_CLASIF_DST_TABLA = """
    MATCH (c:Clasificacion {clave: $clasif_clave})
    MATCH (t:Tabla {nombre: $nombre, zona: $zona})
    MERGE (c)-[:ESCRIBE_EN]->(t)
    """

    LINK_CLASIF_DST_ARCHIVO = """
    MATCH (c:Clasificacion {clave: $clasif_clave})
    MATCH (t:Archivo {nombre: $nombre, zona: $zona})
    MERGE (c)-[:ESCRIBE_EN]->(t)
    """

    LINK_LINEAGE = """
    MATCH (src:Tabla {nombre: $src_name, zona: $src_zona})
    MATCH (dst:Tabla {nombre: $dst_name, zona: $dst_zona})
    MERGE (src)-[:ALIMENTA_A]->(dst)
    """

    LINK_LINEAGE_ARCHIVO_TABLA = """
    MATCH (src:Archivo {nombre: $src_name, zona: $src_zona})
    MATCH (dst:Tabla {nombre: $dst_name, zona: $dst_zona})
    MERGE (src)-[:ALIMENTA_A]->(dst)
    """

    LINK_LINEAGE_ARCHIVO_ARCHIVO = '''
    MATCH (src:Archivo {nombre: $src_name, zona: $src_zona})
    MATCH (dst:Archivo {nombre: $dst_name, zona: $dst_zona})
    MERGE (src)-[:ALIMENTA_A]->(dst)
    '''

    LINK_LINEAGE_TABLA_ARCHIVO = '''
    MATCH (src:Tabla {nombre: $src_name, zona: $src_zona})
    MATCH (dst:Archivo {nombre: $dst_name, zona: $dst_zona})
    MERGE (src)-[:ALIMENTA_A]->(dst)
    '''

    LINK_LINEAGE_TABLA_TABLA = '''
    MATCH (src:Tabla {nombre: $src_name, zona: $src_zona})
    MATCH (dst:Tabla {nombre: $dst_name, zona: $dst_zona})
    MERGE (src)-[:ALIMENTA_A]->(dst)
    '''

class Neo4jLoader:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def delete_all(self):
        with self.driver.session() as session:
            session.execute_write(lambda tx: tx.run(GraphQueries.DELETE_ALL))

    def close(self):
        self.driver.close()

    def ensure_constraints(self):
        # We use a managed transaction even for schema changes to be safe, 
        # though strictly schema ops are often autocommit. 
        # Using execute_write ensures retry logic if needed.
        with self.driver.session() as session:
            for q in GraphQueries.CONSTRAINTS:
                try:
                    # 'execute_write' is the modern replacement for 'write_transaction'
                    session.execute_write(lambda tx: tx.run(q))
                except Exception:
                    # Ignore errors if constraint already exists
                    pass
                
    def load_term_lineage(self, term_data: Dict, lineage_rows: List[LineageRow]):
        """
        Transactional function to load one Atlas Term and all its related lineage.
        """
        if not lineage_rows:
            return

        with self.driver.session() as session:
            session.execute_write(self._tx_load_term, term_data, lineage_rows)

    def _tx_load_term(self, tx, term_data, lineage_rows: List[LineageRow]):
        # 1. Merge Atlas Term
        glossary = term_data.get("glossary") or "default"
        term_name = term_data.get("term")
        origin = term_data.get('origin')
        term_clave = f"{glossary}::{term_name}"
        
        tx.run(GraphQueries.MERGE_ATLAS_TERM,
            clave=term_clave,
            nombre=term_name,
            guid=term_data.get("guid"),
            glossary=glossary,
            origin=origin)
        
        # Group by Classification (still needed for term-classification linking)
        clasif_map = {}
        for row in lineage_rows:
            c_name = row.clasificacion
            c_key = f"{term_clave}::{c_name}"
            if c_key not in clasif_map:
                clasif_map[c_key] = {"name": c_name}
        
        # 2. Create Classifications and link to Term
        for c_key, data in clasif_map.items():
            tx.run(GraphQueries.MERGE_CLASIFICACION,
                clave=c_key,
                nombre=data['name'],
                tipo=data['name'],
                term_clave=term_clave,
                attrs_json="{}")
            
            tx.run(GraphQueries.LINK_TERM_CLASIF,
                term_clave=term_clave,
                clasif_clave=c_key)
        
        # 3. Process all tables from lineage rows
        # Track unique tables to avoid duplicate MERGE operations
        processed_tables = set()
        
        for row in lineage_rows:
            # Process source table
            if row.origen:
                src_zona = row.origen_zona or "Desconocida"
                src_name = row.origen
                src_key = (src_name, src_zona)
                
                if src_key not in processed_tables:

                    if row.origen_zona != 'Landing':
                        tx.run(
                            GraphQueries.MERGE_TABLA,
                            nombre=src_name,
                            zona=src_zona,
                            clave=f"{src_zona}::{src_name}"
                        )
                    else:
                        tx.run(
                            GraphQueries.MERGE_ARCHIVO,
                            nombre=src_name,
                            zona=src_zona,
                            clave=f"{src_zona}::{src_name}"
                        )
                    processed_tables.add(src_key)
            
            # Process destination table
            if row.destino:
                dst_zona = row.destino_zona or "Desconocida"
                dst_name = row.destino
                dst_key = (dst_name, dst_zona)
                
                if dst_key not in processed_tables:
                    if row.destino_zona != 'Landing':
                        tx.run(
                            GraphQueries.MERGE_TABLA,
                            nombre=dst_name,
                            zona=dst_zona,
                            clave=f"{dst_zona}::{dst_name}"
                        )
                    else:
                        tx.run(
                            GraphQueries.MERGE_ARCHIVO,
                            nombre=dst_name,
                            zona=dst_zona,
                            clave=f"{dst_zona}::{dst_name}"
                        )
                    processed_tables.add(dst_key)
        
        # 4. Link Classifications to Tables and Create Lineages
        for row in lineage_rows:
            c_name = row.clasificacion
            c_key = f"{term_clave}::{c_name}"
            
            # Link classification to source table
            if row.origen:
                src_zona = row.origen_zona or "Desconocida"
                src_name = row.origen
                
                if row.origen_zona != 'Landing':
                    tx.run(
                        GraphQueries.LINK_CLASIF_SRC_TABLA,
                        clasif_clave=c_key,
                        nombre=src_name,
                        zona=src_zona
                    )
                else:
                    tx.run(
                        GraphQueries.LINK_CLASIF_SRC_ARCHIVO,
                        clasif_clave=c_key,
                        nombre=src_name,
                        zona=src_zona
                    )
            
            # Link classification to destination table
            if row.destino:
                dst_zona = row.destino_zona or "Desconocida"
                dst_name = row.destino
                
                if row.destino_zona != 'Landing':
                    tx.run(
                        GraphQueries.LINK_CLASIF_DST_TABLA,
                        clasif_clave=c_key,
                        nombre=dst_name,
                        zona=dst_zona)
                else:
                    tx.run(
                        GraphQueries.LINK_CLASIF_DST_ARCHIVO,
                        clasif_clave=c_key,
                        nombre=dst_name,
                        zona=dst_zona
                    )
            
            # Create lineage relationship (src -> dst) if both exist
            if row.origen and row.destino:
                src_zona = row.origen_zona or "Desconocida"
                src_name = row.origen
                dst_zona = row.destino_zona or "Desconocida"
                dst_name = row.destino

                if src_zona == 'Landing' and dst_zona == 'Landing':                
                    tx.run(
                        GraphQueries.LINK_LINEAGE_ARCHIVO_ARCHIVO,
                        src_name=src_name,
                        src_zona=src_zona,
                        dst_name=dst_name,
                        dst_zona=dst_zona
                    )
                elif src_zona == 'Landing':
                    tx.run(
                        GraphQueries.LINK_LINEAGE_ARCHIVO_TABLA,
                        src_name=src_name,
                        src_zona=src_zona,
                        dst_name=dst_name,
                        dst_zona=dst_zona
                    )
                elif dst_zona == 'Landing':
                    tx.run(
                        GraphQueries.LINK_LINEAGE_TABLA_ARCHIVO,
                        src_name=src_name,
                        src_zona=src_zona,
                        dst_name=dst_name,
                        dst_zona=dst_zona
                    )
                else:
                    tx.run(
                        GraphQueries.LINK_LINEAGE_TABLA_TABLA,
                        src_name=src_name,
                        src_zona=src_zona,
                        dst_name=dst_name,
                        dst_zona=dst_zona
                    )