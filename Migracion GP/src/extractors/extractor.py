import re
import logging
from .dataclasses_extractor import (
    LineageContext,
    ClassificationInfo,
    LineageResult,
    LineageRow,
    TermLineageResult,
    Table,
)
from .zone_service import ZoneService
from .sql_utils import SQLUtils
from ..apache_atlas import Atlas
from ..parse_sql import (
    looks_like_sql,
)

from ..logger import setup_logger
logger = setup_logger(name='extractor')


ENTIDADES = ("bsc", "bsj", "ber", "bsf")
SUBENTORNOS = ("pr", "de")

PATH_REGEX = re.compile(
    r"""
    ^(
        # Unix absolute path
        (/[^/\0]+)+/? |

        # UNC path (\\server\share)
        (\\\\[^\\/\0]+\\[^\\/\0]+(?:\\[^\\/\0]+)*) |

        # Relative paths (./, ../, folder/file)
        (\.{1,2}[\\/].+ |
         [^\\/\0]+([\\/][^\\/\0]+)+)
    )$
    """,
    re.VERBOSE,
)

def looks_like_path(v: str) -> bool:
    return bool(PATH_REGEX.match(v))    

def _strip_schema(table_name: str) -> str:
    """
    If table name includes a schema (schema.table), keep only table.
    """
    return table_name.split(".", 1)[-1]


def _to_regex(table_name: str) -> re.Pattern:
    """
    Convert a table name with variables into a compiled regex.
    """
    table_name = _strip_schema(table_name)

    regex = re.escape(table_name)

    regex = regex.replace(
        re.escape("$ENTIDAD"),
        f"({'|'.join(ENTIDADES)})"
    )
    regex = regex.replace(
        re.escape("$SUBENTORNO"),
        f"({'|'.join(SUBENTORNOS)})"
    )

    return re.compile(f"^{regex}$")


def tables_equal(name_a: str, name_b: str) -> bool:
    """
    Compare two table names allowing variables on either side.
    Schema (if present) is ignored.
    """
    name_a = _strip_schema(name_a)
    name_b = _strip_schema(name_b)

    regex_a = _to_regex(name_a)
    regex_b = _to_regex(name_b)

    return bool(
        regex_a.fullmatch(name_b)
        or regex_b.fullmatch(name_a)
    )



class Extractor:
    '''
    Extracts lineage from SQL using Apache Atlas
    '''
    
    DEST_PATTERNS = [
        # Matches "tabla" followed by "destino", allowing underscores or spaces as separators
        re.compile(r"tabla[_ ].*destino", re.IGNORECASE), 
        # Matches "destino" as a standalone component within an underscore-separated string
        re.compile(r"(^|[_ ])destino([_ ]|$)", re.IGNORECASE)
    ]

    SRC_PATTERNS = [
        # Matches "tabla" followed by "origen"
        re.compile(r"tabla[_ ].*origen", re.IGNORECASE)
    ]

    INSERT_QUERY_PATTERNS = [
        # Matches "query" and "insert" (or "insertar") in any order/position
        re.compile(r"query.*insert(ar)?", re.IGNORECASE), 
        # Specific match for the "insertkudu" prefix followed by "query"
        re.compile(r"insertkudu.*query", re.IGNORECASE)
    ]
    
    def __init__(self) -> None:
        self.atlas = Atlas()

    def _extract_by_classification(self, entity_detail: dict) -> list[ClassificationInfo]:
        results = []
        
        classifications = entity_detail.get("classifications") or []
        for classification in classifications:

            # 1. Initialize Context for this classification
            context = self._process_classification_attributes(classification)
            
            # 2. Build final Source/Dest maps
            lineage = self._resolve_lineage(context)
            
            results.append(ClassificationInfo(
                nombre=context.type_name,
                tipo=context.type_name,
                attrs=context.attrs,
                LineageResults=lineage
            ))
            
        return results
    
    def _process_json_key_value(self, key: str, val: object, ctx: LineageContext) -> None:

        if not key: 
            return
        key_clean = str(key).lower().strip()
        val_str = str(val or "")

    
        logger.debug(f"Processing key: {key_clean} with value type: {type(val)}")

        if not val_str or not key_clean:
            logger.debug('Skipping empty value')
            return

        # Chequear si tiene algun prefijo de Landing
        landing_prefixes = (
            prefix for prefix, zone in ZoneService.ZONE_TOKEN_TO_FUNC.items() 
            if zone == 'Landing'
        )
        has_landing_prefix = any(prefix in key_clean for prefix in landing_prefixes)
        
        match_ing_table = re.match(r'^\s*ing[_ ]tabla[_ ]([a-z0-9_]+)\s*:?\s*$', key_clean)
        match_ing_query = re.match(r'^\s*ing[_ ]query[_ ]([a-z0-9_]+)\s*:?\s*$', key_clean)

        # 2.2. Intenta capturar la zona con la pre-query (Chequeando un DELETE FROM)
        if "pre-query" in key_clean and "delete" in val_str.lower():
            logger.debug(f"Processing DELETE SQL from key: {key_clean}")
            self._parse_pre_query(val_str, ctx)
        
        # 2.3. Intenta capturar la zona con la post-query (Chequeando un UPDATE)
        elif "post-query" in key_clean and "update" in val_str.lower():
            logger.debug(f"Processing UPDATE SQL from key: {key_clean}")
            self._parse_post_query(val_str, ctx)
            
        elif "inserthbase" in key_clean and "tabla" in key_clean:
            logger.debug(f"Processing HBASE TABLE from key: {key_clean}")
            ctx.hbase_dsts.append((val_str, "refinado"))
            
        elif "inserthbase" in key_clean and "query" in key_clean:
            logger.debug(f"Processing HBASE QUERY from key: {key_clean}")
            ctx.hbase_queries.append((val_str, None))
            
        elif "origen" in key_clean and "query" in key_clean:
            logger.debug(f"Processing ORIGEN QUERY SQL from key: {key_clean}")
            ctx.sql_queries.append((val_str, "Origen"))

        # Detectar archivos de Landing si tiene algun prefijo de Landing en la clave                   
        elif has_landing_prefix and 'encoding' not in key_clean:
            logger.debug(f'Processing FILE from key: {key_clean}')
            zone_real = 'Landing'
            ctx.explicit_src.append((val_str, zone_real))
            ctx.explicit_dst.append((val_str, zone_real))

        # Detectar claves tipo ING Tabla ZONA o ING Query ZONA. Si detecta una de esas dos, 
        # usa lógica particular para extraer el par.
        elif match_ing_table:
            logger.debug(f"Processing ING TABLE from key: {key_clean}")
            zone_token = match_ing_table.group(1)
            zone_real = ZoneService.normalize_token(zone_token) or ZoneService.infer_from_text(zone_token) or "Desconocida"
            for t in SQLUtils.collect_tables(val):
                _, name = ZoneService.canonicalize_table_pair(t, zone_real)
                if name:
                    ctx.explicit_dst.append((name, zone_real))
        
        elif match_ing_query:
            logger.debug(f"Processing ING QUERY from key: {key_clean}")
            zone_token = match_ing_query.group(1)
            zone_real = ZoneService.normalize_token(zone_token) or ZoneService.infer_from_text(zone_token)
            ctx.sql_queries.append((val_str, zone_real))
        
        # 2.4 Revisa si es uno de los INSERT_QUERY_PATTERNS. Si es asi, busca tablas en las queries SQL.
        elif any(p.search(key_clean) for p in self.INSERT_QUERY_PATTERNS):
            logger.debug(f"Processing INSERT QUERY from key: {key_clean}")
            ctx.sql_queries.append((val_str, ZoneService.infer_from_text(key_clean))) # -> Esto queda simplificado porque despues se pasan por el ZoneService

        # 2.5 Revisa si la clave es uno de los DEST_PATTERNS.
        elif any(p.search(key_clean) for p in self.DEST_PATTERNS):
            logger.debug(f"Processing DESTINATION from key: {key_clean}")
            for t in SQLUtils.collect_tables(val):
                ctx.explicit_dst.append((t, ZoneService.infer_from_text(key_clean))) # -> Esto queda simplificado porque despues se pasan por el ZoneService
        
        # 2.6 Revisa si la clave es uno de los SRC_PATTERNS
        elif any(p.search(key_clean) for p in self.SRC_PATTERNS):
            logger.debug(f"Processing SOURCE from key: {key_clean}")
            for t in SQLUtils.collect_tables(val):
                ctx.explicit_src.append((t, ZoneService.infer_from_text(key_clean))) # -> Esto queda simplificado porque despues se pasan por el ZoneService

        # 2.7 Revisa si "insert" está en la clave pero "destino" no y no es una INSERT_QUERY_PATTERN
        elif looks_like_sql(val_str):
            logger.debug(f"Processing INSERT SQL from key: {key_clean}")
            for t in SQLUtils.collect_tables(val):
                logger.debug(f"Found table in INSERT SQL: {t}")
                ctx.explicit_src.append((t, ZoneService.infer_from_table_name(t))) # -> Esto queda simplificado porque despues se pasan por el ZoneService
                
        elif looks_like_path(val_str):
            logger.debug(f"Processing PATH from key: {key_clean}")
            ctx.explicit_src.append((val_str, "Landing"))
            ctx.explicit_dst.append((val_str, "Landing"))


    def _process_drs_entity(self, entity: dict) -> LineageContext:
        """
        Parses all attributes and organizes them into logical groups 
        (explicit sources, sql attributes, ING tags).
        """
                
        ctx = LineageContext()

        for key, val in entity.items():
            self._process_json_key_value(key, val, ctx)

        return ctx

    def _process_classification_attributes(self, classification: dict) -> LineageContext:
        """
        Parses all attributes and organizes them into logical groups 
        (explicit sources, sql attributes, ING tags).
        """
        type_name = classification.get("typeName") or classification.get("type") or "Clasificacion"
        attrs = classification.get("attributes", {}) or {}
        
        ctx = LineageContext(
            type_name=type_name,
            attrs=attrs,
        )

        for key, val in attrs.items():
            self._process_json_key_value(key, val, ctx)

        return ctx

    def _parse_post_query(self, sql: str, ctx: LineageContext) -> None:
        """Extracts table from UPDATE ... to determine zone override."""

        tables, _ = SQLUtils.extract_tables_and_dest(sql) # Esto retorna un SET

        # Fallback for datamart patterns regex
        if not tables:
             dm_pattern = re.compile(r"(?:\${?[a-z0-9_]+}?_)*(datamart_[a-z0-9_]+)\.([a-z0-9_]+)", re.IGNORECASE)
             for match in dm_pattern.finditer(sql):
                 # CORRECCIÓN: Usar .add() en lugar de .append() porque tables es un set
                 tables.add(f"{match.group(2)}")

        for t in tables:
            zone, name = ZoneService.canonicalize_table_pair(t, ZoneService.infer_from_table_name(t))
            if name:
                ctx.post_update.append((name, zone))

    def _parse_pre_query(self, sql: str, ctx: LineageContext) -> None:
        """Extracts table from DELETE FROM ... to determine zone override."""

        tables, _ = SQLUtils.extract_tables_and_dest(sql) # Esto retorna un SET

        # Fallback for datamart patterns regex
        if not tables:
             dm_pattern = re.compile(r"(?:\${?[a-z0-9_]+}?_)*(datamart_[a-z0-9_]+)\.([a-z0-9_]+)", re.IGNORECASE)
             for match in dm_pattern.finditer(sql):
                 # CORRECCIÓN: Usar .add() en lugar de .append()
                 tables.add(f"{match.group(0)}")

        for t in tables:
            zone, name = ZoneService.canonicalize_table_pair(t, ZoneService.infer_from_table_name(t))
            
            if name:
                ctx.pre_delete.append((name, zone))
    
    def _debug_tables(self, sources: list[Table], destinations: list[Table]) -> None:
        # give count of sources and destinations by zone
        zone_count_src = {}
        for src in sources:
            zone_count_src[src.zone] = zone_count_src.get(src.zone, 0) + 1
        zone_count_dst = {}
        for dst in destinations:
            zone_count_dst[dst.zone] = zone_count_dst.get(dst.zone, 0) + 1
        logger.debug(f"Sources by zone: {zone_count_src}. Total sources: {len(sources)}")
        logger.debug(f"Destinations by zone: {zone_count_dst}. Total destinations: {len(destinations)}")
                        

    def _resolve_lineage(self, ctx: LineageContext) -> LineageResult:

        logger.debug("Resolving lineage...")
        sources: list[Table] = []
        destinations: list[Table] = []
        hbase_sources: list[Table] = []
        hbase_destinations: list[Table] = []

        # 1. Procesar SQL Queries (Insert/Queries/ING Queries)
        for sql, zone in ctx.sql_queries:
            srcs, dst = SQLUtils.extract_tables_and_dest(sql)
            for s in srcs:
                src_zone, src_name = ZoneService.canonicalize_table_pair(s, ZoneService.infer_from_table_name(s) or zone)
                sources.append(Table(name=src_name, zone=src_zone))
                logger.debug(f"Extracted from SQL: source={s}, inferred_zone={src_zone} , default_zone={zone}")
            if dst:
                dst_zone, dst_name = ZoneService.canonicalize_table_pair(dst, ZoneService.infer_from_table_name(dst) or zone)
                destinations.append(Table(name=dst_name, zone=dst_zone))
                
        #procesar hbase
        for hbase_query, zone in ctx.hbase_queries:
            hbase_srcs, _ = SQLUtils.extract_tables_and_dest(hbase_query)
            for s in hbase_srcs:
                src_zone, src_name = ZoneService.canonicalize_table_pair(s, "HBase")
                hbase_sources.append(Table(name=src_name, zone=src_zone))
                logger.debug(f"Extracted from HBase SQL: source={s}, inferred_zone={src_zone} , default_zone={zone}")
                
        for hbase_dst, zone in ctx.hbase_dsts:
            dst_zone, dst_name = ZoneService.canonicalize_table_pair(hbase_dst, zone)
            hbase_destinations.append(Table(name=dst_name, zone=dst_zone))

        # 2. Procesar Explícitos (ING Tabla, Claves 'origen', 'destino')
        # Estos vienen de collect_tables que ya limpia strings simples
        for t, z in ctx.explicit_src:
            zone_src, name_src = ZoneService.canonicalize_table_pair(t, z)
            sources.append(Table(name=name_src, zone=zone_src))
            logger.debug(f"Added explicit source: table={name_src}, zone={zone_src}")
            
        for t, z in ctx.explicit_dst:
            zone_dst, name_dst = ZoneService.canonicalize_table_pair(t, z)
            destinations.append(Table(name=name_dst, zone=zone_dst))
            logger.debug(f"Added explicit destination: table={name_dst}, zone={zone_dst}")

        tablas_consumo = [table for table in sources + destinations if table.zone == "Consumo"]
        tablas_pre_post_query = [(name, zone, "pre") for name, zone in ctx.pre_delete] + \
                                [(name, zone, "post") for name, zone in ctx.post_update]
        
        for name, zone, src_dst in tablas_pre_post_query:
            inserted = False
            for tc in tablas_consumo:
                if tables_equal(name, tc.name):
                    tc.zone = zone or tc.zone
                    tc.name = name or tc.name
                    inserted = True
                    logger.debug(f"Overriding zone for table {name} to {zone} based on {src_dst}-query.")
                    break
            if not inserted and src_dst == "post":
                destinations.append(Table(name=name, zone=zone))
                logger.debug(f"Adding new destination table {name} with zone {zone} from post-update.")                     
            elif not inserted and src_dst == "pre":
                logger.warning(f"No tables found matching pre-delete table {name} to override zone.")

        self._debug_tables(sources, destinations)

        relations = ZoneService._order_tables(sources, destinations)
        relations_hbase = ZoneService._order_tables(hbase_sources, hbase_destinations)
        relations.extend(relations_hbase)

        logger.debug(f"Resolved Lineage Relations: {relations}")
        return LineageResult(
            relations=relations
        )
        
    def _get_table_name_only(self, schema_and_name: str, zone: str | None) -> str:
        """Extracts only the table name from a full schema.table string."""
        if zone == "Datamart":
            return schema_and_name
        parts = schema_and_name.split(".")
        return parts[-1] if parts else schema_and_name

    def _build_lineage_rows(
        self, 
        display_name: str, 
        clasificacion: str,
        lineage: LineageResult
    ) -> list[LineageRow]:
        """Build lineage rows from sources and destinations dictionaries."""
        rows: list[LineageRow] = []
        for relation in lineage.relations:
            sources = relation.sources if len(relation.sources) > 0 else [None]
            destinations = relation.destination if len(relation.destination) > 0 else [None]
            for src in sources:
                for dst in destinations:
                    row = LineageRow(
                        atlas_term=display_name,
                        clasificacion=clasificacion,
                        origen=self._get_table_name_only(src.name, src.zone) if src else None,
                        origen_zona=src.zone if src else None,
                        destino=self._get_table_name_only(dst.name, dst.zone) if dst else None,
                        destino_zona=dst.zone if dst else None
                    )
                    rows.append(row)
        return rows

    def process_atlas_term(self, display_name: str, entity: dict[str, object]) -> TermLineageResult:
        '''
        Procesa una entidad de Atlas y extrae su linaje en formato plano.

        Args:
            display_name (str): Nombre para mostrar de la entidad.
            entity (dict): Detalle de la entidad desde Atlas O DRS.

        Returns:
            TermLineageResult: Linaje completo con información de zonas.
        '''
        rows: list[LineageRow] = []
        
        logger.debug(f"Processing Atlas term: {display_name}")

        # Si hay clasificaciones el archivo viene de Atlas
        if entity.get('classifications'):
            logger.debug('Procesando entidad con clasificaciones de Atlas.')
            
            # Extrae las clasificaciones y construye las tuplas fuente/destino
            clasifs = self._extract_by_classification(entity)

            # Por cada resultado se arma el formato plano            
            for c in clasifs:
                rows.extend(self._build_lineage_rows(
                    display_name=display_name,
                    clasificacion=c.nombre,
                    lineage=c.LineageResults
                    
                ))

        # Si no hay clasificaciones, el archivo viene de DRS
        else:  
            logger.debug('Procesando entidad sin clasificaciones (DRS).')

            # Extraer sources y destinations para la entidad
            context = self._process_drs_entity(entity)
            lineage = self._resolve_lineage(context)

            rows.extend(self._build_lineage_rows(
                display_name=display_name,
                clasificacion="",
                lineage=lineage
            ))

        return TermLineageResult(term=display_name, status="ok", rows=rows)