import sqlglot
from sqlglot import exp
import re
import logging
from typing import Optional

from ..logger import setup_logger
logger = setup_logger(name='sql_utils')

class SQLUtils:
    """
    Utilidades de parseo SQL usando SQLGlot con sanitización de variables.
    """

    CTE_DEFINITION_REGEX = re.compile(r'(\w+)\s+AS\s*\(', re.IGNORECASE)
    FROM_JOIN_REGEX = re.compile(r'\b(?:FROM|JOIN)\s+([a-zA-Z0-9_.\"`]+)', re.IGNORECASE)
    INSERT_INTO_REGEX = re.compile(r'\b(?:INSERT\s+INTO|UPDATE|TABLE|INTO)\s+([a-zA-Z0-9_.\"`]+)', re.IGNORECASE)

    @staticmethod
    def _sanitize_sql(sql: str) -> str:
        """
        Reemplaza las variables $VAR por marcadores válidos SQL (var_VAR).
        """
        # 1. Quitar comentarios
        sql_clean = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.S)
        sql_clean = re.sub(r'--.*$', '', sql_clean, flags=re.M)
        
        # 2. Neutralizar strings (fechas, formatters, etc.) dentro de las funciones
        #sql_clean = re.sub(r"('[^']*'|\"[^\"]*\")", r"'STRING_LITERAL_MARKER'", sql_clean)

        # 2. Reemplazar $ por var_
        sql_clean = re.sub(r'\$INSERT_STATEMENT', ' ', sql_clean, flags=re.IGNORECASE)
        sanitized_sql = sql_clean.replace('$', 'var_')
        return sanitized_sql

    @staticmethod
    def _restore_name(name: str) -> str:
        """Devuelve el nombre original cambiando var_ por $"""
        if not name: 
            return ""
        return name.replace('var_', '$')

    @staticmethod
    def extract_tables_and_dest(sql_text: str) -> tuple[set[str], Optional[str]]:
        if not sql_text or not sql_text.strip():
            return set(), None

        clean_sql = SQLUtils._sanitize_sql(sql_text)

        try:
            # Usamos 'hive' ya que el SQL usa sintaxis compatible (coalesce, cast, etc)
            parsed_list = sqlglot.parse(clean_sql, read='hive')
            if not parsed_list: return set(), None
            parsed = parsed_list[0]
        except Exception as e:

            try:
                logger.warning(f"Error parsing SQL with dialect Hive: {e}")
                parsed_list = sqlglot.parse(clean_sql, read=None)
                if not parsed_list: return set(), None
                parsed = parsed_list[0]
            except Exception as e:

                logger.warning(f"Error parsing SQL with no dialect: {e}")
                try:
                    sources, dest = SQLUtils._extract_via_regex(clean_sql)
                    return sources, dest
                except Exception as e:
                    logger.error('Error parsing SQL with regex: {e}. Impossible to extract tables and destination')
                    return set(), None

        # 1. Identificar nombres de CTEs (para no confundirlas con tablas reales)
        # Buscamos en todo el árbol porque puede haber CTEs anidadas
        cte_names = {cte.alias.upper() for cte in parsed.find_all(exp.CTE) if cte.alias}

        logging.debug(f'CTE Names: {cte_names}')

        sources: set[str] = set()
        destination: Optional[str] = None

        # 2. Detectar destino (INSERT / UPDATE)
        if isinstance(parsed, exp.Insert):
            target = parsed.this
            destination = SQLUtils._restore_name(target.sql()).lower()
        elif isinstance(parsed, exp.Update):
            target = parsed.this
            destination = SQLUtils._restore_name(target.sql()).lower()

        # 3. Extraer todas las tablas del árbol SQL
        for table_node in parsed.find_all(exp.Table):

            t_name = table_node.name
            t_db = table_node.db if table_node.db else None
            
            if not t_name:
                continue

            name_upper = t_name.upper()


            # Si el nombre es una CTE, la ignoramos 
            # (sus tablas internas ya saldrán como exp.Table)
            # Chequeamos que t_db sea None porque las ctes no tienen schema
            if name_upper in cte_names and t_db is None:
                continue

            logging.debug(f'Found table: {t_db}.{t_name} (IN CTEs: {name_upper in cte_names}, Has DB: {t_db is not None})')

            # Reconstruimos el nombre limpio
            full_name = f"{t_db}.{t_name}" if t_db else t_name
            full_name = SQLUtils._restore_name(full_name).lower()
            full_name = full_name.replace('"', '').replace('`', '').replace("'", "")

            # Evitamos que el destino aparezca como fuente
            if destination and full_name == destination:
                continue
            
            # Filtro de seguridad para alias cortos o ruido
            if len(full_name) > 1:
                sources.add(full_name)

        logging.debug(f'SQLExtraction - Sources: {sources}')
        logging.debug(f'SQLExtraction - Destination: {destination}')

        return sources, destination

    @staticmethod
    def _extract_via_regex(sql_text: str) -> tuple[set[str], Optional[str]]:
        # 1. Limpieza inicial: eliminamos comentarios
        sql_clean = re.sub(r'--.*', '', sql_text)
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)

        # 2. Identificar CTEs siguiendo tu regla: 
        # Debe tener el patrón "nombre AS (" y NO tener un punto
        cte_candidates = SQLUtils.CTE_DEFINITION_REGEX.findall(sql_clean)
        ctes = {c.lower() for c in cte_candidates if '.' not in c}

        # 3. Identificar Destino
        dest_match = SQLUtils.INSERT_INTO_REGEX.search(sql_clean)
        destination = None
        if dest_match:
            destination = dest_match.group(1).lower().replace('"', '').replace('`', '')

        # 4. Extraer fuentes y aplicar validación cruzada
        matches = SQLUtils.FROM_JOIN_REGEX.findall(sql_clean)
        raw_sources = {m.lower().replace('"', '').replace('`', '') for m in matches}

        sources = set()
        for s in raw_sources:
            # Regla 1: Si tiene un punto, es casi seguro una tabla real (schema.tabla)
            has_schema = '.' in s
            
            # Regla 2: No debe estar en la lista de CTEs detectadas
            is_cte = s in ctes
            
            # Regla 3: No es el destino de la query
            is_destination = (s == destination)

            if (has_schema or not is_cte) and not is_destination and len(s) > 1:
                sources.add(s)

        return sources, destination

    @staticmethod
    def collect_tables(val) -> list[str]:
        """Extrae tablas de una lista o string SQL."""
        if not val: 
            return []
        items = val if isinstance(val, (list, tuple, set)) else [val]
        results = []
        
        for item in items:
            s = str(item or "").strip()
            if not s: 
                continue
            
            if re.search(r'\b(select|insert|update|from|join)\b', s, re.IGNORECASE):
                srcs, dst = SQLUtils.extract_tables_and_dest(s)
                results.extend(list(srcs))
                if dst: 
                    results.append(dst)
            else:
                for piece in re.split(r'[,\n;]+', s):
                    clean_piece = piece.strip().replace('`', '').replace('"', '')
                    # Fix extra: si viene "tabla as t", limpiarlo también aquí
                    clean_piece = re.split(r'\s+as\s+', clean_piece, flags=re.IGNORECASE)[0]
                    clean_piece = clean_piece.split(' ')[0] # Split por espacio por si "tabla alias"
                    
                    if clean_piece: 
                        results.append(clean_piece.lower())

        logging.debug(f'Tablas encontradas: {results}')
        
        return results