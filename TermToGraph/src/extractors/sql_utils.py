import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel
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

    # Matches *innermost* NiFi EL: ${VAR}, ${VAR:fn(args)}, etc.
    # [^{}] excludes both braces so nested ${…} are resolved inside-out.
    _NIFI_EL_REGEX = re.compile(r'\$\{([^{}]+)\}')

    # Detects a var_ placeholder sitting where an SQL operator should be,
    # e.g.  "col var_Filtro CAST(…)"  →  "col = CAST(…)"
    _OPERATOR_VAR_RE = re.compile(
        r'(\b\w+(?:\.\w+)*)\s+(var_\w+)\s+'
        r'(?=CAST\b|\bSELECT\b|\(|\'|"|\d)',
        re.IGNORECASE,
    )
    _SQL_STRUCTURAL_KW = frozenset({
        'from', 'join', 'into', 'set', 'update', 'select', 'insert',
        'on', 'table', 'by', 'values', 'inner', 'outer', 'left', 'right',
        'cross', 'full', 'delete', 'create', 'alter', 'drop', 'where',
        'having', 'group', 'order', 'limit', 'union', 'except', 'intersect',
        'as', 'merge', 'upsert', 'using', 'and', 'or', 'not', 'in',
        'between', 'like', 'is', 'case', 'when', 'then', 'else', 'end',
    })

    @staticmethod
    def _nifi_el_to_placeholder(m: re.Match) -> str:
        """Convert a NiFi EL match like ${ENTORNO:substring(0,2)} → var_ENTORNO."""
        content = m.group(1)
        # Keep only the variable name (strip :function calls)
        var_name = re.split(r'[:\s]', content, maxsplit=1)[0]
        # Ensure it is a valid SQL identifier fragment
        var_name = re.sub(r'[^a-zA-Z0-9_]', '_', var_name)
        return f"var_{var_name}"

    @classmethod
    def _fix_operator_vars(cls, sql: str) -> str:
        """Replace var_XXX in operator position with '='."""
        def _repl(m: re.Match) -> str:
            preceding = m.group(1)
            if preceding.lower() in cls._SQL_STRUCTURAL_KW:
                return m.group(0)  # don't touch keyword contexts
            return f"{preceding} = "
        return cls._OPERATOR_VAR_RE.sub(_repl, sql)

    @staticmethod
    def _sanitize_sql(sql: str) -> str:
        """
        Reemplaza las variables NiFi EL (${…}) y $VAR por marcadores válidos SQL (var_VAR).
        Also normalises non-standard SQL constructs so sqlglot can parse them.
        """
        # 1. Quitar comentarios
        sql_clean = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.S)
        sql_clean = re.sub(r'--.*$', '', sql_clean, flags=re.M)

        # 2. Reemplazar expresiones NiFi EL completas ${…} → var_VARNAME
        #    Loop to resolve nested EL like ${fn(${inner})} inside-out.
        prev = None
        while prev != sql_clean:
            prev = sql_clean
            sql_clean = SQLUtils._NIFI_EL_REGEX.sub(
                SQLUtils._nifi_el_to_placeholder, sql_clean
            )

        # 3. Strip orphaned braces left over from nested EL
        sql_clean = re.sub(r'[{}]', '', sql_clean)

        # 4. Reemplazar marcadores sueltos restantes ($VAR sin llaves)
        sql_clean = re.sub(r'\$INSERT_STATEMENT', ' ', sql_clean, flags=re.IGNORECASE)
        sql_clean = sql_clean.replace('$', 'var_')

        # 5. UPSERT INTO → INSERT INTO  (Kudu/Impala, not in sqlglot)
        sql_clean = re.sub(r'\bUPSERT\s+INTO\b', 'INSERT INTO', sql_clean, flags=re.IGNORECASE)

        # 6. SELECT TOP N → SELECT  (T-SQL, not in Hive dialect)
        sql_clean = re.sub(r'\bSELECT\s+TOP\s+\d+\b', 'SELECT', sql_clean, flags=re.IGNORECASE)

        # 7. Variable placeholders in operator position
        #    e.g. "col var_Filtro CAST(…)" → "col = CAST(…)"
        sql_clean = SQLUtils._fix_operator_vars(sql_clean)

        # 8. Strip PARTITION(...) clause (Hive dynamic-partition INSERT)
        sql_clean = re.sub(r'\bPARTITION\s*\([^)]*\)', '', sql_clean, flags=re.IGNORECASE)

        # 9. Strip Impala-specific statements that sqlglot cannot parse.
        sql_clean = re.sub(r'\bCOMPUTE\s+STATS\b[^;]*;?', '', sql_clean, flags=re.IGNORECASE)
        sql_clean = re.sub(r'\bINVALIDATE\s+METADATA\b[^;]*;?', '', sql_clean, flags=re.IGNORECASE)

        # 10. Collapse spaced comparison operators:  > = → >=,  < = → <=,  ! = → !=
        sql_clean = re.sub(r'>\s+=', '>=', sql_clean)
        sql_clean = re.sub(r'<\s+=', '<=', sql_clean)
        sql_clean = re.sub(r'!\s+=', '!=', sql_clean)

        return sql_clean

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

        # Try several sqlglot dialects before falling back to regex.
        # Two passes: strict first (raises on error), then lenient
        # (ErrorLevel.WARN returns partial AST — good enough for table names).
        parsed = None
        dialect_errors: list[tuple[str, Exception]] = []
        dialects: list[tuple[str, str | None]] = [
            ('Hive', 'hive'),
            ('Spark', 'spark'),
            ('TSQL', 'tsql'),
            ('Default', None),
        ]

        # Pass 1 — strict
        for label, dialect in dialects:
            try:
                parsed_list = sqlglot.parse(clean_sql, read=dialect)
                if parsed_list and parsed_list[0] is not None:
                    parsed = parsed_list[0]
                    break
            except Exception as e:
                dialect_errors.append((label, e))
                logger.debug("Dialect %s (strict) failed: %s", label, e)

        # Pass 2 — lenient (partial AST)
        # Temporarily silence sqlglot's own logger so its internal
        # ERROR/WARNING messages don't pollute the application logs.
        if parsed is None:
            _sg_logger = logging.getLogger("sqlglot")
            _sg_prev_level = _sg_logger.level
            _sg_logger.setLevel(logging.CRITICAL)
            try:
                for label, dialect in dialects:
                    try:
                        parsed_list = sqlglot.parse(
                            clean_sql, read=dialect, error_level=ErrorLevel.WARN,
                        )
                        if parsed_list and parsed_list[0] is not None:
                            parsed = parsed_list[0]
                            logger.debug(
                                "Dialect %s (lenient) produced partial AST", label,
                            )
                            break
                    except Exception as e:
                        logger.debug("Dialect %s (lenient) failed: %s", label, e)
            finally:
                _sg_logger.setLevel(_sg_prev_level)

        if parsed is None:
            # All dialects failed — log once with the details, then try regex.
            if dialect_errors:
                labels = ', '.join(lbl for lbl, _ in dialect_errors)
                logger.warning(
                    "All SQL dialects (%s) failed for statement (first 500 chars): %.500s",
                    labels, clean_sql,
                )
            try:
                sources, dest = SQLUtils._extract_via_regex(clean_sql)
                return sources, dest
            except Exception as e:
                logger.error('Regex fallback also failed: %s', e)
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