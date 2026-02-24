import re
from typing import Optional, Tuple
from ..parse_sql import (
    clean_table_name,
)
from .dataclasses_extractor import (
    Table,
    LineageRelation,
)

from ..logger import setup_logger
logger = setup_logger(name='zone_service')

class ZoneService:
    """
    Centralizes all logic related to Zone naming conventions, 
    ordering, and identification.
    """
    
    ZONE_ORDER = ["Origen", "Landing", "Raw", "Curado", "Refinado", "Consumo", "Datamart"]
    _ZONE_ORDER_INDEX = {z: i for i, z in enumerate(ZONE_ORDER)}

    # Regex compilation
    ZONE_TOKEN_RE = re.compile(
        r'^(land|1raw|2cur|3ref|4con|raw|ref|cur|dm|mart|datamart|data_analytics|kudu|insertkudu|landing|lnd|land|con|conformado)$', 
        re.IGNORECASE
    )
    PLACEHOLDER_PREFIX_RE = re.compile(r'^(?:(?:\$|var_|\${?)[a-z0-9]+}?_)+', re.IGNORECASE)
    KNOWN_ZONE_PREFIX = r"(?:1land|2cur|3ref|4con|1raw|raw|ref|cur|dm|mart|datamart(?:_[a-z0-9_]+)?|data_analytics|kudu|insertkudu|landing|lnd|land|con|consumo)"

    # Mappings
    ZONE_TOKEN_TO_FUNC = {
        # Origen
        "origen": "Origen",

        # Landing
        "landing": "Landing", 
        "land": "Landing", 
        "lnd": "Landing", 
        "1land": "Landing",
        "file": "Landing", 
        "archivo": "Landing", 
        "path": "Landing", 
        "directorio": "Landing",
        "carpeta": "Landing", 
        "folder": "Landing",

        # Raw
        "raw": "Raw", 
        "1raw": "Raw",
        
        # Refinado
        "ref": "Refinado", 
        "3ref": "Refinado", 
        "refinado": "Refinado",
        
        # Curado
        "cur": "Curado", 
        "2cur": "Curado",
        "curado": "Curado",

        # Consumo
        "con": "Consumo", 
        "4con": "Consumo", 
        "consumo": "Consumo", 
        "kudu": "Consumo", 
        "insertkudu": "Consumo",
        
        # Datamart
        "dm": "Datamart", 
        "mart": "Datamart", 
        "datamart": "Datamart",
        
        # Deprecados
        "sftp": "SFTP", 
        "ftp": "SFTP", 
    }

    @classmethod
    def get_order_index(cls, zone: str) -> int:
        if not zone: 
            return -1
        z = zone.lower()
        if z.startswith("datamart_") or z == "data_analytics":
            return cls._ZONE_ORDER_INDEX["Datamart"]
        return cls._ZONE_ORDER_INDEX.get(zone, -1)

    @classmethod
    def normalize_token(cls, token: Optional[str]) -> Optional[str]:
        '''Converts a token to a normalized zone name.'''
        if not token: 
            return None
        t = token.lower()
        if t in cls.ZONE_TOKEN_TO_FUNC:
            return cls.ZONE_TOKEN_TO_FUNC[t]
        if t.startswith("datamart_") or t == "data_analytics":
            return t
        return None

    @classmethod
    def infer_from_text(cls, s: Optional[str]) -> Optional[str]:
        """Heuristic to guess zone from a raw string (table name or attribute key)."""
        if not s:
            return None

        txt = str(s).lower()

        # Specific overrides (substring-based on purpose)
        if "kudu" in txt or "insertkudu" in txt:
            return "Consumo"
        if "data_analytics" in txt:
            return "data_analytics"
        if "sftp" in txt or "ftp" in txt:
            return "SFTP"
        if "archivo" in txt:
            return "Landing"

        # Regex checks
        checks = [
            # datamart_<name> → return full token
            (
                r"(^|[^a-z0-9])(datamart_[a-z0-9_]+)($|[^a-z0-9])",
                lambda m: m.group(2),
            ),

            # Zone identifiers
            (r"(^|[^a-z0-9])(raw|1raw)($|[^a-z0-9])", "Raw"),
            (r"(^|[^a-z0-9])(ref|refinado|3ref)($|[^a-z0-9])", "Refinado"),
            (r"(^|[^a-z0-9])(cur|curado|2cur)($|[^a-z0-9])", "Curado"),
            (r"(^|[^a-z0-9])(land|lnd|landing|1land|archivo)($|[^a-z0-9])", "Landing"),
            (r"(^|[^a-z0-9])(con|consumo|4con)($|[^a-z0-9])", "Consumo"),

            # Generic semantic hints
            (r"(^|[^a-z0-9])(dm|mart|datamart)($|[^a-z0-9])", "Datamart"),
            (r"(^|[^a-z0-9])(archivo|file|path|directorio|carpeta|folder)($|[^a-z0-9])", "file"),
        ]

        for pattern, result in checks:
            m = re.search(pattern, txt)
            if m:
                return result(m) if callable(result) else result

        return None
    
    
    @classmethod
    def canonicalize_table_pair(cls, raw_table: str, zone_hint: Optional[str] = None) -> Tuple[Optional[str], str]:
        """
        Returns (functional_zone, canonical_name).
        For example, parses '3ref.mytable' -> ('Refinado', 'mytable')
        """

        s = (raw_table or "").strip().replace("`", "").replace("[", "").replace("]", "")
        if s.startswith('"') and s.endswith('"') and len(s) >= 2:
            s = s[1:-1]
        
        if not s: 
            return None, ""
        s_low = s.lower()

        # Helper to decide zone priority
        def resolve_zone(z_token):
            return (cls.normalize_token(z_token) or 
                    cls.normalize_token(zone_hint) or 
                    cls.normalize_token(cls.infer_from_text(z_token)))

        if "." not in s_low:
            # No schema, use hint
            return (cls.normalize_token(zone_hint) or cls.normalize_token(cls.infer_from_text(zone_hint)), s_low)

        left, right = s_low.split(".", 1)
        clean_left = cls.PLACEHOLDER_PREFIX_RE.sub("", left) 
        
        # Si la limpieza dejó el string vacío (raro, pero posible), usamos el original
        if not clean_left: 
            clean_left = left

        m_dm = re.search(r'(?:^|_)datamart_([a-z0-9_]+)$', clean_left, re.IGNORECASE)

        if m_dm:
            # Encontró datamart_campaigns al final del esquema.
            # Extraemos 'campaigns' (group 1) y construimos 'campaigns.tabla'
            real_schema_suffix = m_dm.group(1)
            return ("Datamart", f"{real_schema_suffix}.{right}")
        
        if clean_left == "data_analytics":
            return ("Datamart", f"analytics.{right}")

        # Check suffix like 'db_3ref'
        m_suffix = re.search(rf'(?:^|_)(({cls.KNOWN_ZONE_PREFIX}))$', left, re.IGNORECASE)
        if m_suffix:
            zone_detected = resolve_zone(m_suffix.group(1))
            return (zone_detected, f"{clean_left}.{right}")

        # Check Datamart special cases
        m_dm = re.match(r'^datamart_([a-z0-9_]+)$', left, re.IGNORECASE)
        if m_dm:
            return ("Datamart", f"{m_dm.group(1)}.{right}")
        
        if left == "data_analytics":
            return ("Datamart", f"analytics.{right}")

        if cls.ZONE_TOKEN_RE.match(left):
            return (resolve_zone(left), right)

        # Default fallback
        final_zone = resolve_zone(left) or resolve_zone(zone_hint)
        return (final_zone, f"{left}.{right}")

    @classmethod
    def infer_from_table_name(cls, table: str) -> Optional[str]:
        # Quick wrapper to check schema then full name
        t_clean = clean_table_name(table or "").lower()
        if "." in t_clean:
            schema = t_clean.split(".", 1)[0]
            z = cls.infer_from_text(schema)
            if z: 
                return z
        return cls.infer_from_text(t_clean)
    
    
    
    @classmethod
    def _order_tables(
        cls,
        sources: list[Table],
        destinations: list[Table],
    ) -> list[LineageRelation]:
        
        for table in sources:
            if table.name.upper() == 'FLOWFILE':
                logger.warning('Flowfile found as a source. This is not supported.')

        relations: list[LineageRelation] = []

        # Group sources by zone index
        sources_by_zone: dict[int, list[Table]] = {}
        for table in sources:
            if table.zone is None:
                continue

            idx = cls.get_order_index(table.zone)
            if idx == -1:
                logger.warning(
                    f"Source table {table.name} has unknown zone '{table.zone}', skipping."
                )
                continue

            sources_by_zone.setdefault(idx, []).append(table)

        logger.debug(
            f"Sources grouped by zone index count: "
            f"{ {k: len(v) for k, v in sources_by_zone.items()} }"
        )

        datamart_idx = cls.get_order_index("Datamart")
        landing_idx = cls.get_order_index("Landing")

        # Determine if same-zone fallback is allowed (non-Datamart)
        dest_zones = {d.zone for d in destinations if d.zone is not None}
        allow_same_zone_fallback = len(destinations) == 1 or len(dest_zones) == 1

        for dest in destinations:
            if dest.zone is None:
                continue

            dest_idx = cls.get_order_index(dest.zone)
            if dest_idx == -1:
                logger.warning(
                    f"Destination table {dest.name} has unknown zone '{dest.zone}', skipping."
                )
                continue

            matched_sources: list[Table] = []

            # Datamart: always allow same-zone match
            if dest_idx == datamart_idx:
                datamart_sources = sources_by_zone.get(datamart_idx)
                if datamart_sources:
                    matched_sources.extend(datamart_sources)

            # Try lower zones first:
            for src_idx in range(dest_idx - 1, -1, -1):
                if src_idx in sources_by_zone:
                    matched_sources.extend(sources_by_zone[src_idx])
                    break

            # Fallback to same zone (controlled)
            if (
                dest_idx in sources_by_zone
                and (allow_same_zone_fallback or dest_idx == datamart_idx)
                and dest_idx != landing_idx
            ):
                matched_sources.extend(sources_by_zone[dest_idx])

            if not matched_sources:
                continue

            relations.append(
                LineageRelation(
                    sources=matched_sources,
                    destination=[dest],
                )
            )
        
        # Ver que zonas quedaron de source y comparar contra el inicio
        # Luego loggear los que faltan
        original_source_zones = {t.zone for t in sources}
        relation_sources = set()
        for rel in relations:
            for source in rel.sources:
                relation_sources.add(source.zone)
        difference = original_source_zones - relation_sources
        if difference:
            for zone in difference:
                if zone is not None:
                    logger.warning('Zone {} detected in a source but not used in any relation'.format(zone))

        return relations



