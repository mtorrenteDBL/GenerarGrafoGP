from dataclasses import dataclass, field
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class LineageContext:
    """Context for processing lineage information from entity attributes."""
    type_name: str = "Clasificacion"
    attrs: Dict[str, object] = field(default_factory=dict)
    explicit_src: List[Tuple[str, Optional[str]]] = field(default_factory=list)  # List of (table, zone_hint)
    explicit_dst: List[Tuple[str, Optional[str]]] = field(default_factory=list)
    sql_queries: List[Tuple[str, Optional[str]]] = field(default_factory=list)  # List of SQL strings  # zone -> [sqls]
    pre_delete: List[Tuple[str, Optional[str]]] = field(default_factory=list)
    post_update: List[Tuple[str, Optional[str]]] = field(default_factory=list)
    hbase_queries: List[Tuple[str, Optional[str]]] = field(default_factory=list)
    hbase_dsts: List[Tuple[str, Optional[str]]] = field(default_factory=list)


@dataclass
class Table:
    name: str
    zone: Optional[str] = None
    
    def __str__(self) -> str:
        name_parts = self.name.split(".")
        if len(name_parts) == 2 and self.zone: 
            return f"{self.zone}.{name_parts[1]}"
        elif self.zone:
            return f"{self.zone}.{self.name}"
        return self.name
    

@dataclass
class LineageRelation:
    sources: List[Table]
    destination: List[Table]

@dataclass
class LineageResult:
    """Result of resolving lineage with sources and destinations."""
    relations: List[LineageRelation] = field(default_factory=list)
    
@dataclass
class ClassificationInfo:
    """Information about a classification with its lineage."""
    nombre: str
    tipo: str
    attrs: Dict[str, object]
    LineageResults: LineageResult

@dataclass
class LineageRow:
    """Single row of lineage information with explicit zone data."""
    atlas_term: str
    clasificacion: str
    origen: Optional[str]
    origen_zona: Optional[str]
    destino: Optional[str]
    destino_zona: Optional[str]
    
    def to_dict(self) -> Dict[str, Optional[str]]:
        """Convert to dict for backward compatibility with CSV export."""
        return {
            "atlas_term": self.atlas_term,
            "clasificacion": self.clasificacion,
            "origen": self.origen if not self.origen_zona else f"{self.origen_zona}.{self.origen}",
            "destino": self.destino if not self.destino_zona else f"{self.destino_zona}.{self.destino}",
        }


@dataclass
class TermLineageResult:
    """Result of processing an Atlas term with full lineage information."""
    term: str
    status: str
    rows: List[LineageRow]
    
    def to_dict(self) -> Dict[str, object]:
        """Convert to dict for backward compatibility."""
        return {
            "term": self.term,
            "status": self.status,
            "rows": [row.to_dict() for row in self.rows]
        }



