import csv

from pathlib import Path
from typing import Optional

from .logger import setup_logger
logger = setup_logger('search')

def find_atlas_term_json(base_dir: Path, term_name: str) -> Optional[Path]:
    """
    Busca recursivamente en base_dir un archivo *.json cuyo stem coincida
    con el nombre del término (primero case sensitive, luego case insensitive).

    Args:
        base_dir (Path): Directorio base donde buscar.
        term_name (str): Nombre del término a buscar.

    Returns:
        Optional[Path]: Path del archivo encontrado, o None si no se encuentra.
    """

    term_name_stripped = term_name.strip()
    if not term_name_stripped:
        return None

    # Buscamos todos los JSONs de la carpeta
    json_files = base_dir.rglob("*.json")
    json_files = list(json_files)

    # Chequeamos si matchea por nombre completo
    for file in json_files:
        if file.stem == term_name_stripped:
            return file

    # Sino chequeamos si matchea por lo menos todo con Lowercase
    for file in json_files:
        if file.stem.lower() == term_name_stripped.lower():
            return file

    return None


def read_atlas_terms_from_csv(csv_path: Path) -> list[str]:
    '''
    Parsea los Atlas Terms del CSV y los devuelve en una lista.

    Args:
        csv_path (Path): Ruta al CSV.

    Returns:
        list[str]: Lista de Atlas Terms
    '''

    terms: list[str] = []
    if not csv_path.exists():
        raise FileNotFoundError(f"No existe el CSV: {csv_path}")

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "atlas_term" not in (reader.fieldnames or []):
            raise ValueError(f"El CSV {csv_path} no tiene la columna 'atlas_term'")
        for row in reader:
            term = (row.get("atlas_term") or "").strip()
            if term:
                terms.append(term)

    # deduplicar preservando orden
    seen, unique = set(), []
    for t in terms:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return unique