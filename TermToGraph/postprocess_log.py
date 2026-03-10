import re
import csv

def analizar_logs_categorizados(archivo_entrada, archivo_salida):
    # Regex para el término
    regex_termino = re.compile(r"Processing term \d+/\d+: ([\w\[\]\(\)\.\$\{\}-]+)")
    
    # Regex para capturar líneas de WARNING o ERROR
    # Captura el nivel (WARNING/ERROR) y el mensaje
    regex_log = re.compile(r"- (?:pipeline|sql_utils|zone_service|extractor) - (WARNING|ERROR) - (.+)")

    # Diccionario de mapeo de categorías
    mapeo_categorias = {
        "No loaded rows": "NO_LOADED_ROWS",
        "detected in a source but not used in any relation": "SOURCE_WITHOUT_DESTINATION",
        "Error parsing SQL with dialect": "ERROR_PARSING_SQL",
        "Term not found neither in Atlas or GitLab": "TERM_NOT_FOUND",
        "Error fetching term from Atlas": "ERROR_LOADING_TERM"
    }

    vistos = set()  # Para evitar duplicados (Term + Category)
    resultados = []
    ultimo_termino = "Desconocido"

    try:
        with open(archivo_entrada, 'r', encoding='utf-8') as f:
            for linea in f:
                linea = linea.strip()

                # 1. Identificar el término actual
                match_term = regex_termino.search(linea)
                if match_term:
                    ultimo_termino = match_term.group(1)
                    continue

                # 2. Identificar Warnings o Errors
                match_log = regex_log.search(linea)
                if match_log:
                    mensaje = match_log.group(2)
                    
                    # 3. Categorizar según las reglas
                    categoria_encontrada = None
                    for patron, categoria in mapeo_categorias.items():
                        if patron in mensaje:
                            categoria_encontrada = categoria
                            break
                    
                    # 4. Si se categorizó y no es duplicado, guardar
                    if categoria_encontrada:
                        id_unico = (ultimo_termino, categoria_encontrada)
                        if id_unico not in vistos:
                            resultados.append({
                                'Atlas Term': ultimo_termino,
                                'Category': categoria_encontrada
                            })
                            vistos.add(id_unico)

        # 5. Generar el CSV
        with open(archivo_salida, 'w', newline='', encoding='utf-8') as f_csv:
            campos = ['Atlas Term', 'Category']
            writer = csv.DictWriter(f_csv, fieldnames=campos)
            writer.writeheader()
            writer.writerows(resultados)

        print(f"Proceso finalizado. Se exportaron {len(resultados)} registros únicos.")

    except FileNotFoundError:
        print(f"Error: El archivo '{archivo_entrada}' no existe.")

# Ejecución
analizar_logs_categorizados('pipeline.log', 'categorias_errores.csv')