# Análisis de Atlas Terms

El proyecto busca analizar los distintos Atlas Terms en formato JSON que parametrizan los flujos de NiFi, para obtener las relaciones de escritura entre tablas (Tabla X alimenta a Tabla Y). 

Se leen los Atlas Terms de "DRS_ATLAS_PROD" (que es una copia del repositorio de GitLab) y de Apache Atlas (en caso de que no hayan sido encontrados en la primera). Luego, analiza los JSONs encontrados en busca de la información de las tablas y zonas (Raw, Curado, Consumo, Etc...)

Las zonas actualmente soportadas son:
- Origen (Microsoft SQL Server, Oracle, etc)
- Landing (Archivos, carpetas, etc.)
- Raw.
- Curado.
- Refinado.
- Consumo.
- Datamart (Tanto datamarts como Data Analytics).

Suposiciones realizadas que se sabe que introducen sesgos en los resultados:
- Solamente se puede leer de una zona anterior o igual a la que se está escribiendo.
- En caso de que en el mismo JSON haya más de una zona de escritura, la regla anterior sola aplica para una zona anterior, no se consideran zonas iguales para armar las relaciones.
- Si el destino es Datamart, si puede leer del mismo Datamart.

# Estructura

```
.
├── src/
│   ├── extractors/
│   │   ├── extractor.py              # Extraer las tablas y zonas del JSON
│   │   ├── dataclasses_extractor.py  # Clases de datos del extractor.
│   │   ├── sql_utils.py              # Utilidades para procesar queries SQL en los JSONs.
│   │   └── zone_service.py           # Utilidades para detectar zonas y ordenarlas.
│   ├── apache_atlas.py               # Clase para buscar Atlas Terms en Apache Atlas
│   ├── config.py                     # Configurar parámetros del script
│   ├── logger.py                     # Configuración de Logging
│   ├── neo4j.py                      # Clase para subir relaciones a Neo4J
│   ├── parse_sql.py                  # Funciones de utilidad para parsear SQL
│   ├── pipeline.py                   # Clase que orquesta la ejecución de todo el proceso
│   └── search.py                     # Funciones de utilizadad para buscar en DRS_ATLAS_PROD
├── .env.example                      # Ejemplo de credenciales necesarias
├── .gitignore                        # Archivos que Git no trackea 
├── main.py                           # Punto de entrada principal
├── postprocess_log.py                # Script de utilidad para analizar errores en los logs.
├── pyproject.toml                    # Dependencias para UV
├── requirements.txt                  # Dependencias si no se desea utilizar UV.
├── uv.lock                           # Archivo de configuración de UV
└── README.md                         # Documentación del proyecto.
```

# Ejecución
1. Instalar uv con `pip install uv`.
2. Sincronizar el proyecto de Git con `uv sync`
3. Configurar la base de Neo4J si se quiere generar el grafo de relaciones. 
4. Configurar el archivo `.env` usando de referencia el `.env.example`.
5. Descargar el archivo `atlas_terms.csv` y la carpeta `DRS_ATLAS_PROD`. Solicitar a [Facundo Joaquín García](mailto:fjgarcia@dblandit.com) (fjgarcia@dblandit.com).
6. Ejecutar con `uv run python main.py` si se quiere generar el grafo de relaciones. Si se desea simplemente exportar como `.csv` el output, se puede utilizar `uv run python main.py --plan-only`.
7. Al ejecutar el paso 6, se generará un archivo `pipeline.log`. Si se desea analizar los errores encontrados durante la ejecución del paso 6, se puede ejecutar `uv run python postprocess_log.py`, el cual generará `categorias_errores.csv`, que cuenta con la información de los problemas encontrados en los distintos Atlas terms.