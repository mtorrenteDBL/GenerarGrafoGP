# GenerarGrafoPetersen

Unified orchestrator for FlowToGraph and Migracion GP pipelines with Neo4j graph database.

## Project Structure

```
GenerarGrafoPetersen/
├── main.py                 # 🆕 Python orchestrator (loads .env and coordinates pipelines)
├── main.sh                 # 🆕 Simple launcher script using uv
├── reset_and_run.sh        # ♻️ Backward compatibility wrapper (calls main.sh)
├── pyproject.toml          # Project dependencies
├── .env                    # Environment configuration (gitignored)
├── .env.example            # 🆕 Example environment configuration
│
├── data/                   # 🆕 All data files (gitignored)
│   ├── atlas_terms.csv     # Atlas terms to migrate
│   ├── info_clusters.csv   # NiFi cluster SSH configurations
│   └── flows/              # Downloaded flow files (*.json, *.xml)
│
├── shared/                 # 🆕 Shared utilities across subprojects
│   ├── __init__.py
│   └── sql_utils.py        # SQL parsing utilities (single source of truth)
│
├── FlowToGraph/            # Subproject: NiFi flow processing (only Python code)
│   ├── flow_pipeline.py    # 🆕 Self-contained entry point
│   ├── fetch_flows.py      # Fetch flows from remote clusters
│   ├── config.py           # ♻️ Uses env variables for Neo4j
│   ├── nifi_parser.py
│   ├── graph_builder.py
│   └── find_sql.py         # ♻️ Imports from shared.sql_utils
│
└── Migracion GP/           # Subproject: Atlas term migration (only Python code)
    ├── atlas_migration.py  # 🆕 Migration entry point
    └── src/
        ├── pipeline.py
        └── extractors/
            └── extractor.py # ♻️ Imports from shared.sql_utils
```

## Key Improvements

### ✅ Eliminated Code Duplication
- **Before**: Two duplicate copies of `sql_utils.py` in FlowToGraph and Migracion GP
- **After**: Single `shared/sql_utils.py` module used by both subprojects

### ✅ Python-Based Orchestration
- **Before**: Bash scripts (`reset_and_run.sh`, `FlowToGraph/main.sh`, `Migracion GP/main.sh`) managed workflow
- **After**: Python `main.py` orchestrates entire pipeline with proper logging and error handling
- **FlowToGraph**: Now self-contained with flow fetching built into `flow_pipeline.py`

### ✅ Centralized Configuration
- **Before**: Multiple `.env` files, hardcoded credentials in config files
- **After**: Single `.env` file loaded once in `main.py`, all modules use environment variables
- **FlowToGraph/config.py**: Now uses `NEO4J_HOST`, `NEO4J_USER`, `NEO4J_PASS` from environment

### ✅ Clean Project Structure
- **Before**: Data files mixed with Python code in subprojects
- **After**: All data in `data/` directory (atlas_terms.csv, info_clusters.csv, flows/)
- **Subprojects**: Contain only Python code, no data files

### ✅ Simplified Execution
- **Before**: Complex nested bash scripts with multiple entry points
- **After**: Single command: `bash main.sh` (or legacy `bash reset_and_run.sh`)

### ✅ Better Modularity
- **FlowToGraph** can run independently: `cd FlowToGraph && uv run python main.py`
- **Migracion GP** remains focused on Atlas migration
- Root orchestrator coordinates both pipelines

### ✅ Better Logging
- All output goes to timestamped log files in `log/orchestrator_YYYYMMDD_HHMMSS.log`
- Console output streams in real-time
- Proper error tracking with exit codes

## Usage

### Production (Linux/Server)

**Important:** Do NOT redirect stdout/stderr at the shell level. Python's logging is configured to write timestamped log files automatically.

```bash
cd /opt/GenerarGrafoGP && bash /opt/GenerarGrafoGP/reset_and_run.sh
```

Or directly:

```bash
cd /opt/GenerarGrafoGP && bash /opt/GenerarGrafoGP/main.sh
```

Logs will be written to: `log/orchestrator_YYYYMMDD_HHMMSS.log`

**For cron jobs:** If you want to capture only the exit code (and email notifications), use `2>/dev/null` to suppress debug output:

```bash
cd /opt/GenerarGrafoGP && bash /opt/GenerarGrafoGP/reset_and_run.sh 2>/dev/null
```

### Local Development

```bash
# Install dependencies
uv sync

# Run the full orchestrator (wipes DB, runs both pipelines)
bash main.sh

# Or run Python directly
uv run python main.py

# Run only FlowToGraph pipeline (fetches and processes all flows by default)
cd FlowToGraph && uv run python flow_pipeline.py

# Process a single specific flow file (legacy mode)
cd FlowToGraph && uv run python flow_pipeline.py --flow flows/my_flow.json --root-name My_Flow

# Run only Migracion GP pipeline
cd "Migracion GP" && uv run python atlas_migration.py
```

## Pipeline Flow

1. **Load Environment**: Reads `.env` for Neo4j and SSH credentials (loaded once)
2. **Wipe Database**: Deletes all nodes and relationships in Neo4j
3. **FlowToGraph Pipeline** (self-contained):
   - **Fetches** flow files from remote NiFi clusters via SSH (saved to `data/flows/`)
   - **Processes** each `.json`/`.xml` flow file
   - Builds graph structure in Neo4j (ProcessGroups, Atlas Terms, Kafka, Tables, etc.)
4. **Migracion GP Pipeline**:
   - Reads `data/atlas_terms.csv`
   - Migrates Atlas lineage metadata to Neo4j
5. **Summary**: Reports success/failure for each pipeline

## FlowToGraph Module

The FlowToGraph subproject is now **self-contained** and can run independently:

### Default Mode (Fetch All Flows)
```bash
cd FlowToGraph
uv run python flow_pipeline.py
```
This will:
1. Fetch all flows from remote clusters (via SSH)
2. Process all downloaded flows into Neo4j

### Single Flow Mode (Legacy)
```bash
cd FlowToGraph
uv run python flow_pipeline.py --flow ../data/flows/my_flow.json --root-name My_Flow
```

## Configuration

### Environment Setup

1. **Copy the example environment file**:
   ```bash
   cp .env.example .env
   ```

2. **Configure your environment variables**:
   ```env
   # Neo4j Configuration
   NEO4J_HOST=bolt://localhost:7687
   NEO4J_USER=neo4j
   NEO4J_PASS=your_password

   # SSH Configuration (for FlowToGraph)
   SSH_USER=your_username
   SSH_PASSWORD=your_password          # Optional if using key auth
   SSH_KEY_PATH=/path/to/private_key   # Optional if using password auth
   SSH_PORT=22                         # Optional, defaults to 22
   ```

**Important**: Environment variables are loaded once in `main.py` and shared across all subprojects.

### Data Files Setup

All data files are stored in the `data/` directory:

- **`data/atlas_terms.csv`**: List of Atlas terms to migrate
- **`data/info_clusters.csv`**: NiFi cluster SSH configurations
- **`data/flows/`**: Downloaded NiFi flow files (auto-populated by fetch_flows)

**First-time setup or migration**:
```bash
# Run the setup helper script to check/migrate data files
bash setup_data.sh
```

This script will:
- Check if the `data/` directory exists
- Migrate CSV files from old locations (FlowToGraph/, Migracion GP/) if found
- Verify required data files are present
- Create the flows directory if needed

### Dependencies

All dependencies are managed in `pyproject.toml`:

- `neo4j>=6.1.0` - Neo4j driver
- `sqlglot>=29.0.1` - SQL parsing
- `paramiko>=4.0.0` - SSH client
- `python-dotenv>=1.2.1` - Environment configuration
- `pandas>=3.0.1` - Data manipulation
- `requests>=2.32.5` - HTTP client
- `sqlparse>=0.5.5` - SQL parsing fallback

## Migration Guide

### For Developers

**Old approach** (duplicated code):
```python
# FlowToGraph/find_sql.py
from sql_utils import SQLUtils

# Migracion GP/src/extractors/extractor.py
from .sql_utils import SQLUtils
```

**New approach** (shared module):
```python
# Both files now use:
from shared.sql_utils import SQLUtils
```

### For CI/CD Pipelines

Use the updated cron command without shell redirection:

```bash
cd /opt/GenerarGrafoGP && bash /opt/GenerarGrafoGP/reset_and_run.sh
```

Logs are automatically written to `log/orchestrator_YYYYMMDD_HHMMSS.log` by Python's logging.

## Benefits

✅ **DRY Principle**: Single source of truth for shared utilities  
✅ **Maintainability**: Update SQL logic in one place  
✅ **Type Safety**: Python orchestration enables better IDE support  
✅ **Error Handling**: Comprehensive exception handling and logging  
✅ **Debugging**: Easier to debug Python than nested bash scripts  
✅ **Testing**: Can unit test orchestration logic  
✅ **Backward Compatible**: Old commands continue to work  

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError: No module named 'shared'`:

1. Ensure you're running from the project root
2. Check `pyproject.toml` has the correct configuration
3. Use `uv run python main.py` instead of bare `python main.py`

### Neo4j Connection Issues

Check your `.env` file has correct credentials:
```bash
grep NEO4J .env
```

### Flow Fetch Failures

Check SSH configuration in `.env` and verify:
```bash
ssh -i $SSH_KEY_PATH $SSH_USER@<cluster-ip>
```

### Missing Data Files

Ensure data files are in the correct location:
```
data/
├── atlas_terms.csv      # Required for Migracion GP
├── info_clusters.csv    # Required for FlowToGraph SSH fetching
└── flows/               # Auto-populated by fetch_flows
```

**If you're migrating from an older version** or setting up for the first time:

1. Run the setup helper script:
   ```bash
   bash setup_data.sh
   ```

2. The script will automatically:
   - Migrate `FlowToGraph/info_clusters.csv` → `data/info_clusters.csv`
   - Migrate `Migracion GP/atlas_terms.csv` → `data/atlas_terms.csv`
   - Create the `data/flows/` directory
   - Verify all required files are present

3. If files are still missing, manually create them:
   ```bash
   mkdir -p data/flows
   # Copy your CSV files to data/
   cp /path/to/your/atlas_terms.csv data/
   cp /path/to/your/info_clusters.csv data/
   ```

**On production servers** (e.g., `/opt/GenerarGrafoGP/`):
- Ensure the `data/` directory exists with proper permissions
- Verify CSV files are present before running the pipeline
- Check log files if errors occur: `log/orchestrator_*.log`

## Future Enhancements

- Add CLI arguments for selective pipeline execution
- Implement retry logic for failed flows
- Add metrics collection and reporting
- Create Docker containerization
- Add pre-commit hooks for code quality

## License

Internal project - All rights reserved
