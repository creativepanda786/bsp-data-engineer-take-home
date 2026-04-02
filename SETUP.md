# Setup Guide

## Prerequisites

- **Python 3.11+**
- **uv** (Python package manager) — [Install uv](https://docs.astral.sh/uv/getting-started/installation/)
- **Git**

## Step 1: Clone and Install

```bash
git clone <repository-url>
cd bsp-data-engineer-take-home
uv sync
```

This installs all dependencies including DuckDB, Prefect, Soda, and Polars.

## Step 2: Generate Seed Data

```bash
uv run python scripts/seed_data.py
```

This creates CSV files in the `data/` directory. The generator uses a fixed random seed for deterministic output.

## Step 3: Run Bronze Ingestion

```bash
PYTHONPATH=. uv run python flows/ingest.py
```

This creates the DuckDB database at `data/pawsfirst.duckdb` and loads all CSV data into bronze tables.

## Step 4: Run Silver Transform

```bash
PYTHONPATH=. uv run python flows/silver_transform.py
```

This creates silver staging views over the bronze tables.

## Step 5: Verify Setup

Open a DuckDB shell to verify data is loaded:

```bash
PYTHONPATH=. uv run python -c "
import duckdb
con = duckdb.connect('data/pawsfirst.duckdb')
for schema in ['bronze', 'silver']:
    tables = con.execute(f\"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'\").fetchall()
    print(f'{schema}: {[t[0] for t in tables]}')
con.close()
"
```

You should see 8 bronze tables and 4 silver views.

## Running Tests

```bash
uv run pytest tests/ -v
```

## Running Soda Contracts

Soda contracts validate data quality. To run bronze contracts:

```bash
uv run soda scan -d pawsfirst -c soda/configuration.yml soda/contracts/bronze/*.yml
```

> **Note:** You'll need to create a `soda/configuration.yml` pointing to your local DuckDB. See the [Soda DuckDB docs](https://docs.soda.io/soda/connect-duckdb.html) for configuration details. This is part of the exercise — setting up Soda configuration is expected.

## Useful DuckDB Queries

```sql
-- List all tables and views
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY table_schema, table_name;

-- Quick row counts
SELECT 'bronze.vet_appointment' as tbl, COUNT(*) as rows FROM bronze.vet_appointment
UNION ALL
SELECT 'bronze.vet_patient', COUNT(*) FROM bronze.vet_patient
UNION ALL
SELECT 'silver.stg_vet_appointment', COUNT(*) FROM silver.stg_vet_appointment
UNION ALL
SELECT 'silver.stg_vet_patient', COUNT(*) FROM silver.stg_vet_patient;
```

## Note on PYTHONPATH

Flow scripts import from the `flows` package. Since this isn't installed as an editable package, you need to set `PYTHONPATH=.` when running flow scripts. Alternatively, you can add the project to your Python path:

```bash
# Option A: Set PYTHONPATH each time
PYTHONPATH=. uv run python flows/ingest.py

# Option B: Install as editable package (add to pyproject.toml first)
# [tool.setuptools.packages.find]
# where = ["."]
# then: uv pip install -e .
```
