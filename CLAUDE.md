# PawsFirst Veterinary Warehouse — AI Assistant Guide

## Project Overview

A Kimball star-schema data warehouse for PawsFirst Veterinary Network built on
**DuckDB** + **Prefect** + **Soda v4**. Data flows through three layers:
`bronze → silver → gold`.

---

## Commands

```bash
# Run all three pipeline flows in order
PYTHONPATH=. uv run python flows/ingest.py
PYTHONPATH=. uv run python flows/silver_transform.py
PYTHONPATH=. uv run python flows/gold_transform.py

# Run Soda contracts for a single layer
for f in soda/contracts/bronze/*.yml; do uv run soda contract verify -c $f -ds soda/configuration.yml; done
for f in soda/contracts/silver/*.yml; do uv run soda contract verify -c $f -ds soda/configuration.yml; done
for f in soda/contracts/gold/*.yml;   do uv run soda contract verify -c $f -ds soda/configuration.yml; done

# Run tests
PYTHONPATH=. uv run pytest tests/ -v

# Seed / reset the database
PYTHONPATH=. uv run python scripts/seed_data.py
```

---

## Architecture

### Schemas

| Schema | Purpose | Object Types |
|--------|---------|--------------|
| `bronze` | Raw ingestion — source fidelity preserved | Tables (`vet_*`) |
| `silver` | Cleaned, typed, deduped | Views (`stg_vet_*`) |
| `gold` | Business-semantic Kimball star schema | Tables + Views |

### Gold Layer Objects

**Dimensions (SCD2)**
- `gold.dim_core_clinic` — clinic master with 1012→1013 rename handled
- `gold.dim_clin_patient` — patient dimension
- `gold.dim_clin_provider` — provider dimension

**Facts**
- `gold.fact_clin_appointment` — one row per appointment
- `gold.fact_fin_invoice` — one row per invoice
- `gold.fact_pipe_referral_stage` — one row per referral stage record

**Reference**
- `gold.ref_core_budget_target` — monthly budget targets by clinic (144 rows = 12 clinics × 12 months)

**Analytical Views**
- `gold.agg_clin_clinic_weekly` — Req 1: weekly appt volume by clinic
- `gold.v_pipe_funnel_summary` — Req 2: referral funnel by month/clinic
- `gold.v_fin_revenue_by_clinic_monthly` — Req 3: revenue vs budget
- `gold.v_ops_provider_utilization_weekly` — Req 4: provider utilization
- `gold.v_clin_duplicate_patients_exceptions` — Req 5: duplicate patient detection
- `gold.v_clin_patient_retention_cohort` — Req 6: patient retention cohorts

---

## Key Conventions

### Naming Pattern
```
{type_prefix}_{domain}_{entity}_{qualifier}
```

| Prefix | Object Type | Domain |
|--------|-------------|--------|
| `dim_` | SCD2 dimension | `core`, `clin`, `pipe` |
| `fact_` | Fact table | `clin`, `fin`, `pipe` |
| `agg_` | Pre-aggregated | `clin` |
| `ref_` | Reference/lookup | `core` |
| `v_` | Analytical view | `fin`, `ops`, `pipe`, `clin` |

### SCD2 Load Pattern
DuckDB does **not** support `MERGE`. Use the `_run_phases` helper with `-- PHASE_BREAK` markers:
1. Create `TEMP VIEW` for staging data with `row_hash`
2. `UPDATE` to expire changed rows (`is_current = FALSE`, set `effective_end`)
3. `INSERT` new/changed rows

**Critical:** `_run_phases` executes each phase as a **single statement** — never splits on `;`
because DuckDB breaks on semicolons inside string literals like `COALESCE(x, '')`.

### Column Conventions
- Surrogate keys: `<entity>_sk` (UUID via `gen_random_uuid()`)
- Natural keys: `<entity>_id`
- Booleans: `is_` or `has_` prefix
- Timestamps: `_at` suffix
- Dates: `_date` suffix
- Date grammar: `{business_event}_{provenance}_{type}` e.g. `appointment_documented_date`

---

## Database Connection

Always use the helper — never hardcode paths:
```python
from flows.common import get_connection, ensure_schemas
con = get_connection()   # connects to data/pawsfirst.duckdb
ensure_schemas(con)      # creates bronze/silver/gold schemas if missing
```

---

## Soda Contracts

- Version: **Soda v4** (`soda contract verify` command, not `soda scan`)
- Dataset identifier format: `pawsfirst/{schema}/{table}` (e.g. `pawsfirst/gold/dim_core_clinic`)
- Data source config: `soda/configuration.yml`
- Run command: `uv run soda contract verify -c <contract.yml> -ds soda/configuration.yml`
- **One** `failed_rows:` check per contract (Soda v4 does not allow duplicate check identities)
- `duplicate` check goes **inside** the column definition, not at table level

---

## Known DuckDB Quirks

1. **No correlated JOIN in UPDATE** — use `INNER JOIN` inside the subquery instead:
   ```sql
   -- WRONG
   UPDATE t SET ... WHERE id IN (SELECT s.id FROM staging s JOIN t ON ...)
   -- CORRECT
   UPDATE t SET ... WHERE id IN (SELECT d.id FROM t d INNER JOIN staging s ON s.id = d.id WHERE ...)
   ```
2. **No MERGE statement** — use DELETE + INSERT pattern
3. **`DATE_TRUNC` returns DATE** (not TIMESTAMP) in views
4. **`EXTRACT()` returns BIGINT** (not DOUBLE)
5. **`SUM(CASE WHEN ...)` returns HUGEINT** (not BIGINT)
6. **Semicolons inside string literals** (e.g. `COALESCE(x, '')`) will break naive `;` splitting

---

## Prefect Tasks

All `@task` decorators must include `cache_policy=NO_CACHE` because `DuckDBPyConnection`
is not serializable for Prefect's cache key computation:
```python
from prefect.cache_policies import NO_CACHE

@task(name="...", retries=1, retry_delay_seconds=5, cache_policy=NO_CACHE, tags=["gold"])
def my_task(con): ...
```

---

## File Structure

```
flows/
  common.py            # get_connection(), ensure_schemas()
  ingest.py            # Bronze layer ingestion (CSV → DuckDB)
  silver_transform.py  # Silver layer (cleaning, typing, dedup views)
  gold_transform.py    # Gold layer (dims, facts, views)
soda/
  configuration.yml    # DuckDB data source config
  contracts/
    bronze/            # 8 bronze contracts
    silver/            # 4 silver contracts
    gold/              # 13 gold contracts
sql/migrations/
  0001_create_bronze_tables.sql
  0002_create_silver_views.sql
  0003_create_gold_layer.sql
tests/
  conftest.py          # Shared fixtures (in-memory DuckDB)
  test_silver_transform.py
docs/
  conventions.md       # Naming & design conventions (read before adding objects)
  business_requirements.md  # 6 stakeholder requirements
```
