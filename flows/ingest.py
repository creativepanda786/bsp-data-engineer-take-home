"""
PawsFirst Veterinary Warehouse — Bronze Ingestion Flow

Loads CSV seed data into DuckDB bronze tables.

Usage:
    uv run python flows/ingest.py
"""

from __future__ import annotations

import time
from pathlib import Path

from prefect import flow, get_run_logger, task

from flows.common import DATA_DIR, ensure_schemas, get_connection, run_migration

TABLE_MAP: dict[str, str] = {
    "clinics.csv": "bronze.vet_clinic",
    "owners.csv": "bronze.vet_owner",
    "patients.csv": "bronze.vet_patient",
    "providers.csv": "bronze.vet_provider",
    "appointments.csv": "bronze.vet_appointment",
    "referrals.csv": "bronze.vet_referral",
    "invoices.csv": "bronze.vet_invoice",
    "budget_targets.csv": "bronze.vet_budget_target",
}

MIGRATION_PATH = Path(__file__).parent.parent / "sql" / "migrations"


@task(
    name="Run bronze migrations",
    retries=0,
    tags=["bronze", "migration"],
)
def run_bronze_migrations() -> None:
    """Execute bronze DDL migrations."""
    logger = get_run_logger()
    con = get_connection()
    try:
        ensure_schemas(con)
        migration_file = MIGRATION_PATH / "0001_create_bronze_tables.sql"
        run_migration(con, str(migration_file))
        logger.info("Bronze migrations applied successfully")
    finally:
        con.close()


@task(
    name="Load CSV to bronze",
    retries=1,
    retry_delay_seconds=5,
    tags=["bronze", "ingest"],
)
def load_csv_to_bronze(csv_filename: str, table_name: str) -> dict:
    """Load a single CSV file into a bronze table."""
    logger = get_run_logger()
    csv_path = DATA_DIR / csv_filename
    con = get_connection()

    try:
        if not csv_path.exists():
            logger.warning(f"CSV not found: {csv_path}")
            return {"table": table_name, "status": "skipped", "rows": 0}

        con.execute(f"DELETE FROM {table_name}")
        con.execute(
            f"""
            INSERT INTO {table_name}
            SELECT * FROM read_csv('{csv_path}', auto_detect=true)
            """
        )

        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Loaded {row_count:,} rows into {table_name}")
        return {"table": table_name, "status": "success", "rows": row_count}

    except Exception as e:
        logger.error(f"Failed to load {csv_filename} into {table_name}: {e}")
        raise
    finally:
        con.close()


@flow(
    name="pawsfirst-bronze-ingest",
    description="Load all CSV seed data into bronze tables",
    retries=0,
    timeout_seconds=300,
)
def bronze_ingest() -> dict:
    """Full bronze ingestion flow."""
    logger = get_run_logger()
    start_time = time.time()

    logger.info("Starting bronze ingestion")
    run_bronze_migrations()

    results = []
    for csv_file, table in TABLE_MAP.items():
        result = load_csv_to_bronze(csv_file, table)
        results.append(result)

    duration = time.time() - start_time
    total_rows = sum(r["rows"] for r in results)
    failed = [r for r in results if r["status"] == "failed"]

    logger.info(
        f"Bronze ingestion complete: {total_rows:,} total rows "
        f"across {len(results)} tables in {duration:.1f}s"
    )

    if failed:
        logger.warning(f"{len(failed)} tables failed: {[r['table'] for r in failed]}")

    return {
        "status": "success" if not failed else "partial_failure",
        "total_rows": total_rows,
        "tables": results,
        "duration_seconds": round(duration, 1),
    }


if __name__ == "__main__":
    result = bronze_ingest()
    print(f"\nResult: {result['status']}")
    for t in result["tables"]:
        print(f"  {t['table']}: {t['rows']:,} rows ({t['status']})")
