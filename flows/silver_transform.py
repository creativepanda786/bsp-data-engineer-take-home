"""
PawsFirst Veterinary Warehouse — Silver Staging Views

Creates cleaned, deduplicated staging views in the silver schema.

Usage:
    PYTHONPATH=. uv run python flows/silver_transform.py
"""

from __future__ import annotations

import time

from prefect import flow, get_run_logger, task

from flows.common import ensure_schemas, get_connection

STG_VET_CLINIC_SQL = """
CREATE OR REPLACE VIEW silver.stg_vet_clinic AS
SELECT
    clinic_id,
    clinic_name,
    city,
    state,
    area,
    CAST(opened_date AS DATE) AS opened_date,
    CAST(closed_date AS DATE) AS closed_date,
    COALESCE(is_current, TRUE) AS is_current,
    renamed_to,
    updated_at
FROM bronze.vet_clinic
"""

STG_VET_PATIENT_SQL = """
CREATE OR REPLACE VIEW silver.stg_vet_patient AS
SELECT
    p.patient_id,
    p.owner_id,
    p.patient_name,
    p.species,
    p.breed,
    CAST(p.date_of_birth AS DATE) AS date_of_birth,
    p.sex,
    p.weight_lbs,
    p.clinic_id,
    c.clinic_name,
    c.area AS clinic_area,
    p.insurance_provider,
    CAST(p.registration_date AS DATE) AS registration_date,
    COALESCE(p.is_active, TRUE) AS is_active,
    p.created_at,
    p.updated_at
FROM bronze.vet_patient p
INNER JOIN silver.stg_vet_clinic c
    ON p.clinic_id = c.clinic_id
    AND c.is_current = TRUE
"""

STG_VET_APPOINTMENT_SQL = """
CREATE OR REPLACE VIEW silver.stg_vet_appointment AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY appointment_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM bronze.vet_appointment
)
SELECT
    appointment_id,
    patient_id,
    provider_id,
    clinic_id,
    service_code,
    service_name,
    service_fee,
    CAST(appointment_date AS DATE) AS appointment_date,
    scheduled_at,
    status,
    duration_minutes,
    CASE WHEN status = 'completed' THEN TRUE END AS is_completed,
    CASE WHEN status = 'cancelled' THEN TRUE ELSE FALSE END AS is_cancelled,
    CASE WHEN status = 'no_show' THEN TRUE ELSE FALSE END AS is_no_show,
    created_at,
    updated_at
FROM deduped
WHERE rn = 1
"""

STG_VET_REFERRAL_SQL = """
CREATE OR REPLACE VIEW silver.stg_vet_referral AS
SELECT
    referral_id,
    patient_id,
    clinic_id,
    referral_source,
    stage,
    stage_entered_at,
    stage_exited_at,
    CASE
        WHEN stage_exited_at IS NOT NULL
        THEN DATEDIFF('day', stage_entered_at, stage_exited_at)
        ELSE NULL
    END AS days_in_stage,
    created_at,
    updated_at
FROM bronze.vet_referral
"""


@task(
    name="Create silver views",
    retries=1,
    retry_delay_seconds=5,
    tags=["silver", "transform"],
)
def create_silver_views() -> dict:
    """Create all silver staging views."""
    logger = get_run_logger()
    con = get_connection()

    views = {
        "stg_vet_clinic": STG_VET_CLINIC_SQL,
        "stg_vet_patient": STG_VET_PATIENT_SQL,
        "stg_vet_appointment": STG_VET_APPOINTMENT_SQL,
        "stg_vet_referral": STG_VET_REFERRAL_SQL,
    }

    results = {}

    try:
        ensure_schemas(con)

        for view_name, sql in views.items():
            try:
                con.execute(sql)
                row_count = con.execute(
                    f"SELECT COUNT(*) FROM silver.{view_name}"
                ).fetchone()[0]
                logger.info(f"Created silver.{view_name} ({row_count:,} rows)")
                results[view_name] = {"status": "success", "rows": row_count}
            except Exception as e:
                logger.error(f"Failed to create silver.{view_name}: {e}")
                results[view_name] = {"status": "failed", "error": str(e)}
    finally:
        con.close()

    return results


@flow(
    name="pawsfirst-silver-transform",
    description="Create silver staging views from bronze data",
    retries=0,
    timeout_seconds=120,
)
def silver_transform() -> dict:
    """Create all silver staging views."""
    logger = get_run_logger()
    start_time = time.time()

    logger.info("Starting silver transform")
    results = create_silver_views()

    duration = time.time() - start_time
    failed = [k for k, v in results.items() if v.get("status") == "failed"]

    logger.info(f"Silver transform complete in {duration:.1f}s")
    if failed:
        logger.warning(f"Failed views: {failed}")

    return {
        "status": "success" if not failed else "partial_failure",
        "views": results,
        "duration_seconds": round(duration, 1),
    }


if __name__ == "__main__":
    result = silver_transform()
    print(f"\nResult: {result['status']}")
    for name, info in result["views"].items():
        status = info["status"]
        rows = info.get("rows", "N/A")
        print(f"  silver.{name}: {rows} rows ({status})")
