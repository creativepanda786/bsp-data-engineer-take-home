"""
PawsFirst Veterinary Warehouse — Silver Staging Views

Creates cleaned, deduplicated staging views in the silver schema.

Bugs fixed:
  - stg_vet_patient: INNER JOIN with is_current = TRUE silently dropped 107 patients
    registered at clinic 1012 (Riverside, renamed to Riverside East). Fixed to LEFT JOIN.
  - stg_vet_appointment: is_completed was missing ELSE FALSE, returning NULL instead of
    FALSE for cancelled, no_show, and scheduled rows.
  - stg_vet_referral: 330 rows had stage_exited_at < stage_entered_at (impossible
    transitions — data entry error in source system). Fixed by nulling out the bad exit
    timestamp and adding an is_invalid_transition flag for monitoring.
New views added (Phase 3 — Complete Silver Layer):
  - stg_vet_provider:      provider roster with cast types and is_currently_active flag
  - stg_vet_invoice:       cleaned invoice/payment data with is_paid / has_insurance flags
  - stg_vet_owner:         owner contact data with duplicate phone/email flags (Req 5)
  - stg_vet_budget_target: monthly budget targets joined with clinic dimension
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
-- BUG FIX: was INNER JOIN with AND c.is_current = TRUE which silently dropped
-- 107 patients registered at clinic 1012 (Riverside, renamed to Riverside East /
-- clinic 1013). Changed to LEFT JOIN so all patients are retained regardless of
-- clinic status.
LEFT JOIN silver.stg_vet_clinic c
    ON p.clinic_id = c.clinic_id
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
    -- BUG FIX: was missing ELSE FALSE, returning NULL instead of FALSE
    -- for cancelled, no_show, and scheduled rows.
    CASE WHEN status = 'completed' THEN TRUE ELSE FALSE END AS is_completed,
    CASE WHEN status = 'cancelled' THEN TRUE ELSE FALSE END AS is_cancelled,
    CASE WHEN status = 'no_show'   THEN TRUE ELSE FALSE END AS is_no_show,
    created_at,
    updated_at
FROM deduped
WHERE rn = 1
"""

STG_VET_REFERRAL_SQL = """
CREATE OR REPLACE VIEW silver.stg_vet_referral AS
-- BUG FIX: 330 rows had stage_exited_at < stage_entered_at (impossible transitions).
-- Root cause: data entry error in the source system — exit timestamps were recorded
-- before entry timestamps. These produced negative days_in_stage values which
-- corrupt funnel analysis. Fix: null out the invalid exit timestamp and flag the
-- row with is_invalid_transition for downstream monitoring.
SELECT
    referral_id,
    patient_id,
    clinic_id,
    referral_source,
    stage,
    stage_entered_at,
    CASE
        WHEN stage_exited_at < stage_entered_at THEN NULL
        ELSE stage_exited_at
    END AS stage_exited_at,
    CASE
        WHEN stage_exited_at IS NOT NULL
         AND stage_exited_at >= stage_entered_at
        THEN DATEDIFF('day', stage_entered_at, stage_exited_at)
        ELSE NULL
    END AS days_in_stage,
    -- Quality flag: TRUE means this row had an impossible transition in the source
    CASE
        WHEN stage_exited_at < stage_entered_at THEN TRUE
        ELSE FALSE
    END AS is_invalid_transition,
    created_at,
    updated_at
FROM bronze.vet_referral
"""
STG_VET_PROVIDER_SQL = """
CREATE OR REPLACE VIEW silver.stg_vet_provider AS
SELECT
    p.provider_id,
    p.first_name,
    p.last_name,
    -- Normalise full_name: fall back to constructed name if source value is blank
    COALESCE(NULLIF(TRIM(p.full_name), ''), p.first_name || ' ' || p.last_name) AS full_name,
    p.credentials,
    p.specialty,
    p.clinic_id,
    c.clinic_name,
    c.area AS clinic_area,
    CAST(p.hire_date AS DATE)        AS hire_date,
    CAST(p.termination_date AS DATE) AS termination_date,
    COALESCE(p.is_active, TRUE)      AS is_active,
    -- Derived flag: provider is currently active (no termination date on record)
    CASE
        WHEN COALESCE(p.is_active, TRUE) = TRUE
         AND p.termination_date IS NULL THEN TRUE
        ELSE FALSE
    END AS is_currently_active,
    p.updated_at
FROM bronze.vet_provider p
LEFT JOIN silver.stg_vet_clinic c
    ON p.clinic_id = c.clinic_id
"""
STG_VET_INVOICE_SQL = """
CREATE OR REPLACE VIEW silver.stg_vet_invoice AS
SELECT
    invoice_id,
    appointment_id,
    patient_id,
    clinic_id,
    service_code,
    CAST(amount         AS DOUBLE) AS amount,
    payment_type,
    CAST(insurance_paid AS DOUBLE) AS insurance_paid,
    CAST(patient_paid   AS DOUBLE) AS patient_paid,
    CAST(invoice_date   AS DATE)   AS invoice_documented_date,
    CAST(paid_date      AS DATE)   AS invoice_paid_date,
    status,
    -- Convenience status flags
    CASE WHEN status = 'paid'        THEN TRUE ELSE FALSE END AS is_paid,
    CASE WHEN status = 'outstanding' THEN TRUE ELSE FALSE END AS is_outstanding,
    CASE WHEN status = 'partial'     THEN TRUE ELSE FALSE END AS is_partial,
    -- TRUE if any portion was covered by insurance
    CASE WHEN COALESCE(insurance_paid, 0) > 0 THEN TRUE ELSE FALSE END AS has_insurance_payment,
    -- Proportion of invoiced amount collected so far (insurance + patient)
    CASE
        WHEN amount > 0
        THEN ROUND((COALESCE(insurance_paid, 0) + COALESCE(patient_paid, 0)) / amount, 4)
        ELSE NULL
    END AS collection_rate,
    created_at,
    updated_at
FROM bronze.vet_invoice
"""
STG_VET_OWNER_SQL = """
CREATE OR REPLACE VIEW silver.stg_vet_owner AS
WITH phone_counts AS (
    -- Pre-compute how many owners share the same phone number.
    -- 94 duplicate phones found in source data — used by Req 5 duplicate detection.
    SELECT phone, COUNT(*) AS phone_owner_count
    FROM bronze.vet_owner
    WHERE phone IS NOT NULL
    GROUP BY phone
),
email_counts AS (
    SELECT email, COUNT(*) AS email_owner_count
    FROM bronze.vet_owner
    WHERE email IS NOT NULL
    GROUP BY email
)
SELECT
    o.owner_id,
    o.first_name,
    o.last_name,
    o.first_name || ' ' || o.last_name AS full_name,
    o.email,
    o.phone,
    o.city,
    o.state,
    -- Duplicate-signal flags for downstream dedup / data quality views
    CASE WHEN COALESCE(pc.phone_owner_count, 1) > 1 THEN TRUE ELSE FALSE END AS has_duplicate_phone,
    COALESCE(pc.phone_owner_count, 1)                                         AS phone_owner_count,
    CASE WHEN COALESCE(ec.email_owner_count, 1) > 1 THEN TRUE ELSE FALSE END AS has_duplicate_email,
    COALESCE(ec.email_owner_count, 1)                                         AS email_owner_count,
    o.created_at,
    o.updated_at
FROM bronze.vet_owner o
LEFT JOIN phone_counts pc ON o.phone = pc.phone
LEFT JOIN email_counts ec ON o.email = ec.email
"""
STG_VET_BUDGET_TARGET_SQL = """
CREATE OR REPLACE VIEW silver.stg_vet_budget_target AS
SELECT
    bt.clinic_id,
    c.clinic_name,
    c.area AS clinic_area,
    bt.year,
    bt.month,
    -- Construct a proper DATE for easy time-series joins (first day of the month)
    CAST(
        bt.year::VARCHAR || '-' || LPAD(bt.month::VARCHAR, 2, '0') || '-01'
    AS DATE)                              AS budget_month_date,
    CAST(bt.target_revenue      AS DOUBLE)  AS target_revenue,
    CAST(bt.target_appointments AS INTEGER) AS target_appointments,
    CAST(bt.target_new_patients AS INTEGER) AS target_new_patients
FROM bronze.vet_budget_target bt
LEFT JOIN silver.stg_vet_clinic c
    ON bt.clinic_id = c.clinic_id
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
        "stg_vet_provider":      STG_VET_PROVIDER_SQL,
        "stg_vet_invoice":       STG_VET_INVOICE_SQL,
        "stg_vet_owner":         STG_VET_OWNER_SQL,
        "stg_vet_budget_target": STG_VET_BUDGET_TARGET_SQL,
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
