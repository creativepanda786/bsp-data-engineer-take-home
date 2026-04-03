-- Migration 0002: Create silver staging views
-- Cleaned, deduplicated views over bronze tables
-- Bugs fixed:
--   1. stg_vet_clinic: COALESCE(is_current) lacked table alias — DuckDB resolved
--      it to the output alias in the same SELECT clause (BinderException). Fixed
--      by aliasing the FROM clause as `vc` and writing `vc.is_current`.
--   2. stg_vet_patient: INNER JOIN with is_current = TRUE dropped 107 patients -> changed to LEFT JOIN
--   3. stg_vet_appointment: is_completed missing ELSE FALSE -> returned NULL instead of FALSE
--   4. stg_vet_referral: 330 rows had stage_exited_at < stage_entered_at -> nulled out + flagged
-- Phase 3 views added:
--   stg_vet_provider, stg_vet_invoice, stg_vet_owner, stg_vet_budget_target

CREATE SCHEMA IF NOT EXISTS silver;

-- BUG FIX: added table alias `vc` so COALESCE(vc.is_current, TRUE) resolves
-- to the source column, not the SELECT-clause alias being defined.
CREATE OR REPLACE VIEW silver.stg_vet_clinic AS
SELECT
    vc.clinic_id,
    vc.clinic_name,
    vc.city,
    vc.state,
    vc.area,
    CAST(vc.opened_date AS DATE) AS opened_date,
    CAST(vc.closed_date AS DATE) AS closed_date,
    COALESCE(vc.is_current, TRUE) AS is_current,
    vc.renamed_to,
    vc.updated_at
FROM bronze.vet_clinic vc;

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
-- 107 patients registered at clinic 1012 (Riverside, renamed to Riverside East).
-- Changed to LEFT JOIN so all patients are retained regardless of clinic status.
LEFT JOIN silver.stg_vet_clinic c
    ON p.clinic_id = c.clinic_id;

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
WHERE rn = 1;

CREATE OR REPLACE VIEW silver.stg_vet_referral AS
-- BUG FIX: 330 rows had stage_exited_at < stage_entered_at (impossible transitions).
-- Null out the invalid exit timestamp and flag with is_invalid_transition.
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
    CASE
        WHEN stage_exited_at < stage_entered_at THEN TRUE
        ELSE FALSE
    END AS is_invalid_transition,
    created_at,
    updated_at
FROM bronze.vet_referral;

-- ============================================================
-- Phase 3: additional silver views
-- ============================================================

CREATE OR REPLACE VIEW silver.stg_vet_provider AS
SELECT
    p.provider_id,
    p.first_name,
    p.last_name,
    COALESCE(NULLIF(TRIM(p.full_name), ''), p.first_name || ' ' || p.last_name) AS full_name,
    p.credentials,
    p.specialty,
    p.clinic_id,
    c.clinic_name,
    c.area AS clinic_area,
    CAST(p.hire_date AS DATE)        AS hire_date,
    CAST(p.termination_date AS DATE) AS termination_date,
    COALESCE(p.is_active, TRUE)      AS is_active,
    CASE
        WHEN COALESCE(p.is_active, TRUE) = TRUE
         AND p.termination_date IS NULL THEN TRUE
        ELSE FALSE
    END AS is_currently_active,
    p.updated_at
FROM bronze.vet_provider p
LEFT JOIN silver.stg_vet_clinic c
    ON p.clinic_id = c.clinic_id;

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
    CASE WHEN status = 'paid'        THEN TRUE ELSE FALSE END AS is_paid,
    CASE WHEN status = 'outstanding' THEN TRUE ELSE FALSE END AS is_outstanding,
    CASE WHEN status = 'partial'     THEN TRUE ELSE FALSE END AS is_partial,
    CASE WHEN COALESCE(insurance_paid, 0) > 0 THEN TRUE ELSE FALSE END AS has_insurance_payment,
    CASE
        WHEN amount > 0
        THEN ROUND((COALESCE(insurance_paid, 0) + COALESCE(patient_paid, 0)) / amount, 4)
        ELSE NULL
    END AS collection_rate,
    created_at,
    updated_at
FROM bronze.vet_invoice;

CREATE OR REPLACE VIEW silver.stg_vet_owner AS
WITH phone_counts AS (
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
    CASE WHEN COALESCE(pc.phone_owner_count, 1) > 1 THEN TRUE ELSE FALSE END AS has_duplicate_phone,
    COALESCE(pc.phone_owner_count, 1)                                         AS phone_owner_count,
    CASE WHEN COALESCE(ec.email_owner_count, 1) > 1 THEN TRUE ELSE FALSE END AS has_duplicate_email,
    COALESCE(ec.email_owner_count, 1)                                         AS email_owner_count,
    o.created_at,
    o.updated_at
FROM bronze.vet_owner o
LEFT JOIN phone_counts pc ON o.phone = pc.phone
LEFT JOIN email_counts ec ON o.email = ec.email;

CREATE OR REPLACE VIEW silver.stg_vet_budget_target AS
SELECT
    bt.clinic_id,
    c.clinic_name,
    c.area AS clinic_area,
    bt.year,
    bt.month,
    CAST(
        bt.year::VARCHAR || '-' || LPAD(bt.month::VARCHAR, 2, '0') || '-01'
    AS DATE)                               AS budget_month_date,
    CAST(bt.target_revenue      AS DOUBLE)  AS target_revenue,
    CAST(bt.target_appointments AS INTEGER) AS target_appointments,
    CAST(bt.target_new_patients AS INTEGER) AS target_new_patients
FROM bronze.vet_budget_target bt
LEFT JOIN silver.stg_vet_clinic c
    ON bt.clinic_id = c.clinic_id;
