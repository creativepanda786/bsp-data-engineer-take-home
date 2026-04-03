-- Migration 0002: Create silver staging views
-- Cleaned, deduplicated views over bronze tables
-- Bugs fixed:
--   1. stg_vet_patient: INNER JOIN with is_current = TRUE dropped 107 patients -> changed to LEFT JOIN
--   2. stg_vet_appointment: is_completed missing ELSE FALSE -> returned NULL instead of FALSE
--   3. stg_vet_referral: 330 rows had stage_exited_at < stage_entered_at -> nulled out + flagged

CREATE SCHEMA IF NOT EXISTS silver;

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
FROM bronze.vet_clinic;

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
    -- Quality flag: TRUE means this row had an impossible transition in the source
    CASE
        WHEN stage_exited_at < stage_entered_at THEN TRUE
        ELSE FALSE
    END AS is_invalid_transition,
    created_at,
    updated_at
FROM bronze.vet_referral;
