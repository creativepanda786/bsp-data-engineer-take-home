-- Migration 0003: Create gold Kimball star-schema layer
-- Dimensions (SCD2): dim_core_clinic, dim_clin_patient, dim_clin_provider
-- Reference:         ref_core_budget_target
-- Facts:             fact_clin_appointment, fact_fin_invoice, fact_pipe_referral_stage
-- Analytical views:  all 6 business requirements (Reqs 1-6)

CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- DIMENSIONS (SCD2)
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_core_clinic (
    clinic_sk       VARCHAR PRIMARY KEY,
    clinic_id       INTEGER NOT NULL,
    clinic_name     VARCHAR,
    city            VARCHAR,
    state           VARCHAR,
    area            VARCHAR,
    opened_date     DATE,
    closed_date     DATE,
    renamed_to      VARCHAR,
    effective_start TIMESTAMP NOT NULL,
    effective_end   TIMESTAMP,
    is_current      BOOLEAN   NOT NULL,
    row_hash        VARCHAR   NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_clin_patient (
    patient_sk                   VARCHAR PRIMARY KEY,
    patient_id                   INTEGER NOT NULL,
    owner_id                     INTEGER,
    patient_name                 VARCHAR,
    species                      VARCHAR,
    breed                        VARCHAR,
    date_of_birth                DATE,
    sex                          VARCHAR,
    weight_lbs                   DOUBLE,
    clinic_id                    INTEGER,
    insurance_provider           VARCHAR,
    has_insurance                BOOLEAN,
    registration_documented_date DATE,
    is_active                    BOOLEAN,
    effective_start              TIMESTAMP NOT NULL,
    effective_end                TIMESTAMP,
    is_current                   BOOLEAN   NOT NULL,
    row_hash                     VARCHAR   NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_clin_provider (
    provider_sk         VARCHAR PRIMARY KEY,
    provider_id         INTEGER NOT NULL,
    full_name           VARCHAR,
    first_name          VARCHAR,
    last_name           VARCHAR,
    credentials         VARCHAR,
    specialty           VARCHAR,
    clinic_id           INTEGER,
    hire_date           DATE,
    termination_date    DATE,
    is_active           BOOLEAN,
    is_currently_active BOOLEAN,
    effective_start     TIMESTAMP NOT NULL,
    effective_end       TIMESTAMP,
    is_current          BOOLEAN   NOT NULL,
    row_hash            VARCHAR   NOT NULL
);

-- ============================================================
-- REFERENCE TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.ref_core_budget_target (
    clinic_id               INTEGER NOT NULL,
    clinic_name             VARCHAR,
    clinic_area             VARCHAR,
    year                    INTEGER NOT NULL,
    month                   INTEGER NOT NULL,
    budget_month_date       DATE    NOT NULL,
    target_revenue          DOUBLE,
    target_appointments     INTEGER,
    target_new_patients     INTEGER,
    PRIMARY KEY (clinic_id, year, month)
);

-- ============================================================
-- FACTS
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fact_clin_appointment (
    appointment_sk              VARCHAR PRIMARY KEY,
    appointment_id              INTEGER NOT NULL,
    patient_sk                  VARCHAR,
    patient_id                  INTEGER,
    provider_sk                 VARCHAR,
    provider_id                 INTEGER,
    clinic_sk                   VARCHAR,
    clinic_id                   INTEGER,
    service_code                VARCHAR,
    service_name                VARCHAR,
    service_fee                 DOUBLE,
    appointment_scheduled_at    TIMESTAMP,
    appointment_documented_date DATE,
    status                      VARCHAR,
    duration_minutes            INTEGER,
    is_completed                BOOLEAN,
    is_cancelled                BOOLEAN,
    is_no_show                  BOOLEAN,
    created_at                  TIMESTAMP,
    updated_at                  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.fact_fin_invoice (
    invoice_sk              VARCHAR PRIMARY KEY,
    invoice_id              INTEGER NOT NULL,
    appointment_id          INTEGER,
    patient_sk              VARCHAR,
    patient_id              INTEGER,
    clinic_sk               VARCHAR,
    clinic_id               INTEGER,
    service_code            VARCHAR,
    amount                  DOUBLE,
    insurance_paid          DOUBLE,
    patient_paid            DOUBLE,
    payment_type            VARCHAR,
    invoice_documented_date DATE,
    invoice_paid_date       DATE,
    status                  VARCHAR,
    is_paid                 BOOLEAN,
    is_outstanding          BOOLEAN,
    is_partial              BOOLEAN,
    has_insurance_payment   BOOLEAN,
    collection_rate         DOUBLE,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.fact_pipe_referral_stage (
    referral_stage_sk     VARCHAR PRIMARY KEY,
    referral_id           INTEGER NOT NULL,
    patient_sk            VARCHAR,
    patient_id            INTEGER,
    clinic_sk             VARCHAR,
    clinic_id             INTEGER,
    referral_source       VARCHAR,
    stage                 VARCHAR,
    stage_sort_order      INTEGER,
    stage_entered_at      TIMESTAMP,
    stage_exited_at       TIMESTAMP,
    days_in_stage         INTEGER,
    is_invalid_transition BOOLEAN,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP
);


-- ============================================================
-- LOAD: DIMENSIONS (SCD2 initial load)
-- ============================================================

INSERT INTO gold.dim_core_clinic
SELECT
    gen_random_uuid() AS clinic_sk,
    clinic_id, clinic_name, city, state, area, opened_date, closed_date, renamed_to,
    CURRENT_TIMESTAMP AS effective_start,
    NULL              AS effective_end,
    TRUE              AS is_current,
    md5(
        COALESCE(clinic_name, '')  || COALESCE(city, '') || COALESCE(state, '') ||
        COALESCE(area, '')         || COALESCE(renamed_to, '') ||
        COALESCE(CAST(opened_date AS VARCHAR), '') ||
        COALESCE(CAST(closed_date AS VARCHAR), '')
    ) AS row_hash
FROM silver.stg_vet_clinic
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_core_clinic);

INSERT INTO gold.dim_clin_patient
SELECT
    gen_random_uuid() AS patient_sk,
    patient_id, owner_id, patient_name, species, breed, date_of_birth, sex,
    weight_lbs, clinic_id, insurance_provider,
    CASE WHEN insurance_provider IS NOT NULL THEN TRUE ELSE FALSE END AS has_insurance,
    registration_date AS registration_documented_date,
    is_active,
    CURRENT_TIMESTAMP AS effective_start,
    NULL              AS effective_end,
    TRUE              AS is_current,
    md5(
        COALESCE(patient_name, '') || COALESCE(species, '') || COALESCE(breed, '') ||
        COALESCE(CAST(date_of_birth AS VARCHAR), '') || COALESCE(sex, '') ||
        COALESCE(CAST(weight_lbs AS VARCHAR), '') || COALESCE(insurance_provider, '') ||
        COALESCE(CAST(is_active AS VARCHAR), '')
    ) AS row_hash
FROM silver.stg_vet_patient
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_clin_patient);

INSERT INTO gold.dim_clin_provider
SELECT
    gen_random_uuid() AS provider_sk,
    provider_id, full_name, first_name, last_name, credentials, specialty, clinic_id,
    hire_date, termination_date, is_active, is_currently_active,
    CURRENT_TIMESTAMP AS effective_start,
    NULL              AS effective_end,
    TRUE              AS is_current,
    md5(
        COALESCE(full_name, '') || COALESCE(credentials, '') || COALESCE(specialty, '') ||
        COALESCE(CAST(clinic_id AS VARCHAR), '') || COALESCE(CAST(is_active AS VARCHAR), '')
    ) AS row_hash
FROM silver.stg_vet_provider
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_clin_provider);

-- ============================================================
-- LOAD: REFERENCE TABLES
-- ============================================================

DELETE FROM gold.ref_core_budget_target;

INSERT INTO gold.ref_core_budget_target
SELECT clinic_id, clinic_name, clinic_area, year, month, budget_month_date,
       target_revenue, target_appointments, target_new_patients
FROM silver.stg_vet_budget_target;

-- ============================================================
-- LOAD: FACTS
-- ============================================================

DELETE FROM gold.fact_clin_appointment;

INSERT INTO gold.fact_clin_appointment
SELECT
    gen_random_uuid()  AS appointment_sk,
    a.appointment_id,
    dp.patient_sk,  a.patient_id,
    dpr.provider_sk, a.provider_id,
    dc.clinic_sk,   a.clinic_id,
    a.service_code, a.service_name, a.service_fee,
    a.scheduled_at   AS appointment_scheduled_at,
    a.appointment_date AS appointment_documented_date,
    a.status, a.duration_minutes, a.is_completed, a.is_cancelled, a.is_no_show,
    a.created_at, a.updated_at
FROM silver.stg_vet_appointment a
LEFT JOIN gold.dim_clin_patient  dp  ON a.patient_id  = dp.patient_id  AND dp.is_current  = TRUE
LEFT JOIN gold.dim_clin_provider dpr ON a.provider_id = dpr.provider_id AND dpr.is_current = TRUE
LEFT JOIN gold.dim_core_clinic   dc  ON a.clinic_id   = dc.clinic_id   AND dc.is_current  = TRUE;

DELETE FROM gold.fact_fin_invoice;

INSERT INTO gold.fact_fin_invoice
SELECT
    gen_random_uuid() AS invoice_sk,
    i.invoice_id, i.appointment_id,
    dp.patient_sk, i.patient_id,
    dc.clinic_sk,  i.clinic_id,
    i.service_code, i.amount, i.insurance_paid, i.patient_paid, i.payment_type,
    i.invoice_documented_date, i.invoice_paid_date, i.status,
    i.is_paid, i.is_outstanding, i.is_partial, i.has_insurance_payment,
    i.collection_rate, i.created_at, i.updated_at
FROM silver.stg_vet_invoice i
LEFT JOIN gold.dim_clin_patient dp ON i.patient_id = dp.patient_id AND dp.is_current = TRUE
LEFT JOIN gold.dim_core_clinic  dc ON i.clinic_id  = dc.clinic_id  AND dc.is_current = TRUE;

DELETE FROM gold.fact_pipe_referral_stage;

INSERT INTO gold.fact_pipe_referral_stage
SELECT
    gen_random_uuid() AS referral_stage_sk,
    r.referral_id,
    dp.patient_sk, r.patient_id,
    dc.clinic_sk,  r.clinic_id,
    r.referral_source, r.stage,
    CASE r.stage
        WHEN 'inquiry'      THEN 1
        WHEN 'consultation' THEN 2
        WHEN 'registered'   THEN 3
        WHEN 'active'       THEN 4
        WHEN 'churned'      THEN 5
        ELSE 99
    END AS stage_sort_order,
    r.stage_entered_at, r.stage_exited_at, r.days_in_stage, r.is_invalid_transition,
    r.created_at, r.updated_at
FROM silver.stg_vet_referral r
LEFT JOIN gold.dim_clin_patient dp ON r.patient_id = dp.patient_id AND dp.is_current = TRUE
LEFT JOIN gold.dim_core_clinic  dc ON r.clinic_id  = dc.clinic_id  AND dc.is_current = TRUE;

-- ============================================================
-- ANALYTICAL VIEWS
-- ============================================================

-- Req 1: Weekly appointment volume by clinic
CREATE OR REPLACE VIEW gold.agg_clin_clinic_weekly AS
SELECT
    dc.clinic_id,
    dc.clinic_name,
    dc.area                                             AS clinic_area,
    dc.state,
    DATE_TRUNC('week', f.appointment_documented_date)   AS week_start_date,
    COUNT(*)                                            AS total_appointments,
    SUM(CASE WHEN f.is_completed THEN 1 ELSE 0 END)    AS completed_appointments,
    SUM(CASE WHEN f.is_cancelled THEN 1 ELSE 0 END)    AS cancelled_appointments,
    SUM(CASE WHEN f.is_no_show  THEN 1 ELSE 0 END)     AS no_show_appointments,
    ROUND(SUM(CASE WHEN f.is_completed THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0), 4) AS completion_rate,
    ROUND(SUM(CASE WHEN f.is_cancelled THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0), 4) AS cancellation_rate,
    ROUND(SUM(CASE WHEN f.is_no_show  THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0), 4)  AS no_show_rate
FROM gold.fact_clin_appointment f
JOIN gold.dim_core_clinic dc ON f.clinic_id = dc.clinic_id AND dc.is_current = TRUE
WHERE f.appointment_documented_date IS NOT NULL
GROUP BY dc.clinic_id, dc.clinic_name, dc.area, dc.state,
         DATE_TRUNC('week', f.appointment_documented_date)
ORDER BY week_start_date, dc.clinic_id;

-- Req 2: Referral funnel by month and clinic
CREATE OR REPLACE VIEW gold.v_pipe_funnel_summary AS
WITH base AS (
    SELECT
        f.clinic_id,
        DATE_TRUNC('month', f.stage_entered_at)     AS funnel_month_date,
        f.referral_source,
        COUNT(DISTINCT CASE WHEN f.stage = 'inquiry'      THEN f.referral_id END) AS inquiry_count,
        COUNT(DISTINCT CASE WHEN f.stage = 'consultation' THEN f.referral_id END) AS consultation_count,
        COUNT(DISTINCT CASE WHEN f.stage = 'registered'   THEN f.referral_id END) AS registered_count,
        COUNT(DISTINCT CASE WHEN f.stage = 'active'       THEN f.referral_id END) AS active_count,
        MEDIAN(CASE WHEN f.stage = 'inquiry'      AND NOT f.is_invalid_transition THEN f.days_in_stage END) AS median_days_inquiry,
        MEDIAN(CASE WHEN f.stage = 'consultation' AND NOT f.is_invalid_transition THEN f.days_in_stage END) AS median_days_consultation,
        MEDIAN(CASE WHEN f.stage = 'registered'   AND NOT f.is_invalid_transition THEN f.days_in_stage END) AS median_days_registered
    FROM gold.fact_pipe_referral_stage f
    WHERE f.stage_entered_at IS NOT NULL
    GROUP BY f.clinic_id, DATE_TRUNC('month', f.stage_entered_at), f.referral_source
)
SELECT
    dc.clinic_id, dc.clinic_name, dc.area AS clinic_area,
    b.funnel_month_date,
    EXTRACT(YEAR  FROM b.funnel_month_date) AS year,
    EXTRACT(MONTH FROM b.funnel_month_date) AS month,
    b.referral_source,
    b.inquiry_count, b.consultation_count, b.registered_count, b.active_count,
    ROUND(b.consultation_count / NULLIF(b.inquiry_count,      0.0), 4) AS inquiry_to_consultation_rate,
    ROUND(b.registered_count   / NULLIF(b.consultation_count, 0.0), 4) AS consultation_to_registered_rate,
    ROUND(b.active_count       / NULLIF(b.registered_count,   0.0), 4) AS registered_to_active_rate,
    ROUND(b.active_count       / NULLIF(b.inquiry_count,      0.0), 4) AS overall_conversion_rate,
    b.median_days_inquiry, b.median_days_consultation, b.median_days_registered
FROM base b
JOIN gold.dim_core_clinic dc ON b.clinic_id = dc.clinic_id AND dc.is_current = TRUE
ORDER BY b.funnel_month_date, dc.clinic_id, b.referral_source;

-- Req 3: Revenue vs budget by clinic by month
CREATE OR REPLACE VIEW gold.v_fin_revenue_by_clinic_monthly AS
WITH monthly_actuals AS (
    SELECT
        f.clinic_id,
        DATE_TRUNC('month', f.invoice_documented_date) AS revenue_month_date,
        SUM(f.amount)                                  AS total_invoiced,
        SUM(f.insurance_paid)                          AS total_insurance_paid,
        SUM(f.patient_paid)                            AS total_patient_paid,
        SUM(f.insurance_paid + f.patient_paid)         AS total_collected,
        ROUND(SUM(f.insurance_paid + f.patient_paid) / NULLIF(SUM(f.amount), 0), 4) AS collection_rate,
        COUNT(DISTINCT CASE WHEN f.has_insurance_payment     THEN f.invoice_id END) AS insurance_invoice_count,
        COUNT(DISTINCT CASE WHEN NOT f.has_insurance_payment THEN f.invoice_id END) AS self_pay_invoice_count
    FROM gold.fact_fin_invoice f
    WHERE f.invoice_documented_date IS NOT NULL
    GROUP BY f.clinic_id, DATE_TRUNC('month', f.invoice_documented_date)
)
SELECT
    dc.clinic_id, dc.clinic_name, dc.area AS clinic_area,
    a.revenue_month_date,
    EXTRACT(YEAR  FROM a.revenue_month_date) AS year,
    EXTRACT(MONTH FROM a.revenue_month_date) AS month,
    a.total_invoiced, a.total_insurance_paid, a.total_patient_paid,
    a.total_collected, a.collection_rate,
    a.insurance_invoice_count, a.self_pay_invoice_count,
    b.target_revenue,
    ROUND(a.total_collected - COALESCE(b.target_revenue, 0), 2)                                  AS revenue_vs_budget,
    ROUND((a.total_collected - COALESCE(b.target_revenue, 0)) / NULLIF(b.target_revenue, 0), 4) AS revenue_vs_budget_pct
FROM monthly_actuals a
JOIN gold.dim_core_clinic dc ON a.clinic_id = dc.clinic_id AND dc.is_current = TRUE
LEFT JOIN gold.ref_core_budget_target b
    ON  a.clinic_id = b.clinic_id
    AND EXTRACT(YEAR  FROM a.revenue_month_date) = b.year
    AND EXTRACT(MONTH FROM a.revenue_month_date) = b.month
ORDER BY a.revenue_month_date, dc.clinic_id;

-- Req 4: Provider utilization weekly
CREATE OR REPLACE VIEW gold.v_ops_provider_utilization_weekly AS
SELECT
    dp.provider_id, dp.full_name AS provider_name, dp.credentials, dp.specialty,
    dc.clinic_id, dc.clinic_name, dc.area AS clinic_area,
    DATE_TRUNC('week', f.appointment_documented_date) AS week_start_date,
    f.service_code, f.service_name,
    COUNT(*)                                           AS total_appointments,
    SUM(CASE WHEN f.is_completed THEN 1 ELSE 0 END)   AS completed_appointments,
    SUM(COALESCE(f.duration_minutes, 0))               AS total_minutes,
    COUNT(*) < 15                                      AS is_below_threshold
FROM gold.fact_clin_appointment f
JOIN gold.dim_clin_provider dp ON f.provider_id = dp.provider_id AND dp.is_current = TRUE
JOIN gold.dim_core_clinic   dc ON f.clinic_id   = dc.clinic_id   AND dc.is_current = TRUE
WHERE f.appointment_documented_date IS NOT NULL
GROUP BY dp.provider_id, dp.full_name, dp.credentials, dp.specialty,
         dc.clinic_id, dc.clinic_name, dc.area,
         DATE_TRUNC('week', f.appointment_documented_date),
         f.service_code, f.service_name
ORDER BY week_start_date, dp.provider_id;

-- Req 5: Duplicate patient detection
CREATE OR REPLACE VIEW gold.v_clin_duplicate_patients_exceptions AS
WITH name_dob_matches AS (
    SELECT
        a.patient_id AS patient_id_a, b.patient_id AS patient_id_b,
        a.patient_name, a.species, a.date_of_birth,
        a.clinic_id AS clinic_id_a, b.clinic_id AS clinic_id_b,
        'same_name_species_dob' AS match_reason
    FROM gold.dim_clin_patient a
    JOIN gold.dim_clin_patient b
        ON  a.patient_id    < b.patient_id
        AND a.patient_name  = b.patient_name
        AND a.species       = b.species
        AND a.date_of_birth = b.date_of_birth
        AND a.is_current = TRUE AND b.is_current = TRUE
),
phone_matches AS (
    SELECT
        pa.patient_id AS patient_id_a, pb.patient_id AS patient_id_b,
        pa.patient_name, pa.species, pa.date_of_birth,
        pa.clinic_id AS clinic_id_a, pb.clinic_id AS clinic_id_b,
        'same_owner_phone' AS match_reason
    FROM silver.stg_vet_owner oa
    JOIN silver.stg_vet_owner ob
        ON  oa.owner_id < ob.owner_id
        AND oa.phone = ob.phone AND oa.phone IS NOT NULL
    JOIN gold.dim_clin_patient pa ON pa.owner_id = oa.owner_id AND pa.is_current = TRUE
    JOIN gold.dim_clin_patient pb ON pb.owner_id = ob.owner_id AND pb.is_current = TRUE
    WHERE pa.patient_id < pb.patient_id
)
SELECT * FROM name_dob_matches
UNION ALL
SELECT * FROM phone_matches
ORDER BY match_reason, patient_id_a;

-- Req 6: Patient retention cohorts
CREATE OR REPLACE VIEW gold.v_clin_patient_retention_cohort AS
WITH first_appointments AS (
    SELECT f.patient_id, f.clinic_id,
           MIN(f.appointment_documented_date) AS first_appointment_date
    FROM gold.fact_clin_appointment f
    WHERE f.is_completed = TRUE
    GROUP BY f.patient_id, f.clinic_id
),
cohorts AS (
    SELECT fa.patient_id, fa.clinic_id, fa.first_appointment_date,
           DATE_TRUNC('month', fa.first_appointment_date) AS cohort_month
    FROM first_appointments fa
),
subsequent AS (
    SELECT c.patient_id, c.clinic_id, c.cohort_month, c.first_appointment_date,
           MIN(f.appointment_documented_date) AS next_appointment_date
    FROM cohorts c
    JOIN gold.fact_clin_appointment f
        ON  f.patient_id   = c.patient_id
        AND f.is_completed = TRUE
        AND f.appointment_documented_date > c.first_appointment_date
    GROUP BY c.patient_id, c.clinic_id, c.cohort_month, c.first_appointment_date
),
referral_source AS (
    SELECT DISTINCT ON (patient_id) patient_id, referral_source
    FROM gold.fact_pipe_referral_stage
    ORDER BY patient_id, stage_entered_at
)
SELECT
    dc.clinic_id, dc.clinic_name, dc.area AS clinic_area,
    c.cohort_month,
    EXTRACT(YEAR  FROM c.cohort_month) AS cohort_year,
    EXTRACT(MONTH FROM c.cohort_month) AS cohort_month_num,
    COALESCE(rs.referral_source, 'unknown') AS referral_source,
    COUNT(DISTINCT c.patient_id) AS cohort_size,
    COUNT(DISTINCT CASE WHEN s.next_appointment_date - c.first_appointment_date <= 30 THEN c.patient_id END) AS retained_30d,
    COUNT(DISTINCT CASE WHEN s.next_appointment_date - c.first_appointment_date <= 60 THEN c.patient_id END) AS retained_60d,
    COUNT(DISTINCT CASE WHEN s.next_appointment_date - c.first_appointment_date <= 90 THEN c.patient_id END) AS retained_90d,
    ROUND(COUNT(DISTINCT CASE WHEN s.next_appointment_date - c.first_appointment_date <= 30 THEN c.patient_id END) / NULLIF(COUNT(DISTINCT c.patient_id), 0.0), 4) AS retention_rate_30d,
    ROUND(COUNT(DISTINCT CASE WHEN s.next_appointment_date - c.first_appointment_date <= 60 THEN c.patient_id END) / NULLIF(COUNT(DISTINCT c.patient_id), 0.0), 4) AS retention_rate_60d,
    ROUND(COUNT(DISTINCT CASE WHEN s.next_appointment_date - c.first_appointment_date <= 90 THEN c.patient_id END) / NULLIF(COUNT(DISTINCT c.patient_id), 0.0), 4) AS retention_rate_90d
FROM cohorts c
LEFT JOIN subsequent      s  ON c.patient_id = s.patient_id AND c.clinic_id = s.clinic_id AND c.cohort_month = s.cohort_month
LEFT JOIN referral_source rs ON c.patient_id = rs.patient_id
JOIN gold.dim_core_clinic dc  ON c.clinic_id  = dc.clinic_id AND dc.is_current = TRUE
GROUP BY dc.clinic_id, dc.clinic_name, dc.area, c.cohort_month,
         COALESCE(rs.referral_source, 'unknown')
ORDER BY c.cohort_month, dc.clinic_id;
