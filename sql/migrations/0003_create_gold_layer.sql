-- Migration 0003: Create gold layer (Kimball star schema)
-- Dimensions (SCD2), facts, reference tables, and analytical views
-- Covers all 6 business requirements

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
    is_current      BOOLEAN NOT NULL,
    row_hash        VARCHAR NOT NULL
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
    is_current                   BOOLEAN NOT NULL,
    row_hash                     VARCHAR NOT NULL
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
    is_current          BOOLEAN NOT NULL,
    row_hash            VARCHAR NOT NULL
);

-- ============================================================
-- REFERENCE TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.ref_core_budget_target (
    clinic_id           INTEGER NOT NULL,
    clinic_name         VARCHAR,
    clinic_area         VARCHAR,
    year                INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    budget_month_date   DATE    NOT NULL,
    target_revenue      DOUBLE,
    target_appointments INTEGER,
    target_new_patients INTEGER,
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
