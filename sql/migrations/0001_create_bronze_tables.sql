-- Migration 0001: Create bronze layer tables
-- Raw ingestion tables preserving source fidelity

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.vet_clinic (
    clinic_id       INTEGER,
    clinic_name     VARCHAR,
    city            VARCHAR,
    state           VARCHAR,
    area            VARCHAR,
    opened_date     DATE,
    closed_date     DATE,
    is_current      BOOLEAN,
    renamed_to      VARCHAR,
    updated_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.vet_owner (
    owner_id        INTEGER,
    first_name      VARCHAR,
    last_name       VARCHAR,
    email           VARCHAR,
    phone           VARCHAR,
    city            VARCHAR,
    state           VARCHAR,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.vet_patient (
    patient_id          INTEGER,
    owner_id            INTEGER,
    patient_name        VARCHAR,
    species             VARCHAR,
    breed               VARCHAR,
    date_of_birth       DATE,
    sex                 VARCHAR,
    weight_lbs          DOUBLE,
    clinic_id           INTEGER,
    insurance_provider  VARCHAR,
    registration_date   DATE,
    is_active           BOOLEAN,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.vet_provider (
    provider_id         INTEGER,
    first_name          VARCHAR,
    last_name           VARCHAR,
    full_name           VARCHAR,
    credentials         VARCHAR,
    specialty           VARCHAR,
    clinic_id           INTEGER,
    hire_date           DATE,
    termination_date    DATE,
    is_active           BOOLEAN,
    updated_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.vet_appointment (
    appointment_id      INTEGER,
    patient_id          INTEGER,
    provider_id         INTEGER,
    clinic_id           INTEGER,
    service_code        VARCHAR,
    service_name        VARCHAR,
    service_fee         DOUBLE,
    appointment_date    DATE,
    scheduled_at        TIMESTAMP,
    status              VARCHAR,
    duration_minutes    INTEGER,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.vet_referral (
    referral_id         INTEGER,
    patient_id          INTEGER,
    clinic_id           INTEGER,
    referral_source     VARCHAR,
    stage               VARCHAR,
    stage_entered_at    TIMESTAMP,
    stage_exited_at     TIMESTAMP,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.vet_invoice (
    invoice_id          INTEGER,
    appointment_id      INTEGER,
    patient_id          INTEGER,
    clinic_id           INTEGER,
    service_code        VARCHAR,
    amount              DOUBLE,
    payment_type        VARCHAR,
    insurance_paid      DOUBLE,
    patient_paid        DOUBLE,
    invoice_date        DATE,
    paid_date           DATE,
    status              VARCHAR,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.vet_budget_target (
    clinic_id               INTEGER,
    clinic_name             VARCHAR,
    year                    INTEGER,
    month                   INTEGER,
    target_revenue          DOUBLE,
    target_appointments     INTEGER,
    target_new_patients     INTEGER
);
