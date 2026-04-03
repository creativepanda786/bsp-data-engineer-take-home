"""
Shared pytest fixtures for PawsFirst warehouse tests.
Uses an in-memory DuckDB connection so tests never touch data/pawsfirst.duckdb.
"""
from __future__ import annotations

import pytest
import duckdb


@pytest.fixture
def con():
    """In-memory DuckDB connection with bronze/silver/gold schemas."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    yield conn
    conn.close()


@pytest.fixture
def con_with_bronze(con):
    """In-memory DuckDB with bronze tables seeded with minimal test data."""
    con.execute("""
        CREATE TABLE bronze.vet_clinic (
            clinic_id    INTEGER,
            clinic_name  VARCHAR,
            city         VARCHAR,
            state        VARCHAR,
            area         VARCHAR,
            opened_date  DATE,
            closed_date  DATE,
            is_current   BOOLEAN,
            renamed_to   VARCHAR,
            updated_at   TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO bronze.vet_clinic VALUES
        (1, 'Paws North',  'Seattle',  'WA', 'North', '2020-01-01', NULL,         TRUE,  NULL,         '2020-01-01 00:00:00'),
        (2, 'Paws South',  'Portland', 'OR', 'South', '2019-06-01', NULL,         TRUE,  NULL,         '2019-06-01 00:00:00'),
        (3, 'Old Clinic',  'Tacoma',   'WA', 'North', '2018-01-01', '2022-12-31', FALSE, 'New Clinic', '2022-12-31 00:00:00')
    """)

    con.execute("""
        CREATE TABLE bronze.vet_patient (
            patient_id    INTEGER,
            owner_id      INTEGER,
            patient_name  VARCHAR,
            species       VARCHAR,
            breed         VARCHAR,
            date_of_birth DATE,
            sex           VARCHAR,
            weight_lbs    DOUBLE,
            clinic_id     INTEGER,
            insurance_provider VARCHAR,
            registration_date  DATE,
            is_active     BOOLEAN,
            created_at    TIMESTAMP,
            updated_at    TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO bronze.vet_patient VALUES
        (101, 1, 'Buddy',   'dog', 'Labrador', '2018-05-01', 'M', 65.0, 1, 'PetPlan', '2021-01-10', TRUE,  '2021-01-10 09:00:00', '2021-01-10 09:00:00'),
        (102, 2, 'Whiskers','cat', 'Siamese',  '2019-03-15', 'F', 10.0, 1, NULL,      '2021-02-01', TRUE,  '2021-02-01 10:00:00', '2021-02-01 10:00:00'),
        (103, 3, 'Buddy',   'dog', 'Labrador', '2018-05-01', 'M', 65.0, 2, NULL,      '2021-03-01', TRUE,  '2021-03-01 11:00:00', '2021-03-01 11:00:00')
    """)

    con.execute("""
        CREATE TABLE bronze.vet_appointment (
            appointment_id   INTEGER,
            patient_id       INTEGER,
            provider_id      INTEGER,
            clinic_id        INTEGER,
            service_code     VARCHAR,
            service_name     VARCHAR,
            service_fee      DOUBLE,
            scheduled_at     TIMESTAMP,
            appointment_date DATE,
            status           VARCHAR,
            duration_minutes INTEGER,
            is_completed     BOOLEAN,
            is_cancelled     BOOLEAN,
            is_no_show       BOOLEAN,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO bronze.vet_appointment VALUES
        (1001, 101, 201, 1, 'WEL', 'Wellness',  150.0, '2023-01-10 09:00:00', '2023-01-10', 'completed', 30, TRUE,  FALSE, FALSE, '2023-01-10 09:00:00', '2023-01-10 09:00:00'),
        (1002, 102, 202, 1, 'DEN', 'Dental',    200.0, '2023-01-11 10:00:00', '2023-01-11', 'cancelled', 45, FALSE, TRUE,  FALSE, '2023-01-11 10:00:00', '2023-01-11 10:00:00'),
        (1003, 103, 201, 2, 'SUR', 'Surgery',   500.0, '2023-01-12 11:00:00', '2023-01-12', 'no_show',   60, FALSE, FALSE, TRUE,  '2023-01-12 11:00:00', '2023-01-12 11:00:00'),
        (1004, 101, 201, 1, 'WEL', 'Wellness',  150.0, '2023-02-10 09:00:00', '2023-02-10', 'completed', 30, TRUE,  FALSE, FALSE, '2023-02-10 09:00:00', '2023-02-10 09:00:00')
    """)

    con.execute("""
        CREATE TABLE bronze.vet_invoice (
            invoice_id              INTEGER,
            appointment_id          INTEGER,
            patient_id              INTEGER,
            clinic_id               INTEGER,
            service_code            VARCHAR,
            amount                  DOUBLE,
            insurance_paid          DOUBLE,
            patient_paid            DOUBLE,
            payment_type            VARCHAR,
            invoice_date            DATE,
            paid_date               DATE,
            status                  VARCHAR,
            created_at              TIMESTAMP,
            updated_at              TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO bronze.vet_invoice VALUES
        (2001, 1001, 101, 1, 'WEL', 150.0, 100.0, 50.0,  'insurance', '2023-01-10', '2023-01-15', 'paid',        '2023-01-10 09:00:00', '2023-01-15 09:00:00'),
        (2002, 1004, 101, 1, 'WEL', 150.0, 0.0,   150.0, 'self_pay',  '2023-02-10', NULL,         'outstanding', '2023-02-10 09:00:00', '2023-02-10 09:00:00')
    """)

    return con
