"""
Tests for silver staging views.
Uses in-memory DuckDB so tests never touch data/pawsfirst.duckdb.
"""
from __future__ import annotations

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_silver_views(con: duckdb.DuckDBPyConnection) -> None:
    """Create all silver views against the in-memory bronze tables."""
    from flows.silver_transform import (
        STG_VET_CLINIC_SQL,
        STG_VET_PATIENT_SQL,
        STG_VET_APPOINTMENT_SQL,
        STG_VET_REFERRAL_SQL,
        STG_VET_INVOICE_SQL,
    )
    for sql in [
        STG_VET_CLINIC_SQL,
        STG_VET_PATIENT_SQL,
        STG_VET_APPOINTMENT_SQL,
        STG_VET_REFERRAL_SQL,
        STG_VET_INVOICE_SQL,
    ]:
        con.execute(sql)


# ---------------------------------------------------------------------------
# stg_vet_clinic
# ---------------------------------------------------------------------------

class TestStgVetClinic:
    def test_row_count(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        count = con_with_bronze.execute("SELECT COUNT(*) FROM silver.stg_vet_clinic").fetchone()[0]
        assert count == 3

    def test_opened_date_cast_to_date(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        dtype = con_with_bronze.execute(
            "SELECT typeof(opened_date) FROM silver.stg_vet_clinic LIMIT 1"
        ).fetchone()[0]
        assert dtype.lower() == "date"

    def test_closed_clinic_has_closed_date(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        row = con_with_bronze.execute(
            "SELECT closed_date, renamed_to FROM silver.stg_vet_clinic WHERE clinic_id = 3"
        ).fetchone()
        assert row[0] is not None
        assert row[1] == "New Clinic"


# ---------------------------------------------------------------------------
# stg_vet_patient
# ---------------------------------------------------------------------------

class TestStgVetPatient:
    def test_all_patients_retained_with_left_join(self, con_with_bronze):
        """BUG FIX: LEFT JOIN keeps patients even if clinic is inactive."""
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_PATIENT_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_PATIENT_SQL)
        count = con_with_bronze.execute(
            "SELECT COUNT(*) FROM silver.stg_vet_patient"
        ).fetchone()[0]
        assert count == 3  # all 3 patients retained, including patient at clinic 2

    def test_has_insurance_derived_correctly(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_PATIENT_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_PATIENT_SQL)
        row = con_with_bronze.execute(
            "SELECT insurance_provider FROM silver.stg_vet_patient WHERE patient_id = 101"
        ).fetchone()
        assert row[0] == "PetPlan"

    def test_patient_without_insurance_has_null(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_PATIENT_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_PATIENT_SQL)
        row = con_with_bronze.execute(
            "SELECT insurance_provider FROM silver.stg_vet_patient WHERE patient_id = 102"
        ).fetchone()
        assert row[0] is None


# ---------------------------------------------------------------------------
# stg_vet_appointment
# ---------------------------------------------------------------------------

class TestStgVetAppointment:
    def test_is_completed_not_null(self, con_with_bronze):
        """BUG FIX: is_completed must never be NULL."""
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_APPOINTMENT_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_APPOINTMENT_SQL)
        null_count = con_with_bronze.execute(
            "SELECT COUNT(*) FROM silver.stg_vet_appointment WHERE is_completed IS NULL"
        ).fetchone()[0]
        assert null_count == 0

    def test_is_cancelled_not_null(self, con_with_bronze):
        """BUG FIX: is_cancelled must never be NULL."""
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_APPOINTMENT_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_APPOINTMENT_SQL)
        null_count = con_with_bronze.execute(
            "SELECT COUNT(*) FROM silver.stg_vet_appointment WHERE is_cancelled IS NULL"
        ).fetchone()[0]
        assert null_count == 0

    def test_is_no_show_not_null(self, con_with_bronze):
        """BUG FIX: is_no_show must never be NULL."""
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_APPOINTMENT_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_APPOINTMENT_SQL)
        null_count = con_with_bronze.execute(
            "SELECT COUNT(*) FROM silver.stg_vet_appointment WHERE is_no_show IS NULL"
        ).fetchone()[0]
        assert null_count == 0

    def test_status_flags_match_status(self, con_with_bronze):
        """Status flags must be consistent with the status column."""
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_APPOINTMENT_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_APPOINTMENT_SQL)
        bad = con_with_bronze.execute("""
            SELECT COUNT(*) FROM silver.stg_vet_appointment
            WHERE (is_completed = TRUE AND status != 'completed')
               OR (is_cancelled = TRUE AND status != 'cancelled')
               OR (is_no_show   = TRUE AND status != 'no_show')
        """).fetchone()[0]
        assert bad == 0

    def test_deduplication_keeps_latest(self, con_with_bronze):
        """Duplicate appointment_ids should be deduplicated, keeping latest updated_at."""
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_APPOINTMENT_SQL
        # Insert a duplicate with a later updated_at
        con_with_bronze.execute("""
            INSERT INTO bronze.vet_appointment VALUES
            (1001, 101, 201, 1, 'WEL', 'Wellness', 150.0,
             '2023-01-10 09:00:00', '2023-01-10', 'completed', 30,
             TRUE, FALSE, FALSE,
             '2023-01-10 09:00:00', '2023-06-01 12:00:00')
        """)
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_APPOINTMENT_SQL)
        count = con_with_bronze.execute(
            "SELECT COUNT(*) FROM silver.stg_vet_appointment WHERE appointment_id = 1001"
        ).fetchone()[0]
        assert count == 1

    def test_row_count(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_APPOINTMENT_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_APPOINTMENT_SQL)
        count = con_with_bronze.execute(
            "SELECT COUNT(*) FROM silver.stg_vet_appointment"
        ).fetchone()[0]
        assert count == 4


# ---------------------------------------------------------------------------
# stg_vet_invoice
# ---------------------------------------------------------------------------

class TestStgVetInvoice:
    def test_is_paid_flag(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_INVOICE_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_INVOICE_SQL)
        row = con_with_bronze.execute(
            "SELECT is_paid, is_outstanding FROM silver.stg_vet_invoice WHERE invoice_id = 2001"
        ).fetchone()
        assert row[0] is True   # is_paid
        assert row[1] is False  # is_outstanding

    def test_has_insurance_payment_flag(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_INVOICE_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_INVOICE_SQL)
        row = con_with_bronze.execute(
            "SELECT has_insurance_payment FROM silver.stg_vet_invoice WHERE invoice_id = 2001"
        ).fetchone()
        assert row[0] is True

    def test_self_pay_has_no_insurance(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_INVOICE_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_INVOICE_SQL)
        row = con_with_bronze.execute(
            "SELECT has_insurance_payment FROM silver.stg_vet_invoice WHERE invoice_id = 2002"
        ).fetchone()
        assert row[0] is False

    def test_collection_rate_calculation(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_INVOICE_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_INVOICE_SQL)
        row = con_with_bronze.execute(
            "SELECT collection_rate FROM silver.stg_vet_invoice WHERE invoice_id = 2001"
        ).fetchone()
        # insurance_paid=100 + patient_paid=50 = 150 / amount=150 = 1.0
        assert row[0] == 1.0

    def test_collection_rate_not_null(self, con_with_bronze):
        from flows.silver_transform import STG_VET_CLINIC_SQL, STG_VET_INVOICE_SQL
        con_with_bronze.execute(STG_VET_CLINIC_SQL)
        con_with_bronze.execute(STG_VET_INVOICE_SQL)
        null_count = con_with_bronze.execute(
            "SELECT COUNT(*) FROM silver.stg_vet_invoice WHERE collection_rate IS NULL AND amount > 0"
        ).fetchone()[0]
        assert null_count == 0
