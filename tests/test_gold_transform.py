"""
Tests for the gold Kimball star-schema layer.
Uses an in-memory DuckDB connection seeded from silver views — never touches
data/pawsfirst.duckdb.
"""
from __future__ import annotations

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_silver_and_gold(con: duckdb.DuckDBPyConnection) -> None:
    """Create all silver views then load all gold objects into con."""
    from flows.silver_transform import (
        STG_VET_CLINIC_SQL,
        STG_VET_PATIENT_SQL,
        STG_VET_APPOINTMENT_SQL,
        STG_VET_REFERRAL_SQL,
        STG_VET_INVOICE_SQL,
        STG_VET_PROVIDER_SQL,
        STG_VET_BUDGET_TARGET_SQL,
    )
    from flows.gold_transform import (
        DIM_CORE_CLINIC_SQL, DIM_CORE_CLINIC_LOAD_SQL,
        DIM_CLIN_PATIENT_SQL, DIM_CLIN_PATIENT_LOAD_SQL,
        DIM_CLIN_PROVIDER_SQL, DIM_CLIN_PROVIDER_LOAD_SQL,
        REF_CORE_BUDGET_TARGET_SQL, REF_CORE_BUDGET_TARGET_LOAD_SQL,
        FACT_CLIN_APPOINTMENT_SQL, FACT_CLIN_APPOINTMENT_LOAD_SQL,
        FACT_FIN_INVOICE_SQL, FACT_FIN_INVOICE_LOAD_SQL,
        AGG_CLIN_CLINIC_WEEKLY_SQL,
        V_FIN_REVENUE_BY_CLINIC_MONTHLY_SQL,
        V_OPS_PROVIDER_UTILIZATION_WEEKLY_SQL,
        _run_phases,
    )

    logger = _FakeLogger()

    # Silver views
    for sql in [
        STG_VET_CLINIC_SQL,
        STG_VET_PATIENT_SQL,
        STG_VET_APPOINTMENT_SQL,
        STG_VET_REFERRAL_SQL,
        STG_VET_INVOICE_SQL,
        STG_VET_PROVIDER_SQL,
        STG_VET_BUDGET_TARGET_SQL,
    ]:
        con.execute(sql)

    # Gold dims
    con.execute(DIM_CORE_CLINIC_SQL)
    _run_phases(con, DIM_CORE_CLINIC_LOAD_SQL, "dim_core_clinic", logger)

    con.execute(DIM_CLIN_PATIENT_SQL)
    _run_phases(con, DIM_CLIN_PATIENT_LOAD_SQL, "dim_clin_patient", logger)

    con.execute(DIM_CLIN_PROVIDER_SQL)
    _run_phases(con, DIM_CLIN_PROVIDER_LOAD_SQL, "dim_clin_provider", logger)

    # Gold ref
    con.execute(REF_CORE_BUDGET_TARGET_SQL)
    _run_phases(con, REF_CORE_BUDGET_TARGET_LOAD_SQL, "ref_core_budget_target", logger)

    # Gold facts
    con.execute(FACT_CLIN_APPOINTMENT_SQL)
    _run_phases(con, FACT_CLIN_APPOINTMENT_LOAD_SQL, "fact_clin_appointment", logger)

    con.execute(FACT_FIN_INVOICE_SQL)
    _run_phases(con, FACT_FIN_INVOICE_LOAD_SQL, "fact_fin_invoice", logger)

    # Gold analytical views
    for sql in [
        AGG_CLIN_CLINIC_WEEKLY_SQL,
        V_FIN_REVENUE_BY_CLINIC_MONTHLY_SQL,
        V_OPS_PROVIDER_UTILIZATION_WEEKLY_SQL,
    ]:
        con.execute(sql)


class _FakeLogger:
    """Minimal stand-in for Prefect's get_run_logger() outside a flow context."""
    def info(self, msg):  pass
    def error(self, msg): pass
    def warning(self, msg): pass


# ---------------------------------------------------------------------------
# Fixture: bronze tables + silver views + gold objects
# ---------------------------------------------------------------------------

@pytest.fixture
def con_with_gold(con_with_bronze):
    """Extends con_with_bronze with a vet_provider table and full gold layer."""
    con_with_bronze.execute("""
        CREATE TABLE bronze.vet_provider (
            provider_id      INTEGER,
            first_name       VARCHAR,
            last_name        VARCHAR,
            full_name        VARCHAR,
            credentials      VARCHAR,
            specialty        VARCHAR,
            clinic_id        INTEGER,
            hire_date        DATE,
            termination_date DATE,
            is_active        BOOLEAN,
            updated_at       TIMESTAMP
        )
    """)
    con_with_bronze.execute("""
        INSERT INTO bronze.vet_provider VALUES
        (201, 'Alice', 'Smith', 'Alice Smith', 'DVM',  'Wellness', 1, '2019-01-01', NULL,         TRUE,  '2019-01-01 00:00:00'),
        (202, 'Bob',   'Jones', 'Bob Jones',   'DVM',  'Dental',   1, '2020-03-01', '2023-06-01', FALSE, '2023-06-01 00:00:00')
    """)

    # vet_referral is required by silver.stg_vet_referral
    con_with_bronze.execute("""
        CREATE TABLE bronze.vet_referral (
            referral_id      INTEGER,
            patient_id       INTEGER,
            clinic_id        INTEGER,
            referral_source  VARCHAR,
            stage            VARCHAR,
            stage_entered_at TIMESTAMP,
            stage_exited_at  TIMESTAMP,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP
        )
    """)
    con_with_bronze.execute("""
        INSERT INTO bronze.vet_referral VALUES
        (3001, 101, 1, 'web', 'inquiry',      '2023-01-01 00:00:00', '2023-01-05 00:00:00', '2023-01-01 00:00:00', '2023-01-05 00:00:00'),
        (3001, 101, 1, 'web', 'consultation', '2023-01-05 00:00:00', '2023-01-10 00:00:00', '2023-01-05 00:00:00', '2023-01-10 00:00:00'),
        (3002, 102, 1, 'vet', 'inquiry',      '2023-01-03 00:00:00', '2023-01-02 00:00:00', '2023-01-03 00:00:00', '2023-01-03 00:00:00')
    """)

    # vet_budget_target required by silver.stg_vet_budget_target
    con_with_bronze.execute("""
        CREATE TABLE bronze.vet_budget_target (
            clinic_id           INTEGER,
            year                INTEGER,
            month               INTEGER,
            target_revenue      DOUBLE,
            target_appointments INTEGER,
            target_new_patients INTEGER
        )
    """)
    con_with_bronze.execute("""
        INSERT INTO bronze.vet_budget_target VALUES
        (1, 2023, 1, 10000.0, 80, 10),
        (2, 2023, 1,  8000.0, 60,  8)
    """)

    build_silver_and_gold(con_with_bronze)
    return con_with_bronze


# ---------------------------------------------------------------------------
# dim_core_clinic
# ---------------------------------------------------------------------------

class TestDimCoreClinic:
    def test_row_count(self, con_with_gold):
        count = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.dim_core_clinic"
        ).fetchone()[0]
        assert count == 3

    def test_no_duplicate_surrogate_keys(self, con_with_gold):
        dups = con_with_gold.execute("""
            SELECT COUNT(*) FROM (
                SELECT clinic_sk FROM gold.dim_core_clinic
                GROUP BY clinic_sk HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        assert dups == 0

    def test_all_rows_have_surrogate_key(self, con_with_gold):
        nulls = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.dim_core_clinic WHERE clinic_sk IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_is_current_populated(self, con_with_gold):
        nulls = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.dim_core_clinic WHERE is_current IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_row_hash_populated(self, con_with_gold):
        nulls = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.dim_core_clinic WHERE row_hash IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_effective_end_null_for_current_rows(self, con_with_gold):
        bad = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.dim_core_clinic
            WHERE is_current = TRUE AND effective_end IS NOT NULL
        """).fetchone()[0]
        assert bad == 0


# ---------------------------------------------------------------------------
# dim_clin_patient
# ---------------------------------------------------------------------------

class TestDimClinPatient:
    def test_row_count(self, con_with_gold):
        count = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.dim_clin_patient"
        ).fetchone()[0]
        assert count == 3

    def test_no_duplicate_surrogate_keys(self, con_with_gold):
        dups = con_with_gold.execute("""
            SELECT COUNT(*) FROM (
                SELECT patient_sk FROM gold.dim_clin_patient
                GROUP BY patient_sk HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        assert dups == 0

    def test_has_insurance_flag_derived_correctly(self, con_with_gold):
        row = con_with_gold.execute("""
            SELECT has_insurance FROM gold.dim_clin_patient
            WHERE patient_id = 101 AND is_current = TRUE
        """).fetchone()
        assert row[0] is True

    def test_no_insurance_flag_false_when_null(self, con_with_gold):
        row = con_with_gold.execute("""
            SELECT has_insurance FROM gold.dim_clin_patient
            WHERE patient_id = 102 AND is_current = TRUE
        """).fetchone()
        assert row[0] is False

    def test_scd2_effective_range_valid(self, con_with_gold):
        """effective_end must be NULL or strictly after effective_start."""
        bad = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.dim_clin_patient
            WHERE effective_end IS NOT NULL
              AND effective_end <= effective_start
        """).fetchone()[0]
        assert bad == 0


# ---------------------------------------------------------------------------
# dim_clin_provider
# ---------------------------------------------------------------------------

class TestDimClinProvider:
    def test_row_count(self, con_with_gold):
        count = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.dim_clin_provider"
        ).fetchone()[0]
        assert count == 2

    def test_no_duplicate_surrogate_keys(self, con_with_gold):
        dups = con_with_gold.execute("""
            SELECT COUNT(*) FROM (
                SELECT provider_sk FROM gold.dim_clin_provider
                GROUP BY provider_sk HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        assert dups == 0

    def test_terminated_provider_not_currently_active(self, con_with_gold):
        row = con_with_gold.execute("""
            SELECT is_currently_active FROM gold.dim_clin_provider
            WHERE provider_id = 202 AND is_current = TRUE
        """).fetchone()
        assert row[0] is False

    def test_active_provider_is_currently_active(self, con_with_gold):
        row = con_with_gold.execute("""
            SELECT is_currently_active FROM gold.dim_clin_provider
            WHERE provider_id = 201 AND is_current = TRUE
        """).fetchone()
        assert row[0] is True


# ---------------------------------------------------------------------------
# fact_clin_appointment
# ---------------------------------------------------------------------------

class TestFactClinAppointment:
    def test_row_count(self, con_with_gold):
        count = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.fact_clin_appointment"
        ).fetchone()[0]
        assert count == 4

    def test_no_duplicate_surrogate_keys(self, con_with_gold):
        dups = con_with_gold.execute("""
            SELECT COUNT(*) FROM (
                SELECT appointment_sk FROM gold.fact_clin_appointment
                GROUP BY appointment_sk HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        assert dups == 0

    def test_patient_sk_resolves_to_dim(self, con_with_gold):
        """Every non-null patient_sk must exist in dim_clin_patient."""
        orphans = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.fact_clin_appointment f
            WHERE f.patient_sk IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM gold.dim_clin_patient d
                WHERE d.patient_sk = f.patient_sk
              )
        """).fetchone()[0]
        assert orphans == 0

    def test_clinic_sk_resolves_to_dim(self, con_with_gold):
        """Every non-null clinic_sk must exist in dim_core_clinic."""
        orphans = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.fact_clin_appointment f
            WHERE f.clinic_sk IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM gold.dim_core_clinic d
                WHERE d.clinic_sk = f.clinic_sk
              )
        """).fetchone()[0]
        assert orphans == 0

    def test_is_completed_never_null(self, con_with_gold):
        nulls = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.fact_clin_appointment WHERE is_completed IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_status_flags_consistent(self, con_with_gold):
        bad = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.fact_clin_appointment
            WHERE (is_completed = TRUE AND status != 'completed')
               OR (is_cancelled = TRUE AND status != 'cancelled')
               OR (is_no_show   = TRUE AND status != 'no_show')
        """).fetchone()[0]
        assert bad == 0


# ---------------------------------------------------------------------------
# fact_fin_invoice
# ---------------------------------------------------------------------------

class TestFactFinInvoice:
    def test_row_count(self, con_with_gold):
        count = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.fact_fin_invoice"
        ).fetchone()[0]
        assert count == 2

    def test_no_duplicate_surrogate_keys(self, con_with_gold):
        dups = con_with_gold.execute("""
            SELECT COUNT(*) FROM (
                SELECT invoice_sk FROM gold.fact_fin_invoice
                GROUP BY invoice_sk HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        assert dups == 0

    def test_collection_rate_between_0_and_1(self, con_with_gold):
        bad = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.fact_fin_invoice
            WHERE collection_rate IS NOT NULL
              AND (collection_rate < 0 OR collection_rate > 1.0001)
        """).fetchone()[0]
        assert bad == 0

    def test_paid_invoice_has_correct_flags(self, con_with_gold):
        row = con_with_gold.execute("""
            SELECT is_paid, is_outstanding, collection_rate
            FROM gold.fact_fin_invoice
            WHERE invoice_id = 2001
        """).fetchone()
        assert row[0] is True   # is_paid
        assert row[1] is False  # is_outstanding
        assert row[2] == 1.0    # fully collected


# ---------------------------------------------------------------------------
# agg_clin_clinic_weekly (Req 1)
# ---------------------------------------------------------------------------

class TestAggClinClinicWeekly:
    def test_produces_rows(self, con_with_gold):
        count = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.agg_clin_clinic_weekly"
        ).fetchone()[0]
        assert count > 0

    def test_completion_rate_between_0_and_1(self, con_with_gold):
        bad = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.agg_clin_clinic_weekly
            WHERE completion_rate < 0 OR completion_rate > 1
        """).fetchone()[0]
        assert bad == 0

    def test_rates_sum_to_at_most_1(self, con_with_gold):
        """completion + cancellation + no_show rates cannot exceed 1 per row."""
        bad = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.agg_clin_clinic_weekly
            WHERE ROUND(completion_rate + cancellation_rate + no_show_rate, 4) > 1.0001
        """).fetchone()[0]
        assert bad == 0

    def test_total_appointments_equals_sum_of_flags(self, con_with_gold):
        bad = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.agg_clin_clinic_weekly
            WHERE total_appointments !=
                  completed_appointments + cancelled_appointments + no_show_appointments
        """).fetchone()[0]
        assert bad == 0


# ---------------------------------------------------------------------------
# v_fin_revenue_by_clinic_monthly (Req 3)
# ---------------------------------------------------------------------------

class TestVFinRevenueByClinicMonthly:
    def test_produces_rows(self, con_with_gold):
        count = con_with_gold.execute(
            "SELECT COUNT(*) FROM gold.v_fin_revenue_by_clinic_monthly"
        ).fetchone()[0]
        assert count > 0

    def test_collection_rate_between_0_and_1(self, con_with_gold):
        bad = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.v_fin_revenue_by_clinic_monthly
            WHERE collection_rate < 0 OR collection_rate > 1.0001
        """).fetchone()[0]
        assert bad == 0

    def test_total_collected_equals_insurance_plus_patient(self, con_with_gold):
        bad = con_with_gold.execute("""
            SELECT COUNT(*) FROM gold.v_fin_revenue_by_clinic_monthly
            WHERE ABS(total_collected - (total_insurance_paid + total_patient_paid)) > 0.01
        """).fetchone()[0]
        assert bad == 0
