# PawsFirst Veterinary Warehouse — Naming & Design Conventions

This document defines the standards for all database objects in the PawsFirst data warehouse. **All new code must follow these conventions.**

---

## 1. Schema Roles

| Schema | Purpose | Who Queries It |
|--------|---------|----------------|
| `bronze` | Raw ingestion (source fidelity preserved) | Pipeline code only |
| `silver` | Cleaned, deduped, typed (VIEWs preferred) | Pipeline code, debugging |
| `gold` | Business-semantic layer (Kimball star schema) | Analysts, dashboards, AI tools |

## 2. Object Naming Pattern

All gold objects follow a three-part naming pattern:

```
{type}_{domain}_{entity}_{qualifier}
```

### Type Prefixes

| Schema | Prefix | Object Type | Example |
|--------|--------|-------------|---------|
| `bronze` | `vet_` | Raw ingestion tables | `bronze.vet_appointment` |
| `silver` | `stg_` | Single-source cleaned views | `silver.stg_vet_appointment` |
| `gold` | `dim_` | Conformed dimensions (SCD2) | `gold.dim_clin_patient` |
| `gold` | `fact_` | Event-grain fact tables | `gold.fact_clin_appointment` |
| `gold` | `agg_` | Pre-aggregated summaries | `gold.agg_clin_clinic_weekly` |
| `gold` | `ref_` | Lookup/mapping tables | `gold.ref_core_budget_target` |
| `gold` | `v_` | Analytical views | `gold.v_fin_revenue_by_clinic_monthly` |

### Domain Vocabulary

| Domain | Abbrev | Scope | Examples |
|--------|--------|-------|---------|
| Clinical | `clin` | Patients, appointments, providers, services | `dim_clin_patient`, `fact_clin_appointment` |
| Pipeline | `pipe` | Referrals, funnel stages, conversion | `dim_pipe_referral`, `v_pipe_funnel_summary` |
| Finance | `fin` | Invoices, revenue, payments, budgets | `fact_fin_invoice`, `v_fin_revenue_by_clinic_monthly` |
| Operations | `ops` | Scheduling, capacity, operational workflows | `v_ops_provider_utilization_weekly` |
| Core | `core` | Cross-domain shared objects (clinics, dates) | `dim_core_clinic`, `ref_core_budget_target` |

When an object could belong to multiple domains, assign it to the domain of its **primary consumer**.

### Qualifier Vocabulary

Required suffix for views (`v_`) and aggregates (`agg_`). Dimensions, facts, and references do not use qualifiers.

| Qualifier | When to Use | Example |
|-----------|------------|---------|
| `_current` | Point-in-time snapshot (latest state) | `v_clin_patient_current` |
| `_weekly` | Weekly time grain | `v_ops_provider_utilization_weekly` |
| `_monthly` | Monthly time grain | `v_fin_revenue_by_clinic_monthly` |
| `_summary` | Aggregated/rolled-up | `v_pipe_funnel_summary` |
| `_trend` | Time series for charting | `v_fin_collection_rate_trend` |
| `_exceptions` | Data quality / anomaly reporting | `v_clin_duplicate_patients_exceptions` |

## 3. Table Name Rules

- **Singular**, not plural: `dim_clin_patient`, not `dim_clin_patients`
- No source-system names in gold: `dim_clin_patient`, not `dim_pawsfirst_patient`

## 4. Column Naming

| Pattern | Convention | Example |
|---------|-----------|---------|
| Surrogate keys | `<entity>_sk` (UUID) | `patient_sk`, `clinic_sk` |
| Natural keys | `<entity>_id` (source type) | `patient_id`, `provider_id` |
| Booleans | `is_` or `has_` prefix | `is_current`, `has_insurance` |
| Timestamps | `_at` suffix | `created_at`, `scheduled_at` |
| Dates | `_date` suffix | `appointment_date`, `hire_date` |

### Date/Timestamp Three-Axis Grammar

Date and timestamp columns encode three independent axes:

```
{business_event}_{provenance}_{type_suffix}
```

**Axis 1 — Business Event** (what happened): `appointment`, `registration`, `referral`

**Axis 2 — Provenance** (how we know):

| Provenance | Meaning | Example |
|------------|---------|---------|
| `scheduled` | System scheduled this event | `appointment_scheduled_at` |
| `confirmed` | Activity crossed a threshold | `registration_confirmed_at` |
| `documented` | A human entered this value | `referral_documented_date` |
| `planned` | Future-facing date | `appointment_planned_date` |
| `detected` | Pipeline/system logic identified state change | `churn_detected_at` |

**Axis 3 — SQL Type**: `_at` = TIMESTAMP, `_date` = DATE

**Rules:**
- All three axes are required. Bare `{event}_{type}` columns (e.g., `appointment_date`) are acceptable only in bronze (source fidelity). Silver and gold must use the full grammar.
- Order is fixed: event → provenance → type.

## 5. SCD2 Pattern

Slowly Changing Dimension Type 2 tables require these columns:

| Column | Type | Purpose |
|--------|------|---------|
| `<entity>_sk` | UUID | Surrogate key (unique per version) |
| `<entity>_id` | source type | Natural/business key (durable) |
| `effective_start` | TIMESTAMP | When this version became active |
| `effective_end` | TIMESTAMP | When this version was superseded (NULL if current) |
| `is_current` | BOOLEAN | TRUE for the latest version |
| `row_hash` | VARCHAR | MD5 hash of tracked attributes for change detection |

**Load pattern:** DELETE + INSERT (DuckDB does not support MERGE).

```sql
-- 1. Delete existing current row for changed records
DELETE FROM gold.dim_clin_patient
WHERE patient_id IN (SELECT patient_id FROM staging_changes)
  AND is_current = TRUE;

-- 2. Insert new version
INSERT INTO gold.dim_clin_patient
SELECT
    gen_random_uuid() AS patient_sk,
    patient_id,
    -- ... attributes ...
    CURRENT_TIMESTAMP AS effective_start,
    NULL AS effective_end,
    TRUE AS is_current,
    md5_hash AS row_hash
FROM staging_changes;
```

## 6. Soda Contract Requirements

Every gold table **must** have a Soda v4 data contract. At minimum:

- Schema check with `allow_extra_columns: true`
- At least one business rule per table (row count, freshness, or failed_rows check)

Contract files go in `soda/contracts/{layer}/`.

## 7. Migration Naming

Sequential numbering: `NNNN_<description>.sql`

Example: `0003_create_dim_clin_patient.sql`

One logical change per migration.

## 8. Credential Rules

All database connections must use the helper in `flows/common.py`. Never hardcode connection strings or paths.

## 9. Git Conventions

### Commit Message Format

```
<type>: <description>
```

**Types**: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

**Example**: `feat: add dim_clin_patient SCD2 dimension`

One logical change per commit.
