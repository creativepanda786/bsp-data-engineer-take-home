# PawsFirst Veterinary Network — Data Engineer Take-Home Exercise

## The Scenario

You've just been hired as a senior data engineer at **PawsFirst**, a growing veterinary clinic network with 12 locations across three regions. The previous data engineer built the initial data warehouse but left before completing it. Your job is to pick up where they left off.

The warehouse uses a medallion architecture (bronze → silver → gold) backed by DuckDB, orchestrated with Prefect, and quality-checked with Soda v4 contracts. The bronze layer is loaded and working. Silver staging views exist but **may have issues**. The gold layer hasn't been built yet.

Your stakeholders have business questions they need answered. You'll need to design and build the gold layer to serve them.

## Getting Started

See [SETUP.md](SETUP.md) for detailed installation and setup instructions.

**Quick start:**

```bash
uv sync
uv run python scripts/seed_data.py
PYTHONPATH=. uv run python flows/ingest.py
PYTHONPATH=. uv run python flows/silver_transform.py
```

## What's Already Built

| Layer | Status | Location |
|-------|--------|----------|
| **Seed data generator** | Working | `scripts/seed_data.py` |
| **Bronze ingestion** | Working | `flows/ingest.py` |
| **Bronze Soda contracts** | Working | `soda/contracts/bronze/` |
| **Silver staging views** | Has issues — investigate before building on top | `flows/silver_transform.py` |
| **Gold layer** | Not built — this is your job | — |

## What to Deliver

Submit your work as a **single PR against the `main` branch**. Your PR description is part of the evaluation — use it to explain your decisions.

Here's what we're looking for, in no particular order. **You do not need to complete everything.** Prioritize depth over breadth — a well-built subset is better than a shallow attempt at everything.

### 1. Data Exploration & Profiling
Explore the seed data and existing pipeline. Understand the entities, relationships, and any quality issues in the data.

### 2. Debug the Silver Layer
The existing silver views have issues. Find them, diagnose the root causes, and fix them. Document what you found.

### 3. Complete the Silver Layer
Additional staging views may be needed for entities not yet covered (billing, referral cleanup, etc.).

### 4. Build the Gold Layer
Design and implement gold-layer objects (dimensions, facts, views) to answer the [business requirements](docs/business_requirements.md). Follow the [conventions](docs/conventions.md).

At minimum, we'd expect to see:
- At least one SCD2 dimension
- At least one fact table
- At least one analytical view answering a stakeholder question

### 5. Add Data Quality Contracts
Write Soda v4 contracts for your silver and gold objects.

### 6. AI Development Configuration
Configure this repository so an AI coding assistant (e.g., Claude Code, Cursor, Copilot) can safely and effectively contribute to the codebase. At minimum, write a `CLAUDE.md` file. Add any additional AI-assistance configuration you think would be valuable.

### 7. Write Migrations
All DDL changes should have corresponding migration files in `sql/migrations/`.

## Conventions

Read [docs/conventions.md](docs/conventions.md) before writing any SQL. It defines naming standards, column patterns, the SCD2 template, and other rules your code must follow.

## Business Requirements

Read [docs/business_requirements.md](docs/business_requirements.md) for the stakeholder questions driving the gold layer.

## Evaluation Criteria

- Data modeling quality (grain, Kimball patterns, entity relationships)
- Convention adherence (naming standards, column patterns, date grammar)
- SQL craft (CTEs, window functions, defensive transformations, idiomatic DuckDB)
- Python & pipeline quality (Prefect patterns, idempotency, error handling, clean code)
- Data quality engineering (Soda contracts, handling data issues)
- Debugging & root cause analysis (diagnosis approach, fix quality)
- AI development configuration (CLAUDE.md quality, guardrails, additional artifacts)
- Git discipline (commit granularity, message quality, branch hygiene)
- Documentation & communication (PR description, design decisions explained)

## Time Expectation

This exercise is designed for **10–12 hours** of focused work. We value quality over quantity — if you run out of time, tell us what you'd do next in your PR description.

## Submission

1. Create a branch from `main`
2. Do your work with clear, incremental commits
3. Open a PR against `main`
4. In the PR description, include:
   - Your design decisions and rationale
   - Bugs you found and how you diagnosed them
   - What you'd do with more time
   - Any assumptions you made
