# Datamak Task Backlog

Last updated: 2026-03-26

This file is the prioritized engineering backlog and working upgrade log for the repository.

## How To Use This File

- Use `docs/ARCHITECTURE.md` as the central workflow reference before changing pipeline behavior.
- Add new upgrade proposals to the "Upgrade Intake Log" section first, then promote them into priority buckets once scoped.
- For each upgrade, capture the motivation, likely files, workflow/status contracts touched, and validation plan.

## Upgrade Intake Log

- [ ] Template for new upgrade entries
  - Goal: one-sentence description of the intended improvement.
  - Why: the problem, risk, or operator pain point this upgrade addresses.
  - Candidate files: scripts, tests, and docs likely to change.
  - Workflow impact: note any affected status transitions, schema contracts, GUI actions, or remote steps.
  - Validation: list the tests or manual checks required before completion.

## P0 - High Priority

- [ ] Normalize `DTWIN_ROOT` defaults to this repo (`Datamak`) across all scripts.
  - Why: several scripts still default to `.../Digital_twin`, which can silently point to the wrong workspace.
  - Candidate files: `db_update/Transp_full_auto/MainSteps_1_launch_on_laptop.sh`, `db_update/Transp_full_auto/MainSteps_3_launch_on_laptop.sh`.
  - Acceptance criteria:
    - All scripts resolve project paths from `DTWIN_ROOT` or script-relative paths.
    - No default points to a different repo.
    - Full-auto step 1/3 scripts run with only `DTWIN_ROOT` set to this repo root.

- [ ] Formalize status transition checks for batch lifecycle.
  - Why: status values are used as contracts between local DB, remote jobs, and GUI actions.
  - Candidate files: `batch/create_batch_database.py`, `batch/deploy_batch_large.py`, `batch/check_launched_batches.py`, `gui/app.py`.
  - Acceptance criteria:
    - Tests assert allowed transitions for `gk_input`, `gk_batch`, `gk_run`.
    - Invalid or skipped transitions are surfaced as warnings/errors.

## P1 - Important

- [ ] Bootstrap surrogate schema in DB creation flow.
  - Why: `gk_surrogate` is currently created lazily in GUI code, while other core tables are bootstrapped in `database/create_gyrokinetic_db.py`.
  - Candidate files: `database/create_gyrokinetic_db.py`, `gui/app.py`, `tests/test_database_schema.py`.
  - Acceptance criteria:
    - `gk_surrogate` (and optional surrogate metadata columns) exists after DB bootstrap.
    - GUI still handles backward compatibility for older DBs.

- [ ] Add tests for Flux full-auto sync path.
  - Why: `db_update/Transp_full_auto` scripts are a key pipeline but have limited automated coverage.
  - Candidate files: `db_update/Transp_full_auto/build_flux_equil_inputs.py`, `db_update/Transp_full_auto/sync_flux_equil_inputs_to_main.py`, new tests under `tests/`.
  - Acceptance criteria:
    - Tests cover upsert behavior for `transp_timeseries`, `data_equil`, `gk_study`, and `gk_input`.
    - Tests verify empty-content handling (`gk_input.status -> ERROR` where applicable).

- [ ] Document and validate remote prerequisites for Flux and Perlmutter workflows.
  - Why: remote execution requires non-obvious assumptions (SSH control sockets, identities, directories, Pyrokinetics venv).
  - Candidate files: `batch/README.md`, `db_update/Transp_full_auto/README.md`, `docs/ARCHITECTURE.md`.
  - Acceptance criteria:
    - One checklist for required remote paths, auth setup, and runtime dependencies.
    - Scripts fail fast with actionable messages when prerequisites are missing.

## P2 - Nice to Have

- [ ] Add lightweight migration/version tracking for schema changes.
  - Why: schema evolves via conditional `ALTER TABLE`; explicit versioning would improve reproducibility.
  - Candidate files: `database/create_gyrokinetic_db.py`, potential new `database/migrations/`.
  - Acceptance criteria:
    - DB exposes a schema version table or pragma-backed version.
    - Startup scripts can report current and expected schema versions.

- [ ] Expand architecture docs with sequence diagrams for major workflows.
  - Why: onboarding and maintenance are easier with visualized pipeline steps.
  - Candidate files: `docs/ARCHITECTURE.md`, optional Mermaid diagrams.
  - Acceptance criteria:
    - At least one sequence diagram each for:
      - batch deploy and sync
      - Flux full-auto ingestion and main DB sync
