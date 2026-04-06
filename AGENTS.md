# Datamak Agent Guide

This file defines persistent rules and conventions for AI/code agents working in this repository.

## Scope

- Repository root: `/Users/jdominsk/Documents/Projects/AIML_database/Datamak`
- Primary database: `gyrokinetic_simulations.db`
- Main language/runtime: Python 3

## Core Principles

- Preserve data integrity first. Changes that affect status transitions or schema require tests.
- Prefer idempotent scripts and additive schema evolution (`CREATE TABLE IF NOT EXISTS`, guarded `ALTER TABLE`).
- Keep behavior aligned between CLI workflows and Flask GUI actions.
- Avoid direct manual SQL edits to production data when a repo script already exists.

## Repository Conventions

- Use `DTWIN_ROOT` to resolve project-relative paths when possible.
- Treat source-location metadata as database state, not user-local GUI/HPC config.
  For Mate, TRANSP, and future equilibrium origins, the source of truth should live in workflow tables such as `data_origin`.
  Do not add source-root fields to the per-user HPC/settings panel.
- For AIML/workflow-supervisor development, prefer this four-layer organization:
  - `Core tool layer`: typed Datamak workflow tools with stable JSON-like input/output contracts
  - `Policy layer`: approval and safety rules, including read-only vs mutating/remote classification
  - `Adapter layer`: thin bindings from the core tools into GUI, MCP, REST, or other interfaces
  - `AI platform layer`: provider-specific prompts, model wiring, and platform integration
- Put workflow logic in the lowest reasonable layer.
  If a capability may be needed by both the GUI and a future AI platform, do not leave it embedded only in `gui/app.py`.
  Factor it into a reusable typed tool/helper module first.
- Keep adapter layers thin.
  Flask routes, MCP handlers, and platform-specific integrations should translate requests and responses, not own workflow semantics.
- Do not expose raw SQL, raw shell commands, or free-form Flask routes as the model interface.
  Models should interact through typed Datamak tools only.
- Do not couple Datamak's workflow logic to one vendor SDK or prompt format.
  Platform-specific code should remain replaceable outer-layer code.
- For DB schema changes, update:
  - `database/create_gyrokinetic_db.py`
  - Relevant tests in `tests/`
- For surrogate features/targets changes, keep these aligned:
  - `db_surrogate/train_gamma_surrogate.py`
  - `db_surrogate/estimate_gamma_surrogate.py`
  - GUI surrogate views in `gui/app.py` and `gui/templates/index.html`
- For batch/HPC flow changes, keep state transitions consistent across:
  - `batch/create_batch_database.py`
  - `batch/deploy_batch*.py`
  - `batch/check_launched_batches.py`
  - `batch/monitor_remote_runs.py`

## Status Semantics (Operational Contract)

- `gk_input.status` commonly uses: `NEW`, `WAIT`, `TORUN`, `BATCH`, `CRASHED`, `SUCCESS`, `ERROR`
- `gk_batch.status` commonly uses: `CREATED`, `SENT`, `LAUNCHED`, `SYNCED`
- `gk_run.status` values are workflow-driven; common states include `TORUN`, `RUNNING`, `SUCCESS`, `CONVERGED`, `CRASHED`

Any change to status names or transitions should be treated as a breaking contract change.

## Preferred Commands

- Create/refresh schema:
  - `python3 database/create_gyrokinetic_db.py --db gyrokinetic_simulations.db`
- Run tests:
  - `.venv/bin/python tests/run_tests.py`
  - `.venv/bin/python -m unittest discover -s tests -p "test_*.py" -v`
- Start GUI:
  - `python3 gui/app.py`

## Safety Rules

- Do not run destructive git commands unless explicitly requested.
- Do not delete or rename batch DB files manually unless the workflow script marks them as empty/archive targets.
- For remote operations (`ssh`, `rsync`, `sbatch`), prefer existing scripts under `batch/` and `db_update/Transp_full_auto/`.
- If a workflow needs source paths for ingestion, prefer database-backed metadata.
  CLI/env source-path overrides are transitional helpers, not the preferred ownership model.
- Validate with targeted tests after changes to:
  - status transition logic
  - schema creation/migration logic
  - surrogate training/estimation flow

## Definition of Done for Code Changes

- Code builds/runs in the local project environment.
- Affected tests pass (or a clear reason is documented if not run).
- Any schema/status behavior changes are documented in `docs/ARCHITECTURE.md` and/or `TASKS.md`.
- AIML-facing changes preserve or improve the four-layer separation:
  core tools -> policy -> adapters -> platform-specific code.
