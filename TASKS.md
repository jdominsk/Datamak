# Datamak Task Backlog

Last updated: 2026-11-25

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

### Datamak Lite agent integration

- [ ] **Keep routine use of the installable Datamak Lite skill within a small token budget.**
  - **Motivation** Researchers should be able to check campaigns and archive status
    through an AI agent without quickly using up their model allowance. The
    local maintainer skill used during development is about 6,500 input
    tokens when read in full. Measure the distributable skill separately.
  - **Design** Ship a short entry skill and load archive, restart, or HPC
    instructions as needed. Return counts, changes, and items needing attention
    by default. Let users request more rows or bounded log excerpts, independent
    of their subscription plan. Reasoning effort remains a setting in the host
    agent where supported.
  - **Workflow impact** Keep polling, provenance writes, and archive receipts
    in local Datamak tools. Use compact typed responses for agent requests.
    Users explicitly request model-assisted summaries or investigation.
  - **Candidate components** The installable Datamak skill and its references,
    Datamak Lite CLI and report helpers, and agent tool adapters.
  - **Target** Aim for about 1,000 Datamak-added input tokens for a routine
    campaign status check, including skill instructions, tool definitions
    presented for the task, and tool results. The total request also includes
    host instructions, conversation history, and the user prompt.
  - **Validation** Compare the current and compact versions on a routine
    campaign status check, an archive lookup, and a failed-run investigation.
    Use the same model, reasoning effort, and data in equivalent sessions.
    Record Datamak-added input size, total cached and uncached input, output
    tokens, and model call counts. Verify that both versions preserve the
    facts and provenance needed for each task.

### Datamak Lite app updates

- [ ] **Deliver GitHub source updates to User builds.**
  - **Role** Developer is the local launcher default. User is an explicit
    build role and uses its own app bundle. Update checks belong to the User app.
  - **Target** Check the published menu-bar source once a day while the User
    app runs. Stage a newer build in isolation and apply it on relaunch. Keep
    the project checkouts, campaign data, and archive flags outside the update.
  - **Status** The updater passed local fixture tests. Live validation needs
    two published app revisions.

### Datamak Lite storage refresh

- [x] **Refresh storage quotas from each machine card.** Reuse authenticated
  SSH when available, offer a Terminal connection when sign-in is needed, and
  retain the last successful quota reading with its scope and timestamp.
  Polaris passed a live check. Aurora and Perlmutter quota parsing was checked
  against the facilities’ documented output.

## P0 - High Priority

- [ ] Normalize `DATAMAK_ROOT` defaults to this repo (`Datamak`) across all scripts.
  - Why: several scripts still default to `.../Digital_twin`, which can silently point to the wrong workspace.
  - Candidate files: `db_update/Transp_full_auto/MainSteps_1_launch_on_laptop.sh`, `db_update/Transp_full_auto/MainSteps_3_launch_on_laptop.sh`.
  - Acceptance criteria:
    - All scripts resolve project paths from `DATAMAK_ROOT` or script-relative paths.
    - No default points to a different repo.
    - Full-auto step 1/3 scripts run with only `DATAMAK_ROOT` set to this repo root.

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
