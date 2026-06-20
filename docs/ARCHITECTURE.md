# Datamak Architecture

Last updated: 2026-03-26

This is the central workflow reference for Datamak. Use it to understand end-to-end data flow, runtime surfaces, and status contracts before changing behavior.

## 1) Purpose

Datamak orchestrates a gyrokinetic workflow around a central SQLite database:

- ingest equilibrium/profile sources (`pfile/gfile` and TRANSP CDF metadata)
- generate GK input definitions and content
- dispatch/run batch jobs on remote HPC systems
- sync run outcomes and convergence metrics
- train/apply surrogate models for rapid estimates

## 2) Repository Component Map

- `database/`
  - Schema bootstrap and seed scripts.
  - Primary entry point: `database/create_gyrokinetic_db.py`.
- `db_update/`
  - Ingestion and data-population scripts (Mate/Alexei/TRANSP workflows).
  - Full-auto Flux pipeline under `db_update/Transp_full_auto/`.
- `batch/`
  - Batch DB creation, remote deployment, remote sync/monitor utilities.
- `db_surrogate/`
  - Surrogate model training, prediction, estimate persistence, model cleanup.
- `gui/`
  - Flask application that exposes workflow actions and data views.
- `db_analysis/`
  - Analysis/report generation (sampling and remote monitoring reports).
- `tests/`
  - Unit/integration-style tests and centralized report runner.
- `docs/`
  - Schema diagrams and architecture documentation.

## 3) Runtime Surfaces

- CLI scripts (primary automation mechanism)
  - Most workflows are shell/Python entry points in `db_update/`, `batch/`, `db_surrogate/`.
- Web UI (operator console)
  - `gui/app.py` wires GUI actions to those same scripts.
- Remote execution
  - Flux: full-auto preprocessing and GK input generation.
  - Perlmutter: batch job launch/execution and monitoring.

## 4) Data Architecture

### 4.1 Main database

- File: `gyrokinetic_simulations.db` at repo root.
- Created/bootstrapped by `database/create_gyrokinetic_db.py`.
- Core relational groups:
  - Source metadata:
    - `data_origin`
    - `data_equil`
    - `flux_action_log`
      - latest staged Flux DB and one GUI-tracked Flux job per `data_origin`
      - important fields: `status`, `slurm_job_id`, `status_detail`,
        `submitted_at`, `status_checked_at`, `synced_at`
  - GK configuration:
    - `gk_code`
    - `gk_model`
      - logically unique on
        `(gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template)`
      - schema bootstrap deduplicates legacy duplicates before creating the unique index
    - `gk_study`
    - `gk_input`
  - Execution tracking:
    - `gk_batch`
    - `gk_run`
    - `gk_convergence_timeseries`
    - `gk_linear_run`
    - `gk_nonlinear_run`
  - Surrogate metadata/results:
    - `gk_surrogate` (created lazily in GUI path today)
    - `sg_estimate`

### 4.2 Auxiliary databases

- Batch DBs (ephemeral per dispatch):
  - `batch/new/batch_database_YYYYMMDD_HHMMSS.db`
  - Contain `gk_run` rows to execute remotely.
- Flux temp DBs (full-auto pipeline):
  - `transp_full_auto/flux_equil_inputs_YYYYMMDD_HHMMSS.db`
  - Used as transfer/sync units before main DB upsert.

### 4.3 Generated reports/artifacts

- Remote monitoring JSON:
  - `db_analysis/remote_monitor_report.json`
- HPC config/test JSON:
  - `db_analysis/hpc_config.json`
  - `db_analysis/hpc_test_result.json`
- Test reports:
  - `tests/reports/unit_test_report_*.txt|json`
- Surrogate model files:
  - `db_surrogate/models/*.pkl` and `*.json`

## 5) End-to-End Workflows

### 5.1 Ingestion of source equilibria

1. Mate Kinetic EFIT path:
  - `db_update/populate_data_equil_from_Mate_KinEFIT.py`
  - Scans local folders for `p*`/`g*`, stores names and file content in `data_equil`.
2. Alexei semi-auto TRANSP path:
  - `db_update/populate_data_equil_from_Alexei_Transp_09.py`
  - Lists remote `*.CDF` files over SSH and inserts metadata rows in `data_equil`.

Both paths ensure/attach a `data_origin` row and then activate selected rows via SQL scripts.

### 5.2 GK study/input creation

1. Create studies from active equilibria and code/model settings.
2. Create `gk_input` rows over psin grid and selected templates.
3. Generate input content with Pyrokinetics:
  - `db_update/create_gk_input_from_pyrokinetic_with_pfile_and_gfile.py`
  - `db_update/create_gk_input_from_pyrokinetic_with_transpfile.py`
4. Backfill parsed geometry/physics/species columns:
  - `db_update/backfill_gk_input_physics.py`

### 5.3 Batch dispatch and remote execution

1. Build batch DB from `gk_input.status='TORUN'`:
  - `batch/create_batch_database.py --copy-torun`
  - Writes `gk_run` rows into batch DB.
  - Updates source `gk_input.status -> BATCH`.
  - Inserts `gk_batch.status='CREATED'`.
2. Deploy to Perlmutter:
  - `batch/deploy_batch_large.py`
  - Transfers batch DB + HPC scripts.
  - Marks empty DBs as `empty_*`.
  - Updates `gk_batch` to `SENT`, then `LAUNCHED`.
3. Remote processing:
  - `job_execute_large.sh` claims/executes next `gk_run`.
4. Sync and monitor:
  - `batch/check_launched_batches.py` pulls unsynced remote rows.
  - `batch/monitor_remote_runs.py` generates consolidated health report.

### 5.4 Flux full-auto workflow (minimal local back-and-forth)

Orchestrated by:

- `db_update/Transp_full_auto/MainSteps_1_launch_on_laptop.sh`
- `db_update/Transp_full_auto/MainSteps_2_launch_on_flux.sh`
- `db_update/Transp_full_auto/MainSteps_3_launch_on_laptop.sh`

Flow:

1. Build temp Flux DB locally and upload templates/scripts.
2. On Flux, populate `data_equil` + `transp_timeseries`, create studies/inputs, run input generation loop.
3. Sync Flux DB back and upsert into main DB with:
  - `db_update/Transp_full_auto/sync_flux_equil_inputs_to_main.py`

The selected full-auto origin is carried through this flow by `data_origin.id`.
`data_origin.name` remains only a compatibility fallback when resolving older
staged DBs or scripts.

The GUI tracks Flux full-auto progress through `flux_action_log` with the
minimal state machine:

- `STAGED`
- `SUBMITTED`
- `RUNNING`
- `DONE`
- `FAILED`
- `SYNCED`

`Check Flux Status` is the explicit refresh step that maps remote Slurm state
back into those GUI-oriented statuses. Only one latest tracked Flux job per
`data_origin` is assumed.
4. Post-sync cleanup:
  - mark empty contents as `ERROR`
  - backfill physics columns

### 5.5 Surrogate modeling workflow

1. Train model from joined `gk_input` + `gk_run`:
  - `db_surrogate/train_gamma_surrogate.py`
2. Persist model artifact (`.pkl`) and metadata (`.json`).
3. Record model metadata in `gk_surrogate` (GUI helper in `gui/app.py`).
4. Estimate all complete inputs:
  - `db_surrogate/estimate_gamma_surrogate.py --surrogate-id <id>`
  - Upserts into `sg_estimate(gk_surrogate_id, gk_input_id, sg_estimate, sg_quality)`.

## 6) State and Contract Conventions

- `gk_input.status` drives eligibility and lifecycle:
  - `WAIT -> TORUN -> BATCH -> (SUCCESS|CRASHED|ERROR)`
- `gk_batch.status` tracks remote handoff:
  - `CREATED -> SENT -> LAUNCHED -> SYNCED`
- `gk_run` stores per-run remote metadata and computed metrics (`t_max`, `gamma_max`, `ky_abs_mean`, `diffusion`).

These status strings are operational contracts across CLI, GUI, and remote scripts.

## 7) External Dependencies

- Python deps in `requirements.txt`: Flask, numpy, scikit-learn.
- Additional runtime deps used by workflows:
  - `pyrokinetics` package (external install)
  - `netCDF4` (for TRANSP `TIME3` handling in full-auto flow)
- External services:
  - Flux SSH host
  - Perlmutter SSH + `sbatch`

## 8) Testing Strategy

- Central test runner: `tests/run_tests.py` with report emission.
- Coverage includes:
  - schema creation/idempotence
  - ingestion/population workflows
  - batch status transitions
  - GUI status mutation endpoints
  - surrogate train/estimate flow

## 9) Known Architectural Constraints

- Some full-auto shell scripts still default `DTWIN_ROOT` to `.../Digital_twin` rather than this repo path.
- `gk_surrogate` table creation currently occurs in GUI initialization path instead of base schema bootstrap.
- No explicit schema migration framework; compatibility is managed by guarded `ALTER TABLE` calls.

## 10) Change Guidance

When changing architecture-critical behavior:

1. Update scripts while preserving status contracts.
2. Add or update tests in `tests/`.
3. If schema-related, update `database/create_gyrokinetic_db.py`.
4. Reflect the change in this file and `TASKS.md` if follow-up work remains.
