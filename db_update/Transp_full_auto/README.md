# Transp Full Auto

This is the active full-auto TRANSP workflow that stages a temporary SQLite
database on Flux, performs the heavy CDF and Pyrokinetics work there, and only
syncs selected products back into the main Datamak database.

`db_update/Transp_full_auto_remote/` should be treated as legacy/deprecated.
The workflow documented here is the one to maintain.

## Runtime Layout

Code lives here:

- `db_update/Transp_full_auto/`

Laptop-side staging directory created by the wrappers:

- `${DTWIN_ROOT}/transp_full_auto`

Flux-side working directory resolved from the Flux profile:

- `${DTWIN_FLUX_BASE_DIR}`
- default shape: `/u/<user>/DTwin/transp_full_auto`

Staged temporary database name pattern:

- `flux_equil_inputs_YYYYMMDD_HHMMSS.db`

## What The Workflow Is For

The goal is to avoid pulling large TRANSP/CDF inputs back to the laptop.
Instead:

1. build a reduced temp DB from the main DB on the laptop
2. upload that temp DB and the required scripts/templates to Flux
3. populate `data_equil`, `transp_timeseries`, `gk_study`, and `gk_input` on Flux
4. run Pyrokinetics/GX input generation on Flux
5. sync the useful results back into `gyrokinetic_simulations.db`

## Step 1: Launch On Laptop

Script:

- `MainSteps_1_launch_on_laptop.sh`

Operator rule:

- create a new staged Flux DB for a given origin only if there is not already
  a valid staged DB recorded for that origin in `flux_action_log`
- if a staged DB already exists for that origin, do not rerun step 1 just to
  continue the same workflow; resume with step 2 on Flux and then step 3 on the
  laptop
- only rerun step 1 when you intentionally want to start a fresh staged run

What it does:

1. Resolves the active Flux profile through `tools/resolve_dtwin_env.py`.
2. Uses `build_flux_equil_inputs.py --db ... --out-dir ...` to create a temp DB.
3. Copies only the schema/data needed to start the remote workflow:
   - copied tables: `data_origin`, `gk_code`, `gk_model`
   - created empty workflow tables: `data_equil`, `gk_study`, `gk_input`
   - ensured table: `transp_timeseries`
4. Copies the `.in` templates from `pyrokinetics/` into the local staging area.
5. Copies `MainSteps_2_launch_on_flux.sh`, `build_flux_equil_inputs.py`, and
   the `flux/` helpers into the local staging area.
6. Writes `datamak_runtime.env` so the uploaded Flux folder has the resolved
   per-user runtime settings.
7. Opens an SSH control connection first so Duo/2FA is completed once, then
   reuses that control path for `rsync`.
8. Uploads the whole staging directory to Flux.
9. Appends a row to `flux_action_log` in the main DB so step 3 can find the
   most recent remote DB automatically. New rows start in status `STAGED`.

Important details:

- The staging folder is `${DTWIN_ROOT}/transp_full_auto`, not this script folder.
- The selected origin is now carried through the Flux workflow by
  `data_origin.id`; `ORIGIN_NAME` is retained only as a compatibility fallback.
- The default origin name is `Alexei Transp 09 (full-auto)` unless `ORIGIN_NAME`
  is overridden in the environment.
- `MainSteps_1_launch_on_laptop.sh` by itself still always creates a new temp DB
  and appends a new `flux_action_log` row. The reuse/submit guard is enforced
  by the higher-level helper `run_on_flux.py`, not by this shell script in
  isolation.

Example:

```bash
bash db_update/Transp_full_auto/MainSteps_1_launch_on_laptop.sh
```

Before running step 1 again for the same origin, first check whether the main
DB already has a staged Flux DB recorded for that origin and whether that DB
still exists on Flux. If it does, continue from that DB instead of creating a
new one.

## Step 2: Launch On Flux

Script:

- `MainSteps_2_launch_on_flux.sh /path/to/flux_equil_inputs_TIMESTAMP.db`

This is the heavy step. It is intended to run on Flux and can take a long time.
By default it is a foreground script, not a scheduler wrapper.

What it does:

1. Loads `datamak_runtime.env` if present.
2. Calls `build_flux_equil_inputs.py --populate-equil --create-studies ...`
   against the temp DB on Flux.
3. Calls `build_flux_equil_inputs.py --create-gk-inputs ...` to build
   `gk_input` rows on the desired `psin` grid.
4. Calls `flux/run_flux_gk_inputs.py --loop ...` to generate GX inputs and write
   them into `gk_input.content`.

What the populate stage changes in the Flux DB:

- scans the Flux `REMOTE_PATH` for `*.CDF`
- creates/updates `data_equil`
- creates `transp_timeseries`
- chooses one random `shot_time` when a series does not yet have one selected
- marks the chosen `data_equil` row `active=1`
- creates missing `gk_study` rows for active equilibria and active models

What the GX-input stage does:

- creates `gk_input` rows for active studies, active models, and the requested
  `psin` grid
- processes rows whose status is `NEW` and whose `content` is empty
- writes successful generated inputs back with status `WAIT`
- marks failures as `CRASHED`

Common environment overrides for step 2:

- `ORIGIN_ID`
- `ORIGIN_NAME`
- `REMOTE_PATH`
- `PSIN_START`
- `PSIN_END`
- `PSIN_STEP`
- `BATCH_SIZE`
- `MAX_MEM_GB`

Example:

```bash
bash ~/DTwin/transp_full_auto/MainSteps_2_launch_on_flux.sh \
  ~/DTwin/transp_full_auto/flux_equil_inputs_20260120_164628.db
```

## Step 3: Sync Back On Laptop

Script:

- `MainSteps_3_launch_on_laptop.sh [remote_db_path] [local_db_path]`

What it does:

1. Resolves the active Flux profile through `tools/resolve_dtwin_env.py`.
2. If no remote DB is passed, reads the most recent `flux_action_log` row from
   the main DB and uses that `flux_db_name`, `remote_host`, and `remote_dir`.
3. Opens an SSH control connection so Duo/2FA is completed once, then reuses it
   for `rsync`.
4. Downloads the remote temp DB into `${DTWIN_ROOT}/transp_full_auto/`.
5. Runs `sync_flux_equil_inputs_to_main.py`.
6. Runs `mark_empty_gk_input_error.py`.
7. Runs `backfill_gk_input_physics.py`.
8. Marks the same `flux_action_log` row as `SYNCED`.

What gets synced into the main DB:

- `transp_timeseries`
- `data_equil`
- `gk_study`
- `gk_input` rows with Flux status `WAIT`

Important sync behavior:

- step 3 does not mirror the full Flux DB state back to the main DB
- `CRASHED` rows on Flux are not imported as a complete remote-state mirror
- `gk_input` upserts in the main DB only overwrite rows whose local status is
  `WAIT` or `ERROR`

Example:

```bash
bash db_update/Transp_full_auto/MainSteps_3_launch_on_laptop.sh
```

## Slurm Status

There is a Slurm wrapper in this folder:

- `flux/run_gk_inputs_slurm.sh`
- `flux/run_mainsteps2_slurm.sh`

What it currently wraps:

- only the GX-input generation loop via `flux/run_gk_inputs_local.sh`
- not the full `MainSteps_2_launch_on_flux.sh`

So the current situation is:

- `MainSteps_2_launch_on_flux.sh` itself is foreground by default
- the heavy `gk_input` generation substep can be submitted through Slurm
- `flux/run_mainsteps2_slurm.sh` can submit the full step-2 sequence through Slurm

Example from the wrapper header:

```bash
cd ~/DTwin/transp_full_auto/flux
sbatch --mem=8G --partition=all --time=04:00:00 \
  run_gk_inputs_slurm.sh \
  ~/DTwin/transp_full_auto/flux_equil_inputs_20260120_164628.db \
  4000 \
  100
```

That means:

- process at most `4000` rows this submission
- in chunks of `100`
- reading and updating the Flux temp DB in place

## How To Check Progress On Flux

List the staged DBs:

```bash
ls -1t ~/DTwin/transp_full_auto/flux_equil_inputs_*.db | head
```

Inspect the latest Slurm output:

```bash
tail -n 40 ~/DTwin/transp_full_auto/slurm-*.out
```

Check the main workflow counts inside a Flux temp DB:

```bash
sqlite3 ~/DTwin/transp_full_auto/flux_equil_inputs_YYYYMMDD_HHMMSS.db "
select count(*) as data_equil from data_equil;
select count(*) as transp_timeseries from transp_timeseries;
select count(*) as gk_study from gk_study;
select count(*) as gk_input from gk_input;
select status, count(*) from gk_input group by status order by 1;
"
```

## GUI Actions

The Equilibria panel now exposes the full-auto Flux workflow through two
origin-aware actions:

- `Run On Flux`
  - script: `run_on_flux.py`
  - behavior: reuses the latest staged Flux DB for the selected origin if one
    is already recorded in `flux_action_log`; otherwise it runs step 1 once to
    create and log a new staged DB
  - after successful `sbatch`, updates that same `flux_action_log` row to
    `status='SUBMITTED'`, stores the Slurm job token in `slurm_job_id`, and
    records `submitted_at`
  - if the latest row for that origin is already `SUBMITTED`, it refuses to
    submit a second parallel job
  - then refreshes the remote helper scripts/templates and submits step 2 on
    Flux through `flux/run_mainsteps2_slurm.sh`

- `Sync Back From Flux`
  - script: `sync_back_from_flux.py`
  - behavior: finds the latest `flux_action_log` row for the selected origin
    and runs step 3 against that exact remote DB
  - after successful sync, updates that row to `status='SYNCED'` and records
    `synced_at`

## `flux_action_log` Status Semantics

The main Datamak DB tracks one workflow row per staged Flux DB. The important
fields are:

- `status`
- `slurm_job_id`
- `status_detail`
- `submitted_at`
- `status_checked_at`
- `synced_at`

The intended state machine is:

- `STAGED`
  - step 1 has created/uploaded a staged Flux DB
  - no GUI-tracked remote submission is currently in flight
  - `Run On Flux` is allowed

- `SUBMITTED`
  - `Run On Flux` has submitted step 2 to Slurm and recorded the returned job
    token
  - the row has a known Slurm job id, but the GUI has not yet confirmed whether
    the job is actually running or already finished
  - do not submit a second parallel job against the same staged DB
  - the next GUI actions should be `Check Flux Status` and/or `Sync Back From Flux`

- `RUNNING`
  - `Check Flux Status` has confirmed that the saved Slurm job is still active
  - `status_detail` stores the last observed Slurm state, for example
    `RUNNING|00:12:34|flux-node01`
  - do not submit a second parallel job against the same staged DB

- `DONE`
  - `Check Flux Status` has confirmed that the saved Slurm job completed
  - the staged Flux DB may now be synchronized back, or the run may be launched
    again later to continue incrementally

- `FAILED`
  - `Check Flux Status` has confirmed a terminal non-success Slurm state such as
    `FAILED`, `TIMEOUT`, or `CANCELLED`
  - the staged Flux DB is still kept; the operator can inspect it, sync it
    back, or relaunch step 2 against the same DB

- `SYNCED`
  - step 3 has synchronized results from that staged DB back into the main DB
  - this clears the submit lock
  - another `Run On Flux` may later reuse the same staged DB if more remote
    work is still needed

`Check Flux Status` is a small GUI helper that reuses the saved `slurm_job_id`
and current Flux profile, prompts for Duo if needed, and updates the latest row
for that origin to one of:

- `SUBMITTED`
- `RUNNING`
- `DONE`
- `FAILED`
- `SYNCED`

For SSH access, the current Flux profile in the Datamak settings is treated as
the authoritative login endpoint. Older `flux_action_log.remote_host` values
such as plain `flux` are refreshed to the configured host when `run_on_flux.py`
or `sync_back_from_flux.py` reuses that row.

The Flux profile may also store an optional `duo_option` (`1`, `2`, or `3`).
When set, laptop-side SSH actions use a small PTY wrapper that waits for the
standard Duo prompt and auto-sends that option. This only selects the prompt
option, for example `1` for Duo Push; the user still approves the login through
the Duo system itself.

## Known Caveats

- `MainSteps_1_launch_on_laptop.sh` uploads step 2 to Flux, but it does not
  automatically start step 2 there.
- The reuse/duplicate-submit guard is enforced by `run_on_flux.py`, not by
  `MainSteps_1_launch_on_laptop.sh` itself.
- `MainSteps_2_launch_on_flux.sh` can be long-running because it both scans CDFs
  and generates GX inputs.
- The Flux temp DB is updated in place. Re-running the GX-input step continues
  from remaining `NEW` rows rather than starting from scratch.
- The current selection of `shot_time` for a TRANSP series is random when a new
  time series is first populated.

## Recorded Investigation: March 27, 2026

When the Flux workspace for `jdominsk` was inspected on March 27, 2026, the
active working directory existed at:

- `~/DTwin/transp_full_auto`

It contained a staged DB:

- `flux_equil_inputs_20260120_164628.db`

with size about `208 MB`.

It also contained multiple `slurm-*.out` files. The tail of
`slurm-1945499.out` showed that the job was still working through remaining
`NEW` rows and then stopped after generating only `100` rows for that batch
submission, with thousands still pending. That means the Flux DB existed and
had been updated by batch jobs, but the run was only partial, not a full drain
of the queued `gk_input` work.
