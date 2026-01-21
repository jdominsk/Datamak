# Batch Workflow

This folder contains scripts to build batch databases, send them to Perlmutter,
and run GX jobs from the queue.

Environment:
- Set `DTWIN_ROOT` to the repo root if you run scripts from elsewhere.

## 1) Create a batch database

Create a new batch DB in `batch/new/` and copy TORUN rows from `gk_input`:

```
python3 batch/create_batch_database.py --copy-torun
```

Notes:
- The default source DB is `gyrokinetic_simulations.db`.
- The batch DB name is timestamped: `batch/new/batch_database_YYYYMMDD_HHMMSS.db`.

## 2) Send + deploy in one step

From inside `batch/` (or anywhere; it defaults to `batch/new`):

```
python3 deploy_batch_large.py
```

What it does:
- Filters `gk_batch` rows with status `CREATED` and matches them to files in `batch/new`.
- Renames empty DBs with the prefix `empty_`.
- Sends non-empty DBs + `hpc/*.sh` to NERSC in a single SSH session.
- Runs `prepare_newbatch_large.sh` and submits the jobs.
- Updates `gk_batch` to `SENT` and `LAUNCHED`.
- Moves sent DBs to `batch/sent`.

## 3) What job_execute.sh does

`job_execute.sh`:
- Reads the next TORUN row from `gk_run`.
- Writes `input_content` to `input_id{ID}_gkinputid{gk_input_id}.in`.
- Updates status to RUNNING, then to SUCCESS or CRASHED.
- Continues until no TORUN rows remain.

## Workflow Diagram

```
gk_input (gyrokinetic_simulations.db)
  |  status=TORUN
  v
create_batch_database.py --copy-torun
  |  -> batch/new/batch_database_TIMESTAMP.db (gk_run)
  |  -> gk_input.status = BATCH
  |  -> gk_batch.status = CREATED
  v
deploy_batch_large.py
  |  -> sends DBs from batch/new + hpc scripts to NERSC newbatch
  |  -> gk_batch.status = SENT
  v
prepare_newbatch_large.sh (NERSC, newbatch/new)
  |  -> ../runXXXX/ + copy DB + scripts
  v
deploy_batch_large.py (remote)
  |  -> sbatch runXXXX/job_submit_large.sh
  |  -> gk_batch.status = LAUNCHED
  v
job_execute.sh (NERSC)
  |  -> gk_run.status = RUNNING -> SUCCESS/CRASHED
  |  -> gk_run.job_id set per run
```
