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

## 2) Send batch databases to Perlmutter

From inside `batch/` (or anywhere; it defaults to `batch/new`):

```
python3 send_batch_database.py
```

What it does:
- Copies non-empty `batch_database_*.db` to
  `jdominsk@perlmutter.nersc.gov:/pscratch/sd/j/jdominsk/DTwin/newbatch`.
- Also copies `hpc/job_submit.sh` and `hpc/job_execute.sh`.
- Logs the transfer in `gk_batch` (in `gyrokinetic_simulations.db`).
- Moves sent DBs to `./sent`.
- Renames empty DBs with the prefix `empty_`.

## 3) Submit jobs on Perlmutter

On Perlmutter, in the folder with the batch database:

```
sbatch job_submit.sh
```

Options:
- Pass a specific DB path:
  `sbatch job_submit.sh /path/to/batch_database_*.db`
- Node count is taken from Slurm (`SLURM_JOB_NUM_NODES`) unless you pass a
  second argument.

## 4) What job_execute.sh does

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
  |  -> batch/batch_database_TIMESTAMP.db (gk_run)
  |  -> gk_input.status = BATCH
  |  -> gk_batch.status = CREATED
  v
send_batch_database.py
  |  -> sends DB + scripts to NERSC newbatch
  |  -> gk_batch.status = SENT
  v
prepare_newbatch.sh (NERSC)
  |  -> ../runXXXX/ + copy DB + scripts
  v
deploy_batch.py
  |  -> sbatch runXXXX/job_submit.sh
  |  -> gk_batch.status = LAUNCHED
  v
job_execute.sh (NERSC)
  |  -> gk_run.status = RUNNING -> SUCCESS/CRASHED
  |  -> gk_run.job_id set per run
```
