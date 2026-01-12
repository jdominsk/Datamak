# Batch Workflow

This folder contains scripts to build batch databases, send them to Perlmutter,
and run GX jobs from the queue.

## 1) Create a batch database

Create a new batch DB in `batch/` and copy TORUN rows from `gk_input`:

```
python3 batch/create_batch_database.py --copy-torun
```

Notes:
- The default source DB is `gyrokinetic_simulations.db`.
- The batch DB name is timestamped: `batch/batch_database_YYYYMMDD_HHMMSS.db`.

## 2) Send batch databases to Perlmutter

From inside `batch/`:

```
python3 send_batch.py
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

