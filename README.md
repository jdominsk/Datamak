# Datamak

Datamak is a Python/SQLite workflow system for fusion-plasma gyrokinetic studies.
It stores source equilibria, generated gyrokinetic inputs, remote batch-execution
state, simulation results, and surrogate-model outputs in one database-backed
workflow.

The repository has two main operator surfaces:

- CLI scripts for ingestion, batch creation, remote execution, syncing, and surrogate workflows
- A Flask GUI for browsing the database and launching common workflow actions

## Start Here

If you are a new developer or an AI agent starting work in this repository,
use this `README.md` as the main bootstrap document, then inspect:

- `database/create_gyrokinetic_db.py`
  Database bootstrap and schema evolution entry point.
- `gui/app.py`
  Flask entry point for the operator GUI.
- `tests/run_tests.py`
  Main test runner.

## Repository Layout

- `database/`
  SQLite schema bootstrap and seed SQL.
- `db_update/`
  Source-ingestion and GK-input creation workflows.
- `db_update/Transp_full_auto/`
  Flux-based full-auto TRANSP workflow.
- `batch/`
  Batch DB creation, remote deployment, sync, and monitoring utilities.
- `db_surrogate/`
  Surrogate-model training, prediction, estimation, and cleanup tools.
- `gui/`
  Flask application, templates, and GUI actions.
- `tests/`
  Unit and workflow tests plus the test runner.

## Prerequisites

- Python 3
- A local virtual environment for this repo
- Python packages from `requirements.txt`
- `pyrokinetics` installed separately if you need GK-input generation workflows
- Access to Flux and/or Perlmutter only if you need remote workflow execution

The base Python requirements are:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

`pyrokinetics` is not installed by `requirements.txt`. Install it from your own
checkout or shared environment if you plan to run input-generation workflows.

## Environment Setup

Datamak expects `DTWIN_ROOT` to point at the repository root. From the repo root:

```bash
export DTWIN_ROOT="$(pwd)"
```

You can also print the command with:

```bash
bash set_dtwin_root.sh
```

To persist it in your shell profile:

```bash
echo 'export DTWIN_ROOT="/absolute/path/to/Datamak"' >> ~/.zshrc
```

## Create or Refresh the Main Database

The main database file is `gyrokinetic_simulations.db` in the repo root.
To create a new DB or apply additive schema updates:

```bash
.venv/bin/python database/create_gyrokinetic_db.py --db gyrokinetic_simulations.db
```

If the file does not exist, this creates it. If it already exists, the script
applies the repository's guarded schema/bootstrap logic.

## Start the Flask GUI

From the repository root:

```bash
export DTWIN_ROOT="$(pwd)"
.venv/bin/python gui/app.py
```

The Flask app runs locally on the default Flask development server. Open:

```text
http://127.0.0.1:5000/
```

Keep that terminal open while using the GUI.

## First GUI Steps

1. Start the server with `.venv/bin/python gui/app.py`.
2. Open `http://127.0.0.1:5000/` in your browser.
3. If you are starting from scratch, make sure `gyrokinetic_simulations.db` exists.
4. Use the GUI's workflow and database panels to inspect rows and run supported actions.
5. Enter per-user Perlmutter and Flux settings in the GUI only if you need remote workflows.

If the page does not load, check the terminal running Flask first.

## Per-User Runtime Configuration

Datamak stores per-user workflow configuration outside the main database.
By default the config path is:

```text
~/.config/datamak/config.json
```

This path can be overridden with:

- `DTWIN_CONFIG`
- `XDG_CONFIG_HOME`

The GUI writes Perlmutter and Flux runtime settings there. Source-location
metadata should not be treated as GUI-local settings; source definitions belong
in workflow/database state.

## Common Commands

Create or refresh schema:

```bash
.venv/bin/python database/create_gyrokinetic_db.py --db gyrokinetic_simulations.db
```

Start the GUI:

```bash
export DTWIN_ROOT="$(pwd)"
.venv/bin/python gui/app.py
```

Run the test suite:

```bash
.venv/bin/python tests/run_tests.py
```

Run unittest discovery directly:

```bash
.venv/bin/python -m unittest discover -s tests -p "test_*.py" -v
```

## Remote Workflow Notes

Remote execution is not required to start the project locally, but it is required
for the full batch/HPC workflow.

- Perlmutter is used for batch job launch, execution, and monitoring.
- Flux is used by the full-auto TRANSP workflow.
- Those flows depend on site access, SSH, and external runtime tools that are not
  fully provisioned by this repository alone.

Start with the local Flask server and local database first. Add HPC configuration
only when the local workflow is working.

## Workflow Summary

The main local workflow is:

1. Create a Python virtual environment and install `requirements.txt`.
2. Export `DTWIN_ROOT` to the repository root.
3. Create or refresh `gyrokinetic_simulations.db`.
4. Start the Flask GUI with `.venv/bin/python gui/app.py`.
5. Open `http://127.0.0.1:5000/` in a browser.
6. Configure Perlmutter or Flux settings in the GUI only if remote workflows are needed.

The broader project supports:

- source-equilibrium ingestion into the SQLite database
- gyrokinetic-input generation
- batch export and remote execution
- sync-back of run results
- surrogate-model training and estimation

## Demo and Supporting Material

- `datamak_quickstart.pdf`
  Operator-facing quickstart material.
- `demo/`
  Demo database helpers and lightweight demo DB creation scripts.

## Notes for AI Agents

- Preserve status contracts across CLI and GUI paths.
- Prefer reusable workflow helpers over embedding workflow logic directly into Flask routes.
- For schema changes, update `database/create_gyrokinetic_db.py` and the relevant tests.
- For status or workflow changes, validate with targeted tests before finishing.
