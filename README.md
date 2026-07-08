# Datamak

## Citation

If you use Datamak, please cite:

Dominski, J., Churchill, R. M., Villar, A. S., & Parra Diaz, F. (2026, March 27).
*DATAMAK* [Computer software].
https://doi.org/10.11578/dc.20260327.2

Datamak is a Python/SQLite workflow system for fusion-plasma gyrokinetic
studies. It stores source equilibria, generated gyrokinetic inputs, remote
batch-execution state, simulation results, and surrogate-model outputs in one
database-backed workflow.

The repository now contains two related layers:

- **Datamak**: the full workflow system, with CLI scripts and a Flask GUI for
  source ingestion, input generation, batch creation, remote execution, result
  syncing, and surrogate workflows.
- **Datamak Lite**: a lightweight campaign-monitoring layer for simulation and
  analysis work. It tracks how runs, pools, datasets, analyses, figures, and
  notes are connected without owning the full execution workflow.

Datamak Lite is deliberately code-agnostic. Project-specific names, physics
context, machine names, and campaign identifiers belong in sidecar metadata or
SQLite databases, not in the generic Lite code or documentation.

## Start Here

For the full Datamak workflow, start with:

- `database/create_gyrokinetic_db.py`: database bootstrap and schema evolution.
- `gui/app.py`: Flask entry point for the operator GUI.
- `batch/`: batch DB creation, remote deployment, sync, and monitoring
  utilities.
- `db_update/`: source-ingestion and GK-input creation workflows.
- `db_surrogate/`: surrogate-model training, prediction, estimation, and
  cleanup tools.
- `tests/run_tests.py`: main full-Datamak test runner.

For Datamak Lite, start with:

- `design/design_plan.md`: product direction and data model.
- `design/sidecar_packet_v1.md`: `datamak_lite.json` sidecar format.
- `agents/workflow.md`: agent-facing development and maintenance workflow.
- `agents/maintenance_protocol.md`: how humans and agents keep Lite metadata
  updated during daily work.
- `docs/lite_overview.md`: overview and relationship to external campaign
  data-management tools.
- `docs/figure_metadata_policy.md`: requirement that generated figures write
  importable metadata.
- `docs/new_code_onboarding.md`: how to integrate another simulation or
  analysis code.
- `presentations/lite_overview/presentation.tex`: Lite overview slide deck.

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

Datamak expects `DATAMAK_ROOT` to point at the repository root. From the repo
root:

```bash
export DATAMAK_ROOT="$(pwd)"
```

You can also print the command with:

```bash
bash set_datamak_root.sh
```

To persist it in your shell profile:

```bash
echo 'export DATAMAK_ROOT="/absolute/path/to/Datamak"' >> ~/.zshrc
```

`DTWIN_ROOT` is still accepted as a compatibility fallback, but new setups should
use `DATAMAK_ROOT`.

## Full Datamak Commands

Create or refresh the main Datamak database:

```bash
.venv/bin/python database/create_gyrokinetic_db.py --db gyrokinetic_simulations.db
```

Start the full Datamak Flask GUI:

```bash
export DATAMAK_ROOT="$(pwd)"
.venv/bin/python gui/app.py
```

Open:

```text
http://127.0.0.1:5000/
```

Run the full Datamak test suite:

```bash
.venv/bin/python tests/run_tests.py
```

## First GUI Steps

1. Start the server with `.venv/bin/python gui/app.py`.
2. Open `http://127.0.0.1:5000/` in your browser.
3. If you are starting from scratch, make sure `gyrokinetic_simulations.db`
   exists.
4. Use the GUI workflow and database panels to inspect rows and run supported
   actions.
5. Enter per-user Perlmutter and Flux settings in the GUI only if you need
   remote workflows.

If the page does not load, check the terminal running Flask first.

## Per-User Runtime Configuration

Datamak stores per-user workflow configuration outside the main database. By
default the config path is:

```text
~/.config/datamak/config.json
```

This path can be overridden with:

- `DTWIN_CONFIG`
- `XDG_CONFIG_HOME`

The GUI writes Perlmutter and Flux runtime settings there. Source-location
metadata should not be treated as GUI-local settings; source definitions belong
in workflow/database state.

## Datamak Lite Core Concepts

Lite stores five generic object types in SQLite:

- `entity`: campaign, simulation, pool, dataset, restart, analysis, figure, or
  paper result.
- `relation`: directed link such as `uses_input`, `restart_from`, `produces`,
  `analyzes`, `plots`, `compares_to`, or `supersedes`.
- `artifact`: concrete file or directory path attached to an entity.
- `metric`: compact scalar metadata or result.
- `note`: Markdown comment, decision, warning, or todo.

Operational status and scientific status are separate. A run can finish
successfully while still being a candidate, superseded, or suspect result.

## Datamak Lite Commands

Run these from this checkout:

```bash
python3 -m datamak_lite.cli init campaign.sqlite
python3 -m datamak_lite.cli seed-demo campaign.sqlite
python3 -m datamak_lite.cli report campaign.sqlite --entity figure_demo_transport_summary
python3 -m datamak_lite.cli serve campaign.sqlite
```

Refresh a full campaign view from one profile:

```bash
python3 -m datamak_lite.cli refresh-campaign path/to/campaign_profile.json
python3 -m datamak_lite.cli refresh-campaign path/to/campaign_profile.json --dry-run
```

A campaign profile centralizes campaign-specific paths outside the generic Lite
package. See `datamak_lite/examples/generic_campaign_profile.json` for the
expected shape.

Import a sidecar packet:

```bash
python3 -m datamak_lite.cli validate-packet path/to/datamak_lite.json
python3 -m datamak_lite.cli import-packet campaign.sqlite path/to/datamak_lite.json
```

Create a generic pool sidecar:

```bash
python3 -m datamak_lite.cli create-pool-marker POOL_ROOT
python3 -m datamak_lite.cli create-pool-packet POOL_ROOT \
  --campaign-uid campaign_example \
  --uses-dataset-uid dataset_example_input \
  --dataset-path /path/to/input_dataset \
  --note "Why this pool was prepared."
```

`create-pool-marker` writes `README.md` and `datamak_pool.json` so future
humans and agents can recognize the directory as a Datamak-style pool.

Run the machine-level acceptance checks before trusting Datamak-style worker
pools on a new HPC system or after changing the scheduler/runtime wrapper:

```bash
python3 tools/datamak_hpc_acceptance.py run \
  --machine MACHINE_NAME \
  --scheduler pbs \
  --datamak-root "$(pwd)" \
  --require-allocation
```

The report is stored under `~/.datamak/hpc_acceptance/MACHINE_NAME/`, with a
machine-specific profile at `~/.datamak/machine_profile_MACHINE_NAME.json`.

Import an existing campaign registry or folder inventory:

```bash
python3 -m datamak_lite.cli import-campaign-registry campaign.sqlite path/to/campaign_registry.db
python3 -m datamak_lite.cli import-folder-inventory campaign.sqlite path/to/folder_inventory.txt
```

Import figure audit metadata:

```bash
python3 -m datamak_lite.cli import-figure-audits campaign.sqlite path/to/figure_directory
```

## User-Level Campaign Index

Datamak Lite keeps campaign databases inside their project workspaces, but it
can register their locations in a user-level index:

```text
~/.datamak/campaigns.json
```

Set `DATAMAK_HOME` or pass `--config-dir` to use a different config directory.
Register a campaign profile explicitly:

```bash
python3 -m datamak_lite.cli register-campaign path/to/campaign_profile.json --set-default
python3 -m datamak_lite.cli list-campaigns
python3 -m datamak_lite.cli resolve-campaign campaign_west57929 --field database
```

`refresh-campaign` updates this index automatically after a successful
non-dry-run refresh. Use `--no-register` to suppress that side effect.

## Metadata Policy

Every important run, pool, analysis, and generated figure should leave a small
metadata trace:

```text
object_root/
  datamak_lite.json
```

The sidecar should contain compact metadata only: stable entity IDs, important
paths, upstream/downstream relations, scalar metrics, and notes. It should not
copy large simulation or analysis data.

The central SQLite database is a queryable campaign view. If it becomes stale,
it can be rebuilt by running `refresh-campaign` on the campaign profile.

## Remote Workflow Notes

Remote execution is not required to start the project locally, but it is
required for the full batch/HPC workflow.

- Perlmutter is used for batch job launch, execution, and monitoring.
- Flux is used by the full-auto TRANSP workflow.
- Those flows depend on site access, SSH, and external runtime tools that are
  not fully provisioned by this repository alone.

Start with the local Flask server and local database first. Add HPC
configuration only when the local workflow is working.

## Tests

```bash
.venv/bin/python tests/run_tests.py
python3 -m unittest discover -s tests -p 'test_datamak_lite_core.py'
python3 -m unittest discover -s tests -p 'test_datamak_hpc_acceptance.py' -v
```
