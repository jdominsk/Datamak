# Datamak

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

## Full Datamak Commands

Create or refresh the main Datamak database:

```bash
.venv/bin/python database/create_gyrokinetic_db.py --db gyrokinetic_simulations.db
```

Start the full Datamak Flask GUI:

```bash
export DTWIN_ROOT="$(pwd)"
.venv/bin/python gui/app.py
```

Run the full Datamak test suite:

```bash
.venv/bin/python tests/run_tests.py
```

## Datamak Lite Core Concepts

Lite stores five generic object types in SQLite:

- `entity`: campaign, simulation, pool, dataset, restart, analysis, figure, or
  paper result.
- `relation`: directed link such as `uses_input`, `restart_from`, `produces`,
  `analyzes`, `plots`, `compares_to`, or `supersedes`.
- `artifact`: concrete file or directory path attached to an entity.
- `metric`: compact scalar metadata or result.
- `note`: Markdown comment, decision, warning, or todo.

Operational status and scientific status are separate.  A run can finish
successfully while still being a candidate, superseded, or suspect result.

## Minimal Commands

Run these from the `Datamak_lite` checkout.

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

A campaign profile centralizes campaign-specific paths outside the generic
Lite package.  See
`datamak_lite/examples/generic_campaign_profile.json` for the expected shape.

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

Datamak Lite keeps the campaign databases inside their project workspaces, but
it can register their locations in a user-level index:

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
paths, upstream/downstream relations, scalar metrics, and notes.  It should not
copy large simulation or analysis data.

The central SQLite database is a queryable campaign view.  If it becomes stale,
it can be rebuilt by running `refresh-campaign` on the campaign profile.

## Tests

```bash
python3 -m unittest discover -s tests -p 'test_datamak_lite_core.py'
python3 -m unittest discover -s tests -p 'test_datamak_hpc_acceptance.py' -v
```
