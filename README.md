# Datamak Lite

Datamak Lite is a lightweight campaign-monitoring layer for simulation and
analysis work.  It tracks how runs, pools, datasets, analyses, figures, and
notes are connected without owning the full execution workflow.

The tool is deliberately code-agnostic.  Project-specific names, physics
context, machine names, and campaign identifiers belong in sidecar metadata or
SQLite databases, not in the generic Lite code or documentation.

## Start Here

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
- `presentations/lite_overview/presentation.tex`: overview slide deck.

## Core Concepts

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

Import an existing campaign registry or folder inventory:

```bash
python3 -m datamak_lite.cli import-campaign-registry campaign.sqlite path/to/campaign_registry.db
python3 -m datamak_lite.cli import-folder-inventory campaign.sqlite path/to/folder_inventory.txt
```

Import figure audit metadata:

```bash
python3 -m datamak_lite.cli import-figure-audits campaign.sqlite path/to/figure_directory
```

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
```
