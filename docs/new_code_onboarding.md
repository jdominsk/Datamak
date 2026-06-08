# Datamak Lite Guide For A New Simulation Code

## What Datamak Lite Is

Datamak Lite is a lightweight provenance and monitoring layer for scientific
simulation campaigns.  It is not tied to any specific code, facility, machine,
or research domain.

It tracks:

- simulations and pools;
- restart chains;
- history or input datasets;
- postprocessing analyses;
- generated figures;
- notes and decisions;
- operational status and scientific status.

The core idea is simple: every important object leaves a small metadata packet
next to it, and these packets can be imported into a central SQLite campaign
registry.

```text
run_or_pool_or_analysis_root/
  datamak_lite.json
```

## First Integration Goal

For a new code, do not start by writing a full GUI or scheduler.  Start by
making one real run visible in Datamak Lite.

The first milestone is:

```text
one run or pool -> one datamak_lite.json -> imported SQLite DB -> readable report
```

If the code or workflow generates figures, there is a parallel requirement:

```text
one generated figure -> one audit JSON or Lite sidecar -> imported SQLite DB
```

See `docs/figure_metadata_policy.md`.  New figures should not be left as only
PNG/PDF files.

## Minimal Vocabulary

Use the generic Datamak Lite concepts.

Entity:
An object worth tracking, such as a campaign, simulation, pool, dataset,
restart file, analysis, figure, or paper result.

Relation:
A directed link between entities, such as `restart_from`, `uses_input`,
`uses_history`, `produces`, `analyzes`, `plots`, `compares_to`, or
`supersedes`.

Artifact:
A concrete file or directory associated with an entity, such as an input file,
output file, log, database, manifest, summary CSV, or figure.

Metric:
A compact scalar value, such as runtime, time step, resolution, final time,
number of particles, error norm, or a fitted coefficient.

Note:
A Markdown comment explaining a decision, warning, todo, or interpretation.

## What To Put In The First Sidecar

Every first sidecar should include:

- one root entity describing the run or pool;
- path to the run or pool root;
- operational status, for example `prepared`, `running`, `success`,
  `crashed`, or `unknown`;
- scientific status, usually `candidate` at first;
- artifacts for the input, manifest, output summary, status database, and logs;
- relations to known upstream objects;
- a note explaining why the run exists.

Example:

```json
{
  "schema_version": 1,
  "root_uid": "pool_mycode_scan_001",
  "entities": [
    {
      "uid": "pool_mycode_scan_001",
      "type": "pool",
      "name": "MyCode parameter scan 001",
      "path": "/scratch/project/mycode/scan_001",
      "status": "prepared",
      "scientific_status": "candidate",
      "description": "First Datamak Lite tracked pool for MyCode.",
      "metadata": {
        "code": "MyCode",
        "case_family": "validation"
      }
    }
  ],
  "relations": [],
  "artifacts": [
    {
      "entity_uid": "pool_mycode_scan_001",
      "kind": "run_root",
      "path": "/scratch/project/mycode/scan_001",
      "description": "Pool root."
    },
    {
      "entity_uid": "pool_mycode_scan_001",
      "kind": "manifest",
      "path": "manifest.json",
      "format": "json",
      "description": "Run manifest."
    }
  ],
  "metrics": [
    {
      "entity_uid": "pool_mycode_scan_001",
      "name": "planned_cases",
      "value": 12,
      "unit": "case"
    }
  ],
  "notes": [
    {
      "entity_uid": "pool_mycode_scan_001",
      "note_type": "comment",
      "markdown_text": "Initial MyCode integration test."
    }
  ]
}
```

Relative artifact paths are resolved relative to the packet location during
import.  Absolute paths and remote-looking paths are preserved.

## Commands

From the `Datamak_lite` checkout:

```bash
python3 -m datamak_lite.cli import-packet CAMPAIGN.sqlite \
  /path/to/run/datamak_lite.json
```

For a remote sidecar:

```bash
python3 -m datamak_lite.cli sync-packet CAMPAIGN.sqlite \
  user@host:/remote/path/to/run/datamak_lite.json \
  --report
```

Render a report:

```bash
python3 -m datamak_lite.cli report CAMPAIGN.sqlite --entity ROOT_UID
```

## How To Design A New Code Adapter

After the first hand-written sidecar works, create a small adapter command for
the code.

Recommended command shape:

```bash
python3 -m datamak_lite.cli create-MYCODE-packet RUN_OR_POOL_ROOT \
  --campaign-uid CAMPAIGN_UID \
  --note "Why this run exists."
```

The adapter should:

1. inspect the run or pool root;
2. read a manifest or status database if present;
3. detect common artifacts;
4. infer safe scalar metadata;
5. write `datamak_lite.json`;
6. optionally import it into a local DB.

Keep the adapter conservative.  It should not decide scientific reference
status unless the user has made that decision explicit.

## Recommended Adapter Inputs

A code-specific adapter should accept:

- `RUN_OR_POOL_ROOT`;
- `--uid` for a stable entity id override;
- `--name` for a human-readable name;
- `--campaign-uid`;
- relation arguments, for example `--uses-dataset-uid`, `--uses-input-uid`, or
  `--restart-from-uid`;
- `--status`;
- `--scientific-status`;
- `--note`;
- `--import-db`;
- `--report`;
- `--dry-run`.

## Recommended Artifact Detection

Look for small files first:

- input files;
- manifest JSON/YAML/TOML;
- pool SQLite database;
- scheduler status database;
- short logs;
- summary CSV/JSON;
- analysis scripts;
- generated figures.

For generated figures, also look for or create figure metadata:

- output PNG/PDF/SVG paths;
- plotting script path;
- input data summaries;
- source simulation/pool/history UIDs when known;
- notes and warnings about the plotted diagnostic.

Do not ingest or copy large raw simulation data.  Store paths to large files as
artifacts, or store paths to compact summaries derived from them.

## Status Policy

Separate operational status from scientific status.

Operational status answers: did the work run?

Examples:

- `prepared`
- `pending`
- `running`
- `success`
- `partial`
- `crashed`
- `interrupted`
- `unknown`

Scientific status answers: should we trust/use it?

Examples:

- `candidate`
- `reference`
- `superseded`
- `suspect`
- `paper-used`

Default to `scientific_status: candidate`.

## Agent Workflow For A New Code

When an agent supports a new code, it should follow this sequence:

1. Identify the run root and the smallest useful metadata.
2. Create one sidecar manually or with a tiny script.
3. Import it and render a report.
4. Ask whether the report answers the user's tracking question.
5. Only then implement the code-specific packet creator.
6. Add a short test using a fake run root.
7. Update this guide or add a code-specific guide if the workflow is reusable.

## GUI Readiness

The GUI should not need to know the code.  It should read entities, relations,
artifacts, metrics, and notes from the SQLite registry.

For a new code, the adapter's job is to populate the generic model.  The GUI's
job is to display the graph, status, metrics, and comments.

## User Quickstart Draft

This section can later become the top-level quickstart.

1. Put a small `datamak_lite.json` next to your simulation, pool, analysis, or
   figure.
2. Import it:

   ```bash
   python3 -m datamak_lite.cli import-packet campaign.sqlite path/to/datamak_lite.json
   ```

3. Render a report:

   ```bash
   python3 -m datamak_lite.cli report campaign.sqlite --entity ROOT_UID
   ```

4. Repeat for related runs and analyses.
5. Add relations so Datamak Lite can answer where a result came from.

The first useful version does not require a GUI, scheduler integration, or large
data ingestion.
