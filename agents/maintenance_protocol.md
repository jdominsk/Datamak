# Datamak Lite Maintenance Protocol

## Purpose

Datamak Lite should stay updated as part of normal work.  The registry is
useful only if an agent or a human can reconstruct what was prepared, launched,
completed, analyzed, superseded, or used in a figure.

The operating rule is:

```text
Every important simulation, pool, analysis, figure, and decision leaves a small
metadata trace that can be imported into Datamak Lite.
```

This document is the first agent-readable operating contract.  It should remain
short, explicit, and executable.

## Generic Tool Boundary

The Lite tool, tests, and generic documentation must not hard-code a specific
campaign, code, machine, path, or research context.  Those details belong in
sidecar packets, imported registries, and user-maintained campaign databases.

Adapters may know how to read a particular external registry format, but they
must translate it into the generic Lite vocabulary.

## Source Of Truth

Use three layers:

1. A sidecar packet next to the object:

   ```text
   run_or_pool_or_analysis_root/datamak_lite.json
   ```

2. A campaign profile that centralizes campaign-specific source locations:

   ```text
   path/to/campaign_profile.json
   ```

3. A central SQLite campaign registry:

   ```text
   path/to/campaign.sqlite
   ```

The sidecar is the durable provenance receipt.  The profile says where Lite
should look for packets, registries, inventories, and figure audits.  The
SQLite database is the queryable campaign view.  If they disagree, prefer the
sidecar plus the actual run directory, then re-import through the profile.

## Agent Responsibilities

When an agent prepares a new object, it should also prepare metadata.

Objects include:

- a simulation input;
- a restart stage;
- a pool of simulations;
- a postprocessing analysis;
- a generated figure;
- a slide or paper figure insertion;
- a manual decision that changes which result is considered reference.

For each object, the agent should:

1. create or update `datamak_lite.json` next to the object;
2. include compact provenance only, not large data;
3. record upstream relations such as `restart_from`, `uses_input`,
   `uses_history`, `produces`, `plots`, `compares_to`, or `supersedes`;
4. record operational status and scientific status separately;
5. attach a short note when the decision is not obvious;
6. tell the user the sidecar path and whether it was imported.

For generated figures, there is an additional required policy:

```text
Every new paper/logbook/presentation/diagnostic figure must write metadata when
the figure is generated.
```

See `docs/figure_metadata_policy.md`.  The current accepted bridge is an audit
JSON next to the figure containing output paths, plotting script, input data
paths, source entity UIDs when known, and any warnings.  Do not leave a new
figure as only a PNG/PDF.

## Sidecar Creation

For a generic pool, use:

```bash
python3 -m datamak_lite.cli create-pool-marker POOL_ROOT
python3 -m datamak_lite.cli create-pool-packet POOL_ROOT \
  --campaign-uid CAMPAIGN_UID \
  --uses-dataset-uid DATASET_UID \
  --dataset-path /path/to/input_or_history_dataset \
  --note "Why this pool was prepared."
```

`create-pool-marker` writes or preserves `README.md` and `datamak_pool.json`.
These files identify the directory as a Datamak-style pool and tell future
agents to use the `datamak` skill before modifying it.

If the packet should immediately update the local registry, add:

```bash
  --import-db /path/to/campaign.sqlite \
  --report
```

If the object type needs metadata the generic creator cannot infer, write a
sidecar directly or add a conservative adapter command.  The adapter should
produce the same generic packet shape.

## Import And Sync

For the normal centralized workflow, use:

```bash
python3 -m datamak_lite.cli refresh-campaign /path/to/campaign_profile.json
```

Use `--dry-run` before changing the DB when you need to check what the profile
will touch:

```bash
python3 -m datamak_lite.cli refresh-campaign /path/to/campaign_profile.json --dry-run
```

If the user explicitly asks for a direct local registry update, or if the
workflow already includes import, use:

```bash
python3 -m datamak_lite.cli validate-packet /local/path/datamak_lite.json
python3 -m datamak_lite.cli import-packet CAMPAIGN.sqlite \
  /local/path/datamak_lite.json
```

For a remote packet:

```bash
python3 -m datamak_lite.cli sync-packet CAMPAIGN.sqlite \
  user@host:/remote/path/datamak_lite.json \
  --report
```

`sync-packet` copies only the small sidecar JSON into the local `packets/`
cache next to the SQLite database, validates it, then imports it.  Do not copy
large simulation or analysis outputs merely to update the registry.

## Manual Responsibilities

Manual edits are allowed and expected during the early design stage.

When updating by hand:

1. edit the nearest `datamak_lite.json`;
2. keep identifiers stable;
3. add a `note` instead of overwriting reasoning;
4. run `import-packet` or `sync-packet`;
5. inspect the report for the root entity.

Manual updates should use the same packet format as agent updates.  Avoid a
separate human-only registry path.

## Required Fields For Daily Use

Every sidecar should include at least:

- one root entity with `uid`, `type`, `name`, `path`, `status`,
  `scientific_status`;
- artifacts for important files or directories;
- relations to known upstream entities;
- scalar metrics that define the run, such as time window, stride, resolution,
  `dt`, walltime request, or particle count;
- a note explaining why the object exists.

Use `scientific_status: candidate` when unsure.

Use `note_type: warning` when:

- a run is partial;
- a diagnostic used a temporary approximation;
- a known bug may affect the result;
- the result is included for comparison but should not be used as reference.

## Status Contract

Keep operational status separate from scientific status.

Operational status examples:

- `planned`
- `prepared`
- `pending`
- `running`
- `success`
- `partial`
- `crashed`
- `interrupted`
- `unknown`

Scientific status examples:

- `candidate`
- `reference`
- `superseded`
- `suspect`
- `paper-used`

Do not mark a result as `reference` unless the user has made that decision or
it is already clearly encoded in campaign notes.

## Remote Data Rule

Do not copy large data just to update Lite.

Allowed by default:

- `datamak_lite.json`;
- small manifest JSON files;
- small SQLite pool status queries;
- small CSV/JSON summaries;
- short log snippets for failure diagnosis.

Not allowed by default:

- large field/history/restart files;
- full diagnostics directories;
- large copied figure-generation caches;
- raw outputs when compact summaries are sufficient.

## Bulk Campaign Import

When a campaign already has a curated local registry, import that before doing
any broad folder scan.  Curated registries usually contain better relations,
notes, and status than raw directories.

Generic registry import:

```bash
python3 -m datamak_lite.cli import-campaign-registry CAMPAIGN.sqlite \
  path/to/campaign_registry.db \
  --report
```

If important remote folders are still missing, import a shallow inventory as
candidate entities:

```bash
python3 -m datamak_lite.cli import-folder-inventory CAMPAIGN.sqlite \
  path/to/folder_inventory.txt
```

Folder-inventory entities are intentionally weaker than curated entries.  They
should be marked by a `todo` note and upgraded later with sidecars or adapter
imports.

## Figure Audit Import

Many plotting scripts already write legacy audit JSON files next to figures.
These are not full Lite sidecars yet, but they should be imported so figures
become visible in the campaign graph.

Use:

```bash
python3 -m datamak_lite.cli import-figure-audits CAMPAIGN.sqlite \
  path/to/figure_directory \
  --report
```

This creates `figure` entities, attaches output files, audit JSON, script, and
input data artifacts, and creates `plots` relations to matched or auto-created
source analysis objects.

Important diffusion-spectrum rule: if a figure audit mentions `abs(D_ky)` or a
similar absolute-value spectrum, Lite records a warning note.  Treat this as a
bug prompt unless the user explicitly says the absolute-value curve was only a
diagnostic test.  Diffusion spectra should normally use `2 Re[D(ky)]`.

## Report After Update

After importing a packet, run or show the equivalent of:

```bash
python3 -m datamak_lite.cli report CAMPAIGN.sqlite --entity ROOT_UID
```

The report should answer:

- What is this object?
- Where is it?
- What upstream data does it depend on?
- What did it produce?
- What notes or warnings matter?

## Failure Handling

If a packet cannot be made complete, make it explicit:

- create the partial packet anyway;
- keep `scientific_status: candidate`;
- add a `todo` or `warning` note;
- tell the user what is missing.

If a remote sync fails because SSH is unavailable, do not retry repeatedly.
Tell the user to refresh their SSH proxy or run the import manually later.

## Implementation Principle

Each automation feature should start with a transparent CLI command.  The GUI
can call the same operation later.  Avoid hidden GUI-only state.
