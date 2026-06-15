# Datamak Lite Agent Workflow

## Purpose

Datamak Lite is a lightweight, code-agnostic provenance and monitoring layer.
It helps track simulations, pools, restarts, reusable datasets, postprocessing
analyses, figures, and live comments.

The core must remain useful for any simulation or analysis code.  Project
names, machine paths, research context, and campaign-specific conventions
belong in metadata, not in generic Lite code or examples.

## Main Rule

Keep the implementation iterative.

For each new concept:

1. implement the smallest useful version;
2. test it on one concrete object;
3. inspect the user experience;
4. adjust the model or interface;
5. only then add the next concept.

Do not build a large framework before the first generic graph/report and
sidecar import loop are usable.

## Maintenance Rule

Lite is intended to be maintained by agents and humans during normal campaign
work.  A simulation, pool, analysis, or figure should not only be created; it
should also leave metadata that Lite can import.

Before preparing or updating campaign objects, read
`agents/maintenance_protocol.md`.  The default lightweight action is to create
or update a `datamak_lite.json` sidecar next to the object.  Import into the
central SQLite registry can happen immediately when requested, or later via
`import-packet` or `sync-packet`; packets should pass `validate-packet` before
they change the registry.

Before creating or regenerating figures, also read
`docs/figure_metadata_policy.md`.  New paper, logbook, presentation, and
diagnostic figures must write metadata at generation time.  A raw PNG/PDF is
not enough for Lite to reconstruct provenance.

For supported object types, agents should use a packet creator instead of
hand-writing JSON.  The generic pool creator is:

```bash
python3 -m datamak_lite.cli create-pool-packet POOL_ROOT ...
```

Sidecar creation is automatic by default for Lite work.  Central registry
import is appropriate when the user asks for Lite to stay updated, but the
agent should still report which packet was written and which database was
updated.

## Architecture Boundaries

The core must be code-agnostic.

Core concepts:

- `entity`
- `relation`
- `artifact`
- `metric`
- `note`
- `event`

Adapters translate specific systems into the generic model.  Examples include:

- a Datamak-style SQLite pool;
- a scheduler status source;
- a code-specific manifest;
- a plotting audit JSON;
- a shallow folder inventory;
- an external catalog export.

Do not put code-, facility-, campaign-, or domain-specific assumptions into the
core schema.  Put them in adapters or imported metadata.

## Source Layout

Use this target layout, creating files only when they are needed:

```text
Datamak_lite/
  design/
  agents/
  docs/
  schema/
  datamak_lite/
    core/
    adapters/
    gui/
    examples/
    cli.py
  tests/
```

The repository is named `Datamak_lite`.  The Python package is named
`datamak_lite`.

## Database Rules

Use SQLite as the source of truth.

Prefer additive, idempotent migrations:

- `CREATE TABLE IF NOT EXISTS`
- guarded schema updates
- no destructive migrations without explicit user approval

Keep operational and scientific status separate:

- operational: `planned`, `pending`, `running`, `success`, `crashed`,
  `interrupted`, `unknown`
- scientific: `candidate`, `reference`, `superseded`, `suspect`, `paper-used`

A run can be operationally successful but scientifically superseded.

## Data Handling

Do not ingest large output files into the database.

Store paths and compact metadata:

- path
- kind
- file size
- modification time
- source summary path
- selected scalar metrics

For large outputs, prefer existing JSON/CSV summaries.  If a summary is
missing, add an adapter command that can generate a small cache explicitly.

## External Campaign Tools

Lite may later interoperate with tools such as DataFed, Globus, DOI archives,
or ADIOS2/HPC Campaign `.ACA` files.  Agents should preserve those identifiers
when they are known, but should not make the local Lite workflow depend on an
external service.

Represent external systems as ordinary artifacts or metadata in the sidecar:

- DataFed record or collection id;
- Globus endpoint and path;
- ADIOS2/HPC Campaign `.ACA` path;
- DOI or publication archive id;
- external workflow-system run id.

The local sidecar and SQLite registry remain the minimum workflow that must
work offline.

## Sidecar Metadata Packets

When preparing a simulation, pool, analysis, or figure, create or update a
small JSON metadata packet next to it:

```text
datamak_lite.json
```

This packet is the first integration point between normal agent-assisted work
and Lite.  It should be useful even if the central SQLite database is not
updated immediately.

Packet responsibilities:

- describe the object as an `entity`;
- list important `artifacts` by path;
- record upstream/downstream `relations`;
- store compact planned or measured `metrics`;
- include short Markdown-style `notes` explaining decisions or warnings.

Do not put large diagnostics or copied output data in the packet.  Store paths
and compact summaries only.

Recommended agent behavior:

1. When preparing a run or pool, write `datamak_lite.json` in the root.
2. Report the packet path to the user.
3. Do not directly mutate a central Lite registry unless the user asks or the
   packet import workflow is already part of the task.
4. If details are uncertain, write a packet with
   `scientific_status: candidate` and add a `todo` or `warning` note rather
   than silently guessing.

## Lineage Metadata

Lite should be able to reconstruct a simulation tree without reading large
outputs.

When preparing or registering simulation work, agents should record:

- `restarts_from` when a run starts from a previous simulation restart file;
- `produces` when a simulation writes a reusable history or data product;
- `uses_history` when GX-R, KTM, tracer, or any downstream analysis reuses a
  saved turbulent field history.

Interpretation:

- Same plasma parameters and same numerics after `restarts_from` means
  continuation.
- Changed plasma parameters or numerics after `restarts_from` means branch.
- Field history files should be metadata on the producing simulation in the
  lineage view, not separate tree nodes. Record their saved time window and
  stride in metadata such as `source_window`, `saved_stride`, `history_stride`,
  or `step_record_stride`.
- Downstream replay/KTM/tracer objects should appear as leaves below the
  turbulent simulation that produced the history they use.
- If the upstream is uncertain, still create the object with a warning or todo
  note. Do not guess silently.

When the packet is on a remote machine, prefer the one-command metadata-only
sync/import workflow:

```bash
python3 -m datamak_lite.cli sync-packet \
  /path/to/local/campaign.sqlite \
  user@host:/remote/run_or_pool_root/datamak_lite.json \
  --report
```

This command copies only the small sidecar JSON into a local `packets/` cache
next to the SQLite database, then imports it.  Do not copy large output,
history, restart, or diagnostic files merely to update the registry.

## Scheduler Metadata For Live Status

The GUI can show a small remote-status marker next to an object's Lite alias
and, when clicked, query the scheduler on the remote machine.  Agents should
record enough metadata for that live check to be precise.

For runs or pools on a scheduler-backed remote system, include at least one of:

- `slurm_job_id`, `job_id`, or `scheduler_job_id`;
- `slurm_job_name` or `job_name`;
- a remote `path`, `run_root`, `run_dir`, or `pool_root` that matches the
  scheduler working directory or command path;
- `remote_host`, `host`, `hpc_host`, `machine`, or `cluster`.

For Perlmutter objects, paths under `/pscratch/` or `/global/` are enough for
Lite to offer a live check button, but a Slurm job id is more reliable.  The
sidecar should also preserve cached operational status when known, for example
`pending`, `running`, `success`, or a compact `status_counts` dictionary for a
pool.

The live query is inspection only.  It must not submit, cancel, restart, or
otherwise mutate scheduler state.

## Notes And Live Document

Comments are first-class data.

Use Markdown notes attached to an entity or relation.  Initial note types:

- `comment`
- `decision`
- `warning`
- `todo`

The GUI should eventually show notes next to the entity or relation they refer
to.  Until then, CLI/static reports are enough.

## GUI Rule

The GUI is only a frontend.

The GUI must read and write the same SQLite database as the CLI.  The database
and core API must not depend on a particular GUI framework.

Initial GUI target:

- campaign overview;
- selected entity details;
- upstream/downstream relation list;
- notes panel;
- status and key metrics table.

Do not start with a complex graph editor.  A simple relation list is acceptable
for the first GUI test.

Current first GUI command:

```bash
python3 -m datamak_lite.cli serve /path/to/campaign.sqlite
```

The first GUI is read-only and dependency-free.  It reuses the Datamak visual
identity: Helvetica/Arial typography, compact panels and tables, white
surfaces, light blue active states, muted gray borders, and status chips.

## First Implementation Loop

Implement a non-GUI first test before building richer views.

Goal:

```text
What produced this figure, and what upstream objects did it depend on?
```

Minimal objects:

1. One campaign.
2. One source simulation producing a reusable dataset.
3. One pool using that dataset.
4. One analysis of the pool output.
5. One figure.

Minimal relations:

- source simulation `produces` dataset;
- pool `uses_input` dataset;
- analysis `analyzes` pool;
- figure `plots` analysis;
- all objects `member_of` campaign.

Minimal output:

- a Markdown or HTML report for the figure;
- upstream dependency list;
- two notes, including one decision.

## Second Implementation Loop

Implement sidecar packet import before adding more GUI complexity.

Goal:

```text
Given a datamak_lite.json file next to a prepared simulation or pool, import it
into the SQLite registry and render the same dependency report.
```

Minimal packet test:

1. Create one realistic generic packet for a prepared pool.
2. Import it into a temporary SQLite database.
3. Verify that entities, artifacts, relations, metrics, and notes appear.
4. Render a report from imported packet data.

Only after this loop feels natural should the GUI be built on top of it.

## Remote And Live Jobs

Do not launch scheduler commands or consume allocations unless the user
explicitly asks in the current turn.

For remote simulations:

- inspect status first from SQLite pool databases when available;
- read logs only when diagnosing a failure or missing status;
- store remote paths and small summaries, not large copied data;
- avoid copying large outputs to the laptop.

## What To Avoid

- Do not make a code-specific schema.
- Do not hard-code campaign-specific paths in the core.
- Do not make a GUI framework a dependency of the database layer.
- Do not scan large binary outputs automatically during dashboard load.
- Do not add automated job submission before the registry/report workflow is
  useful.
- Do not introduce many status names without documenting their meaning.

## Useful References

- `../design/design_plan.md`
- `../design/sidecar_packet_v1.md`
- `../docs/figure_metadata_policy.md`
- Existing Datamak batch worker pattern when adapting a worker pool.
