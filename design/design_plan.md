# Datamak Lite Design Plan

## Goal

Datamak Lite helps users follow a campaign where many runs, pools, restarts,
datasets, analyses, figures, and notes are linked together.  The core model
must be independent of any one code, facility, machine, or research domain.

The tool should answer questions such as:

- Which run, pool, or analysis produced this figure?
- Which input dataset or restart did this object use?
- Which result is currently the reference one, and which ones are superseded?
- Why did we make this choice?
- Which objects are running, pending, interrupted, or suspect?

Campaign-specific names and paths belong in imported metadata.  They should not
be hard-coded in the Lite package, generic documentation, tests, or examples.

## Development Rule

The design can be ambitious, but each implementation step must be small.

For every new concept:

1. implement the simplest useful version;
2. test it on one concrete object;
3. adjust the user experience;
4. only then add the next concept.

The first version should not try to automate everything.  Manual registration
of a few important objects is acceptable if it validates the data model and user
experience.

## Operational Requirement

Lite should be maintainable by an agent or manually by the user without hidden
state.  Every workflow that prepares, launches, postprocesses, or plots an
important object should have an explicit metadata step.

The minimum operational contract is:

1. write a `datamak_lite.json` sidecar next to the object;
2. store compact provenance, paths, metrics, relations, and notes;
3. import the packet into the central SQLite registry with a documented command
   when the user wants the campaign view updated;
4. keep the same workflow usable by humans and agents.

Automation should reduce bookkeeping, but the metadata remains inspectable and
editable.

## Relation To Datamak And External Campaign Tools

Lite should reuse Datamak's practical workflow ideas without becoming a copy of
the full Datamak workflow database.

- Datamak-style pools can own execution state: scheduled rows, worker claims,
  status transitions, and restart policy.
- Lite is a visibility layer.  It records what happened, how objects are
  connected, which result is trusted, and what decisions were made.

The two should remain complementary.  A pool database can manage execution,
while Lite records compact provenance and relations through sidecar packets and
imports.

External campaign-management systems are interoperability targets, not
dependencies for the first version.  Lite can attach external catalog or archive
identifiers as ordinary artifacts or metadata:

- DataFed record or collection id;
- Globus endpoint/path;
- ADIOS2/HPC Campaign `.ACA` file;
- DOI or publication archive id;
- external workflow-system run id.

This keeps the current user experience simple while preserving a path toward
larger data-management systems.

## Core Concepts

The core should be code-agnostic.  Code-, scheduler-, filesystem-, and
presentation-specific support should live in adapters that write into the same
generic model.

### Entity

An entity is any object worth tracking.

Examples:

- campaign
- simulation
- pool
- job
- restart file
- input or history dataset
- analysis
- figure
- slide
- paper result

Simple first fields:

- `id`
- `type`
- `name`
- `path`
- `status`
- `scientific_status`
- `description`
- `created_at`
- `updated_at`

Operational status and scientific status should be separate.  A run can finish
successfully while still being superseded or suspect.

### Relation

A relation links two entities.

Examples:

- `restarts_from`
- `uses_input`
- `uses_history`
- `produces`
- `analyzes`
- `plots`
- `shown_in`
- `supersedes`
- `compares_to`
- `member_of`

The key feature is that a campaign is not only chronological; it is a graph of
dependencies and decisions.

### Lineage View

One important campaign view is a tree of simulation lineage.

Lineage rules:

- A root is a main-plasma simulation that starts from the initial state, usually
  from `t=0`.
- A restart with the same plasma and numerical parameters is a continuation.
- A restart with changed plasma parameters or numerical parameters is a branch.
- Saved field histories are not separate tree nodes in the lineage view. They
  are summarized as a `history` column on the main-plasma row, using the saved
  time window and stride when known.
- GX-R replay, KTM, tracer, and other downstream analyses are leaves below the
  turbulent simulation that produced the history they use.
- If a downstream analysis cannot be placed under a parent because `uses_history`
  or `produces` relations are missing, the GUI should show it in an unplaced
  section instead of hiding it.

Agents should use `restarts_from`, `produces`, and `uses_history` relations to
make this view work.  A sidecar packet for a new restart or downstream study
should include these relations whenever the upstream object is known.

### Artifact

An artifact is a concrete file or directory associated with an entity.

Examples:

- input file
- output file
- restart file
- history file
- JSON summary
- CSV summary
- PNG/PDF figure
- pool database
- log file

Simple first fields:

- `entity_id`
- `kind`
- `path`
- `format`
- `description`
- `exists`
- `size_bytes`
- `mtime`

### Metric

A metric stores a compact scalar value, never a large raw diagnostic.

Examples:

- `runtime_hours`
- `time_start`
- `time_end`
- `dt`
- `effective_stride`
- `planned_cases`
- `completed_cases`
- code- or analysis-specific scalar results

Simple first fields:

- `entity_id`
- `name`
- `value`
- `unit`
- `context_json`
- `source_artifact_id`

### Note

A note is a Markdown comment attached to an entity or relation.

Examples:

- "This run is superseded because the input normalization changed."
- "This dataset used fixed dt=0.0012."
- "Use this result for the current report."

Simple first fields:

- `entity_id`
- `relation_id` optional
- `author`
- `created_at`
- `note_type`
- `markdown_text`

Initial note types:

- `comment`
- `decision`
- `warning`
- `todo`

## Sidecar Metadata Packets

When a user or agent prepares a new simulation, pool, analysis, or figure, the
first lightweight integration step is to write a JSON metadata packet next to
the object:

```text
run_or_pool_root/
  datamak_lite.json
```

This file is a local provenance receipt.  It should be useful even before the
object is imported into a central Lite database.

The packet should contain only compact metadata:

- entity identity, type, name, path, status, and scientific status;
- artifacts such as inputs, pool databases, logs, summary JSON/CSV files, and
  important output paths;
- relations to known upstream objects, such as `uses_input`, `uses_history`,
  `restart_from`, `produces`, `plots`, or `compares_to`;
- selected scalar metrics or planned parameters, such as time window, stride,
  resolution, or requested wall time;
- notes explaining why the object was prepared or what decision it supports.

The sidecar packet avoids relying on memory or hidden agent state.  If the
central registry is missing or out of date, Lite can rebuild it later by
importing packets found in object folders.

Initial packet policy:

1. Prefer creating or updating `datamak_lite.json` when preparing an object.
2. Import packets into the central SQLite database when the user wants the
   campaign view updated.
3. Keep packet creation cheap enough that it does not slow down iteration.
4. Do not store large diagnostics inside the packet; store paths and compact
   summaries.

## Campaign Profiles

Campaign-specific refresh configuration lives in a profile JSON outside the
generic Lite code.  The profile centralizes:

- campaign UID and display name;
- SQLite database path;
- local sidecar packet roots;
- explicit sidecar packet paths;
- remote sidecar packet paths;
- curated registry paths;
- folder-inventory paths;
- figure-audit roots.

The generic command is:

```bash
python3 -m datamak_lite.cli refresh-campaign path/to/campaign_profile.json
```

This is the preferred way to rebuild or update the campaign view.  It keeps
project-specific source locations in data, not in the package.

## Figure Metadata Requirement

Figures are first-class campaign objects.  When a paper, logbook,
presentation, or diagnostic figure is generated, the plotting workflow must
also write metadata that Lite can import.  A raw PNG/PDF is not sufficient.

The current bridge format is a compact audit JSON already used by many plotting
scripts.  It should include:

- figure output paths;
- plotting script path;
- input data paths;
- source entity UIDs when known;
- key plotting assumptions such as time window, stride, averaging window,
  normalization, or model;
- warnings for partial data, temporary approximations, or known diagnostic
  bugs.

The future native version should be a Lite sidecar packet describing a `figure`
entity and relations such as `plots`, `shown_in`, `compares_to`, or
`supersedes`.  The legacy importer exists so current audit JSON files are not
lost, but new figure-generation code should still write metadata at generation
time.

## First Generic Data Model

The first test graph should register only a small number of domain-neutral
objects.

Recommended first entities:

1. One campaign.
2. One source simulation producing a reusable dataset.
3. One pool using that dataset.
4. One analysis of the pool output.
5. One figure using the analysis.

Recommended first relations:

- source simulation `produces` dataset.
- pool `uses_input` dataset.
- analysis `analyzes` pool.
- figure `plots` analysis.
- all objects `member_of` campaign.

Recommended first notes:

- A comment on the dataset explaining its time window or scope.
- A decision note on the figure explaining why it is useful.

## Folder Plan

Keep all Lite work inside the `Datamak_lite` checkout.  The Python package is
named `datamak_lite`.

Target layout:

```text
Datamak_lite/
  README.md
  design/
    design_plan.md
    sidecar_packet_v1.md
  docs/
  agents/
  schema/
    datamak_lite_schema.sql
  datamak_lite/
    core/
    adapters/
    gui/
    examples/
    cli.py
  tests/
```

Adapters should remain conservative.  They translate known metadata into the
generic model; they should not hard-code campaign-specific assumptions.

## GUI Direction

A lightweight GUI is useful because Lite is both a monitor and a live document.
The database should not depend on the GUI.  The GUI should read and write the
same SQLite database used by the command-line tools.

First GUI views:

- campaign overview;
- entity detail with upstream and downstream relations;
- notes panel;
- status table;
- simple dependency view.

## First Simple Test

The first implementation test should be a command-line and static report test,
not a full GUI.

Test objective:

```text
Register a tiny generic graph and render a readable summary answering:
"What produced this figure, and what upstream objects did it depend on?"
```

Minimal workflow:

1. Create a SQLite database.
2. Insert five entities:
   - one campaign;
   - one source simulation;
   - one reusable dataset;
   - one pool;
   - one analysis;
   - one figure.
3. Insert relations linking them.
4. Attach two Markdown notes.
5. Render one Markdown or HTML report for the figure.

Success criteria:

- The report shows the figure.
- It lists the upstream pool, analysis, dataset, and source simulation.
- It shows at least one decision note.
- The schema feels natural enough to add another object without changing the
  database structure.

Only after this test feels useful should we add richer GUI behavior.

## Next Integration Test

After the first static graph report, the next data-focused step should be a
sidecar packet importer:

1. write one realistic `datamak_lite.json` packet for a prepared pool;
2. import it into a Lite SQLite database;
3. render the same dependency report from imported packet data;
4. adjust the packet fields before adding more automation.

The first GUI should come after this packet import loop, so the GUI is built
around the workflow that agents and users will actually use.
