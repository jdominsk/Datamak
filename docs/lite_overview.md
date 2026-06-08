# Datamak Lite Overview

Datamak Lite is a lightweight campaign-monitoring layer for simulation and
analysis work.  It is designed for campaigns where runs, restarts, reusable
datasets, pools, postprocessing analyses, figures, slides, and decisions are
connected in a graph rather than a simple chronological logbook.

The design is code-agnostic.  Any project-specific code name, machine name,
path convention, or research context should be stored in metadata imported into
Lite, not in the generic tool.

Lite can still ship reusable campaign-type templates.  A template is an opt-in
domain layer, not a core assumption.  For example, a project may define a
campaign type with recommended metadata keys, semantic title rules, and overview
grouping conventions.  A campaign profile can reference that template and then
override it locally.

## What Lite Tracks

Lite tracks five generic concepts:

- `entity`: a simulation, pool, dataset, restart, analysis, figure, slide,
  paper result, or campaign.
- `relation`: a directed connection such as `uses_input`, `uses_history`,
  `restart_from`, `produces`, `plots`, `compares_to`, or `supersedes`.
- `artifact`: a concrete file or directory path attached to an entity.
- `metric`: a compact scalar value such as runtime, time window, stride,
  resolution, or an analysis result.
- `note`: a Markdown-style comment, warning, todo, or decision.

Operational status and scientific status are separate.  A run can finish
successfully while still being superseded, suspect, or only a candidate result.

## How Lite Stays Updated

Lite is intended to be maintained during normal work by the user or by an
agent.  The durable trace is a small sidecar packet next to the object:

```text
run_or_pool_or_analysis_root/
  datamak_lite.json
```

The packet contains compact metadata only: paths, relations, selected metrics,
and notes.  It does not copy large output, restart, or history files.  The
packet can then be imported into a central SQLite database used by reports and
the GUI.

This gives two layers:

- the sidecar is the local provenance receipt next to the object;
- the campaign profile is the list of sidecar roots, registries, inventories,
  and figure audit roots to refresh;
- the SQLite database is the queryable campaign view.

If the central database is stale, it can be rebuilt by importing sidecars and
curated registries through the profile.

## Relationship With Datamak

Datamak and Lite should share workflow discipline but solve different
problems.

- Datamak-style pools can manage execution state: run rows, worker claims,
  scheduler events, and status transitions.
- Lite records the compact provenance and decision graph around those objects.

Lite should not require a project to use Datamak for execution.  A hand-written
sidecar, a code-specific adapter, or an external registry import can all feed
the same Lite database.

## Relationship With External Campaign Tools

Large campaign-data systems are complementary to Lite.  Lite starts as a local,
human-readable campaign graph and decision record, while external systems may
own data federation, transfer, long-term cataloging, and archive publication.

Useful external concepts include:

- DataFed records or collections;
- Globus endpoint/path references;
- ADIOS2/HPC Campaign `.ACA` archives;
- DOIs or publication archive identifiers;
- external workflow-system run IDs.

Lite can store those identifiers as artifacts or metadata without depending on
the external service for normal local use.

## First Practical Workflow

The first practical workflow is:

1. Prepare a simulation, pool, analysis, or figure.
2. Write or update `datamak_lite.json` next to it.
3. Keep the campaign profile pointed at the relevant packet roots, registries,
   inventories, and figure audit roots.
4. Run `refresh-campaign`.
5. Inspect the report or GUI.
6. Add notes or relation fixes while the decision is still fresh.

For a campaign profile:

```bash
python3 -m datamak_lite.cli refresh-campaign /path/to/campaign_profile.json
python3 -m datamak_lite.cli refresh-campaign /path/to/campaign_profile.json --dry-run
```

If the campaign belongs to a reusable domain pattern, point the profile to a
campaign-type template:

```json
{
  "campaign_type": "my_domain_pattern",
  "campaign_type_template": "path/to/template",
  "display_title_rules": "local_title_rules.md"
}
```

The template should help agents produce meaningful titles and metadata, while
the local rules file captures project-specific vocabulary.

For a generic pool:

```bash
python3 -m datamak_lite.cli create-pool-packet POOL_ROOT \
  --campaign-uid campaign_example \
  --uses-dataset-uid dataset_example_input \
  --dataset-path /path/to/input_dataset \
  --note "Why this pool was prepared." \
  --import-db /path/to/campaign.sqlite \
  --report
```

For a hand-written sidecar:

```bash
python3 -m datamak_lite.cli validate-packet /path/to/datamak_lite.json
```

For a remote sidecar:

```bash
python3 -m datamak_lite.cli sync-packet \
  /path/to/campaign.sqlite \
  user@host:/remote/run/datamak_lite.json \
  --report
```

This copies metadata only.

## Figure Metadata

Many plotting scripts write audit JSON files next to generated figures.  Lite
can import those legacy audit files as a bridge toward native figure sidecars:

```bash
python3 -m datamak_lite.cli import-figure-audits \
  /path/to/campaign.sqlite \
  path/to/figure_directory \
  --report
```

This importer creates `figure` entities, attaches audit JSON, output files,
plotting scripts, and input data paths as artifacts, and creates `plots`
relations to matched or auto-created source analysis objects.

The figure-audit importer is intentionally conservative.  It does not assume
that every legacy audit JSON fully describes provenance.  Unmatched source paths
are imported as candidate source entities.  If an audit text mentions an
absolute-value diffusion spectrum such as `abs(D_ky)`, Lite adds a warning note
because diffusion spectra should normally use `2 Re[D(ky)]`.

## Source References

- ORNL Workflow Systems group:
  <https://www.ornl.gov/group/workflow-systems>
- DataFed introduction:
  <https://ornl.github.io/DataFed/system/introduction.html>
- DataFed system overview:
  <https://ornl.github.io/DataFed/system/overview.html>
- ADIOS2/HPC Campaign Management:
  <https://adios2.readthedocs.io/en/latest/advanced/campaign_management.html>
- HPC Campaign documentation:
  <https://hpc-campaign.readthedocs.io>
- DataFed GitHub:
  <https://github.com/ORNL/DataFed>
