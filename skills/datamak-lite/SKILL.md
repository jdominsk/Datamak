---
name: datamak-lite
description: Find and maintain simulation and analysis provenance with Datamak Lite, including literature catalogue lookup and registration, campaign discovery, run status, input/output and restart/history availability, figure lineage, source-code links, and compact metadata updates. Use for research-record lookup and maintenance, not scientific calculations or interpretation.
---

# Datamak Lite

## Local installation

During setup, adapt this section with the verified Datamak checkout and Python
paths and the user's chosen skill scope. In the source distribution, the
checkout is two levels above this file. An installed copy must use its recorded
checkout path or `DATAMAK_ROOT`; do not assume its parent is the source tree.
Run the CLI from that checkout with the selected interpreter. Read
`agents/maintenance_protocol.md` there for packet/import conventions, and
`docs/figure_metadata_policy.md` when generating figures.

## Find records before searching files

Use Datamak Lite first for simulation provenance, status, produced files,
restart/history availability, case lists, and diagnostic inputs when a campaign
database, registry, sidecar, or pool database exists.

1. Check the explicitly named database/profile first. Otherwise discover the
   campaign through `$DATAMAK_HOME/campaigns.json` or `~/.datamak/campaigns.json`:

   ```bash
   python3 -m datamak_lite.cli list-campaigns
   python3 -m datamak_lite.cli resolve-campaign CAMPAIGN_UID --field database
   ```

2. Inspect the relevant campaign and entity records, including upstream
   relations, file paths, statuses, and notes:

   ```bash
   python3 -m datamak_lite.cli campaign-status /path/to/campaign.sqlite
   python3 -m datamak_lite.cli list /path/to/campaign.sqlite
   python3 -m datamak_lite.cli report /path/to/campaign.sqlite --entity ENTITY_UID
   ```

3. If records are missing or stale, say so and inspect only the necessary
   sidecars, inputs, summaries, logs, or source files. A recorded path or status
   is provenance evidence, not proof that a file still exists or a job is live.
   Resolve ambiguous campaign or dataset mappings before changing records.

## Complete the provenance loop

As part of authorized research work, keep a compact metadata trace for new or
updated simulations, analyses, results, figures, and their source code. Record
input/output paths and parameters, analysis/plotting scripts, relevant code
revision when known, and dependencies such as `uses_input`, `uses_history`,
`produces`, `plots`, or `uses_code`. Keep operational status separate from
scientific status; do not declare a result scientifically validated because
its run finished.

When fallback investigation resolves a metadata gap, record the verified facts
in the relevant sidecar and import them when campaign maintenance is authorized.
Preserve identifiers and existing relations. Ask before an ambiguous update;
otherwise report unresolved gaps explicitly rather than leaving guessed data.

Use the maintained CLI instead of ad hoc production SQL writes:

```bash
python3 -m datamak_lite.cli validate-packet /path/to/datamak_lite.json
python3 -m datamak_lite.cli import-packet /path/to/campaign.sqlite /path/to/datamak_lite.json
python3 -m datamak_lite.cli report /path/to/campaign.sqlite --entity ROOT_UID
```

Use `refresh-campaign PROFILE` for an established profile workflow after
checking its sources; use `sync-packet` for an authorized small remote sidecar
transfer. Report the packet path, the database updated, and remaining uncertainty.
If write access or authorization is unavailable, provide the update to apply
instead of claiming it was recorded.

## Keep the tool lightweight

- Store paths, relations, notes and compact metrics; leave large simulation
  outputs in their existing storage. Respect the user's transfer limits.
- Prefer existing project adapters and metadata over broad directory scans.
- Datamak records diagnostics and results; domain analysis code owns formulas,
  transport calculations, and scientific interpretation.
- Setup and provenance maintenance do not authorize job submission, resource
  allocation, simulation changes, or changes to global assistant settings.

## Distributed literature reviews

Use the literature catalogue for “have I reviewed this paper?” and review/PDF
location questions. It defaults to `$DATAMAK_HOME/literature.sqlite` or
`~/.datamak/literature.sqlite`; campaign DB placement stays unchanged.

```bash
python3 -m datamak_lite.cli literature-reviews
python3 -m datamak_lite.cli literature-find 'title words or DOI' --json
python3 -m datamak_lite.cli literature-discover /user/supplied/review/root
python3 -m datamak_lite.cli literature-import /selected/review --dry-run
python3 -m datamak_lite.cli literature-import /selected/review
```

Consult existing PDF/note paths before downloading or repeating extraction.
Register only authorized review folders. Preserve local Markdown as the source
of scientific claims and user positions; review depth and provisional membership
belong to each review's use of a paper. Exact identifiers can resolve paper
identity; title similarity only suggests duplicates. Catalogue-only work does
not regenerate review reports/slides or alter the selected local corpus. See
the checkout's README section “Literature Catalogue” for metadata fields and
`literature-add-paper` enrichment examples.
