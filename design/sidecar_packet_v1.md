# Datamak Lite Sidecar Packet v1

Datamak Lite sidecar packets are small JSON files written next to a simulation,
pool, analysis, or figure:

```text
run_or_pool_root/
  datamak_lite.json
```

The packet is a provenance receipt.  It should be useful even before import
into a central SQLite database.

## Shape

```json
{
  "schema_version": 1,
  "root_uid": "pool_or_run_uid",
  "entities": [],
  "relations": [],
  "artifacts": [],
  "metrics": [],
  "notes": []
}
```

## Entities

Each entity needs at least:

```json
{
  "uid": "unique_stable_id",
  "type": "pool",
  "name": "Human-readable name"
}
```

Optional fields:

```json
{
  "path": "/path/to/object",
  "status": "planned",
  "scientific_status": "candidate",
  "description": "Short description",
  "metadata": {}
}
```

Status and scientific status are separate.  A run can be operationally
successful but scientifically superseded or suspect.

## Relations

Relations link entities by uid:

```json
{
  "source_uid": "pool_uid",
  "relation_type": "uses_history",
  "target_uid": "history_uid",
  "note": "Short optional note",
  "metadata": {}
}
```

Common relation types:

- `member_of`
- `uses_history`
- `restart_from`
- `produces`
- `plots`
- `compares_to`
- `supersedes`

## Artifacts

Artifacts point to files or important directories:

```json
{
  "entity_uid": "pool_uid",
  "kind": "pool_db",
  "path": "pool.db",
  "format": "sqlite",
  "description": "SQLite worker-pool database",
  "metadata": {}
}
```

Relative paths are resolved relative to the packet location during import.
Absolute paths and URI-like paths are preserved.

### External Catalog Or Archive References

External systems should also be represented as artifacts or metadata, not as a
separate schema in the first Lite version.  This keeps the packet simple while
allowing later interoperability with larger campaign-management systems.

Examples:

```json
{
  "entity_uid": "pool_uid",
  "kind": "external_catalog",
  "path": "datafed:collection_or_record_id",
  "format": "datafed",
  "description": "DataFed record or collection for this campaign object"
}
```

```json
{
  "entity_uid": "simulation_uid",
  "kind": "campaign_archive",
  "path": "/path/to/archive.aca",
  "format": "adios2-hpc-campaign",
  "description": "ADIOS2/HPC Campaign archive metadata for the run outputs"
}
```

Other useful formats include `globus`, `doi`, `publication_archive`, and
`external_workflow_id`.  The current rule is to keep Lite as the local
campaign graph and attach external archive/catalog identifiers when they exist.

## Metrics

Metrics store compact scalar values only:

```json
{
  "entity_uid": "pool_uid",
  "name": "effective_stride",
  "value": 8,
  "unit": "step",
  "context": {}
}
```

Do not store arrays or large diagnostics in the packet.  Store paths to summary
JSON/CSV files as artifacts.

## Notes

Notes are Markdown-style comments attached to entities:

```json
{
  "entity_uid": "pool_uid",
  "note_type": "decision",
  "author": "optional",
  "markdown_text": "Why this simulation was prepared."
}
```

Initial note types:

- `comment`
- `decision`
- `warning`
- `todo`

## Import

Validate first:

```bash
python3 -m datamak_lite.cli validate-packet path/to/datamak_lite.json
```

Then import:

```bash
python3 -m datamak_lite.cli import-packet campaign.sqlite path/to/datamak_lite.json
```

The import is designed to be idempotent for the same packet: entities,
relations, artifacts, metrics, and identical notes should not multiply on a
second import.

## Remote Sync And Import

For Perlmutter or another remote machine, the packet remains next to the
simulation or pool.  Datamak Lite can copy only this small JSON sidecar into a
local cache and import it in one command:

```bash
python3 -m datamak_lite.cli sync-packet \
  /path/to/local/campaign.sqlite \
  user@host:/remote/run_or_pool_root/datamak_lite.json \
  --report
```

The default cache directory is next to the SQLite database:

```text
DB_DIR/
  campaign.sqlite
  packets/
    datamak_lite_<hash>.json
```

This is the preferred workflow for laptop-side campaign monitoring because it
copies metadata only, validates the copied sidecar, and does not copy
simulation data.  Use `--dry-run` to check where a remote packet would be
cached before copying it.

Remote sync uses `scp` with SSH batch mode and a timeout so it fails cleanly
instead of waiting for interactive input:

```bash
python3 -m datamak_lite.cli sync-packet campaign.sqlite \
  user@host:/remote/run/datamak_lite.json \
  --timeout 30
```

## Pool Packet Creation

The generic automated packet creator is for pool-like run directories:

```bash
python3 -m datamak_lite.cli create-pool-marker POOL_ROOT
python3 -m datamak_lite.cli create-pool-packet POOL_ROOT \
  --campaign-uid CAMPAIGN_UID \
  --uses-dataset-uid DATASET_UID \
  --dataset-path /path/to/input_or_history_dataset \
  --note "Why this pool was prepared."
```

The command writes:

```text
POOL_ROOT/datamak_lite.json
```

`create-pool-marker` separately writes `README.md` and `datamak_pool.json` so
the directory is recognizable as a Datamak-style pool.  The Lite sidecar then
records `datamak_pool.json` as an artifact when present.

It records the pool root, obvious artifacts such as `replay_pool.db`,
`pool_manifest.json`, `analysis/`, `cases/`, and launch/status scripts when
present.  It also infers simple metadata such as `t600_t700`, `effstride8`, and
`stride1` from the folder name when possible.

To create, import, and print the report in one step:

```bash
python3 -m datamak_lite.cli create-pool-packet POOL_ROOT \
  --campaign-uid CAMPAIGN_UID \
  --uses-dataset-uid DATASET_UID \
  --dataset-path /path/to/input_or_history_dataset \
  --import-db /path/to/campaign.sqlite \
  --report
```
