# Figure Metadata Policy

Every new generated scientific figure should leave metadata that Datamak Lite
can import.

This is a campaign-operational requirement.  A figure without metadata is only
an image; Lite cannot reliably answer what produced it, what data it used, or
whether it is superseded or suspect.

## Agent Rule

When an agent creates or regenerates a paper, logbook, presentation, or
diagnostic figure, it must also create or update figure metadata in the same
turn.

The metadata should be written next to the figure output or in the same figure
directory.  The current accepted bridge format is a compact audit JSON file.
Future figure workflows can write a native Datamak Lite packet, but they still
need to contain the same information.

## Minimum Metadata

Every figure metadata file should include:

- output figure paths, for example `output_png`, `output_pdf`, or `outputs`;
- the plotting script path;
- input data paths, such as CSV, JSON, NetCDF, HDF5, TOML, or NPZ summaries;
- source simulation, pool, analysis, or history UIDs when known;
- the figure purpose or quantity plotted;
- the averaging window, time window, stride, model, or normalization when
  relevant;
- paper/logbook/slide destination when known;
- warnings for approximations, partial data, temporary diagnostics, or known
  bugs.

Do not embed large arrays in the metadata.  Store paths to compact summaries
or source files instead.

## Recommended Legacy Audit JSON Shape

Until every plotting script writes a native `datamak_lite.json` packet, use an
audit JSON next to the figure:

```json
{
  "script": "Paper/scripts/plot_example.py",
  "output_png": "Paper/figure/example.png",
  "output_pdf": "Paper/figure/example.pdf",
  "input_csvs": [
    "analysis/example/summary.csv"
  ],
  "source_entities": [
    "pool_example_replay",
    "dataset_example_input"
  ],
  "quantity": "time-averaged D, V, and PF",
  "time_window": "t=600-700",
  "averaging_window": "tau > 50",
  "notes": "Reference curve for the current paper draft."
}
```

The current Lite bridge can import this with:

```bash
python3 -m datamak_lite.cli import-figure-audits CAMPAIGN.sqlite \
  Paper/figure \
  Presentations/Ongoing_work/figure
```

## Preferred Future Native Packet

The preferred future format is a Datamak Lite sidecar packet:

```text
figure_or_analysis_root/
  datamak_lite.json
```

The root entity should have:

```json
{
  "uid": "figure_example",
  "type": "figure",
  "name": "Example transport figure",
  "path": "Paper/figure/example.png",
  "status": "available",
  "scientific_status": "candidate"
}
```

Relations should include:

- `figure --plots--> source_analysis_or_pool`
- `figure --shown_in--> slide_or_paper_section` when known;
- `figure --compares_to--> reference_result` when relevant;
- `figure --supersedes--> old_figure` when replacing a previous figure.

## Diffusion Spectrum Rule

Diffusion spectra should normally use `2 Re[D(k_y)]`.

If a figure uses `abs(D_ky)`, `|D(k_y)|`, or any absolute-value diffusion
spectrum, the metadata must explicitly say it is a diagnostic test.  Otherwise
it should be treated as a warning or bug.  The current figure-audit importer
adds a warning note when it detects this pattern.

## When Metadata Is Missing

If an agent finds an existing figure without metadata:

1. do not silently ignore the gap;
2. create a best-effort audit JSON from the plotting script and available
   input paths;
3. mark uncertain provenance with a warning or todo note;
4. import it into Lite only after the uncertainty is explicit.

This keeps Lite useful without pretending that reconstructed provenance is as
strong as metadata written at figure-generation time.
