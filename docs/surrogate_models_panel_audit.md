# Surrogate Models Panel Audit

Date: 2026-03-28

This note captures the current audit of the `Surrogate Models` tab in the Datamak GUI so we can refine it gradually.

## Scope

Relevant files:

- `gui/templates/index.html`
- `gui/app.py`
- `gui/actions.py`
- `db_surrogate/train_gamma_surrogate.py`
- `db_surrogate/estimate_gamma_surrogate.py`
- `db_surrogate/delete_surrogate_model.py`
- `tests/test_surrogate_workflow.py`

## Main Findings

### 1. Training scope and inference scope do not match

Training can be filtered by:

- `data_origin`
- `gk_run.status`

That filtering is applied in:

- `gui/actions.py`
- `db_surrogate/train_gamma_surrogate.py`

But `Compute estimates` currently predicts for every complete `gk_input` row in the database, with no origin or status filter.

That behavior is implemented in:

- `db_surrogate/estimate_gamma_surrogate.py`

Implication:

- a model trained on one origin can silently annotate all origins
- the panel does not currently warn the user about that scope expansion

### 2. `Map to = ALL` is not one model

The UI option says:

- `ALL (train gamma_max, ky_abs_mean, diffusion)`

But the backend does not train one multi-output model.
It runs three separate surrogate trainings serially:

- `gk_run.gamma_max`
- `gk_run.ky_abs_mean`
- `gk_run.diffusion`

That behavior is implemented in:

- `gui/app.py`

Implication:

- operationally valid
- semantically misleading in the UI unless explained more explicitly

### 3. The panel is driven by `gk_surrogate`, not by files on disk

The visible registry shown in the panel is loaded from the `gk_surrogate` table.
Model files present under `db_surrogate/models` are not enough by themselves to appear in the GUI.

Implication:

- orphaned `.pkl` / `.json` model files are invisible in the panel
- the GUI state depends on metadata insertion back into `gk_surrogate`

There is also a legacy helper that scans model JSON files directly, but it appears unused by the current panel.

### 4. Opening the page can mutate schema

`index()` currently calls `ensure_gk_surrogate_table()` while building the page.

Implication:

- request-time `CREATE TABLE IF NOT EXISTS`
- request-time `ALTER TABLE` if columns are missing

This is convenient for compatibility, but it is not ideal long term.

### 5. Test coverage is still thin for this panel

The current surrogate test coverage mainly verifies a basic:

- train
- then estimate

happy path on a synthetic database.

I did not find focused regression coverage for:

- `Map to = ALL`
- delete flow end-to-end through the panel logic
- estimate-summary interpretation
- the global-vs-origin mismatch in estimate generation
- panel rendering decisions

## What The Panel Is Doing

The tab is really three tools combined.

### A. Surrogate registry

The top table lists saved surrogate models from `gk_surrogate`, including:

- name
- label
- maps-to target
- creation time
- training status filter
- train/test row counts
- test-set metrics

Each row has a `Run` button that triggers surrogate estimation for that model.

### B. Estimate summary and management

The middle section lets the user:

- choose a surrogate model
- view estimate summary
- compute estimates
- delete the model

The summary is not just a row count.
It computes:

- total estimate count
- estimate and uncertainty statistics
- comparison against available ground truth
- MAE
- RMSE
- R²
- bias
- median relative error
- coverage within `sg_quality`

Ground truth is taken from the target column in `gk_run` when available.

### C. Training form

The bottom section launches RandomForest training with options for:

- model name
- target column
- origin filter
- run status filter
- test size
- number of trees
- max depth
- minimum leaf size
- `log1p(target)`

The training script:

- reads engineered plasma/equilibrium features from `gk_input`
- joins target values from `gk_run`
- trains a `RandomForestRegressor`
- saves a pickle model and JSON metadata
- then the GUI writes a corresponding entry into `gk_surrogate`

## Results Panel Integration

The surrogate system is not isolated to the `Surrogate Models` tab.
It also extends the `Results` panel by injecting synthetic axes:

- `SG <id> <label> - estimate`
- `SG <id> <label> - error`
- `SG <id> <label> - diff`

So once surrogate estimates exist in `sg_estimate`, they become plottable alongside normal `gk_input` / `gk_run` quantities.

## Operational Summary

Current end-to-end behavior:

1. Train surrogate from `gk_input` + `gk_run`
2. Save model files under `db_surrogate/models`
3. Insert metadata into `gk_surrogate`
4. Run prediction across all eligible `gk_input`
5. Save predictions into `sg_estimate`
6. Reuse those estimates in the summary table and the Results plots

## Open Design Questions

These are the main issues worth addressing gradually:

1. Should `Compute estimates` be global, or should it optionally respect origin and/or status filters?
2. Should `Map to = ALL` be relabeled to make it explicit that it creates three separate models?
3. Should the GUI support re-registering model files found on disk but missing from `gk_surrogate`?
4. Should schema creation/migration be moved out of page render and into an explicit migration path?
5. Should panel-specific tests be added before larger surrogate refactors?

## Suggested Next Steps

Low-risk next improvements:

1. Clarify the UI wording for `ALL`
2. Add a note that estimation currently runs over all eligible `gk_input` rows
3. Add tests for:
   - `ALL`
   - delete flow
   - estimate scope
4. Decide whether estimate generation should support origin-local mode

