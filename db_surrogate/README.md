# Surrogate Models

This folder contains scripts and artifacts for training and using surrogate models.

## Surrogate models (gk_input -> gk_run targets)

### Features (must match the Input Sampling panel)

```
temp_ratio, dens_ratio, electron_tprim, electron_fprim, ion_tprim, ion_fprim,
ion_vnewk, electron_vnewk, mass_ratio, Rmaj, qinp, shat, shift, akappa,
akappri, tri, tripri, betaprim
```

Ratio features are computed on the fly from base columns in `gk_input`:

- `temp_ratio = ion_temp / electron_temp`
- `dens_ratio = ion_dens / electron_dens`
- `mass_ratio = ion_mass / (electron_mass * 1836)`

### Train

Train on all available SUCCESS/CONVERGED runs:

```bash
python3 db_surrogate/train_gamma_surrogate.py \
  --db /Users/jdominsk/Documents/Projects/AIML_database/Datamak/gyrokinetic_simulations.db
```

Name the surrogate (recommended):

```bash
python3 db_surrogate/train_gamma_surrogate.py --name gamma_v1
```

Select a target (mapsto):

```bash
python3 db_surrogate/train_gamma_surrogate.py --mapsto gk_run.ky_abs_mean
python3 db_surrogate/train_gamma_surrogate.py --mapsto gk_run.diffusion
```

Include all statuses (no status filtering):

```bash
python3 db_surrogate/train_gamma_surrogate.py --statuses ALL
```

Optional filters:

```bash
python3 db_surrogate/train_gamma_surrogate.py --origin "DIII-D"
python3 db_surrogate/train_gamma_surrogate.py --origin-id 3
```

Optional holdout split:

```bash
python3 db_surrogate/train_gamma_surrogate.py --test-size 0.2
```

### Predict

Provide a CSV with the feature columns above. Output adds `pred_gamma_max` and
`pred_gamma_max_std` (tree ensemble uncertainty estimate).

```bash
python3 db_surrogate/predict_gamma_surrogate.py \
  --model db_surrogate/models/gamma_surrogate_YYYYMMDD_HHMMSS.pkl \
  --input-csv new_inputs.csv \
  --output-csv gamma_preds.csv
```

### Estimate all gk_input rows into sg_estimate

```bash
python3 db_surrogate/estimate_gamma_surrogate.py --surrogate-id 1
```

### Notes

- Training uses a RandomForestRegressor for a robust baseline and uncertainty
  estimate via the tree ensemble standard deviation.
- The model is trained on all data by default; use `--test-size` for evaluation.
- The model metadata is saved alongside the pickle as a JSON file.
