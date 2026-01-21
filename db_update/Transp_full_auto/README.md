Transp Full Auto (On-Flux Workflow)

Purpose:
Run the full-auto Transp -> GK input generation entirely on Flux using a
temporary database. Only the final data products are copied back to the
local main DB.

Intended flow:
1) Launch on laptop: `MainSteps_1_launch_on_laptop.sh`
   - Build temp DB.
   - Copy templates locally.
   - Rsync temp DB + templates to Flux.
2) Launch on Flux: `MainSteps_2_launch_on_flux.sh /path/to/flux_equil_inputs_TIMESTAMP.db`
   - Use `build_flux_equil_inputs.py` to populate `data_equil` + `transp_timeseries`,
     set `shot_time`, `active=1`, and create `gk_study` rows.
   - Create `gk_input` rows.
   - Run Pyrokinetics to fill `gk_input.content`.
3) Launch on laptop: `MainSteps_3_launch_on_laptop.sh [local_db_path]`
   - Rsync the Flux DB back locally.
   - Sync into the main DB and backfill physics columns.

Planned next steps:
- Select random time per `data_equil` and create `gk_study` + `gk_input`.
- Run Pyrokinetics on Flux to generate GX inputs for those rows.

Notes:
- This folder is for the "minimize back-and-forth" workflow.
- Scripts should assume Flux-local paths and avoid copying CDF files locally.
