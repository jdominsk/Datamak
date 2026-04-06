Run unit tests from the project root:

```bash
.venv/bin/python -m unittest discover -s tests -p "test_*.py" -v
```

Centralized run with reports (recommended):

```bash
.venv/bin/python tests/run_tests.py
```

This writes:
- `tests/reports/unit_test_report_<timestamp>.txt`
- `tests/reports/unit_test_report_<timestamp>.json`
- `tests/reports/unit_test_report_latest.txt`
- `tests/reports/unit_test_report_latest.json`

Current coverage includes:
- database schema creation/idempotence/seeding (`database/create_gyrokinetic_db.py`)
- equilibrium population workflows from Mate/Alexei inputs (`db_update/populate_data_equil_from_Mate_KinEFIT.py`, `db_update/populate_data_equil_from_Alexei_Transp_09.py`)
- batch workflow transitions for TORUN -> BATCH + `gk_run` copy (`batch/create_batch_database.py`)
- batch remote sync orchestration with mocked SSH (`batch/check_launched_batches.py`)
- GUI workflow status transitions via Flask routes (`/update_status`, `/update_status_bulk`)
- surrogate workflow: training + estimate population (`db_surrogate/train_gamma_surrogate.py`, `db_surrogate/estimate_gamma_surrogate.py`)
