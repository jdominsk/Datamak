import contextlib
import io
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from db_surrogate.estimate_gamma_surrogate import main as estimate_main
from db_surrogate.train_gamma_surrogate import DIRECT_COLUMNS, RATIO_INPUT_COLUMNS
from db_surrogate.train_gamma_surrogate import main as train_main


class SurrogateWorkflowTests(unittest.TestCase):
    def test_train_then_estimate_populates_sg_estimate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "surrogate.db"
            model_path = Path(tmpdir) / "unit_surrogate.pkl"
            meta_path = Path(tmpdir) / "unit_surrogate.pkl.json"

            complete_rows = self._create_training_db(db_path)

            with mock.patch.object(
                sys,
                "argv",
                [
                    "train_gamma_surrogate.py",
                    "--db",
                    str(db_path),
                    "--statuses",
                    "SUCCESS",
                    "--mapsto",
                    "gk_run.gamma_max",
                    "--n-estimators",
                    "8",
                    "--min-samples-leaf",
                    "1",
                    "--seed",
                    "7",
                    "--model-out",
                    str(model_path),
                    "--meta-out",
                    str(meta_path),
                ],
            ), contextlib.redirect_stdout(io.StringIO()):
                train_main()

            self.assertTrue(model_path.exists())
            self.assertTrue(meta_path.exists())

            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE gk_surrogate (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        model_path TEXT,
                        meta_path TEXT
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO gk_surrogate (model_path, meta_path) VALUES (?, ?)",
                    (str(model_path), str(meta_path)),
                )
                conn.commit()

            with mock.patch.object(
                sys,
                "argv",
                [
                    "estimate_gamma_surrogate.py",
                    "--db",
                    str(db_path),
                    "--surrogate-id",
                    "1",
                ],
            ), contextlib.redirect_stdout(io.StringIO()):
                estimate_main()

            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT gk_input_id, sg_estimate, sg_quality
                    FROM sg_estimate
                    WHERE gk_surrogate_id = 1
                    ORDER BY gk_input_id
                    """
                ).fetchall()

            self.assertEqual(len(rows), complete_rows)
            self.assertTrue(all(float(row[1]) >= 0.0 for row in rows))
            self.assertTrue(all(float(row[2]) >= 0.0 for row in rows))

    @staticmethod
    def _create_training_db(db_path: Path) -> int:
        cols = DIRECT_COLUMNS + RATIO_INPUT_COLUMNS
        col_def = ", ".join(f"{col} REAL" for col in cols)
        placeholders = ", ".join(["?"] * (len(cols) + 1))
        insert_sql = f"INSERT INTO gk_input (id, {', '.join(cols)}) VALUES ({placeholders})"

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE gk_input (
                    id INTEGER PRIMARY KEY,
                    {col_def}
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gk_run (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gk_input_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    gamma_max REAL
                )
                """
            )

            rows = []
            complete_row_count = 0
            for i in range(1, 8):
                values = {
                    "electron_tprim": 1.0 + 0.01 * i,
                    "electron_fprim": 0.8 + 0.01 * i,
                    "ion_tprim": 1.2 + 0.02 * i,
                    "ion_fprim": 0.9 + 0.01 * i,
                    "ion_vnewk": 0.02 + 0.001 * i,
                    "electron_vnewk": 0.03 + 0.001 * i,
                    "Rmaj": 1.4 + 0.01 * i,
                    "qinp": 1.7 + 0.02 * i,
                    "shat": 0.5 + 0.01 * i,
                    "shift": 0.1 + 0.01 * i,
                    "akappa": 1.5 + 0.01 * i,
                    "akappri": 0.2 + 0.01 * i,
                    "tri": 0.25 + 0.01 * i,
                    "tripri": 0.15 + 0.005 * i,
                    "betaprim": 0.01 + 0.001 * i,
                    "ion_temp": 1.5 + 0.05 * i,
                    "electron_temp": 1.0 + 0.02 * i,
                    "ion_dens": 1.4 + 0.03 * i,
                    "electron_dens": 1.1 + 0.02 * i,
                    "ion_mass": 2.0,
                    "electron_mass": 1.0,
                }
                # Make one row incomplete to verify estimate stage filtering.
                if i == 7:
                    values["electron_temp"] = None
                else:
                    complete_row_count += 1
                rows.append((i, *[values[col] for col in cols]))

            conn.executemany(insert_sql, rows)
            conn.executemany(
                """
                INSERT INTO gk_run (gk_input_id, status, gamma_max)
                VALUES (?, 'SUCCESS', ?)
                """,
                [(i, 0.1 * i + 0.4) for i in range(1, 8)],
            )
            conn.commit()

        return complete_row_count


if __name__ == "__main__":
    unittest.main()
