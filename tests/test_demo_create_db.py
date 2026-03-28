import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DemoDatabaseExportTests(unittest.TestCase):
    def test_export_demo_db_keeps_only_selected_origins_and_related_rows(self) -> None:
        module = load_module(
            "create_demo_db_module",
            PROJECT_ROOT / "demo" / "create_demo_db.py",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            source_db = Path(tmpdir) / "source.db"
            dest_db = Path(tmpdir) / "demo.db"
            with sqlite3.connect(source_db) as conn:
                conn.executescript(
                    """
                    CREATE TABLE data_origin (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        origin TEXT NOT NULL,
                        copy TEXT NOT NULL,
                        file_type TEXT NOT NULL,
                        tokamak TEXT NOT NULL DEFAULT 'NSTX',
                        creation_date TEXT NOT NULL DEFAULT (datetime('now')),
                        color TEXT
                    );
                    CREATE TABLE data_equil (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER NOT NULL,
                        folder_path TEXT NOT NULL,
                        pfile TEXT,
                        pfile_content TEXT,
                        gfile TEXT,
                        gfile_content TEXT,
                        transpfile TEXT,
                        shot_number TEXT,
                        shot_time REAL,
                        active INTEGER NOT NULL DEFAULT 0,
                        creation_date TEXT NOT NULL DEFAULT (datetime('now')),
                        shot_variant TEXT,
                        comment TEXT,
                        FOREIGN KEY (data_origin_id) REFERENCES data_origin(id)
                    );
                    CREATE TABLE flux_action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER,
                        data_origin_name TEXT,
                        flux_db_name TEXT NOT NULL,
                        remote_host TEXT NOT NULL,
                        remote_dir TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT (datetime('now')),
                        status TEXT NOT NULL DEFAULT 'STAGED',
                        slurm_job_id TEXT,
                        submitted_at TEXT,
                        synced_at TEXT,
                        status_detail TEXT,
                        status_checked_at TEXT,
                        FOREIGN KEY (data_origin_id) REFERENCES data_origin(id)
                    );
                    CREATE TABLE gk_batch (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        batch_database_name TEXT NOT NULL,
                        remote_folder TEXT NOT NULL,
                        remote_host TEXT,
                        status TEXT NOT NULL
                    );
                    CREATE TABLE gk_code (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        version TEXT NOT NULL,
                        creation_date TEXT NOT NULL DEFAULT (datetime('now'))
                    );
                    CREATE TABLE gk_convergence_timeseries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gk_run_id INTEGER NOT NULL,
                        gk_input_id INTEGER NOT NULL,
                        phi2_tot_f32 BLOB NOT NULL,
                        n_points INTEGER NOT NULL DEFAULT 100,
                        creation_date TEXT NOT NULL DEFAULT (datetime('now'))
                    );
                    CREATE TABLE gk_input (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gk_study_id INTEGER NOT NULL,
                        gk_model_id INTEGER NOT NULL,
                        file_name TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        content TEXT NOT NULL,
                        psin REAL NOT NULL,
                        status TEXT NOT NULL,
                        comment TEXT NOT NULL DEFAULT '',
                        creation_date TEXT NOT NULL DEFAULT (datetime('now')),
                        FOREIGN KEY (gk_study_id) REFERENCES gk_study(id) ON DELETE CASCADE,
                        FOREIGN KEY (gk_model_id) REFERENCES gk_model(id)
                    );
                    CREATE TABLE gk_linear_run (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gk_study_id INTEGER NOT NULL,
                        ky REAL NOT NULL,
                        gamma REAL NOT NULL,
                        omega REAL NOT NULL,
                        status TEXT NOT NULL,
                        creation_date TEXT NOT NULL DEFAULT (datetime('now')),
                        FOREIGN KEY (gk_study_id) REFERENCES gk_study(id) ON DELETE CASCADE
                    );
                    CREATE TABLE gk_model (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        active INTEGER NOT NULL DEFAULT 0,
                        gk_code_id INTEGER NOT NULL,
                        is_linear INTEGER NOT NULL DEFAULT 0,
                        is_adiabatic INTEGER NOT NULL DEFAULT 0,
                        is_electrostatic INTEGER NOT NULL DEFAULT 1,
                        input_template TEXT NOT NULL,
                        creation_date TEXT NOT NULL DEFAULT (datetime('now'))
                    );
                    CREATE TABLE gk_nonlinear_run (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gk_study_id INTEGER NOT NULL,
                        t_min REAL NOT NULL,
                        t_max REAL NOT NULL,
                        status TEXT NOT NULL,
                        creation_date TEXT NOT NULL DEFAULT (datetime('now')),
                        FOREIGN KEY (gk_study_id) REFERENCES gk_study(id) ON DELETE CASCADE
                    );
                    CREATE TABLE gk_run (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        remote_id INTEGER NOT NULL,
                        gk_input_id INTEGER,
                        gk_batch_id INTEGER,
                        input_folder TEXT,
                        job_folder TEXT,
                        archive_folder TEXT,
                        input_name TEXT,
                        nb_nodes INTEGER,
                        job_id TEXT,
                        status TEXT,
                        input_content TEXT,
                        remote_host TEXT,
                        remote_folder TEXT,
                        creation_date TEXT NOT NULL DEFAULT (datetime('now')),
                        t_max REAL,
                        ky_abs_mean REAL,
                        gamma_max REAL,
                        diffusion REAL,
                        nb_restart INTEGER
                    );
                    CREATE TABLE gk_study (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_equil_id INTEGER NOT NULL,
                        gk_code_id INTEGER NOT NULL,
                        comment TEXT NOT NULL,
                        creation_date TEXT NOT NULL DEFAULT (datetime('now')),
                        FOREIGN KEY (data_equil_id) REFERENCES data_equil(id),
                        FOREIGN KEY (gk_code_id) REFERENCES gk_code(id)
                    );
                    CREATE TABLE gk_surrogate (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        origin_id INTEGER,
                        origin_name TEXT
                    );
                    CREATE TABLE sg_estimate (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gk_surrogate_id INTEGER NOT NULL,
                        gk_input_id INTEGER NOT NULL,
                        sg_estimate REAL,
                        sg_quality REAL,
                        UNIQUE (gk_surrogate_id, gk_input_id)
                    );
                    CREATE TABLE transp_timeseries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER NOT NULL,
                        shot_number TEXT NOT NULL,
                        shot_variant TEXT,
                        time_array TEXT NOT NULL,
                        creation_date TEXT NOT NULL DEFAULT (datetime('now')),
                        FOREIGN KEY (data_origin_id) REFERENCES data_origin(id)
                    );
                    CREATE INDEX idx_gk_run_input ON gk_run(gk_input_id);
                    """
                )
                conn.executemany(
                    """
                    INSERT INTO data_origin (id, name, origin, copy, file_type, color)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (1, "Kinetic EFIT (Mate)", "Google drive", "Google drive", "EFIT", "#1f77b4"),
                        (2, "Transp 09 (semi-auto)", "/p/transparch/result/NSTX/09", "/copy/09", "TRANSP", "#d62728"),
                        (3, "Transp 09 (full-auto) NEW", "/p/transparch/result/NSTX/09", "/copy/09", "TRANSP", "#2ca02c"),
                    ],
                )
                conn.execute(
                    "INSERT INTO gk_code (id, name, version) VALUES (1, 'GX', '0.0')"
                )
                conn.execute(
                    """
                    INSERT INTO gk_model (
                        id, active, gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template
                    ) VALUES (1, 1, 1, 1, 1, 1, 'gx_template.in')
                    """
                )
                conn.executemany(
                    """
                    INSERT INTO data_equil (
                        id, data_origin_id, folder_path, transpfile, shot_number, shot_variant, shot_time, active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (10, 1, "/eq/1", "eq1.CDF", "1", "A01", 0.1, 1),
                        (20, 2, "/eq/2", "eq2.CDF", "2", "A02", 0.2, 1),
                        (30, 3, "/eq/3", "eq3.CDF", "3", "A03", 0.3, 1),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO transp_timeseries (id, data_origin_id, shot_number, shot_variant, time_array)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (100, 1, "1", "A01", "[0.1]"),
                        (200, 2, "2", "A02", "[0.2]"),
                        (300, 3, "3", "A03", "[0.3]"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO gk_study (id, data_equil_id, gk_code_id, comment)
                    VALUES (?, ?, ?, ?)
                    """,
                    [
                        (1000, 10, 1, "s1"),
                        (2000, 20, 1, "s2"),
                        (3000, 30, 1, "s3"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO gk_input (
                        id, gk_study_id, gk_model_id, file_name, file_path, content, psin, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (1100, 1000, 1, "i1", "/i1", "c1", 0.3, "SUCCESS"),
                        (2200, 2000, 1, "i2", "/i2", "c2", 0.4, "SUCCESS"),
                        (3300, 3000, 1, "i3", "/i3", "c3", 0.5, "WAIT"),
                        (3301, 3000, 1, "i4", "/i4", "c4", 0.6, "WAIT"),
                        (3302, 3000, 1, "i5", "/i5", "c5", 0.7, "WAIT"),
                        (3303, 3000, 1, "i6", "/i6", "c6", 0.8, "ERROR"),
                        (3304, 3000, 1, "i7", "/i7", "c7", 0.9, "ERROR"),
                        (3305, 3000, 1, "i8", "/i8", "c8", 1.0, "CRASHED"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO gk_batch (id, batch_database_name, remote_folder, remote_host, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (700, "batch700.db", "/remote/700", "host", "SYNCED"),
                        (800, "batch800.db", "/remote/800", "host", "SYNCED"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO gk_run (id, remote_id, gk_input_id, gk_batch_id, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (7100, 1, 1100, 700, "SUCCESS"),
                        (7200, 2, 2200, 800, "SUCCESS"),
                        (7300, 3, 3300, 700, "RUNNING"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO gk_convergence_timeseries (id, gk_run_id, gk_input_id, phi2_tot_f32)
                    VALUES (?, ?, ?, ?)
                    """,
                    [
                        (9001, 7100, 1100, b"\x00"),
                        (9002, 7200, 2200, b"\x00"),
                        (9003, 7300, 3300, b"\x00"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO gk_linear_run (id, gk_study_id, ky, gamma, omega, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (9101, 1000, 0.1, 0.2, 0.3, "SUCCESS"),
                        (9102, 2000, 0.1, 0.2, 0.3, "SUCCESS"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO gk_nonlinear_run (id, gk_study_id, t_min, t_max, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (9201, 3000, 0.0, 1.0, "RUNNING"),
                        (9202, 2000, 0.0, 1.0, "SUCCESS"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO gk_surrogate (id, name, origin_id, origin_name)
                    VALUES (?, ?, ?, ?)
                    """,
                    [
                        (501, "sg-1", 3, "Transp 09 (full-auto) NEW"),
                        (502, "sg-2", 2, "Transp 09 (semi-auto)"),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO sg_estimate (id, gk_surrogate_id, gk_input_id, sg_estimate, sg_quality)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (601, 501, 3300, 1.2, 0.8),
                        (603, 501, 3302, 1.4, 0.6),
                        (602, 502, 2200, 2.3, 0.7),
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO flux_action_log (
                        id, data_origin_id, data_origin_name, flux_db_name, remote_host, remote_dir
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (801, 1, "Kinetic EFIT (Mate)", "flux1.db", "flux", "/u/demo"),
                        (802, 2, "Transp 09 (semi-auto)", "flux2.db", "flux", "/u/demo"),
                        (803, 3, "Transp 09 (full-auto) NEW", "flux3.db", "flux", "/u/demo"),
                    ],
                )
                conn.commit()

            copied = module.export_demo_db(str(source_db), str(dest_db), [1, 3])

            self.assertTrue(dest_db.exists())
            self.assertEqual(copied["data_origin"], 2)
            self.assertEqual(copied["data_equil"], 2)
            self.assertEqual(copied["gk_study"], 2)
            self.assertEqual(copied["gk_input"], 5)
            self.assertEqual(copied["gk_run"], 2)

            with sqlite3.connect(dest_db) as conn:
                transp_rows = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM sqlite_master
                    WHERE type = 'table' AND name = 'transp_timeseries'
                    """
                ).fetchone()
                self.assertEqual(int(transp_rows[0]), 0)
                origin_ids = [
                    int(row[0])
                    for row in conn.execute("SELECT id FROM data_origin ORDER BY id").fetchall()
                ]
                self.assertEqual(origin_ids, [1, 3])
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM data_equil WHERE data_origin_id = 2").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM gk_input WHERE id = 2200").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM gk_input WHERE id = 3301").fetchone()[0],
                    1,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM gk_input WHERE id = 3302").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM gk_input WHERE id = 3303").fetchone()[0],
                    1,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM gk_input WHERE id = 3304").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM gk_input WHERE id = 3305").fetchone()[0],
                    1,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM gk_batch WHERE id = 800").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM gk_surrogate WHERE id = 502").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM sg_estimate WHERE id = 602").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM sg_estimate WHERE id = 603").fetchone()[0],
                    0,
                )
                integrity = conn.execute("PRAGMA integrity_check").fetchone()
                self.assertEqual(str(integrity[0]).lower(), "ok")


if __name__ == "__main__":
    unittest.main()
