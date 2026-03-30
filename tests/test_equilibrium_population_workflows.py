import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from db_update.populate_data_equil_from_Alexei_Transp_09 import (
    get_or_create_origin_id as get_or_create_alexei_origin_id,
)
from db_update.populate_data_equil_from_Alexei_Transp_09 import (
    insert_equil_rows,
    list_remote_cdf_files,
    parse_transpfile,
)
from db_update.populate_data_equil_from_Mate_KinEFIT import (
    build_pairs,
    get_or_create_origin_id as get_or_create_mate_origin_id,
)
from db_update.populate_data_equil_from_Mate_KinEFIT import insert_pairs
from db_update.Transp_full_auto.build_flux_equil_inputs import (
    canonicalize_flux_gk_models,
    create_gk_inputs,
    populate_equil_and_timeseries,
)
from db_update.Transp_full_auto.sync_flux_equil_inputs_to_main import (
    build_flux_to_main_gk_model_id_map,
)
from database.create_gyrokinetic_db import ensure_gk_input_status_allows_new


def _create_min_equil_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE data_origin (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            origin TEXT NOT NULL,
            copy TEXT NOT NULL,
            file_type TEXT NOT NULL,
            tokamak TEXT NOT NULL DEFAULT 'NSTX'
        )
        """
    )
    conn.execute(
        """
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
            shot_variant TEXT,
            active INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()


class EquilibriumPopulationWorkflowTests(unittest.TestCase):
    def test_flux_gk_model_duplicates_are_canonicalized(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                """
                CREATE TABLE gk_model (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gk_code_id INTEGER NOT NULL,
                    is_linear INTEGER NOT NULL DEFAULT 1,
                    is_adiabatic INTEGER NOT NULL DEFAULT 0,
                    is_electrostatic INTEGER NOT NULL DEFAULT 0,
                    input_template TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gk_input (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gk_study_id INTEGER NOT NULL,
                    gk_model_id INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    content TEXT NOT NULL,
                    psin REAL NOT NULL,
                    status TEXT NOT NULL,
                    comment TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                INSERT INTO gk_model (
                    id, gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template, active
                ) VALUES
                    (1, 1, 1, 1, 1, 'gx_template_miller_linear_adiabe.in', 1),
                    (6, 1, 1, 1, 1, 'gx_template_miller_linear_adiabe.in', 1)
                """
            )
            conn.execute(
                """
                INSERT INTO gk_input (
                    id, gk_study_id, gk_model_id, file_name, file_path, content, psin, status, comment
                ) VALUES
                    (10, 20, 1, '', '', '', 0.5, 'NEW', ''),
                    (11, 20, 6, 'gx.in', '/tmp/gx.in', 'content', 0.5, 'WAIT', 'done')
                """
            )
            conn.commit()

            remapped = canonicalize_flux_gk_models(conn)
            conn.commit()

            self.assertEqual(remapped, 1)
            model_ids = [
                int(row[0]) for row in conn.execute("SELECT id FROM gk_model ORDER BY id").fetchall()
            ]
            self.assertEqual(model_ids, [1])
            rows = conn.execute(
                """
                SELECT gk_model_id, file_name, file_path, content, status, comment
                FROM gk_input
                ORDER BY id
                """
            ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(
                (
                    int(rows[0][0]),
                    str(rows[0][1]),
                    str(rows[0][2]),
                    str(rows[0][3]),
                    str(rows[0][4]),
                    str(rows[0][5]),
                ),
                (1, "gx.in", "/tmp/gx.in", "content", "WAIT", "done"),
            )
        finally:
            conn.close()

    def test_sync_maps_flux_gk_model_ids_by_signature(self) -> None:
        flux = sqlite3.connect(":memory:")
        main = sqlite3.connect(":memory:")
        try:
            for conn in (flux, main):
                conn.execute(
                    """
                    CREATE TABLE gk_model (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gk_code_id INTEGER NOT NULL,
                        is_linear INTEGER NOT NULL DEFAULT 1,
                        is_adiabatic INTEGER NOT NULL DEFAULT 0,
                        is_electrostatic INTEGER NOT NULL DEFAULT 0,
                        input_template TEXT NOT NULL,
                        active INTEGER NOT NULL DEFAULT 1
                    )
                    """
                )
            main.execute(
                """
                INSERT INTO gk_model (
                    id, gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template, active
                ) VALUES (1, 1, 1, 1, 1, 'gx_template_miller_linear_adiabe.in', 1)
                """
            )
            flux.execute(
                """
                INSERT INTO gk_model (
                    id, gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template, active
                ) VALUES (6, 1, 1, 1, 1, 'gx_template_miller_linear_adiabe.in', 1)
                """
            )
            flux.commit()
            main.commit()

            mapping = build_flux_to_main_gk_model_id_map(flux, main)

            self.assertEqual(mapping, {6: 1})
        finally:
            flux.close()
            main.close()

    def test_flux_create_gk_inputs_repairs_legacy_status_constraint(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE data_origin (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    origin TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE data_equil (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_origin_id INTEGER NOT NULL,
                    folder_path TEXT,
                    transpfile TEXT,
                    shot_time REAL,
                    active INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gk_code (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gk_model (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gk_code_id INTEGER NOT NULL,
                    is_linear INTEGER NOT NULL DEFAULT 1,
                    is_adiabatic INTEGER NOT NULL DEFAULT 0,
                    is_electrostatic INTEGER NOT NULL DEFAULT 0,
                    input_template TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gk_study (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_equil_id INTEGER NOT NULL,
                    gk_code_id INTEGER NOT NULL,
                    comment TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE gk_input (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gk_study_id INTEGER NOT NULL,
                    gk_model_id INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    content TEXT NOT NULL,
                    psin REAL NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('WAIT', 'TORUN', 'BATCH', 'CRASHED', 'SUCCESS', 'ERROR')),
                    comment TEXT NOT NULL DEFAULT '',
                    geo_option TEXT,
                    rhoc REAL,
                    Rmaj REAL,
                    R_geo REAL,
                    qinp REAL,
                    shat REAL,
                    shift REAL,
                    akappa REAL,
                    akappri REAL,
                    tri REAL,
                    tripri REAL,
                    betaprim REAL,
                    beta REAL,
                    electron_z REAL,
                    electron_mass REAL,
                    electron_dens REAL,
                    electron_temp REAL,
                    electron_temp_ev REAL,
                    electron_tprim REAL,
                    electron_fprim REAL,
                    electron_vnewk REAL,
                    ion_z REAL,
                    ion_mass REAL,
                    ion_dens REAL,
                    ion_temp REAL,
                    ion_temp_ev REAL,
                    ion_tprim REAL,
                    ion_fprim REAL,
                    ion_vnewk REAL,
                    creation_date TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                "INSERT INTO data_origin (id, name, origin) VALUES (4, 'Transp 10 (full-auto)', '/remote/transp')"
            )
            conn.execute(
                "INSERT INTO data_equil (id, data_origin_id, folder_path, transpfile, shot_time, active) VALUES (10, 4, '/remote/transp', '123456A01.CDF', 1.2, 1)"
            )
            conn.execute("INSERT INTO gk_code (id, name) VALUES (1, 'GX')")
            conn.execute(
                """
                INSERT INTO gk_model (
                    id, gk_code_id, is_linear, is_adiabatic, is_electrostatic, input_template, active
                )
                VALUES (1, 1, 1, 1, 1, 'gx_template.in', 1)
                """
            )
            conn.execute(
                "INSERT INTO gk_study (id, data_equil_id, gk_code_id, comment) VALUES (20, 10, 1, 'demo')"
            )

            ensure_gk_input_status_allows_new(conn)
            created = create_gk_inputs(conn, 4, [0.5], "NEW")

            self.assertEqual(created, 1)
            row = conn.execute(
                "SELECT gk_study_id, gk_model_id, psin, status FROM gk_input"
            ).fetchone()
            self.assertEqual((int(row[0]), int(row[1]), float(row[2]), str(row[3])), (20, 1, 0.5, "NEW"))
        finally:
            conn.close()

    def test_flux_populate_skips_unreadable_cdf_and_continues(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE data_origin (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    origin TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE data_equil (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_origin_id INTEGER NOT NULL,
                    folder_path TEXT,
                    pfile TEXT,
                    pfile_content TEXT,
                    gfile TEXT,
                    gfile_content TEXT,
                    transpfile TEXT,
                    shot_time REAL,
                    shot_number TEXT,
                    shot_variant TEXT,
                    active INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                INSERT INTO data_origin (name, origin)
                VALUES ('Transp 10 (full-auto)', '/remote/transp')
                """
            )
            conn.commit()

            with mock.patch(
                "db_update.Transp_full_auto.build_flux_equil_inputs.list_cdf_files",
                return_value=["139048Z04.CDF", "139048Z05.CDF"],
            ), mock.patch(
                "db_update.Transp_full_auto.build_flux_equil_inputs.read_time_array",
                side_effect=[
                    PermissionError("[Errno 13] Permission denied"),
                    [1.1, 1.2],
                ],
            ), mock.patch(
                "db_update.Transp_full_auto.build_flux_equil_inputs.random.choice",
                return_value=1.1,
            ):
                inserted_equil, inserted_ts, skipped_files = populate_equil_and_timeseries(
                    conn,
                    None,
                    "Transp 10 (full-auto)",
                    "/remote/transp",
                    False,
                )
            conn.commit()

            self.assertEqual(inserted_equil, 1)
            self.assertEqual(inserted_ts, 1)
            self.assertEqual(skipped_files, 1)

            rows = conn.execute(
                """
                SELECT transpfile, shot_number, shot_variant, shot_time, active
                FROM data_equil
                ORDER BY id
                """
            ).fetchall()
            self.assertEqual(
                [(str(r[0]), str(r[1]), str(r[2]), float(r[3]), int(r[4])) for r in rows],
                [("139048Z05.CDF", "139048", "Z05", 1.1, 1)],
            )
        finally:
            conn.close()

    def test_flux_populate_prefers_origin_id_over_stale_name(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE data_origin (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    origin TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE data_equil (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_origin_id INTEGER NOT NULL,
                    folder_path TEXT,
                    pfile TEXT,
                    pfile_content TEXT,
                    gfile TEXT,
                    gfile_content TEXT,
                    transpfile TEXT,
                    shot_time REAL,
                    shot_number TEXT,
                    shot_variant TEXT,
                    active INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                INSERT INTO data_origin (id, name, origin)
                VALUES (4, 'Alexei Transp 10 (full-auto)', '/remote/transp')
                """
            )
            conn.commit()

            with mock.patch(
                "db_update.Transp_full_auto.build_flux_equil_inputs.list_cdf_files",
                return_value=["139048Z05.CDF"],
            ), mock.patch(
                "db_update.Transp_full_auto.build_flux_equil_inputs.read_time_array",
                return_value=[1.1, 1.2],
            ), mock.patch(
                "db_update.Transp_full_auto.build_flux_equil_inputs.random.choice",
                return_value=1.2,
            ):
                inserted_equil, inserted_ts, skipped_files = populate_equil_and_timeseries(
                    conn,
                    4,
                    "Transp 10 (full-auto)",
                    "/remote/transp",
                    False,
                )
            conn.commit()

            self.assertEqual((inserted_equil, inserted_ts, skipped_files), (1, 1, 0))
            row = conn.execute(
                """
                SELECT data_origin_id, transpfile, shot_time, active
                FROM data_equil
                """
            ).fetchone()
            self.assertEqual(
                (int(row[0]), str(row[1]), float(row[2]), int(row[3])),
                (4, "139048Z05.CDF", 1.2, 1),
            )
        finally:
            conn.close()

    def test_mate_build_and_insert_pairs_with_dedup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir) / "shot_a"
            folder.mkdir(parents=True)
            pfile = folder / "p1234"
            gfile = folder / "g1234"
            pfile.write_text("p-content", encoding="utf-8")
            gfile.write_text("g-content", encoding="utf-8")

            pairs = build_pairs(tmpdir)
            self.assertEqual(len(pairs), 1)

            conn = sqlite3.connect(":memory:")
            try:
                _create_min_equil_schema(conn)
                origin_name = "Kinetic EFIT (Mate)"
                origin_id = get_or_create_mate_origin_id(
                    conn,
                    origin_name,
                    "Google drive",
                    tmpdir,
                )
                inserted_first = insert_pairs(conn, origin_id, origin_name, pairs)
                inserted_second = insert_pairs(conn, origin_id, origin_name, pairs)
                conn.commit()

                self.assertEqual(inserted_first, 1)
                self.assertEqual(inserted_second, 0)

                row = conn.execute(
                    """
                    SELECT do.file_type, de.pfile, de.gfile, de.pfile_content, de.gfile_content
                    FROM data_equil AS de
                    JOIN data_origin AS do ON do.id = de.data_origin_id
                    WHERE de.data_origin_id = ?
                    """,
                    (origin_id,),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(str(row[0]), "EFIT")
                self.assertEqual(str(row[1]), "p1234")
                self.assertEqual(str(row[2]), "g1234")
                self.assertEqual(str(row[3]), "p-content")
                self.assertEqual(str(row[4]), "g-content")
            finally:
                conn.close()

    def test_mate_insert_pairs_rejects_cross_folder_pair(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            folder_a = Path(tmpdir) / "a"
            folder_b = Path(tmpdir) / "b"
            folder_a.mkdir(parents=True)
            folder_b.mkdir(parents=True)
            pfile = folder_a / "p1"
            gfile = folder_b / "g1"
            pfile.write_text("p", encoding="utf-8")
            gfile.write_text("g", encoding="utf-8")

            conn = sqlite3.connect(":memory:")
            try:
                _create_min_equil_schema(conn)
                origin_id = get_or_create_mate_origin_id(
                    conn,
                    "Kinetic EFIT (Mate)",
                    "Google drive",
                    tmpdir,
                )
                with self.assertRaises(SystemExit):
                    insert_pairs(
                        conn,
                        origin_id,
                        "Kinetic EFIT (Mate)",
                        [(str(pfile), str(gfile))],
                    )
            finally:
                conn.close()

    def test_alexei_parse_and_insert_with_dedup(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            _create_min_equil_schema(conn)
            origin_id = get_or_create_alexei_origin_id(
                conn,
                "Transp 09 (semi-auto)",
                "/remote/transp",
                "/local/copy",
            )
            files = ["204118A05.CDF", "204118A06.CDF"]
            inserted_first = insert_equil_rows(conn, origin_id, "/remote/transp", files)
            inserted_second = insert_equil_rows(conn, origin_id, "/remote/transp", files)
            conn.commit()

            self.assertEqual(inserted_first, 2)
            self.assertEqual(inserted_second, 0)

            rows = conn.execute(
                """
                SELECT do.file_type, de.transpfile, de.shot_number, de.shot_variant, de.active
                FROM data_equil AS de
                JOIN data_origin AS do ON do.id = de.data_origin_id
                WHERE de.data_origin_id = ?
                ORDER BY de.transpfile
                """,
                (origin_id,),
            ).fetchall()
            self.assertEqual(
                [(str(r[0]), str(r[1]), str(r[2]), str(r[3]), int(r[4])) for r in rows],
                [
                    ("TRANSP", "204118A05.CDF", "204118", "A05", 0),
                    ("TRANSP", "204118A06.CDF", "204118", "A06", 0),
                ],
            )
            self.assertEqual(parse_transpfile("204118A05.CDF"), ("204118", "A05"))
        finally:
            conn.close()

    def test_alexei_remote_listing_returns_basenames(self) -> None:
        mocked = subprocess.CompletedProcess(
            args=["ssh"],
            returncode=0,
            stdout="/path/one/204118A05.CDF\n/path/two/204118A06.CDF\n",
            stderr="",
        )
        with mock.patch(
            "db_update.populate_data_equil_from_Alexei_Transp_09.subprocess.run",
            return_value=mocked,
        ):
            files = list_remote_cdf_files("operator@flux", "/remote/transp")
        self.assertEqual(files, ["204118A05.CDF", "204118A06.CDF"])


if __name__ == "__main__":
    unittest.main()
