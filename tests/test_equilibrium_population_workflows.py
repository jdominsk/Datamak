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


def _create_min_equil_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE data_origin (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            origin TEXT NOT NULL,
            copy TEXT NOT NULL,
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
                origin_name = "Mate Kinetic EFIT"
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
                    SELECT pfile, gfile, pfile_content, gfile_content
                    FROM data_equil
                    WHERE data_origin_id = ?
                    """,
                    (origin_id,),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(str(row[0]), "p1234")
                self.assertEqual(str(row[1]), "g1234")
                self.assertEqual(str(row[2]), "p-content")
                self.assertEqual(str(row[3]), "g-content")
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
                    "Mate Kinetic EFIT",
                    "Google drive",
                    tmpdir,
                )
                with self.assertRaises(SystemExit):
                    insert_pairs(
                        conn,
                        origin_id,
                        "Mate Kinetic EFIT",
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
                "Alexei Transp 09 (semi-auto)",
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
                SELECT transpfile, shot_number, shot_variant, active
                FROM data_equil
                WHERE data_origin_id = ?
                ORDER BY transpfile
                """,
                (origin_id,),
            ).fetchall()
            self.assertEqual(
                [(str(r[0]), str(r[1]), str(r[2]), int(r[3])) for r in rows],
                [
                    ("204118A05.CDF", "204118", "A05", 0),
                    ("204118A06.CDF", "204118", "A06", 0),
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
            files = list_remote_cdf_files("jdominsk@flux", "/remote/transp")
        self.assertEqual(files, ["204118A05.CDF", "204118A06.CDF"])


if __name__ == "__main__":
    unittest.main()
