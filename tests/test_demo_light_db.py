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


class DemoLightDatabaseTests(unittest.TestCase):
    def test_create_demo_light_db_blanks_only_no_run_content(self) -> None:
        module = load_module(
            "create_demo_light_db_module",
            PROJECT_ROOT / "demo" / "create_demo_light_db.py",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            source_db = Path(tmpdir) / "demo.db"
            dest_db = Path(tmpdir) / "demo_light.db"
            with sqlite3.connect(source_db) as conn:
                conn.executescript(
                    """
                    CREATE TABLE gk_input (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        content TEXT NOT NULL
                    );
                    CREATE TABLE gk_run (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        gk_input_id INTEGER
                    );
                    """
                )
                conn.executemany(
                    "INSERT INTO gk_input (id, content) VALUES (?, ?)",
                    [
                        (1, "keep-me"),
                        (2, "blank-me"),
                        (3, ""),
                    ],
                )
                conn.execute("INSERT INTO gk_run (id, gk_input_id) VALUES (?, ?)", (10, 1))
                conn.commit()

            stats = module.create_demo_light_db(str(source_db), str(dest_db))

            self.assertTrue(dest_db.exists())
            self.assertEqual(stats["total_no_run_rows"], 2)
            self.assertEqual(stats["nonempty_before"], 1)
            self.assertEqual(stats["nonempty_after"], 0)
            self.assertGreater(stats["bytes_before"], 0)
            self.assertEqual(stats["bytes_after"], 0)

            with sqlite3.connect(dest_db) as conn:
                rows = conn.execute(
                    "SELECT id, content FROM gk_input ORDER BY id"
                ).fetchall()
                self.assertEqual(
                    [(int(row[0]), str(row[1])) for row in rows],
                    [
                        (1, "keep-me"),
                        (2, ""),
                        (3, ""),
                    ],
                )
                integrity = conn.execute("PRAGMA integrity_check").fetchone()
                self.assertEqual(str(integrity[0]).lower(), "ok")


if __name__ == "__main__":
    unittest.main()
