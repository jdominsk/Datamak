import os
import sqlite3
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class MultiUserShellWrapperTests(unittest.TestCase):
    def test_mainsteps_1_generates_flux_runtime_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            script_path = root / "db_update" / "Transp_full_auto" / "MainSteps_1_launch_on_laptop.sh"
            script_path.parent.mkdir(parents=True)
            script_path.write_text(
                (
                    PROJECT_ROOT
                    / "db_update"
                    / "Transp_full_auto"
                    / "MainSteps_1_launch_on_laptop.sh"
                ).read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            self._write_file(
                root / "tools" / "resolve_dtwin_env.py",
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    import sys

                    print("export DTWIN_FLUX_USER=fluxuser")
                    print("export DTWIN_FLUX_HOST=flux.example.org")
                    print("export DTWIN_FLUX_REMOTE=fluxuser@flux.example.org")
                    print("export DTWIN_FLUX_BASE_DIR=/u/fluxuser/DTwin/transp_full_auto")
                    print("export DTWIN_FLUX_PYTHON=/u/fluxuser/venv/bin/python")
                    """
                ),
                executable=True,
            )
            self._write_file(
                root / "tools" / "ssh_with_duo.py",
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    import os
                    import subprocess
                    import sys

                    args = sys.argv[1:]
                    if "--" in args:
                        args = args[args.index("--") + 1 :]
                    raise SystemExit(subprocess.run(args).returncode)
                    """
                ),
                executable=True,
            )
            self._write_file(
                root / "db_update" / "Transp_full_auto" / "build_flux_equil_inputs.py",
                textwrap.dedent(
                    """\
                    #!/usr/bin/env python3
                    import argparse
                    import sqlite3
                    from pathlib import Path

                    parser = argparse.ArgumentParser()
                    parser.add_argument("--db")
                    parser.add_argument("--out-dir")
                    args = parser.parse_args()
                    out_dir = Path(args.out_dir)
                    out_dir.mkdir(parents=True, exist_ok=True)
                    db_path = out_dir / "flux_equil_inputs_20260326_000000.db"
                    sqlite3.connect(db_path).close()
                    """
                ),
                executable=True,
            )
            self._write_file(
                root / "db_update" / "Transp_full_auto" / "MainSteps_2_launch_on_flux.sh",
                "#!/bin/bash\n",
                executable=True,
            )
            self._write_file(root / "db_update" / "Transp_full_auto" / "flux" / "placeholder.txt", "ok\n")
            self._write_file(root / "pyrokinetics" / "template.in", "template\n")

            db_path = root / "gyrokinetic_simulations.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE data_origin (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        file_type TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE flux_action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_origin_id INTEGER,
                        data_origin_name TEXT,
                        flux_db_name TEXT,
                        remote_host TEXT,
                        remote_dir TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO data_origin (name, file_type)
                    VALUES ('Transp 09 (full-auto)', 'TRANSP')
                    """
                )
                conn.commit()

            bin_dir = root / "bin"
            self._write_file(
                bin_dir / "ssh",
                "#!/bin/bash\nexit 0\n",
                executable=True,
            )
            self._write_file(
                bin_dir / "rsync",
                textwrap.dedent(
                    """\
                    #!/bin/bash
                    set -euo pipefail
                    log_file="${MOCK_RSYNC_LOG:?}"
                    printf '%s\n' "$*" >> "$log_file"
                    args=()
                    skip_next=0
                    for arg in "$@"; do
                      if [ "$skip_next" -eq 1 ]; then
                        skip_next=0
                        continue
                      fi
                      case "$arg" in
                        -a|-v) continue ;;
                        -av) continue ;;
                        -e) skip_next=1; continue ;;
                      esac
                      args+=("$arg")
                    done
                    if [ "${#args[@]}" -ge 2 ]; then
                      dest="${args[${#args[@]}-1]}"
                      src="${args[${#args[@]}-2]}"
                      if [[ "$dest" != *:* ]]; then
                        mkdir -p "$dest"
                        if [[ "$src" == *"*"* ]]; then
                          eval "cp $src \"$dest\""
                        else
                          cp -R "$src" "$dest"
                        fi
                      fi
                    fi
                    """
                ),
                executable=True,
            )

            rsync_log = root / "mock_rsync.log"
            env = os.environ.copy()
            env["DTWIN_ROOT"] = str(root)
            env["PATH"] = f"{bin_dir}:{env['PATH']}"
            env["MOCK_RSYNC_LOG"] = str(rsync_log)

            subprocess.run(["bash", str(script_path)], check=True, env=env, cwd=root)

            runtime_env = root / "tmp" / "transp_full_auto" / "datamak_runtime.env"
            self.assertTrue(runtime_env.exists())
            content = runtime_env.read_text(encoding="utf-8")
            self.assertIn("export DTWIN_FLUX_REMOTE=fluxuser@flux.example.org", content)
            self.assertIn("export DTWIN_FLUX_BASE_DIR=/u/fluxuser/DTwin/transp_full_auto", content)
            self.assertIn("export DTWIN_FLUX_PYTHON=/u/fluxuser/venv/bin/python", content)

            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT data_origin_name, flux_db_name, remote_host, remote_dir
                    FROM flux_action_log
                    """
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "Transp 09 (full-auto)")
            self.assertEqual(str(row[1]), "flux_equil_inputs_20260326_000000.db")
            self.assertEqual(str(row[2]), "fluxuser@flux.example.org")
            self.assertEqual(str(row[3]), "/u/fluxuser/DTwin/transp_full_auto")
            self.assertIn("fluxuser@flux.example.org:/u/fluxuser/DTwin/transp_full_auto/", rsync_log.read_text(encoding="utf-8"))

    def test_run_gk_inputs_local_sources_runtime_env_python(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            flux_dir = root / "db_update" / "Transp_full_auto" / "flux"
            flux_dir.mkdir(parents=True)
            script_path = flux_dir / "run_gk_inputs_local.sh"
            script_path.write_text(
                (
                    PROJECT_ROOT
                    / "db_update"
                    / "Transp_full_auto"
                    / "flux"
                    / "run_gk_inputs_local.sh"
                ).read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            self._write_file(flux_dir / "run_flux_gk_inputs.py", "print('ok')\n")
            self._write_file(
                root / "db_update" / "Transp_full_auto" / "datamak_runtime.env",
                f"export DTWIN_FLUX_PYTHON={root / 'fake-flux-python'}\n",
            )

            bin_dir = root / "bin"
            python_log = root / "python.log"
            sqlite_counter = root / "sqlite_counter.txt"
            sqlite_counter.write_text("0", encoding="utf-8")
            self._write_file(
                bin_dir / "sqlite3",
                textwrap.dedent(
                    f"""\
                    #!/bin/bash
                    set -euo pipefail
                    counter_file="{sqlite_counter}"
                    count="$(cat "$counter_file")"
                    if [ "$count" = "0" ]; then
                      echo 1
                      echo 1 > "$counter_file"
                    else
                      echo 0
                    fi
                    """
                ),
                executable=True,
            )
            self._write_file(
                root / "fake-flux-python",
                textwrap.dedent(
                    f"""\
                    #!/bin/bash
                    printf '%s\\n' "$@" >> "{python_log}"
                    """
                ),
                executable=True,
            )

            db_path = root / "flux_equil_inputs.db"
            db_path.write_text("", encoding="utf-8")
            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}:{env['PATH']}"

            subprocess.run(
                ["bash", str(script_path), str(db_path), "1", "1"],
                check=True,
                env=env,
                cwd=root,
            )

            log_text = python_log.read_text(encoding="utf-8")
            self.assertIn(str(flux_dir / "run_flux_gk_inputs.py"), log_text)
            self.assertIn("--db", log_text)
            self.assertIn(str(db_path), log_text)

    def test_slurm_wrappers_anchor_to_submit_dir_and_keep_directives_first(self) -> None:
        wrapper_paths = [
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "flux" / "run_gk_inputs_slurm.sh",
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "flux" / "run_mainsteps2_slurm.sh",
        ]
        for path in wrapper_paths:
            text = path.read_text(encoding="utf-8")
            self.assertIn('ROOT_DIR="${SLURM_SUBMIT_DIR:-}"', text)
            self.assertIn("set +eu", text)
            self.assertIn("set -euo pipefail", text)
            self.assertIn('echo "[wrapper] ROOT_DIR=${ROOT_DIR}"', text)
            lines = [line for line in text.splitlines() if line.strip()]
            self.assertGreaterEqual(len(lines), 2)
            self.assertTrue(lines[0].startswith("#!/bin/bash"))
            self.assertTrue(lines[1].startswith("#SBATCH "))

    def test_mainsteps_2_caps_generated_rows_per_job_by_default(self) -> None:
        text = (
            PROJECT_ROOT / "db_update" / "Transp_full_auto" / "MainSteps_2_launch_on_flux.sh"
        ).read_text(encoding="utf-8")
        self.assertIn('MAX_ROWS_PER_JOB="${MAX_ROWS_PER_JOB:-1000}"', text)
        self.assertIn('--max-rows "${MAX_ROWS_PER_JOB}"', text)

    @staticmethod
    def _write_file(path: Path, content: str, executable: bool = False) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        if executable:
            path.chmod(0o755)
