import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from dtwin_config import (
    get_user_config_path,
    load_gui_workflow_config,
    require_source_path,
    resolve_flux_profile,
    resolve_perlmutter_profile,
    save_gui_workflow_config,
)


class DtwinConfigTests(unittest.TestCase):
    def test_derived_defaults_follow_current_user_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            missing_legacy = Path(tmpdir) / "legacy.json"
            with mock.patch.dict(
                os.environ,
                {"DTWIN_CONFIG": str(config_path), "USER": "alice"},
                clear=False,
            ), mock.patch(
                "dtwin_config.get_legacy_config_path",
                return_value=missing_legacy,
            ), mock.patch(
                "pathlib.Path.home",
                return_value=Path("/Users/alice"),
            ):
                perlmutter = resolve_perlmutter_profile()
                flux = resolve_flux_profile()

            self.assertEqual(perlmutter["user"], "alice")
            self.assertEqual(perlmutter["host"], "perlmutter.nersc.gov")
            self.assertEqual(perlmutter["remote"], "alice@perlmutter.nersc.gov")
            self.assertEqual(perlmutter["identity"], "/Users/alice/.ssh/nersc")
            self.assertEqual(perlmutter["base_dir"], "/pscratch/sd/a/alice/DTwin")
            self.assertEqual(perlmutter["batch_dir"], "/pscratch/sd/a/alice/DTwin/newbatch")
            self.assertEqual(perlmutter["gx_path"], "/global/homes/a/alice/GX/gx_next6")

            self.assertEqual(flux["user"], "alice")
            self.assertEqual(flux["host"], "flux")
            self.assertEqual(flux["remote"], "alice@flux")
            self.assertEqual(flux["base_dir"], "/u/alice/DTwin/transp_full_auto")
            self.assertEqual(flux["python_bin"], "/u/alice/pyrokinetics/.venv/bin/python")
            self.assertEqual(flux["duo_option"], "")

    def test_environment_overrides_beat_user_local_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "perlmutter": {
                            "user": "cfguser",
                            "host": "cfg-perlmutter",
                            "base_dir": "/cfg/base",
                            "batch_dir": "/cfg/base/newbatch",
                            "gx_path": "/cfg/gx",
                        },
                        "flux": {
                            "user": "cfgflux",
                            "host": "cfg-host",
                            "base_dir": "/cfg/flux/base",
                            "python_bin": "/cfg/flux/python",
                        },
                        "sources": {
                            "mate_root": "/cfg/mate",
                            "transp_copy_root_09": "/cfg/transp09",
                            "transp_copy_root_10": "/cfg/transp10",
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "DTWIN_CONFIG": str(config_path),
                    "DTWIN_PERLMUTTER_USER": "envuser",
                    "DTWIN_PERLMUTTER_HOST": "env-perlmutter",
                    "DTWIN_PERLMUTTER_BASE_DIR": "/env/base",
                    "DTWIN_PERLMUTTER_BATCH_DIR": "/env/base/newbatch",
                    "DTWIN_GX_PATH": "/env/gx",
                    "DTWIN_FLUX_USER": "envflux",
                    "DTWIN_FLUX_HOST": "env-flux-host",
                    "DTWIN_FLUX_BASE_DIR": "/env/flux/base",
                    "DTWIN_FLUX_PYTHON": "/env/flux/python",
                    "DTWIN_FLUX_DUO_OPTION": "1",
                    "DTWIN_MATE_ROOT": "/env/mate",
                    "DTWIN_TRANSP_COPY_ROOT_09": "/env/transp09",
                    "DTWIN_TRANSP_COPY_ROOT_10": "/env/transp10",
                },
                clear=False,
            ):
                perlmutter = resolve_perlmutter_profile()
                flux = resolve_flux_profile()
                gui = load_gui_workflow_config()

            self.assertEqual(perlmutter["user"], "envuser")
            self.assertEqual(perlmutter["host"], "env-perlmutter")
            self.assertEqual(perlmutter["base_dir"], "/env/base")
            self.assertEqual(perlmutter["batch_dir"], "/env/base/newbatch")
            self.assertEqual(perlmutter["gx_path"], "/env/gx")

            self.assertEqual(flux["user"], "envflux")
            self.assertEqual(flux["host"], "env-flux-host")
            self.assertEqual(flux["base_dir"], "/env/flux/base")
            self.assertEqual(flux["python_bin"], "/env/flux/python")
            self.assertEqual(flux["duo_option"], "1")

            self.assertNotIn("mate_root", gui)
            self.assertNotIn("transp_copy_root_09", gui)
            self.assertNotIn("transp_copy_root_10", gui)

    def test_explicit_override_beats_environment_and_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "perlmutter": {"user": "cfguser", "host": "cfg-perlmutter"},
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {
                    "DTWIN_CONFIG": str(config_path),
                    "DTWIN_PERLMUTTER_USER": "envuser",
                    "DTWIN_PERLMUTTER_HOST": "env-perlmutter",
                },
                clear=False,
            ):
                perlmutter = resolve_perlmutter_profile(
                    {"remote": "explicit@perlmutter.override", "base_dir": "/explicit/base"}
                )

            self.assertEqual(perlmutter["user"], "explicit")
            self.assertEqual(perlmutter["host"], "perlmutter.override")
            self.assertEqual(perlmutter["remote"], "explicit@perlmutter.override")
            self.assertEqual(perlmutter["base_dir"], "/explicit/base")

    def test_flux_host_is_preserved_verbatim(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            with mock.patch.dict(
                os.environ,
                {
                    "DTWIN_CONFIG": str(config_path),
                    "USER": "alice",
                    "DTWIN_FLUX_HOST": "flux-login2",
                },
                clear=False,
            ):
                flux = resolve_flux_profile()

            self.assertEqual(flux["host"], "flux-login2")
            self.assertEqual(flux["remote"], "alice@flux-login2")

    def test_require_source_path_fails_with_actionable_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.json"
            missing_legacy = Path(tmpdir) / "legacy.json"
            with mock.patch.dict(
                os.environ,
                {"DTWIN_CONFIG": str(config_path)},
                clear=False,
            ), mock.patch(
                "dtwin_config.get_legacy_config_path",
                return_value=missing_legacy,
            ):
                with self.assertRaises(SystemExit) as ctx:
                    require_source_path("mate_root")

            self.assertIn("DTWIN_MATE_ROOT", str(ctx.exception))

    def test_save_gui_workflow_config_round_trips_to_user_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "nested" / "config.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "sources": {
                            "mate_root": "/sources/mate",
                            "transp_copy_root_09": "/sources/09",
                            "transp_copy_root_10": "/sources/10",
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {"DTWIN_CONFIG": str(config_path)},
                clear=False,
            ), mock.patch(
                "dtwin_config.get_legacy_config_path",
                return_value=Path(tmpdir) / "missing-legacy.json",
            ):
                written_path = save_gui_workflow_config(
                    {
                        "ssh_user": "perlmutter-user",
                        "ssh_host": "perlmutter-host",
                        "ssh_identity": "~/.ssh/id_perlmutter",
                        "ssh_control_path": "/tmp/control-path",
                        "ssh_control_persist": "20m",
                        "ssh_connect_timeout": "17",
                        "monitor_timeout": "240",
                        "perlmutter_base_dir": "/perlmutter/base",
                        "perlmutter_batch_dir": "/perlmutter/base/newbatch",
                        "gx_path": "/perlmutter/gx",
                        "flux_user": "flux-user",
                        "flux_host": "flux-host",
                        "flux_base_dir": "/flux/base",
                        "flux_python_bin": "/flux/python",
                        "flux_duo_option": "1",
                    }
                )

                gui = load_gui_workflow_config()
                current_user_path = get_user_config_path()
                payload = json.loads(config_path.read_text(encoding="utf-8"))

            self.assertEqual(written_path, config_path)
            self.assertEqual(current_user_path, config_path)
            self.assertEqual(gui["config_path"], str(config_path))
            self.assertEqual(gui["ssh_user"], "perlmutter-user")
            self.assertEqual(gui["ssh_host"], "perlmutter-host")
            self.assertEqual(gui["ssh_connect_timeout"], "17")
            self.assertEqual(gui["monitor_timeout"], "240")
            self.assertEqual(gui["flux_user"], "flux-user")
            self.assertEqual(gui["flux_python_bin"], "/flux/python")
            self.assertEqual(gui["flux_duo_option"], "1")
            self.assertEqual(payload["sources"]["mate_root"], "/sources/mate")
            self.assertEqual(payload["sources"]["transp_copy_root_09"], "/sources/09")
            self.assertEqual(payload["sources"]["transp_copy_root_10"], "/sources/10")
            self.assertNotIn("mate_root", gui)
