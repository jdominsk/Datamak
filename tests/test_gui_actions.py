import tempfile
import unittest
from pathlib import Path
from unittest import mock

from werkzeug.datastructures import MultiDict

from gui.actions import (
    ACTIONS,
    ActionSpec,
    ActionValidationError,
    ResolvedAction,
    resolve_action_request,
)
from gui.app import app


def _load_hpc_config() -> dict[str, str]:
    return {
        "ssh_user": "jdominsk",
        "ssh_host": "perlmutter.nersc.gov",
        "ssh_identity": "/Users/jdominsk/.ssh/nersc",
        "ssh_control_path": "/tmp/datamak_ssh_%r@%h_%p",
        "ssh_control_persist": "10m",
        "ssh_connect_timeout": "30",
        "monitor_timeout": "120",
    }


def _resolve_perlmutter_profile() -> dict[str, str]:
    return {
        "user": "profile-user",
        "host": "profile-host",
    }


class ActionRegistryTests(unittest.TestCase):
    def test_flux_action_requires_selected_origin(self) -> None:
        with self.assertRaises(ActionValidationError) as ctx:
            resolve_action_request(
                "run_on_flux",
                MultiDict({"db": "/tmp/main.db", "panel": "equilibria"}),
                default_db="/tmp/default.db",
                load_hpc_config_fn=_load_hpc_config,
                resolve_perlmutter_profile_fn=_resolve_perlmutter_profile,
            )
        self.assertEqual(ctx.exception.message, "A selected data origin is required for this Flux action.")
        self.assertEqual(ctx.exception.panel, "equilibria")

    def test_flux_action_accepts_origin_id_and_common_redirect_params(self) -> None:
        resolved = resolve_action_request(
            "run_on_flux",
            MultiDict(
                {
                    "db": "/tmp/main.db",
                    "panel": "equilibria",
                    "origin_id": "4",
                    "hpc_open": "1",
                    "hpc_tab": "flux",
                    "equilibria_valid_only": "1",
                }
            ),
            default_db="/tmp/default.db",
            load_hpc_config_fn=_load_hpc_config,
            resolve_perlmutter_profile_fn=_resolve_perlmutter_profile,
        )
        self.assertEqual(resolved.spec.key, "run_on_flux")
        self.assertEqual(resolved.db_path, "/tmp/main.db")
        self.assertEqual(resolved.panel, "equilibria")
        self.assertEqual(resolved.extra_args, ["--origin-id", "4"])
        self.assertEqual(
            resolved.redirect_params,
            {
                "hpc": "1",
                "hpc_tab": "flux",
                "origin_id": "4",
                "equilibria_valid_only": "1",
            },
        )

    def test_monitor_remote_runs_adds_user_timeout_and_env_overrides(self) -> None:
        resolved = resolve_action_request(
            "monitor_remote_runs",
            MultiDict({"db": "/tmp/main.db", "panel": "equilibria", "run_analyze": "1"}),
            default_db="/tmp/default.db",
            load_hpc_config_fn=_load_hpc_config,
            resolve_perlmutter_profile_fn=_resolve_perlmutter_profile,
        )
        self.assertEqual(
            resolved.extra_args,
            ["--user", "jdominsk", "--run-analyze", "--timeout", "120"],
        )
        self.assertEqual(
            resolved.env_overrides,
            {
                "DTWIN_SSH_IDENTITY": "/Users/jdominsk/.ssh/nersc",
                "DTWIN_SSH_CONTROL_PATH": "/tmp/datamak_ssh_%r@%h_%p",
                "DTWIN_SSH_CONTROL_PERSIST": "10m",
                "DTWIN_SSH_CONNECT_TIMEOUT": "30",
            },
        )

    def test_open_ssh_and_test_hpc_connection_derive_host_user_and_env(self) -> None:
        for action_name in ("open_ssh_pipe", "test_hpc_connection"):
            with self.subTest(action_name=action_name):
                resolved = resolve_action_request(
                    action_name,
                    MultiDict({"db": "/tmp/main.db", "panel": "equilibria"}),
                    default_db="/tmp/default.db",
                    load_hpc_config_fn=_load_hpc_config,
                    resolve_perlmutter_profile_fn=_resolve_perlmutter_profile,
                )
                self.assertEqual(
                    resolved.extra_args,
                    ["--host", "perlmutter.nersc.gov", "--user", "jdominsk"],
                )
                self.assertIn("DTWIN_SSH_IDENTITY", resolved.env_overrides or {})

    def test_train_gamma_surrogate_requires_name_and_builds_expected_args(self) -> None:
        with self.assertRaises(ActionValidationError) as ctx:
            resolve_action_request(
                "train_gamma_surrogate",
                MultiDict({"db": "/tmp/main.db", "panel": "surrogate"}),
                default_db="/tmp/default.db",
                load_hpc_config_fn=_load_hpc_config,
                resolve_perlmutter_profile_fn=_resolve_perlmutter_profile,
            )
        self.assertEqual(ctx.exception.message, "Surrogate name is required.")
        self.assertEqual(ctx.exception.panel, "surrogate")

        resolved = resolve_action_request(
            "train_gamma_surrogate",
            MultiDict(
                {
                    "db": "/tmp/main.db",
                    "panel": "surrogate",
                    "surrogate_name": "demo-model",
                    "surrogate_mapsto": "ALL",
                    "surrogate_statuses": "SUCCESS,CONVERGED",
                    "surrogate_origin_id": "7",
                    "surrogate_origin": "Transp 10 (full-auto)",
                    "surrogate_test_size": "0.2",
                    "surrogate_n_estimators": "300",
                    "surrogate_max_depth": "12",
                    "surrogate_min_samples_leaf": "4",
                    "surrogate_log1p": "1",
                }
            ),
            default_db="/tmp/default.db",
            load_hpc_config_fn=_load_hpc_config,
            resolve_perlmutter_profile_fn=_resolve_perlmutter_profile,
        )
        self.assertEqual(
            resolved.extra_args,
            [
                "--name",
                "demo-model",
                "--mapsto-all",
                "--statuses",
                "SUCCESS,CONVERGED",
                "--origin-id",
                "7",
                "--origin",
                "Transp 10 (full-auto)",
                "--test-size",
                "0.2",
                "--n-estimators",
                "300",
                "--max-depth",
                "12",
                "--min-samples-leaf",
                "4",
                "--log1p-target",
            ],
        )

    def test_surrogate_id_actions_require_id_and_preserve_redirect_params(self) -> None:
        with self.assertRaises(ActionValidationError) as ctx:
            resolve_action_request(
                "run_surrogate_estimate",
                MultiDict({"db": "/tmp/main.db", "panel": "surrogate"}),
                default_db="/tmp/default.db",
                load_hpc_config_fn=_load_hpc_config,
                resolve_perlmutter_profile_fn=_resolve_perlmutter_profile,
            )
        self.assertEqual(ctx.exception.message, "Surrogate id is required.")
        self.assertEqual(ctx.exception.panel, "surrogate")

        resolved = resolve_action_request(
            "delete_surrogate_model",
            MultiDict(
                {
                    "db": "/tmp/main.db",
                    "panel": "equilibria",
                    "origin_id": "4",
                    "surrogate_id": "19",
                }
            ),
            default_db="/tmp/default.db",
            load_hpc_config_fn=_load_hpc_config,
            resolve_perlmutter_profile_fn=_resolve_perlmutter_profile,
        )
        self.assertEqual(resolved.panel, "surrogate")
        self.assertEqual(resolved.extra_args, ["--surrogate-id", "19"])
        self.assertEqual(
            resolved.redirect_params,
            {
                "origin_id": "4",
                "surrogate_id": "19",
            },
        )


class SuggestionActionRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "gui_actions.db"
        self.client = app.test_client()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_suggestion_action_uses_registry_and_records_feedback(self) -> None:
        resolved = ResolvedAction(
            spec=ACTIONS["create_batch_db"],
            db_path=str(self.db_path),
            panel="equilibria",
            extra_args=[],
            redirect_params={},
            env_overrides=None,
        )
        response_obj = app.response_class("", status=302, headers={"Location": "/"})
        with mock.patch("gui.app.resolve_action_request", return_value=resolved) as resolve_mock, mock.patch(
            "gui.app.record_ai_feedback"
        ) as feedback_mock, mock.patch("gui.app.log_usage"), mock.patch(
            "gui.app._start_action", return_value=response_obj
        ) as start_mock:
            response = self.client.post(
                "/suggestion_action",
                data={
                    "db": str(self.db_path),
                    "panel": "equilibria",
                    "action_name": "create_batch_db",
                    "suggestion_id": "batch-next",
                    "suggestions_open": "1",
                },
            )
        self.assertEqual(response.status_code, 302)
        feedback_mock.assert_called_once_with("batch-next", "create_batch_db")
        resolve_mock.assert_called_once()
        self.assertEqual(resolve_mock.call_args.kwargs["base_redirect_params"], {"suggestions": "1"})
        start_mock.assert_called_once()
        started = start_mock.call_args.args[0]
        self.assertEqual(started.spec.key, "create_batch_db")
        self.assertEqual(started.redirect_params, {"suggestions": "1"})
