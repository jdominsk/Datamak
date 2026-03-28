import os
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence


APP_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.environ.get("DTWIN_ROOT", os.path.dirname(APP_DIR))
DB_UPDATE_DIR = os.path.join(PROJECT_DIR, "db_update")
BATCH_BASE_DIR = os.path.join(PROJECT_DIR, "batch")

_TRUTHY_EQUIVALENTS = {"1", "true", "on", "yes"}


@dataclass(frozen=True)
class ActionSpec:
    key: str
    label: str
    script: str
    args: Sequence[str] = ()
    use_db: bool = False
    db_arg: Optional[str] = None
    capture_output: bool = False


@dataclass
class ActionDispatch:
    extra_args: List[str] = field(default_factory=list)
    redirect_params: Dict[str, object] = field(default_factory=dict)
    env_overrides: Optional[Dict[str, str]] = None
    panel: str = "action"


@dataclass(frozen=True)
class ResolvedAction:
    spec: ActionSpec
    db_path: str
    panel: str
    extra_args: List[str]
    redirect_params: Dict[str, object]
    env_overrides: Optional[Dict[str, str]]


class ActionValidationError(Exception):
    def __init__(
        self,
        message: str,
        *,
        panel: Optional[str] = None,
        redirect_params: Optional[Dict[str, object]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.panel = panel
        self.redirect_params = redirect_params or {}


@dataclass
class ActionRequestContext:
    form_data: Any
    db_path: str
    panel: str
    redirect_params: Dict[str, object]
    hpc_config: Mapping[str, str]
    perlmutter_profile: Mapping[str, object]
    hpc_user: str


ActionBuilder = Callable[[ActionRequestContext, ActionDispatch], ActionDispatch]


def _make_spec(
    key: str,
    label: str,
    script: str,
    *,
    args: Optional[Sequence[str]] = None,
    use_db: bool = False,
    db_arg: Optional[str] = None,
    capture_output: bool = False,
) -> ActionSpec:
    return ActionSpec(
        key=key,
        label=label,
        script=script,
        args=tuple(args or ()),
        use_db=use_db,
        db_arg=db_arg,
        capture_output=capture_output,
    )


ACTIONS: Dict[str, ActionSpec] = {
    "populate_mate": _make_spec(
        "populate_mate",
        "Populate",
        os.path.join(DB_UPDATE_DIR, "populate_data_equil_from_Mate_KinEFIT.py"),
    ),
    "populate_alexei": _make_spec(
        "populate_alexei",
        "Populate",
        os.path.join(DB_UPDATE_DIR, "populate_data_equil_from_Alexei_Transp_09.py"),
    ),
    "populate_alexei_fullauto": _make_spec(
        "populate_alexei_fullauto",
        "Populate",
        os.path.join(DB_UPDATE_DIR, "populate_data_equil_from_Alexei_Transp_09_fullauto.py"),
    ),
    "populate_alexei_fullauto_10": _make_spec(
        "populate_alexei_fullauto_10",
        "Populate",
        os.path.join(DB_UPDATE_DIR, "populate_data_equil_from_Alexei_Transp_10_fullauto.py"),
    ),
    "create_inputs_mate": _make_spec(
        "create_inputs_mate",
        "Create GK Inputs",
        os.path.join(DB_UPDATE_DIR, "create_gk_input_from_pyrokinetic_with_pfile_and_gfile.py"),
    ),
    "create_inputs_transp": _make_spec(
        "create_inputs_transp",
        "Create GK Inputs",
        os.path.join(DB_UPDATE_DIR, "create_gk_input_from_pyrokinetic_with_transpfile.py"),
    ),
    "create_inputs_transp_fullauto": _make_spec(
        "create_inputs_transp_fullauto",
        "Create GK Inputs",
        os.path.join(DB_UPDATE_DIR, "create_gk_input_from_pyrokinetic_with_transpfile_fullauto.py"),
        use_db=True,
        db_arg="--db",
    ),
    "run_on_flux": _make_spec(
        "run_on_flux",
        "Run On Flux",
        os.path.join(DB_UPDATE_DIR, "Transp_full_auto", "run_on_flux.py"),
        use_db=True,
        db_arg="--db",
    ),
    "sync_back_from_flux": _make_spec(
        "sync_back_from_flux",
        "Sync Back From Flux",
        os.path.join(DB_UPDATE_DIR, "Transp_full_auto", "sync_back_from_flux.py"),
        use_db=True,
        db_arg="--db",
    ),
    "check_flux_status": _make_spec(
        "check_flux_status",
        "Check Flux Status",
        os.path.join(DB_UPDATE_DIR, "Transp_full_auto", "check_flux_job_status.py"),
        use_db=True,
        db_arg="--db",
    ),
    "create_batch_db": _make_spec(
        "create_batch_db",
        "Create Batch DB",
        os.path.join(PROJECT_DIR, "batch", "create_batch_database.py"),
        args=("--copy-torun",),
        use_db=True,
        db_arg="--source-db",
    ),
    "deploy_batch_db": _make_spec(
        "deploy_batch_db",
        "Deploy Batch DB",
        os.path.join(PROJECT_DIR, "batch", "deploy_batch.py"),
    ),
    "deploy_batch_db_large": _make_spec(
        "deploy_batch_db_large",
        "Deploy Batch DB (Large)",
        os.path.join(PROJECT_DIR, "batch", "deploy_batch_large.py"),
    ),
    "check_launched_batches": _make_spec(
        "check_launched_batches",
        "Check Launched Batches",
        os.path.join(PROJECT_DIR, "batch", "check_launched_batches.py"),
        args=("--remote-check",),
        use_db=True,
        db_arg="--db",
        capture_output=True,
    ),
    "train_gamma_surrogate": _make_spec(
        "train_gamma_surrogate",
        "Train Gamma Surrogate",
        os.path.join(PROJECT_DIR, "db_surrogate", "train_gamma_surrogate.py"),
        use_db=True,
        db_arg="--db",
        capture_output=True,
    ),
    "run_surrogate_estimate": _make_spec(
        "run_surrogate_estimate",
        "Run Surrogate",
        os.path.join(PROJECT_DIR, "db_surrogate", "estimate_gamma_surrogate.py"),
        use_db=True,
        db_arg="--db",
        capture_output=True,
    ),
    "monitor_remote_runs": _make_spec(
        "monitor_remote_runs",
        "Check Simulations",
        os.path.join(PROJECT_DIR, "batch", "monitor_remote_runs.py"),
        use_db=True,
        db_arg="--db",
        capture_output=True,
    ),
    "open_ssh_pipe": _make_spec(
        "open_ssh_pipe",
        "Open SSH Pipe",
        os.path.join(PROJECT_DIR, "batch", "open_ssh_control_master.py"),
        capture_output=True,
    ),
    "test_hpc_connection": _make_spec(
        "test_hpc_connection",
        "Test SSH (ls -lrt ~)",
        os.path.join(PROJECT_DIR, "batch", "test_hpc_connection.py"),
        capture_output=True,
    ),
    "mark_remote_running_interrupted": _make_spec(
        "mark_remote_running_interrupted",
        "Mark RUNNING as INTERRUPTED",
        os.path.join(PROJECT_DIR, "batch", "mark_remote_running_interrupted.py"),
        use_db=True,
        db_arg="--db",
        capture_output=True,
    ),
    "mark_remote_run_restart": _make_spec(
        "mark_remote_run_restart",
        "Mark run as RESTART",
        os.path.join(PROJECT_DIR, "batch", "mark_remote_run_restart.py"),
        use_db=True,
        db_arg="--db",
        capture_output=True,
    ),
    "launch_remote_slurm_job": _make_spec(
        "launch_remote_slurm_job",
        "Launch SLURM job",
        os.path.join(PROJECT_DIR, "batch", "launch_remote_slurm_job.py"),
        use_db=True,
        db_arg="--db",
        capture_output=True,
    ),
    "mark_empty_gk_input_error": _make_spec(
        "mark_empty_gk_input_error",
        "Mark empty gk_input as ERROR",
        os.path.join(DB_UPDATE_DIR, "mark_empty_gk_input_error.py"),
        use_db=True,
        db_arg="--db",
        capture_output=True,
    ),
    "delete_surrogate_model": _make_spec(
        "delete_surrogate_model",
        "Delete surrogate model",
        os.path.join(PROJECT_DIR, "db_surrogate", "delete_surrogate_model.py"),
        use_db=True,
        db_arg="--db",
        capture_output=True,
    ),
}


def _get_value(form_data: Any, key: str, default: str = "") -> str:
    value = form_data.get(key, default) if hasattr(form_data, "get") else default
    if value is None:
        return default
    return str(value)


def _get_list(form_data: Any, key: str) -> List[str]:
    if hasattr(form_data, "getlist"):
        return [str(item) for item in form_data.getlist(key)]
    value = form_data.get(key) if hasattr(form_data, "get") else None
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return [str(value)]


def _normalize_panel(panel: str) -> str:
    panel = (panel or "action").strip() or "action"
    if panel == "hpc":
        return "action"
    return panel


def _build_common_redirect_params(form_data: Any) -> Dict[str, object]:
    redirect_params: Dict[str, object] = {}
    if _get_value(form_data, "hpc_open").strip():
        redirect_params["hpc"] = "1"
    hpc_tab = _get_value(form_data, "hpc_tab").strip().lower()
    if hpc_tab in {"perlmutter", "flux"}:
        redirect_params["hpc_tab"] = hpc_tab
    origin_id = _get_value(form_data, "origin_id").strip()
    if origin_id.isdigit():
        redirect_params["origin_id"] = origin_id
    if _get_value(form_data, "equilibria_valid_only").strip().lower() in _TRUTHY_EQUIVALENTS:
        redirect_params["equilibria_valid_only"] = "1"
    return redirect_params


def _apply_hpc_env_overrides(context: ActionRequestContext, dispatch: ActionDispatch) -> ActionDispatch:
    env_overrides: Dict[str, str] = {}
    identity = str(context.hpc_config.get("ssh_identity") or "").strip()
    control_path = str(context.hpc_config.get("ssh_control_path") or "").strip()
    control_persist = str(context.hpc_config.get("ssh_control_persist") or "").strip()
    connect_timeout = str(context.hpc_config.get("ssh_connect_timeout") or "").strip()
    if identity:
        env_overrides["DTWIN_SSH_IDENTITY"] = identity
    if control_path:
        env_overrides["DTWIN_SSH_CONTROL_PATH"] = control_path
    if control_persist:
        env_overrides["DTWIN_SSH_CONTROL_PERSIST"] = control_persist
    if connect_timeout:
        env_overrides["DTWIN_SSH_CONNECT_TIMEOUT"] = connect_timeout
    dispatch.env_overrides = env_overrides or None
    return dispatch


def _default_builder(context: ActionRequestContext, dispatch: ActionDispatch) -> ActionDispatch:
    return dispatch


def _check_launched_batches_builder(
    context: ActionRequestContext, dispatch: ActionDispatch
) -> ActionDispatch:
    plots_limit = _get_value(context.form_data, "plots_limit").strip()
    if plots_limit.isdigit():
        dispatch.extra_args.extend(["--plots-limit", plots_limit])
        if int(plots_limit) > 0:
            dispatch.extra_args.append("--sync-plots")
            dispatch.extra_args.extend(["--plots-dir", os.path.join(BATCH_BASE_DIR, "plots")])
    batch_name = _get_value(context.form_data, "batch_name").strip()
    if batch_name:
        dispatch.extra_args.extend(["--batch", batch_name])
    return dispatch


def _monitor_remote_runs_builder(
    context: ActionRequestContext, dispatch: ActionDispatch
) -> ActionDispatch:
    _apply_hpc_env_overrides(context, dispatch)
    dispatch.extra_args.extend(["--user", context.hpc_user])
    if _get_value(context.form_data, "run_analyze").strip():
        dispatch.extra_args.append("--run-analyze")
    monitor_timeout = str(context.hpc_config.get("monitor_timeout") or "").strip()
    if monitor_timeout:
        dispatch.extra_args.extend(["--timeout", monitor_timeout])
    return dispatch


def _mark_remote_running_interrupted_builder(
    context: ActionRequestContext, dispatch: ActionDispatch
) -> ActionDispatch:
    _apply_hpc_env_overrides(context, dispatch)
    batch_name = _get_value(context.form_data, "batch_name").strip()
    if batch_name:
        dispatch.extra_args.extend(["--batch", batch_name])
    dispatch.extra_args.extend(["--follow-monitor", "--monitor-user", context.hpc_user])
    monitor_timeout = str(context.hpc_config.get("monitor_timeout") or "").strip()
    if monitor_timeout:
        dispatch.extra_args.extend(["--monitor-timeout", monitor_timeout])
    return dispatch


def _mark_remote_run_restart_builder(
    context: ActionRequestContext, dispatch: ActionDispatch
) -> ActionDispatch:
    _apply_hpc_env_overrides(context, dispatch)
    batch_name = _get_value(context.form_data, "batch_name").strip()
    if batch_name:
        dispatch.extra_args.extend(["--batch", batch_name])
    for run_id in _get_list(context.form_data, "run_id"):
        if run_id.isdigit():
            dispatch.extra_args.extend(["--run-id", run_id])
    dispatch.extra_args.extend(["--follow-monitor", "--monitor-user", context.hpc_user])
    monitor_timeout = str(context.hpc_config.get("monitor_timeout") or "").strip()
    if monitor_timeout:
        dispatch.extra_args.extend(["--monitor-timeout", monitor_timeout])
    return dispatch


def _launch_remote_slurm_job_builder(
    context: ActionRequestContext, dispatch: ActionDispatch
) -> ActionDispatch:
    _apply_hpc_env_overrides(context, dispatch)
    batch_name = _get_value(context.form_data, "batch_name").strip()
    if batch_name:
        dispatch.extra_args.extend(["--batch", batch_name])
    dispatch.extra_args.extend(["--user", context.hpc_user])
    dispatch.extra_args.extend(["--follow-monitor", "--monitor-user", context.hpc_user])
    monitor_timeout = str(context.hpc_config.get("monitor_timeout") or "").strip()
    if monitor_timeout:
        dispatch.extra_args.extend(["--monitor-timeout", monitor_timeout])
    return dispatch


def _open_ssh_pipe_builder(context: ActionRequestContext, dispatch: ActionDispatch) -> ActionDispatch:
    _apply_hpc_env_overrides(context, dispatch)
    host = str(context.hpc_config.get("ssh_host") or "").strip() or str(
        context.perlmutter_profile.get("host") or ""
    ).strip()
    dispatch.extra_args.extend(["--host", host])
    if context.hpc_user:
        dispatch.extra_args.extend(["--user", context.hpc_user])
    return dispatch


def _test_hpc_connection_builder(
    context: ActionRequestContext, dispatch: ActionDispatch
) -> ActionDispatch:
    return _open_ssh_pipe_builder(context, dispatch)


def _train_gamma_surrogate_builder(
    context: ActionRequestContext, dispatch: ActionDispatch
) -> ActionDispatch:
    model_name = _get_value(context.form_data, "surrogate_name").strip()
    if not model_name:
        raise ActionValidationError("Surrogate name is required.", panel="surrogate")
    dispatch.extra_args.extend(["--name", model_name])
    mapsto = _get_value(context.form_data, "surrogate_mapsto").strip()
    if mapsto and mapsto.upper() == "ALL":
        dispatch.extra_args.append("--mapsto-all")
    elif mapsto:
        dispatch.extra_args.extend(["--mapsto", mapsto])
    statuses = _get_value(context.form_data, "surrogate_statuses").strip()
    if statuses:
        dispatch.extra_args.extend(["--statuses", statuses])
    origin_id = _get_value(context.form_data, "surrogate_origin_id").strip()
    if origin_id.isdigit():
        dispatch.extra_args.extend(["--origin-id", origin_id])
    origin_name = _get_value(context.form_data, "surrogate_origin").strip()
    if origin_name:
        dispatch.extra_args.extend(["--origin", origin_name])
    test_size = _get_value(context.form_data, "surrogate_test_size").strip()
    if test_size:
        dispatch.extra_args.extend(["--test-size", test_size])
    n_estimators = _get_value(context.form_data, "surrogate_n_estimators").strip()
    if n_estimators:
        dispatch.extra_args.extend(["--n-estimators", n_estimators])
    max_depth = _get_value(context.form_data, "surrogate_max_depth").strip()
    if max_depth:
        dispatch.extra_args.extend(["--max-depth", max_depth])
    min_samples_leaf = _get_value(context.form_data, "surrogate_min_samples_leaf").strip()
    if min_samples_leaf:
        dispatch.extra_args.extend(["--min-samples-leaf", min_samples_leaf])
    if _get_value(context.form_data, "surrogate_log1p").strip():
        dispatch.extra_args.append("--log1p-target")
    return dispatch


def _surrogate_id_builder(context: ActionRequestContext, dispatch: ActionDispatch) -> ActionDispatch:
    surrogate_id = _get_value(context.form_data, "surrogate_id").strip()
    if not surrogate_id.isdigit():
        raise ActionValidationError("Surrogate id is required.", panel="surrogate")
    dispatch.extra_args.extend(["--surrogate-id", surrogate_id])
    dispatch.panel = "surrogate"
    dispatch.redirect_params["surrogate_id"] = surrogate_id
    return dispatch


def _flux_action_builder(context: ActionRequestContext, dispatch: ActionDispatch) -> ActionDispatch:
    origin_id = _get_value(context.form_data, "origin_id").strip()
    origin_name = _get_value(context.form_data, "origin_name").strip()
    if origin_id.isdigit():
        dispatch.extra_args.extend(["--origin-id", origin_id])
    elif origin_name:
        dispatch.extra_args.extend(["--origin-name", origin_name])
    else:
        raise ActionValidationError(
            "A selected data origin is required for this Flux action.",
            panel=context.panel,
            redirect_params=dict(dispatch.redirect_params),
        )
    return dispatch


ACTION_BUILDERS: Dict[str, ActionBuilder] = {
    "check_launched_batches": _check_launched_batches_builder,
    "monitor_remote_runs": _monitor_remote_runs_builder,
    "mark_remote_running_interrupted": _mark_remote_running_interrupted_builder,
    "mark_remote_run_restart": _mark_remote_run_restart_builder,
    "launch_remote_slurm_job": _launch_remote_slurm_job_builder,
    "open_ssh_pipe": _open_ssh_pipe_builder,
    "test_hpc_connection": _test_hpc_connection_builder,
    "train_gamma_surrogate": _train_gamma_surrogate_builder,
    "run_surrogate_estimate": _surrogate_id_builder,
    "delete_surrogate_model": _surrogate_id_builder,
    "run_on_flux": _flux_action_builder,
    "sync_back_from_flux": _flux_action_builder,
    "check_flux_status": _flux_action_builder,
}


def resolve_action_request(
    action_name: str,
    form_data: Any,
    *,
    default_db: str,
    load_hpc_config_fn: Callable[[], Mapping[str, str]],
    resolve_perlmutter_profile_fn: Callable[[], Mapping[str, object]],
    base_redirect_params: Optional[Dict[str, object]] = None,
    panel_override: Optional[str] = None,
) -> ResolvedAction:
    spec = ACTIONS.get(action_name)
    if spec is None:
        raise ActionValidationError(f"Unknown action '{action_name}'.")

    db_path = _get_value(form_data, "db", default_db)
    panel = _normalize_panel(panel_override or _get_value(form_data, "panel", "action"))
    redirect_params = _build_common_redirect_params(form_data)
    if base_redirect_params:
        redirect_params.update(base_redirect_params)

    hpc_config = load_hpc_config_fn()
    perlmutter_profile = resolve_perlmutter_profile_fn()
    hpc_user = str(hpc_config.get("ssh_user") or "").strip() or str(
        perlmutter_profile.get("user") or ""
    ).strip()

    context = ActionRequestContext(
        form_data=form_data,
        db_path=db_path,
        panel=panel,
        redirect_params=redirect_params,
        hpc_config=hpc_config,
        perlmutter_profile=perlmutter_profile,
        hpc_user=hpc_user,
    )
    dispatch = ActionDispatch(panel=panel, redirect_params=dict(redirect_params))
    builder = ACTION_BUILDERS.get(action_name, _default_builder)
    dispatch = builder(context, dispatch)
    return ResolvedAction(
        spec=spec,
        db_path=db_path,
        panel=dispatch.panel,
        extra_args=list(dispatch.extra_args),
        redirect_params=dict(dispatch.redirect_params),
        env_overrides=dict(dispatch.env_overrides) if dispatch.env_overrides else None,
    )


def with_redirect_params(
    resolved: ResolvedAction,
    extra_redirect_params: Optional[Dict[str, object]] = None,
    *,
    panel: Optional[str] = None,
) -> ResolvedAction:
    redirect_params = dict(resolved.redirect_params)
    if extra_redirect_params:
        redirect_params.update(extra_redirect_params)
    return replace(
        resolved,
        panel=panel or resolved.panel,
        redirect_params=redirect_params,
    )
