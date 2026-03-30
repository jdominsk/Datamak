import sqlite3
import time
from typing import Any, Callable, Dict, List, Optional


def _normalize_state_class(state: str) -> str:
    normalized = (state or "UNKNOWN").strip().lower()
    return "".join(ch if ch.isalnum() else "-" for ch in normalized).strip("-") or "unknown"


def _first_nonempty_note(notes: List[object]) -> Optional[str]:
    for note in notes:
        text = str(note or "").strip()
        if text:
            return text
    return None


def _recommend_equilibria_ai_action(
    *,
    selected_origin_details: Optional[Dict[str, object]],
    equilibria_summary: Dict[str, int],
    equilibria_workflow_status: Dict[str, object],
    equilibria_actions: List[Dict[str, str]],
    equilibria_action_notes: List[str],
    flux_action_state: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    origin_name = str((selected_origin_details or {}).get("name") or "").strip()
    if not origin_name:
        return {"available": False}

    stage_map = {
        str(item.get("label") or ""): item
        for item in list(equilibria_workflow_status.get("stages") or [])
        if isinstance(item, dict)
    }
    workflow_notes = [
        str(note or "").strip()
        for note in list(equilibria_workflow_status.get("notes") or [])
        if str(note or "").strip()
    ]
    action_by_key = {
        str(item.get("key") or ""): str(item.get("title") or "").strip()
        for item in equilibria_actions
        if str(item.get("key") or "").strip()
    }
    available_action_titles = [
        str(item.get("title") or "").strip()
        for item in equilibria_actions
        if str(item.get("title") or "").strip()
    ]

    flux_state = str((stage_map.get("Flux workflow") or {}).get("state") or "").strip().upper()
    flux_detail = str((stage_map.get("Flux workflow") or {}).get("detail") or "").strip()
    gk_study_state = str((stage_map.get("gk_study") or {}).get("state") or "").strip().upper()
    gk_input_state = str((stage_map.get("gk_input") or {}).get("state") or "").strip().upper()
    gk_input_detail = str((stage_map.get("gk_input") or {}).get("detail") or "").strip()
    gk_batch_state = str((stage_map.get("gk_batch") or {}).get("state") or "").strip().upper()
    gk_batch_detail = str((stage_map.get("gk_batch") or {}).get("detail") or "").strip()
    gk_run_state = str((stage_map.get("gk_run") or {}).get("state") or "").strip().upper()
    gk_run_detail = str((stage_map.get("gk_run") or {}).get("detail") or "").strip()

    equilibria_total = int(equilibria_summary.get("equilibria_total") or 0)
    transp_total = int(equilibria_summary.get("transp_timeseries_total") or 0)
    gk_input_total = int(equilibria_summary.get("gk_input_total") or 0)

    summary_bits = [f"{equilibria_total} equilibria"]
    if transp_total > 0:
        summary_bits.append(f"{transp_total} transp series")
    summary_bits.append(f"{gk_input_total} GK inputs")
    summary_line = " | ".join(summary_bits)

    assessment = "Needs review"
    assessment_state = "PENDING"
    current_state = "The advisor needs more workflow signal from the selected origin."
    blocker = "No clear next blocker could be inferred yet."
    recommendation = "Inspect the workflow status below."
    why = _first_nonempty_note(workflow_notes) or _first_nonempty_note(equilibria_action_notes) or "No stronger origin-specific rule matched."
    confidence = "LOW"
    confidence_class = "low"
    recommended_action: Optional[Dict[str, str]] = None

    def use_action(key: str) -> Optional[Dict[str, str]]:
        title = action_by_key.get(key)
        if not title:
            return None
        return {"key": key, "title": title}

    populate_action = use_action("populate_mate") or use_action("populate_alexei")
    create_inputs_action = use_action("create_inputs_mate") or use_action("create_inputs_transp")
    run_on_flux_action = use_action("run_on_flux")
    check_flux_action = use_action("check_flux_status")
    sync_back_action = use_action("sync_back_from_flux")

    if gk_run_state in {"ACTIVE", "MIXED"}:
        assessment = "Remote runs are now the active stage"
        assessment_state = gk_run_state
        current_state = "This origin already has remote simulation runs linked to its inputs."
        blocker = "The next decision depends on run progress and failures, not on more input generation."
        recommendation = "Use Batch monitoring below."
        why = gk_run_detail or "Remote run rows already exist for this origin."
        confidence = "HIGH"
        confidence_class = "high"
    elif gk_batch_state == "ACTIVE":
        assessment = "A batch database is already in flight"
        assessment_state = "ACTIVE"
        current_state = "Inputs for this origin have already been packaged into at least one batch DB."
        blocker = "The batch stage is already active, so upstream preparation is no longer the limiting step."
        recommendation = "Use Batch monitoring below."
        why = gk_batch_detail or "A created, sent, or launched batch DB is already linked to this origin."
        confidence = "HIGH"
        confidence_class = "high"
    elif gk_input_total > 0:
        assessment = "Inputs are ready for batch launch"
        assessment_state = "READY"
        current_state = "This origin already has generated gk_input rows available for selection."
        blocker = "Nothing appears blocked upstream; the next step is choosing and launching a representative batch."
        recommendation = "Use Launch Batch of Simulations below."
        why = gk_input_detail or "gk_input rows already exist for the selected origin."
        confidence = "HIGH"
        confidence_class = "high"
    elif create_inputs_action and equilibria_total > 0:
        assessment = "Equilibria exist, but GK inputs are still missing"
        assessment_state = gk_input_state or "PENDING"
        current_state = "The origin has equilibria available, but the input-generation step has not populated gk_input yet."
        blocker = "The workflow is stalled between prepared equilibria and generated inputs."
        recommendation = f"{create_inputs_action['title']}."
        why = _first_nonempty_note(workflow_notes) or "This origin exposes a direct input-generation action and already has equilibria rows."
        confidence = "HIGH"
        confidence_class = "high"
        recommended_action = create_inputs_action
    elif populate_action and equilibria_total == 0:
        assessment = "Equilibria have not been populated yet"
        assessment_state = "EMPTY"
        current_state = "This origin still has no data_equil rows in the main database."
        blocker = "Everything downstream depends on creating the initial equilibria entries first."
        recommendation = f"{populate_action['title']}."
        why = _first_nonempty_note(equilibria_action_notes) or "This origin exposes a direct populate action and has no equilibria yet."
        confidence = "HIGH"
        confidence_class = "high"
        recommended_action = populate_action
    elif run_on_flux_action:
        if flux_state in {"SUBMITTED", "RUNNING"}:
            assessment = "Flux preparation is currently running"
            assessment_state = "ACTIVE"
            current_state = "A Flux job is already recorded for this origin."
            blocker = "Rerunning now would risk launching a parallel job for the same staged DB."
            recommendation = f"{(check_flux_action or sync_back_action or run_on_flux_action)['title']}."
            why = flux_detail or _first_nonempty_note(equilibria_action_notes) or "The current Flux workflow is already in progress."
            confidence = "HIGH"
            confidence_class = "high"
            recommended_action = check_flux_action or sync_back_action or run_on_flux_action
        elif flux_state == "DONE" and sync_back_action:
            assessment = "Flux build is ready to import"
            assessment_state = "READY"
            current_state = "The remote Flux workflow appears complete, but the results are not yet applied to the main DB."
            blocker = "Downstream local workflow state will not update until the staged Flux DB is synced back."
            recommendation = f"{sync_back_action['title']}."
            why = flux_detail or "The Flux action log reports DONE for the selected origin."
            confidence = "HIGH"
            confidence_class = "high"
            recommended_action = sync_back_action
        elif flux_state == "FAILED":
            assessment = "The last Flux attempt failed"
            assessment_state = "FAILED"
            current_state = "The most recent Flux job for this origin did not finish cleanly."
            blocker = "The staged remote DB may be incomplete, so the failure should be checked before continuing."
            recommendation = f"{(check_flux_action or run_on_flux_action)['title']}."
            why = flux_detail or "The Flux action log reports a failed state for this origin."
            confidence = "HIGH"
            confidence_class = "high"
            recommended_action = check_flux_action or run_on_flux_action
        else:
            assessment = "Flux generation is the next workflow step"
            assessment_state = "PENDING"
            current_state = "This full-auto origin still needs its Flux step to populate studies and inputs."
            blocker = "The workflow has not yet completed the remote generation stage."
            recommendation = f"{run_on_flux_action['title']}."
            why = _first_nonempty_note(workflow_notes) or _first_nonempty_note(equilibria_action_notes) or "Run On Flux stages or reuses the remote DB and drives the next workflow step."
            confidence = "HIGH"
            confidence_class = "high"
            recommended_action = run_on_flux_action
    elif gk_study_state == "READY" and gk_input_total == 0:
        assessment = "Studies exist but inputs are missing"
        assessment_state = "PENDING"
        current_state = "The workflow reached gk_study, but no gk_input rows were created for this origin."
        blocker = "Input generation has not completed, so the workflow cannot move into batch preparation."
        recommendation = "Inspect the workflow status below before rerunning the generation step."
        why = _first_nonempty_note(workflow_notes) or "gk_study rows exist without any linked gk_input rows."
        confidence = "MEDIUM"
        confidence_class = "medium"

    return {
        "available": True,
        "mode_label": "Rule-based v1",
        "summary_line": summary_line,
        "assessment": assessment,
        "assessment_state": assessment_state,
        "assessment_state_class": _normalize_state_class(assessment_state),
        "current_state": current_state,
        "blocker": blocker,
        "recommendation": recommendation,
        "why": why,
        "confidence": confidence,
        "confidence_class": confidence_class,
        "recommended_action": recommended_action,
        "available_actions": available_action_titles,
        "origin_name": origin_name,
        "flux_status": str((flux_action_state or {}).get("status") or "").strip(),
    }


def _workflow_tool_get_origin_workflow_state(
    *,
    selected_origin_details: Optional[Dict[str, object]],
    equilibria_summary: Dict[str, int],
    equilibria_workflow_status: Dict[str, object],
) -> Dict[str, Any]:
    origin_id = int((selected_origin_details or {}).get("id") or 0)
    origin_name = str((selected_origin_details or {}).get("name") or "").strip()
    file_type = str((selected_origin_details or {}).get("file_type") or "").strip()
    tokamak = str((selected_origin_details or {}).get("tokamak") or "").strip()
    summary_bits = [f"{int(equilibria_summary.get('equilibria_total') or 0)} equilibria"]
    transp_total = int(equilibria_summary.get("transp_timeseries_total") or 0)
    if transp_total > 0:
        summary_bits.append(f"{transp_total} transp series")
    summary_bits.append(f"{int(equilibria_summary.get('gk_input_total') or 0)} GK inputs")
    return {
        "name": "get_origin_workflow_state",
        "summary": " | ".join(summary_bits),
        "payload": {
            "origin_id": origin_id,
            "origin_name": origin_name,
            "file_type": file_type,
            "tokamak": tokamak,
            "summary": dict(equilibria_summary),
            "stages": list(equilibria_workflow_status.get("stages") or []),
            "notes": list(equilibria_workflow_status.get("notes") or []),
        },
    }


def _workflow_tool_list_allowed_actions(
    equilibria_actions: List[Dict[str, str]],
) -> Dict[str, Any]:
    actions_payload = [
        {"key": str(item.get("key") or ""), "title": str(item.get("title") or "").strip()}
        for item in equilibria_actions
        if str(item.get("key") or "").strip() and str(item.get("title") or "").strip()
    ]
    if actions_payload:
        summary = ", ".join(item["title"] for item in actions_payload)
    else:
        summary = "No direct GUI action is currently allowed."
    return {
        "name": "list_allowed_actions",
        "summary": summary,
        "payload": {"actions": actions_payload},
    }


def _workflow_tool_check_flux_status(
    flux_action_state: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    status = str((flux_action_state or {}).get("status") or "").strip().upper()
    detail = str((flux_action_state or {}).get("status_detail") or "").strip()
    slurm_job_id = str((flux_action_state or {}).get("slurm_job_id") or "").strip()
    flux_db_name = str((flux_action_state or {}).get("flux_db_name") or "").strip()
    parts = [part for part in (status or "No Flux action recorded", detail, slurm_job_id, flux_db_name) if part]
    return {
        "name": "check_flux_status",
        "summary": " | ".join(parts),
        "payload": {
            "status": status,
            "detail": detail,
            "slurm_job_id": slurm_job_id,
            "flux_db_name": flux_db_name,
        },
    }


def _workflow_tool_check_simulations(
    *,
    equilibria_workflow_status: Dict[str, object],
    equilibria_monitor_report: Optional[Dict[str, object]],
) -> Dict[str, Any]:
    stage_map = {
        str(item.get("label") or ""): item
        for item in list(equilibria_workflow_status.get("stages") or [])
        if isinstance(item, dict)
    }
    gk_batch = stage_map.get("gk_batch") or {}
    gk_run = stage_map.get("gk_run") or {}
    batch_count = len(list((equilibria_monitor_report or {}).get("batches") or []))
    parts = [
        str(gk_run.get("detail") or "").strip(),
        str(gk_batch.get("detail") or "").strip(),
    ]
    if batch_count > 0:
        parts.append(f"{batch_count} monitored batch DB(s)")
    summary = " | ".join(part for part in parts if part) or "No batch-monitor detail is currently available."
    return {
        "name": "check_simulations",
        "summary": summary,
        "payload": {
            "gk_batch": dict(gk_batch),
            "gk_run": dict(gk_run),
            "batch_count": batch_count,
        },
    }


def _run_equilibria_tool_supervisor(
    *,
    selected_origin_details: Optional[Dict[str, object]],
    equilibria_summary: Dict[str, int],
    equilibria_workflow_status: Dict[str, object],
    equilibria_actions: List[Dict[str, str]],
    equilibria_action_notes: List[str],
    flux_action_state: Optional[Dict[str, str]],
    equilibria_monitor_report: Optional[Dict[str, object]],
) -> Dict[str, Any]:
    baseline = _recommend_equilibria_ai_action(
        selected_origin_details=selected_origin_details,
        equilibria_summary=equilibria_summary,
        equilibria_workflow_status=equilibria_workflow_status,
        equilibria_actions=equilibria_actions,
        equilibria_action_notes=equilibria_action_notes,
        flux_action_state=flux_action_state,
    )
    if not baseline.get("available"):
        return baseline

    stage_map = {
        str(item.get("label") or ""): item
        for item in list(equilibria_workflow_status.get("stages") or [])
        if isinstance(item, dict)
    }
    tool_trace = [
        _workflow_tool_get_origin_workflow_state(
            selected_origin_details=selected_origin_details,
            equilibria_summary=equilibria_summary,
            equilibria_workflow_status=equilibria_workflow_status,
        ),
        _workflow_tool_list_allowed_actions(equilibria_actions),
    ]
    flux_tools_needed = bool(flux_action_state) or any(
        str(item.get("key") or "").strip()
        in {"run_on_flux", "check_flux_status", "sync_back_from_flux"}
        for item in equilibria_actions
    )
    if flux_tools_needed:
        tool_trace.append(_workflow_tool_check_flux_status(flux_action_state))
    gk_batch_state = str((stage_map.get("gk_batch") or {}).get("state") or "").strip().upper()
    gk_run_state = str((stage_map.get("gk_run") or {}).get("state") or "").strip().upper()
    if equilibria_monitor_report or gk_batch_state or gk_run_state:
        tool_trace.append(
            _workflow_tool_check_simulations(
                equilibria_workflow_status=equilibria_workflow_status,
                equilibria_monitor_report=equilibria_monitor_report,
            )
        )

    advice = dict(baseline)
    advice["mode_label"] = "Tool-calling supervisor v1"
    advice["tool_trace"] = tool_trace
    advice["tool_names"] = [str(item.get("name") or "") for item in tool_trace if str(item.get("name") or "")]
    advice["tool_policy"] = "Read-only typed tools only | actions remain approval-gated"
    advice["tool_trace_text"] = " -> ".join(advice["tool_names"])
    return advice


def build_workflow_panel_context(
    *,
    conn: sqlite3.Connection,
    tables: List[str],
    selected_panel: str,
    origin_id: Optional[int],
    sampling_origin_id: Optional[int],
    plasma_origin_id: Optional[int],
    equilibria_valid_only: bool,
    monitor_report: Optional[Dict[str, object]],
    eqp_analyze: bool,
    eqp_ion_tprim_min: Optional[float],
    eqp_max: int,
    eqp_coverage_enabled: bool,
    eqp_target: int,
    eqp_method: str,
    data_origin_color_fn: Callable[[str, Optional[str]], str],
    get_data_origins_fn: Callable[[sqlite3.Connection], List[tuple[int, str, Optional[str]]]],
    get_data_origin_details_fn: Callable[[sqlite3.Connection], List[Dict[str, object]]],
    get_equilibria_origin_summary_fn: Callable[[sqlite3.Connection, int, List[str]], Dict[str, int]],
    get_equilibria_preview_fn: Callable[
        [sqlite3.Connection, int, bool],
        tuple[List[str], List[sqlite3.Row], int],
    ],
    get_latest_flux_action_state_fn: Callable[
        [sqlite3.Connection, int, str], Optional[Dict[str, str]]
    ],
    get_equilibria_origin_workflow_status_fn: Callable[
        [sqlite3.Connection, int, str, str, List[str], Optional[Dict[str, str]]],
        Dict[str, object],
    ],
    get_equilibria_origin_actions_fn: Callable[
        [str, Optional[str], Optional[Dict[str, str]]],
        tuple[List[Dict[str, str]], List[str]],
    ],
    filter_monitor_report_for_origin_fn: Callable[
        [Optional[Dict[str, object]], Optional[str]], Optional[Dict[str, object]]
    ],
    get_equil_plasma_dataset_fn: Callable[..., tuple[List[Dict[str, float]], int]],
    get_equil_plasma_status_counts_fn: Callable[..., Dict[str, int]],
    build_sampling_report_fn: Callable[[List[Dict[str, float]], int, List[str]], Dict[str, object]],
    build_sampling_coverage_fn: Callable[[List[Dict[str, float]], List[str], int], Dict[str, object]],
    build_sampling_selection_fn: Callable[..., Dict[str, object]],
    build_kmeans_selection_fn: Callable[..., Dict[str, object]],
    equil_plasma_columns: List[str],
    mhd_id_column: str,
) -> Dict[str, Any]:
    data_origins: List[tuple[int, str, Optional[str]]] = []
    selected_origin_name: Optional[str] = None
    origin_details: List[Dict[str, object]] = []
    selected_origin_details: Optional[Dict[str, object]] = None
    equilibria_summary = {
        "equilibria_total": 0,
        "equilibria_active": 0,
        "gk_input_total": 0,
        "transp_timeseries_total": 0,
    }
    equilibria_preview_columns: List[str] = []
    equilibria_preview_rows: List[sqlite3.Row] = []
    equilibria_preview_total = 0
    equilibria_actions: List[Dict[str, str]] = []
    equilibria_action_notes: List[str] = []
    flux_action_state: Optional[Dict[str, str]] = None
    equilibria_workflow_status: Dict[str, object] = {"stages": [], "notes": []}
    equilibria_monitor_report: Optional[Dict[str, object]] = monitor_report
    data_origin_colors: Dict[str, str] = {}
    equilibria_ai_advisor: Dict[str, Any] = {"available": False}
    eqp_report: Optional[Dict[str, object]] = None
    eqp_coverage: Optional[Dict[str, object]] = None
    eqp_selection: Optional[Dict[str, object]] = None
    eqp_status_counts: Optional[Dict[str, int]] = None

    if "data_origin" in tables:
        data_origins = get_data_origins_fn(conn)
        origin_details = get_data_origin_details_fn(conn)
        data_origin_colors = {
            origin_name: data_origin_color_fn(origin_name, origin_color)
            for _, origin_name, origin_color in data_origins
        }
        if origin_id is None and data_origins:
            origin_id = data_origins[0][0]
        if origin_id is not None:
            for origin_detail in origin_details:
                data_origin_id = int(origin_detail.get("id") or 0)
                if data_origin_id == origin_id:
                    selected_origin_details = origin_detail
                    selected_origin_name = str(origin_detail.get("name") or "")
                    break
        if selected_origin_details is not None and origin_id is not None:
            selected_origin_label = str(selected_origin_details.get("name") or "")
            equilibria_summary = get_equilibria_origin_summary_fn(conn, origin_id, tables)
            (
                equilibria_preview_columns,
                equilibria_preview_rows,
                equilibria_preview_total,
            ) = get_equilibria_preview_fn(conn, origin_id, equilibria_valid_only)
            flux_action_state = get_latest_flux_action_state_fn(
                conn,
                origin_id,
                selected_origin_label,
            )
            equilibria_workflow_status = get_equilibria_origin_workflow_status_fn(
                conn,
                origin_id,
                selected_origin_label,
                str(selected_origin_details.get("file_type") or ""),
                tables,
                flux_action_state,
            )
            equilibria_actions, equilibria_action_notes = get_equilibria_origin_actions_fn(
                selected_origin_label,
                str(selected_origin_details.get("file_type") or ""),
                flux_action_state,
            )
            equilibria_ai_advisor = _run_equilibria_tool_supervisor(
                selected_origin_details=selected_origin_details,
                equilibria_summary=equilibria_summary,
                equilibria_workflow_status=equilibria_workflow_status,
                equilibria_actions=equilibria_actions,
                equilibria_action_notes=equilibria_action_notes,
                flux_action_state=flux_action_state,
                equilibria_monitor_report=equilibria_monitor_report,
            )
            equilibria_monitor_report = filter_monitor_report_for_origin_fn(
                monitor_report,
                selected_origin_name,
            )
        if sampling_origin_id is None:
            sampling_origin_id = origin_id
        if plasma_origin_id is None:
            plasma_origin_id = origin_id

    if eqp_analyze and selected_panel in {"equil-plasma-sampling", "equilibria"} and {
        "gk_input",
        "gk_study",
        "data_equil",
    }.issubset(tables):
        dataset, total_rows = get_equil_plasma_dataset_fn(
            conn,
            origin_id,
            ion_tprim_min=eqp_ion_tprim_min,
        )
        wait_dataset, wait_total_rows = get_equil_plasma_dataset_fn(
            conn,
            origin_id,
            status_filter="WAIT",
            ion_tprim_min=eqp_ion_tprim_min,
        )
        eqp_status_counts = get_equil_plasma_status_counts_fn(
            conn,
            origin_id,
            ion_tprim_min=eqp_ion_tprim_min,
        )
        eqp_max_effective = eqp_max if eqp_coverage_enabled else wait_total_rows
        start = time.perf_counter()
        eqp_report = build_sampling_report_fn(dataset, total_rows, equil_plasma_columns)
        eqp_report["duration_sec"] = time.perf_counter() - start
        if eqp_coverage_enabled:
            start = time.perf_counter()
            eqp_coverage = build_sampling_coverage_fn(
                wait_dataset,
                equil_plasma_columns,
                eqp_max_effective,
            )
            eqp_coverage["duration_sec"] = time.perf_counter() - start
        if eqp_method == "kmeans":
            start = time.perf_counter()
            eqp_selection = build_kmeans_selection_fn(
                wait_dataset,
                equil_plasma_columns,
                eqp_target,
                eqp_max_effective,
                id_column=mhd_id_column,
            )
            eqp_selection["duration_sec"] = time.perf_counter() - start
        else:
            start = time.perf_counter()
            eqp_selection = build_sampling_selection_fn(
                wait_dataset,
                equil_plasma_columns,
                eqp_target,
                eqp_max_effective,
                id_column=mhd_id_column,
            )
            eqp_selection["duration_sec"] = time.perf_counter() - start

    return {
        "origin_id": origin_id,
        "sampling_origin_id": sampling_origin_id,
        "plasma_origin_id": plasma_origin_id,
        "data_origins": data_origins,
        "selected_origin_name": selected_origin_name,
        "origin_details": origin_details,
        "selected_origin_details": selected_origin_details,
        "equilibria_summary": equilibria_summary,
        "equilibria_preview_columns": equilibria_preview_columns,
        "equilibria_preview_rows": equilibria_preview_rows,
        "equilibria_preview_total": equilibria_preview_total,
        "equilibria_actions": equilibria_actions,
        "equilibria_action_notes": equilibria_action_notes,
        "flux_action_state": flux_action_state,
        "equilibria_workflow_status": equilibria_workflow_status,
        "equilibria_monitor_report": equilibria_monitor_report,
        "equilibria_ai_advisor": equilibria_ai_advisor,
        "data_origin_colors": data_origin_colors,
        "eqp_report": eqp_report,
        "eqp_coverage": eqp_coverage,
        "eqp_selection": eqp_selection,
        "eqp_status_counts": eqp_status_counts,
    }
