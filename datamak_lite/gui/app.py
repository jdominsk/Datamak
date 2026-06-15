from __future__ import annotations

import html
import json
import mimetypes
import os
import re
import sqlite3
import subprocess
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from datamak_lite.core.campaign_status import (
    AttentionItem,
    CampaignStatus,
    EntityRef,
    build_campaign_status,
    resolve_database_path,
)
from datamak_lite.core.campaign_use_map import (
    CampaignUseMap,
    HistoryMap,
    HistoryUseGroup,
    UseMapObject,
    build_campaign_use_map,
    metadata_summary,
    object_summary_label,
)
from datamak_lite.core.db import init_db
from datamak_lite.core.display_titles import display_group_for_object, display_title_for_object, metadata_from_json
from datamak_lite.core.repository import LiteRepository


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
PREVIEW_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp"}


def serve(db_path: str | Path, *, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    source_path = Path(db_path).expanduser()
    profile_path = source_path if source_path.suffix.lower() == ".json" else None
    db_path = init_db(resolve_database_path(source_path))
    handler = _make_handler(Path(db_path), profile_path=profile_path)
    server = ThreadingHTTPServer((host, port), handler)
    print(f"Datamak Lite GUI: http://{host}:{port}/?db={Path(db_path).name}")
    print(f"Database: {db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping Datamak Lite GUI")
    finally:
        server.server_close()


def _make_handler(db_path: Path, *, profile_path: Path | None = None) -> type[BaseHTTPRequestHandler]:
    class LiteRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path in ("", "/"):
                query = parse_qs(parsed.query)
                entity_uid = (query.get("entity") or [None])[0]
                view = (query.get("view") or [None])[0]
                lineage_sort = (query.get("lineage_sort") or [None])[0]
                html_text = render_index(
                    db_path,
                    selected_uid=entity_uid,
                    view=view,
                    lineage_sort=lineage_sort,
                    profile_path=profile_path,
                )
                self._send_text(html_text, "text/html; charset=utf-8")
                return
            if parsed.path == "/static/lite.css":
                self._send_text(LITE_CSS, "text/css; charset=utf-8")
                return
            if parsed.path == "/artifact":
                query = parse_qs(parsed.query)
                artifact_id = (query.get("id") or [None])[0]
                self._send_artifact(artifact_id)
                return
            if parsed.path == "/remote-status":
                query = parse_qs(parsed.query)
                entity_uid = (query.get("uid") or [None])[0]
                payload = _check_remote_status(db_path, entity_uid)
                self._send_json(payload)
                return
            if parsed.path == "/health":
                self._send_text("ok\n", "text/plain; charset=utf-8")
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

        def log_message(self, format: str, *args: object) -> None:
            if self.path == "/health":
                return
            super().log_message(format, *args)

        def _send_text(self, text: str, content_type: str) -> None:
            payload = text.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _send_json(self, payload: dict[str, object]) -> None:
            data = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_artifact(self, artifact_id: str | None) -> None:
            try:
                artifact = _load_artifact_for_preview(db_path, artifact_id)
                file_path = _resolve_existing_artifact_path(str(artifact["path"]), db_path)
            except (KeyError, ValueError):
                self.send_error(HTTPStatus.NOT_FOUND, "Artifact not found")
                return
            if file_path is None or not _is_preview_image(file_path, artifact["format"]):
                self.send_error(HTTPStatus.NOT_FOUND, "No local image preview available")
                return
            payload = file_path.read_bytes()
            content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return LiteRequestHandler


def render_index(
    db_path: str | Path,
    *,
    selected_uid: str | None = None,
    view: str | None = None,
    lineage_sort: str | None = None,
    profile_path: str | Path | None = None,
) -> str:
    profile_path = _profile_path_from_source(db_path, profile_path)
    db_path = resolve_database_path(db_path)
    with LiteRepository(db_path) as repo:
        entities = repo.list_entities()
        status = build_campaign_status(repo)
        relations = _load_relations(repo)
        if not entities:
            return _page("Datamak Lite", _empty_state(Path(db_path), status))
        profile_config = _load_profile_config(profile_path)
        campaign_type = _campaign_type(profile_config) or _infer_campaign_type_from_entities(entities)
        selected = _select_entity(repo, entities, selected_uid)
        selected_id = int(selected["id"])
        outgoing = repo.outgoing_relations(selected_id)
        incoming = repo.incoming_relations(selected_id)
        artifacts = repo.artifacts_for_entity(selected_id)
        metrics = repo.metrics_for_entity(selected_id)
        notes = repo.notes_for_entity(selected_id)

    if selected_uid or view == "objects":
        active_view = "object"
    elif view == "campaign":
        active_view = "campaign"
    else:
        active_view = "lineage"
    if active_view == "lineage":
        body = f"""
        {_render_topbar(db_path, active_view)}
        {_render_lineage(entities, relations, campaign_type, sort_mode=lineage_sort)}
        """
        return _page("Datamak Lite - Simulations", body)
    if active_view == "campaign":
        body = f"""
        {_render_topbar(db_path, active_view)}
        {_render_campaign_info(status, _profile_overview(profile_config))}
        """
        return _page("Datamak Lite - Campaign", body)

    body = f"""
    {_render_topbar(db_path, active_view)}
    <main class="layout">
      <aside class="sidebar">
        {_render_entity_nav(entities, selected['uid'], selected['type'], campaign_type)}
      </aside>
      <section class="details">
        {_render_entity_header(selected, campaign_type)}
        {_render_figure_preview(selected, artifacts, db_path)}
        {_render_relation_grid(selected, outgoing, incoming)}
        <div class="two-column">
          {_render_metrics(metrics)}
          {_render_notes(notes)}
        </div>
        {_render_artifacts(artifacts)}
        {_render_metadata(selected)}
      </section>
    </main>
    """
    return _page(f"Datamak Lite - {selected['name']}", body)


def _render_topbar(db_path: Path, active_view: str) -> str:
    lineage_active = " active" if active_view == "lineage" else ""
    campaign_active = " active" if active_view == "campaign" else ""
    objects_active = " active" if active_view == "object" else ""
    return f"""
    <header class="topbar">
      <div>
        <div class="eyebrow">Datamak Lite</div>
      </div>
      <nav class="top-nav" aria-label="Primary">
        <a class="top-nav-link{campaign_active}" href="/?view=campaign">Campaign</a>
        <a class="top-nav-link{lineage_active}" href="/">Simulations</a>
        <a class="top-nav-link{objects_active}" href="/?view=objects">Browse Objects</a>
      </nav>
      <div class="db-pill" title="{_e(str(db_path))}">{_e(Path(db_path).name)}</div>
    </header>
    """


def _select_entity(repo: LiteRepository, entities: list[sqlite3.Row], selected_uid: str | None) -> sqlite3.Row:
    if selected_uid:
        try:
            return repo.get_entity(selected_uid)
        except KeyError:
            pass
    for entity in entities:
        if entity["type"] == "campaign":
            return entity
    return entities[0]


def _load_relations(repo: LiteRepository) -> list[sqlite3.Row]:
    return list(
        repo.conn.execute(
            """
            SELECT
              r.*,
              source.uid AS source_uid,
              source.name AS source_name,
              source.type AS source_type,
              target.uid AS target_uid,
              target.name AS target_name,
              target.type AS target_type
            FROM relation r
            JOIN entity source ON source.id = r.source_entity_id
            JOIN entity target ON target.id = r.target_entity_id
            ORDER BY r.relation_type, source.name, target.name
            """
        )
    )


def _profile_path_from_source(source: str | Path, explicit: str | Path | None) -> Path | None:
    if explicit is not None:
        return Path(explicit).expanduser()
    source_path = Path(source).expanduser()
    return source_path if source_path.suffix.lower() == ".json" else None


def _load_profile_config(profile_path: Path | None) -> dict[str, object]:
    if profile_path is None:
        return {}
    try:
        data = json.loads(profile_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _profile_overview(profile_config: dict[str, object]) -> dict[str, object]:
    overview = profile_config.get("overview")
    return overview if isinstance(overview, dict) else {}


def _campaign_type(profile_config: dict[str, object]) -> str | None:
    value = profile_config.get("campaign_type")
    return value if isinstance(value, str) and value else None


def _infer_campaign_type_from_entities(entities: list[sqlite3.Row]) -> str | None:
    gx_markers = (
        "gx-r",
        "gx_r",
        "gxr",
        "gx replay",
        "gx-r-replay",
        "normal_gx",
        "normal-gx",
        "ktm",
        "tracer_phase_factor",
        "kinetic_electron_de",
        "step_replay",
    )
    for entity in entities:
        metadata = metadata_from_json(entity["metadata_json"])
        text = " ".join(
            str(part)
            for part in (
                entity["type"],
                entity["uid"],
                entity["name"],
                entity["path"] or "",
                metadata.get("category", ""),
                metadata.get("code", ""),
                metadata.get("physics_mode", ""),
            )
        ).lower()
        if any(marker in text for marker in gx_markers):
            return "gx_impurity_turbulence"
    return None


def _render_stats(stats: dict[str, int]) -> str:
    total = sum(stats.values())
    chips = [f'<span class="chip"><strong>{total}</strong> entities</span>']
    for type_name, count in stats.items():
        chips.append(f'<span class="chip">{_e(type_name)} <strong>{count}</strong></span>')
    return f"""
    <section class="panel stats-panel">
      <h2>Overview</h2>
      <div class="chip-row">{''.join(chips)}</div>
    </section>
    """


def _render_overview(
    entities: list[sqlite3.Row],
    use_map: CampaignUseMap,
    campaign_type: str | None,
) -> str:
    return f"""
    <main class="overview-page">
      {_render_physics_object_list(entities, use_map, campaign_type)}
    </main>
    """


def _render_overview2(entities: list[sqlite3.Row], campaign_type: str | None) -> str:
    rows = _main_plasma_input_rows(entities, campaign_type)
    sections = [
        (
            "Normal GX / Impurity-Coupled Runs",
            ["#", "Setup", "Short identity", "Impurities", "e-/field", "Resolution", "Time step", "Main difference"],
            [
                _overview2_table_row(
                    row.number,
                    row.setup,
                    row.short_identity,
                    row.impurities,
                    row.e_field,
                    row.resolution,
                    row.time_step,
                    row.main_difference,
                )
                for row in rows
                if row.group == "normal"
            ],
        ),
        (
            "Main-Plasma Source Histories",
            ["#", "Setup", "Short identity", "e-/field", "Resolution", "Time step", "Saved history", "Main difference"],
            [
                _overview2_table_row(
                    row.number,
                    row.setup,
                    row.short_identity,
                    row.e_field,
                    row.resolution,
                    row.time_step,
                    row.saved_history,
                    row.main_difference,
                )
                for row in rows
                if row.group == "source"
            ],
        ),
        (
            "Adiabatic-Electron Source Histories",
            ["#", "Setup", "Short identity", "e-/field", "Resolution", "Time step", "Saved history", "Main difference"],
            [
                _overview2_table_row(
                    row.number,
                    row.setup,
                    row.short_identity,
                    row.e_field,
                    row.resolution,
                    row.time_step,
                    row.saved_history,
                    row.main_difference,
                )
                for row in rows
                if row.group == "adiabatic"
            ],
        ),
    ]
    content = []
    for title, headers, section_rows in sections:
        table = _table(headers, section_rows) if section_rows else '<p class="muted">No rows in this group.</p>'
        content.append(
            f"""
            <section class="focus-panel overview2-section">
              <h2>{_e(title)}</h2>
              {table}
            </section>
            """
        )
    return f"""
    <main class="overview-page overview2-page">
      {"".join(content)}
    </main>
    """


def _render_lineage(
    entities: list[sqlite3.Row],
    relations: list[sqlite3.Row],
    campaign_type: str | None,
    *,
    sort_mode: str | None = None,
) -> str:
    sort_mode = "latest" if sort_mode == "latest" else "default"
    headers = _lineage_header_labels(campaign_type)
    entity_by_uid = {str(entity["uid"]): entity for entity in entities}
    main_uids = {
        uid
        for uid, entity in entity_by_uid.items()
        if _lineage_is_main_plasma(entity, campaign_type)
    }
    restart_children: dict[str, list[str]] = {}
    restart_parent_by_child: dict[str, str] = {}
    produced_histories: dict[str, list[str]] = {}
    users_by_history: dict[str, list[str]] = {}

    for relation in relations:
        source_uid = str(relation["source_uid"])
        target_uid = str(relation["target_uid"])
        relation_type = str(relation["relation_type"])
        if relation_type == "restarts_from":
            restart_children.setdefault(target_uid, []).append(source_uid)
            restart_parent_by_child[source_uid] = target_uid
        elif relation_type == "produces" and target_uid in entity_by_uid:
            produced_histories.setdefault(source_uid, []).append(target_uid)
        elif relation_type == "uses_history" and target_uid in entity_by_uid:
            users_by_history.setdefault(target_uid, []).append(source_uid)

    for mapping in (restart_children, produced_histories, users_by_history):
        for parent_uid in list(mapping):
            mapping[parent_uid] = sorted(
                dict.fromkeys(uid for uid in mapping[parent_uid] if uid in entity_by_uid),
                key=lambda uid: _lineage_sort_key(entity_by_uid[uid], campaign_type),
            )

    root_candidates = [
        uid
        for uid in main_uids
        if uid not in restart_parent_by_child
    ]
    if sort_mode == "latest":
        root_uids = sorted(
            root_candidates,
            key=lambda uid: (
                -_lineage_tree_latest_date(uid, entity_by_uid, restart_children, produced_histories, users_by_history),
                _lineage_sort_key(entity_by_uid[uid], campaign_type),
            ),
        )
    else:
        root_uids = sorted(root_candidates, key=lambda uid: _lineage_sort_key(entity_by_uid[uid], campaign_type))
    rendered_main: set[str] = set()
    rendered_histories: set[str] = set()
    rendered_leaves: set[str] = set()
    tree_rows = []
    for uid in root_uids:
        tree_rows.append(
            _render_lineage_main_node(
                uid,
                entity_by_uid,
                restart_children,
                produced_histories,
                users_by_history,
                campaign_type,
                level=0,
                rendered_main=rendered_main,
                rendered_histories=rendered_histories,
                rendered_leaves=rendered_leaves,
                stack=set(),
            )
        )

    unplaced_main_rows = []
    for uid in sorted(main_uids - rendered_main, key=lambda item: _lineage_sort_key(entity_by_uid[item], campaign_type)):
        unplaced_main_rows.append(_render_lineage_row(entity_by_uid[uid], campaign_type, level=0, role="main"))

    downstream_uids = {
        uid
        for uid, entity in entity_by_uid.items()
        if str(entity["type"]) in {"pool", "analysis"} and _lineage_is_downstream_study(entity, campaign_type)
    }
    unplaced_leaf_rows = []
    for uid in sorted(downstream_uids - rendered_leaves, key=lambda item: _lineage_sort_key(entity_by_uid[item], campaign_type)):
        unplaced_leaf_rows.append(_render_lineage_row(entity_by_uid[uid], campaign_type, level=0, role="leaf"))

    tree_content = (
        f"""
        <div class="lineage-header">
          <span>{_e(headers[0])}</span>
          <span>{_e(headers[1])}</span>
          <span>{_e(headers[2])}</span>
          <span>{_e(headers[3])}</span>
          <span>{_e(headers[4])}</span>
          <span>{_e(headers[5])}</span>
          <span>{_e(headers[6])}</span>
          <span></span>
        </div>
        """
        + "".join(tree_rows)
        if tree_rows
        else '<p class="muted">No root main-plasma simulations are recorded yet.</p>'
    )
    unplaced_main = _render_lineage_unplaced_section(
        "Unplaced main-plasma nodes",
        "These look like main-plasma simulations but are not connected to a root with a restart relation yet.",
        unplaced_main_rows,
    )
    unplaced_leaves = _render_lineage_unplaced_section(
        "Unplaced replay / KTM leaves",
        "These downstream studies need a uses_history relation to appear below their parent turbulence.",
        unplaced_leaf_rows,
    )
    return f"""
    <main class="overview-page lineage-page">
      <section class="focus-panel lineage-panel">
        <h2>Simulations</h2>
        <p class="panel-help">{_e(_lineage_help_text(campaign_type))}</p>
        {_render_lineage_sort_controls(sort_mode)}
        <div class="lineage-tree">{tree_content}</div>
      </section>
      {unplaced_main}
      {unplaced_leaves}
    </main>
    """


def _lineage_header_labels(campaign_type: str | None) -> list[str]:
    if campaign_type == "xgc_west_edge_campaign":
        return [
            "XGC simulation / study",
            "Machine / status",
            "Scan identity",
            "Species / profiles",
            "Physics switches",
            "Resolution / progress",
            "History",
        ]
    return [
        "Simulation / study",
        "Setup",
        "Short identity",
        "Impurities",
        "e-/field",
        "Resolution",
        "History",
    ]


def _lineage_help_text(campaign_type: str | None) -> str:
    if campaign_type == "xgc_west_edge_campaign":
        return (
            "Rows are XGC simulations. The columns highlight practical WEST57929 comparison axes: "
            "machine/status, density and temperature profile family, species, physics switches, "
            "grid/nphi/node/timestep choices, and latest local catalog progress."
        )
    return (
        "Roots are main-plasma simulations. Continuations keep the same setup; branches change plasma "
        "or numerical parameters. Saved histories are summarized in the history column, and GX-R/KTM/tracer "
        "studies appear as leaves below the turbulence that they use."
    )


def _render_lineage_main_node(
    uid: str,
    entity_by_uid: dict[str, sqlite3.Row],
    restart_children: dict[str, list[str]],
    produced_histories: dict[str, list[str]],
    users_by_history: dict[str, list[str]],
    campaign_type: str | None,
    *,
    level: int,
    role: str = "main",
    rendered_main: set[str],
    rendered_histories: set[str],
    rendered_leaves: set[str],
    stack: set[str],
) -> str:
    if uid in stack or uid not in entity_by_uid:
        return ""
    rendered_main.add(uid)
    history_uids = produced_histories.get(uid, [])
    for history_uid in history_uids:
        rendered_histories.add(history_uid)
    history_text = _lineage_history_summary(history_uids, entity_by_uid)
    child_stack = set(stack)
    child_stack.add(uid)
    branch_children = []
    for child_uid in restart_children.get(uid, []):
        child_role = _lineage_restart_role(entity_by_uid[uid], entity_by_uid[child_uid])
        branch_children.append(
            _render_lineage_main_node(
                child_uid,
                entity_by_uid,
                restart_children,
                produced_histories,
                users_by_history,
                campaign_type,
                level=level + 1,
                role=child_role,
                rendered_main=rendered_main,
                rendered_histories=rendered_histories,
                rendered_leaves=rendered_leaves,
                stack=child_stack,
            )
        )
    leaf_rows = []
    seen_leaf_uids: set[str] = set()
    for history_uid in history_uids:
        for user_uid in users_by_history.get(history_uid, []):
            if user_uid not in entity_by_uid or user_uid in seen_leaf_uids:
                continue
            seen_leaf_uids.add(user_uid)
            rendered_leaves.add(user_uid)
            leaf_rows.append(
                _render_lineage_row(
                    entity_by_uid[user_uid],
                    campaign_type,
                    level=level + 1,
                    role="leaf",
                    history_text="",
                )
            )
    row = _render_lineage_row(
        entity_by_uid[uid],
        campaign_type,
        level=level,
        role=role,
        history_text=history_text,
        collapsible=bool(leaf_rows),
    )
    if leaf_rows:
        row = f"""
        <details class="lineage-leaf-details">
          {row}
          <div class="lineage-leaf-list">{"".join(leaf_rows)}</div>
        </details>
        """
    return row + "".join(branch_children)


def _render_lineage_history_node(
    uid: str,
    entity_by_uid: dict[str, sqlite3.Row],
    users_by_history: dict[str, list[str]],
    campaign_type: str | None,
    *,
    level: int,
    rendered_histories: set[str],
    rendered_leaves: set[str],
) -> str:
    if uid not in entity_by_uid:
        return ""
    rendered_histories.add(uid)
    rows = [_render_lineage_row(entity_by_uid[uid], campaign_type, level=level, role="history")]
    for user_uid in users_by_history.get(uid, []):
        if user_uid not in entity_by_uid:
            continue
        rendered_leaves.add(user_uid)
        rows.append(_render_lineage_row(entity_by_uid[user_uid], campaign_type, level=level + 1, role="leaf"))
    return "".join(rows)


def _render_lineage_row(
    entity: sqlite3.Row,
    campaign_type: str | None,
    *,
    level: int,
    role: str,
    history_text: str = "",
    collapsible: bool = False,
) -> str:
    display = _display_title_for_row(entity, campaign_type)
    title = _lineage_title(entity, display.title, campaign_type)
    href = "/?" + urlencode({"entity": entity["uid"]})
    badge = _lineage_role_badge(role, entity, campaign_type)
    columns = _lineage_overview2_columns(entity, campaign_type, role)
    title_text = ""
    history_html = f'<span class="lineage-history">{_e(history_text)}</span>' if history_text else '<span class="lineage-history"></span>'
    toggle = (
        '<span class="lineage-toggle" aria-hidden="true"></span>'
        if collapsible
        else '<span class="lineage-toggle lineage-toggle-placeholder" aria-hidden="true"></span>'
    )
    detail_link = (
        f'<a class="lineage-detail-link" href="{href}" title="Open object details" '
        f'aria-label="Open object details for {_e(title)}" onclick="event.stopPropagation()">i</a>'
    )
    tag = "summary" if collapsible else "article"
    leaf_summary = _lineage_leaf_summary_cell(entity, title, role)
    if leaf_summary:
        data_area = leaf_summary
    else:
        data_area = columns + history_html
    return f"""
    <{tag} class="lineage-row lineage-level-{min(level, 6)} lineage-role-{_class_token(role)}{' lineage-row-clickable' if collapsible else ''}">
      <div class="lineage-main-cell">
        <div class="lineage-title-line">
          {badge}
          {_lineage_alias_badge(entity)}
          {_lineage_remote_status_badge(entity)}
          {title_text}
        </div>
      </div>
      {data_area}
      <span class="lineage-actions">{detail_link}{toggle}</span>
    </{tag}>
    """


def _lineage_alias_badge(entity: sqlite3.Row) -> str:
    metadata = metadata_from_json(entity["metadata_json"])
    alias = metadata.get("lite_alias")
    if not isinstance(alias, str) or not alias.strip():
        return ""
    return f'<span class="lineage-alias-text">#{_e(alias.strip())}</span>'


def _lineage_remote_status_badge(entity: sqlite3.Row) -> str:
    hint = _remote_status_hint(entity)
    if not hint["available"]:
        return ""
    state = _cached_remote_state(entity, hint)
    if state == "hidden":
        return ""
    label = _remote_status_short_label(state)
    title = _remote_status_title_for_hint(hint, state)
    return (
        f'<button type="button" class="remote-status-badge remote-status-{_class_token(state)}" '
        f'data-remote-status-uid="{_e(entity["uid"])}" title="{_e(title)}" '
        f'aria-label="{_e(title)}" onclick="event.stopPropagation()">{_e(label)}</button>'
    )


def _lineage_leaf_summary_cell(entity: sqlite3.Row, title: str, role: str) -> str:
    if role != "leaf":
        return ""
    metadata = metadata_from_json(entity["metadata_json"])
    pieces = []
    for key in ("category", "models", "model", "variants", "z_values", "effective_stride", "n_cases", "skip_saved_steps"):
        value = metadata.get(key)
        if value not in (None, "", []):
            pieces.append(f"{key}={_overview2_format(value)}")
    summary = "; ".join(pieces)
    if summary:
        summary = f"{title} | {summary}"
    else:
        summary = title
    return f'<span class="lineage-leaf-summary">{_e(summary)}</span>'


def _lineage_overview2_columns(entity: sqlite3.Row, campaign_type: str | None, role: str) -> str:
    if role not in {"main", "continuation", "branch", "restart"}:
        return "".join('<span class="lineage-data-cell"></span>' for _ in range(5))
    metadata = metadata_from_json(entity["metadata_json"])
    if campaign_type == "xgc_west_edge_campaign":
        values = [
            metadata.get("lineage_setup", ""),
            metadata.get("lineage_short_identity", ""),
            metadata.get("lineage_species_profiles", ""),
            metadata.get("lineage_physics", ""),
            metadata.get("lineage_numerics", ""),
        ]
        return "".join(f'<span class="lineage-data-cell">{_e(_overview2_format(value))}</span>' for value in values)
    display = _display_title_for_row(entity, campaign_type)
    row = _Overview2Row(
        number=0,
        group=_overview2_group(metadata, entity, display.title),
        setup=_overview2_setup_label(entity, metadata),
        short_identity=_overview2_short_identity(entity, metadata, display.title),
        impurities=_overview2_impurities(metadata),
        e_field=_overview2_e_field(metadata),
        resolution=_overview2_resolution(metadata),
        time_step=_overview2_time_step(metadata),
        saved_history=_overview2_saved_history(metadata),
        main_difference=_overview2_main_difference(entity, metadata),
    )
    values = [
        row.setup,
        row.short_identity,
        row.impurities,
        row.e_field,
        row.resolution,
    ]
    return "".join(f'<span class="lineage-data-cell">{_e(value)}</span>' for value in values)


def _lineage_role_badge(role: str, entity: sqlite3.Row, campaign_type: str | None) -> str:
    labels = {
        "main": "XGC" if campaign_type == "xgc_west_edge_campaign" else "GX",
        "continuation": "continuation",
        "branch": "branch",
        "restart": "branch",
        "history": "history",
        "leaf": "leaf",
    }
    badge_class = role
    if role == "leaf":
        kind = _overview_kind_for_row(entity, campaign_type) if str(entity["type"]) in {"pool", "analysis"} else ""
        if kind == "Kinetic Trace Model":
            labels["leaf"] = "KTM"
            badge_class = "ktm"
        elif kind == "Replay":
            labels["leaf"] = "GX-R"
            badge_class = "gxr"
    label = labels.get(role, role.replace("_", " "))
    return f'<span class="lineage-role-badge lineage-role-badge-{_class_token(badge_class)}">{_e(label)}</span>'


def _render_lineage_unplaced_section(title: str, help_text: str, rows: list[str]) -> str:
    if not rows:
        return ""
    return f"""
    <section class="focus-panel lineage-panel">
      <h2>{_e(title)}</h2>
      <p class="panel-help">{_e(help_text)}</p>
      <div class="lineage-tree lineage-unplaced">{"".join(rows)}</div>
    </section>
    """


def _render_lineage_sort_controls(sort_mode: str) -> str:
    default_class = " active" if sort_mode == "default" else ""
    latest_class = " active" if sort_mode == "latest" else ""
    return f"""
    <div class="lineage-sort-controls" aria-label="Simulation sort options">
      <span>Sort</span>
      <a class="lineage-sort-link{default_class}" href="/">Default</a>
      <a class="lineage-sort-link{latest_class}" href="/?lineage_sort=latest">Latest run</a>
    </div>
    """


def _lineage_is_main_plasma(entity: sqlite3.Row, campaign_type: str | None) -> bool:
    if str(entity["type"]) == "simulation":
        return True
    if str(entity["type"]) in {"pool", "analysis"}:
        return _overview_kind_for_row(entity, campaign_type) == "Main Plasma GX"
    return False


def _lineage_is_downstream_study(entity: sqlite3.Row, campaign_type: str | None) -> bool:
    if str(entity["type"]) not in {"pool", "analysis"}:
        return False
    return _overview_kind_for_row(entity, campaign_type) in {"Replay", "Kinetic Trace Model"}


def _lineage_title(entity: sqlite3.Row, display_title: str, campaign_type: str | None) -> str:
    if str(entity["type"]) in {"pool", "analysis"}:
        kind = _overview_kind_for_row(entity, campaign_type)
        if kind:
            return _overview_title_for_row(entity, display_title, kind)
    return display_title


def _lineage_subtitle(entity: sqlite3.Row) -> str:
    metadata = metadata_from_json(entity["metadata_json"])
    pieces = []
    if str(entity["type"]) == "history_file":
        saved = _overview2_saved_history(metadata)
        if saved:
            pieces.append(saved)
    else:
        e_field = _overview2_e_field(metadata)
        if e_field:
            pieces.append(e_field)
        resolution = _overview2_resolution(metadata)
        if resolution:
            pieces.append(resolution)
        time_step = _overview2_time_step(metadata)
        if time_step:
            pieces.append(time_step)
    status = str(entity["status"] or "")
    if status and status != "unknown":
        pieces.append(status)
    return " | ".join(piece for piece in pieces if piece)


def _lineage_history_summary(history_uids: list[str], entity_by_uid: dict[str, sqlite3.Row]) -> str:
    windows = []
    for uid in history_uids:
        entity = entity_by_uid.get(uid)
        if entity is None:
            continue
        metadata = metadata_from_json(entity["metadata_json"])
        window = _lineage_history_window(entity, metadata)
        if not window:
            continue
        parts = [window]
        stride = _lineage_history_stride(entity, metadata)
        fields = _lineage_history_fields(entity, metadata)
        if stride:
            parts.append(f"stride={stride}")
        if fields:
            parts.append(fields)
        windows.append(", ".join(parts))
    return "; ".join(dict.fromkeys(windows))


def _lineage_history_window(entity: sqlite3.Row, metadata: dict[str, object]) -> str:
    start = metadata.get("time_start") or metadata.get("record_start_time")
    end = metadata.get("time_end") or metadata.get("record_tmax") or metadata.get("t_max") or metadata.get("target_t_max")
    if isinstance(end, (int, float)) and end > 1e9:
        end = None
    if start not in (None, "") and end not in (None, ""):
        return f"t={_overview2_format(start)}-{_overview2_format(end)}"
    text = f"{entity['uid']} {entity['name']} {entity['path'] or ''}"
    match = re.search(r"t(\d+(?:p\d+)?)_t(\d+(?:p\d+)?)", text)
    if match:
        return "t=" + "-".join(value.replace("p", ".") for value in match.groups())
    source_window = metadata.get("source_window")
    if isinstance(source_window, str) and source_window:
        match = re.search(r"(\d+(?:\.\d+)?)\s*(?:\.\.|->|-)\s*(\d+(?:\.\d+)?)", source_window)
        if match:
            return "t=" + "-".join(match.groups())
    return ""


def _lineage_history_stride(entity: sqlite3.Row, metadata: dict[str, object]) -> str:
    stride = metadata.get("saved_stride") or metadata.get("history_stride") or metadata.get("step_record_stride")
    if stride not in (None, "", []):
        return _overview2_format(stride)
    text = " ".join(str(part) for part in (entity["uid"], entity["name"], entity["path"] or "", metadata.get("source_window", "")))
    match = re.search(r"stride\s*[= ]\s*(\d+)", text, flags=re.IGNORECASE)
    return match.group(1) if match else ""


def _lineage_history_fields(entity: sqlite3.Row, metadata: dict[str, object]) -> str:
    fields = metadata.get("history_fields") or metadata.get("fields")
    if fields:
        return _overview2_format(fields).replace("Phi_step", "phi").replace("Apar_step", "Apar").replace("Bpar_step", "Bpar")
    text = " ".join(str(part) for part in (entity["uid"], entity["name"], entity["path"] or "", metadata.get("source_window", ""))).lower()
    has_phi = "phi" in text or "step_fields" in text or "fields" in text
    has_apar = "apar" in text or "full em" in text or "full-em" in text
    has_bpar = "bpar" in text or "full em" in text or "full-em" in text
    if has_phi and has_apar and has_bpar:
        return "phi/Apar/Bpar"
    if has_phi:
        return "phi"
    return ""


def _lineage_restart_role(parent: sqlite3.Row, child: sqlite3.Row) -> str:
    parent_metadata = metadata_from_json(parent["metadata_json"])
    child_metadata = metadata_from_json(child["metadata_json"])
    comparable_keys = [
        "electron_model",
        "field_model",
        "ntheta",
        "nx",
        "ny",
        "nhermite",
        "nlaguerre",
        "nspecies",
        "z",
        "z_values",
        "impurity_elements",
        "models",
        "variants",
        "aLn",
        "aLT",
        "fprim",
        "tprim",
        "temp_alti8p19",
        "temp_alti8p19_titeq",
    ]
    parent_values = _lineage_parameter_signature(parent_metadata, comparable_keys)
    child_values = _lineage_parameter_signature(child_metadata, comparable_keys)
    if parent_values and child_values and parent_values == child_values:
        return "continuation"
    parent_setup = _overview2_setup_label(parent, parent_metadata)
    child_setup = _overview2_setup_label(child, child_metadata)
    parent_efield = _overview2_e_field(parent_metadata)
    child_efield = _overview2_e_field(child_metadata)
    parent_resolution = _overview2_resolution(parent_metadata)
    child_resolution = _overview2_resolution(child_metadata)
    parent_difference = _overview2_main_difference(parent, parent_metadata)
    child_difference = _overview2_main_difference(child, child_metadata)
    if (
        parent_setup == child_setup
        and parent_efield == child_efield
        and parent_resolution == child_resolution
        and parent_difference == child_difference
    ):
        return "continuation"
    return "branch"


def _lineage_parameter_signature(metadata: dict[str, object], keys: list[str]) -> dict[str, object]:
    return {key: metadata[key] for key in keys if key in metadata and metadata[key] not in (None, "", [])}


def _lineage_sort_key(entity: sqlite3.Row, campaign_type: str | None) -> tuple[str, str, str]:
    metadata = metadata_from_json(entity["metadata_json"])
    setup = _overview2_setup_label(entity, metadata)
    display = _display_title_for_row(entity, campaign_type)
    return (setup, str(entity["type"]), display.title)


def _lineage_tree_latest_date(
    uid: str,
    entity_by_uid: dict[str, sqlite3.Row],
    restart_children: dict[str, list[str]],
    produced_histories: dict[str, list[str]],
    users_by_history: dict[str, list[str]],
    *,
    visited: set[str] | None = None,
) -> int:
    if visited is None:
        visited = set()
    if uid in visited or uid not in entity_by_uid:
        return 0
    visited.add(uid)
    latest = _lineage_entity_run_date(entity_by_uid[uid])
    for child_uid in restart_children.get(uid, []):
        latest = max(
            latest,
            _lineage_tree_latest_date(
                child_uid,
                entity_by_uid,
                restart_children,
                produced_histories,
                users_by_history,
                visited=visited,
            ),
        )
    for history_uid in produced_histories.get(uid, []):
        if history_uid not in entity_by_uid:
            continue
        latest = max(latest, _lineage_entity_run_date(entity_by_uid[history_uid]))
        for user_uid in users_by_history.get(history_uid, []):
            if user_uid in entity_by_uid:
                latest = max(latest, _lineage_entity_run_date(entity_by_uid[user_uid]))
    return latest


def _lineage_entity_run_date(entity: sqlite3.Row) -> int:
    metadata = metadata_from_json(entity["metadata_json"])
    clean_metadata = {
        key: value
        for key, value in metadata.items()
        if "curation" not in str(key).lower()
        and "alias" not in str(key).lower()
    }
    text = " ".join(
        str(part)
        for part in (
            entity["uid"],
            entity["name"],
            entity["path"] or "",
            json.dumps(clean_metadata, sort_keys=True),
        )
    )
    candidates = [int(match.group(0)) for match in re.finditer(r"20\d{6}", text)]
    for match in re.finditer(r"(20\d{2})-(\d{2})-(\d{2})", text):
        candidates.append(int("".join(match.groups())))
    return max(candidates) if candidates else 0


class _Overview2Row:
    def __init__(
        self,
        *,
        number: int,
        group: str,
        setup: str,
        short_identity: str,
        impurities: str,
        e_field: str,
        resolution: str,
        time_step: str,
        saved_history: str,
        main_difference: str,
    ) -> None:
        self.number = number
        self.group = group
        self.setup = setup
        self.short_identity = short_identity
        self.impurities = impurities
        self.e_field = e_field
        self.resolution = resolution
        self.time_step = time_step
        self.saved_history = saved_history
        self.main_difference = main_difference


def _main_plasma_input_rows(entities: list[sqlite3.Row], campaign_type: str | None) -> list[_Overview2Row]:
    candidates: list[tuple[sqlite3.Row, dict[str, object], str]] = []
    for entity in entities:
        metadata = metadata_from_json(entity["metadata_json"])
        kind = (
            _overview_kind_for_row(entity, campaign_type)
            if str(entity["type"]) in {"pool", "analysis"}
            else "Main Plasma GX" if str(entity["type"]) == "simulation" else ""
        )
        if str(entity["type"]) == "simulation" or kind == "Main Plasma GX":
            display = _display_title_for_row(entity, campaign_type)
            candidates.append((entity, metadata, display.title))

    ordered = sorted(candidates, key=lambda item: (_overview2_group_rank(item[1], item[0], item[2]), str(item[0]["name"])))
    rows = []
    for number, (entity, metadata, title) in enumerate(ordered, start=1):
        group = _overview2_group(metadata, entity, title)
        rows.append(
            _Overview2Row(
                number=number,
                group=group,
                setup=_overview2_setup_label(entity, metadata),
                short_identity=_overview2_short_identity(entity, metadata, title),
                impurities=_overview2_impurities(metadata),
                e_field=_overview2_e_field(metadata),
                resolution=_overview2_resolution(metadata),
                time_step=_overview2_time_step(metadata),
                saved_history=_overview2_saved_history(metadata),
                main_difference=_overview2_main_difference(entity, metadata),
            )
        )
    return rows


def _overview2_table_row(*values: object) -> str:
    cells = "".join(f"<td>{_e(_overview2_format(value))}</td>" for value in values)
    return f"<tr>{cells}</tr>"


def _overview2_group_rank(metadata: dict[str, object], entity: sqlite3.Row, title: str) -> int:
    group = _overview2_group(metadata, entity, title)
    return {"normal": 0, "source": 1, "adiabatic": 2}.get(group, 3)


def _overview2_group(metadata: dict[str, object], entity: sqlite3.Row, title: str) -> str:
    text = " ".join(
        str(part)
        for part in (
            entity["uid"],
            entity["name"],
            entity["path"] or "",
            metadata.get("category", ""),
            metadata.get("physics_mode", ""),
            title,
        )
    ).lower()
    if _overview2_has_normal_gx(metadata, text):
        return "normal"
    if "adiabatic" in str(metadata.get("electron_model", "")).lower() or "adiabatic" in title.lower():
        return "adiabatic"
    return "source"


def _overview2_has_normal_gx(metadata: dict[str, object], text: str) -> bool:
    return bool(
        metadata.get("normal_gx")
        or str(metadata.get("physics_mode", "")).lower() == "normal_gx"
        or "normal_gx" in text
        or "normal-gx" in text
    )


def _overview2_short_identity(entity: sqlite3.Row, metadata: dict[str, object], title: str) -> str:
    text = " ".join(str(part) for part in (entity["uid"], entity["name"], entity["path"] or "")).lower()
    if "wmatched_fresh_w20_trace_vs_coupled" in text:
        return "W20 trace vs coupled from matched t=600"
    if "normal_gx_kinelec_trace_gxrres_zscan" in text:
        return "Normal-GX trace-W Z scan"
    if "aug_boron_trace_coupled" in text:
        return "AUG-like B trace/coupled"
    if "coll1eminus5_t500" in text:
        return "Trace-W restart coll1e-5 to t=500"
    if "tplus100_coll1eminus5" in text:
        return "Trace-W restart +100 to t=600"
    if "wmatched_stride1_t600_t700" in text:
        return "Matched WEST stride-1 t=600-700"
    if "wmatched_history_20260530" in text:
        return "Matched WEST history 20260530"
    if "wmatched" in text and "t600_t700" in text:
        return "Matched WEST t=600-700 source"
    if "alti819" in text or "alti8p19" in text:
        return "High a/LTi=8.19 source"
    if "westbase_aln_over8" in text:
        return "AUG-33399-gradient WEST case"
    if "comparison" in text:
        return "kinetic_electron_de_comparison_20260604"
    if "history_20260505_highres" in text:
        return "Older high-res kinetic-e history"
    if "history_stride3" in text:
        return "Kinetic-e stride-3 t=600-750"
    if "kinetic_electron_de_history_20260505" in text:
        return "Older kinetic-e history"
    if "itg_titeq" in text and "nl10_dt8e4_test" in text:
        return "Equal-gradient nl=10 dt8e-4 test"
    if "stride1_t800_t1000" in text:
        return "Equal-gradient nl=10 stride-1 t=800-1000"
    if "itg_titeq" in text:
        return "Equal-gradient history"
    if "tem_ti0_ky160" in text:
        return "TEM Ti0 ky160 source"
    if "tem_ti0_y020" in text:
        return "TEM Ti0 y020 ny120 source"
    if "source_gkinput6_t2000" in text:
        return "gkinput6 t2000 record500"
    if "ntheta72_l12_m20" in text:
        return "ntheta72 L12 M20 hc005"
    if "ntheta72_m12" in text:
        return "ntheta72 M12 hc05"
    if "gkinput6" in text:
        return "gkinput6 adiabatic parent"
    return _overview2_clean_title(title)


def _overview2_setup_label(entity: sqlite3.Row, metadata: dict[str, object]) -> str:
    text = " ".join(
        str(part)
        for part in (
            entity["uid"],
            entity["name"],
            entity["path"] or "",
            metadata.get("category", ""),
            metadata.get("source_window", ""),
            metadata.get("cases", ""),
        )
    ).lower()
    if "aug_boron" in text or "34021" in text or "34415" in text:
        return "AUG-like 34021/34415 on WEST geometry"
    if "westbase_aln_over8" in text or "33399" in text:
        return "AUG #33399-gradient WEST case"
    if "alti819" in text or "alti8p19" in text:
        return "WEST high a/LTi=8.19"
    if "itg_titeq" in text or "titeq" in text or "equal-gradient" in text:
        return "WEST a/LTi=a/LTe, Ti=Te"
    if "tem_ti0" in text:
        return "WEST TEM-style, a/LTi=0"
    if "gkinput6" in text or "adiabatic" in str(metadata.get("electron_model", "")).lower():
        return "WEST baseline, adiabatic e-"
    if "wmatched" in text:
        return "WEST baseline, W-matched kinetic e-"
    if "normal_gx_kinelec_trace_gxrres" in text:
        return "WEST baseline, kinetic e-"
    if "kinetic_electron_de_history" in text or "kinetic_e_de" in text:
        return "WEST baseline, kinetic e-"
    if metadata.get("metadata_confidence"):
        return "uncurated / inferred"
    return ""


def _overview2_clean_title(title: str) -> str:
    return (
        title.replace("Main Plasma GX with ", "")
        .replace("Main Plasma GX: ", "")
        .replace("Main Plasma GX ", "")
    )


def _overview2_impurities(metadata: dict[str, object]) -> str:
    z_value = metadata.get("z")
    z_values = metadata.get("z_values")
    elements = metadata.get("impurity_elements")
    if elements:
        element_text = _overview2_format(elements)
        if z_values:
            return f"{element_text} Z={_overview2_format(z_values)}"
        if z_value not in (None, "", []):
            return f"{element_text} Z={_overview2_format(z_value)}"
        return element_text
    if z_values:
        if _overview2_has_w(metadata):
            return f"W Z={_overview2_format(z_values)}"
        return f"Z={_overview2_format(z_values)}"
    if metadata.get("w_species") and z_value not in (None, "", []):
        return f"W Z={_overview2_format(z_value)}"
    if metadata.get("w_species"):
        return f"W species={_overview2_format(metadata.get('w_species'))}"
    cases = metadata.get("cases")
    return _overview2_format(cases)


def _overview2_has_w(metadata: dict[str, object]) -> bool:
    values = metadata.get("impurity_elements")
    return isinstance(values, list) and any(str(value).lower() == "w" for value in values)


def _overview2_e_field(metadata: dict[str, object]) -> str:
    electron = metadata.get("electron_model")
    field = metadata.get("field_model")
    if not electron and metadata.get("kinetic_electrons"):
        electron = "kinetic e-"
    if not field:
        fapar = metadata.get("fapar")
        fbpar = metadata.get("fbpar")
        if fapar == 0 and fbpar == 0:
            field = "ES"
        elif fapar == 1 and fbpar == 1:
            field = "full-EM"
    return ", ".join(part for part in (_overview2_format(electron), _overview2_format(field)) if part)


def _overview2_resolution(metadata: dict[str, object]) -> str:
    spatial = [metadata.get("ntheta"), metadata.get("nx"), metadata.get("ny")]
    if any(value not in (None, "") for value in spatial):
        text = " x ".join(_overview2_format(value) if value not in (None, "") else "?" for value in spatial)
    else:
        text = ""
    moments = []
    if metadata.get("nhermite") not in (None, ""):
        moments.append(f"H{_overview2_format(metadata.get('nhermite'))}")
    if metadata.get("nlaguerre") not in (None, ""):
        moments.append(f"L{_overview2_format(metadata.get('nlaguerre'))}")
    return " ".join(part for part in (text, " ".join(moments)) if part)


def _overview2_time_step(metadata: dict[str, object]) -> str:
    fixed = metadata.get("fixed_dt")
    dt = metadata.get("dt")
    cfl = metadata.get("cfl")
    if fixed is True:
        return f"fixed dt={_overview2_format(dt)}" if dt not in (None, "") else "fixed dt"
    if fixed is False:
        parts = ["adaptive"]
        if cfl not in (None, ""):
            parts.append(f"cfl={_overview2_format(cfl)}")
        if dt not in (None, ""):
            parts.append(f"init dt={_overview2_format(dt)}")
        return " ".join(parts)
    if dt not in (None, ""):
        return f"dt={_overview2_format(dt)}"
    if cfl not in (None, ""):
        return f"cfl={_overview2_format(cfl)}"
    return ""


def _overview2_saved_history(metadata: dict[str, object]) -> str:
    parts = []
    fields = metadata.get("history_fields") or metadata.get("fields")
    if fields:
        parts.append(_overview2_format(fields))
    stride = metadata.get("history_stride") or metadata.get("step_record_stride")
    if stride:
        parts.append(f"stride={_overview2_format(stride)}")
    start = metadata.get("record_start_time")
    end = metadata.get("record_tmax") or metadata.get("t_max") or metadata.get("target_t_max")
    if isinstance(end, (int, float)) and end > 1e9:
        end = None
    if start not in (None, "") or end not in (None, ""):
        parts.append(f"t={_overview2_format(start)}->{_overview2_format(end)}")
    return "; ".join(parts)


def _overview2_main_difference(entity: sqlite3.Row, metadata: dict[str, object]) -> str:
    parts = []
    variants = metadata.get("variants")
    models = metadata.get("models")
    cases = metadata.get("cases")
    if variants:
        parts.append(f"variants={_overview2_format(variants)}")
    if models:
        parts.append(f"models={_overview2_format(models)}")
    if cases:
        parts.append(f"cases={_overview2_format(cases)}")
    if metadata.get("vnewk_values"):
        parts.append(f"vnewk={_overview2_format(metadata.get('vnewk_values'))}")
    if metadata.get("temp_alti8p19") or metadata.get("temp_alti8p19_titeq"):
        parts.append("Ti/Te variants")
    if metadata.get("metadata_confidence"):
        parts.append(f"metadata={_overview2_format(metadata.get('metadata_confidence'))}")
    if not parts:
        source_window = metadata.get("source_window")
        if source_window:
            parts.append(_overview2_format(source_window))
    return "; ".join(parts)


def _overview2_format(value: object) -> str:
    if value in (None, "", []):
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:g}"
    if isinstance(value, list):
        if len(value) > 6:
            return ", ".join(_overview2_format(item) for item in value[:5]) + ", ..."
        return ", ".join(_overview2_format(item) for item in value)
    return str(value)


def _render_campaign_info(status: CampaignStatus, profile_overview: dict[str, object]) -> str:
    campaign_name = status.campaign.name if status.campaign else "Unspecified campaign"
    headline = str(profile_overview.get("headline") or campaign_name)
    summary = str(
        profile_overview.get("summary")
        or "Lite maps parent simulations, reusable data products, downstream analyses, and figures."
    )
    questions = _render_profile_list(profile_overview.get("questions"), empty="")
    return f"""
    <main class="campaign-page">
      <section class="overview-hero">
        <div>
          <div class="eyebrow">Campaign Overview</div>
          <h2>{_e(headline)}</h2>
          <p>{_e(summary)}</p>
          {questions}
        </div>
        <a class="primary-action" href="/?view=objects">Browse objects</a>
      </section>

      {_render_physics_workflow(profile_overview)}

      <section class="overview-grid">
        {_render_review_queue(status)}
        <aside class="overview-side">
          {_render_contents_summary(status)}
          {_render_status_summary(status)}
        </aside>
      </section>
    </main>
    """


def _render_profile_list(value: object, *, empty: str) -> str:
    if not isinstance(value, list) or not value:
        return empty
    items = "".join(f"<li>{_e(item)}</li>" for item in value if isinstance(item, str) and item)
    if not items:
        return empty
    return f'<ul class="overview-question-list">{items}</ul>'


def _render_physics_workflow(profile_overview: dict[str, object]) -> str:
    workflow = profile_overview.get("workflow")
    if not isinstance(workflow, list) or not workflow:
        workflow = [
            "Parent simulations produce reusable data products.",
            "Downstream studies reuse those data products with different models or parameters.",
            "Figures and diagnostics make the comparison visible.",
        ]
    cards = "".join(f"<li>{_e(item)}</li>" for item in workflow if isinstance(item, str) and item)
    return f"""
    <section class="panel physics-workflow">
      <div class="eyebrow">How To Use This Campaign Data</div>
      <h2>Physics workflow</h2>
      <ol>{cards}</ol>
    </section>
    """


def _render_physics_object_list(
    entities: list[sqlite3.Row],
    use_map: CampaignUseMap,
    campaign_type: str | None,
) -> str:
    children_by_parent = _children_by_parent_uid(use_map, campaign_type)
    nested_child_uids = {
        child.entity.uid
        for children in children_by_parent.values()
        for child in children
    }
    rows = []
    for entity in entities:
        if str(entity["type"]) == "simulation":
            row = _physics_overview_row(entity, campaign_type, level=0)
            if row:
                rows.append(row)
                rows.extend(
                    _physics_overview_object_row(child, campaign_type, level=1)
                    for child in children_by_parent.get(str(entity["uid"]), [])
                )
    for entity in entities:
        if str(entity["uid"]) in nested_child_uids or str(entity["type"]) not in {"pool", "analysis"}:
            continue
        row = _physics_overview_row(entity, campaign_type, level=0)
        if row:
            rows.append(row)
    if not rows:
        content = '<p class="muted">No simulations, replay pools, tracer analyses, or KTM studies are recorded yet.</p>'
    else:
        content = f'<div class="physics-object-list">{"".join(rows)}</div>'
    return f"""
    <section class="focus-panel physics-object-map">
      <div class="eyebrow">Overview</div>
      <h2>Simulations and impurity-response studies</h2>
      {content}
    </section>
    """


def _physics_overview_row(
    entity: sqlite3.Row,
    campaign_type: str | None,
    *,
    level: int,
) -> str:
    entity_type = str(entity["type"])
    if entity_type == "simulation":
        kind = "Main Plasma GX"
    elif entity_type in {"pool", "analysis"}:
        kind = _overview_kind_for_row(entity, campaign_type)
        if not kind:
            return ""
    else:
        return ""

    display = _display_title_for_row(entity, campaign_type)
    title = _overview_title_for_row(entity, display.title, kind)
    return _physics_title_row(uid=str(entity["uid"]), title=title, level=level)


def _physics_overview_object_row(obj: UseMapObject, campaign_type: str | None, *, level: int) -> str:
    kind = _overview_kind_for_object(
        entity_type=obj.entity.type,
        raw_name=obj.entity.name,
        metadata=obj.metadata,
        path=obj.path,
        campaign_type=campaign_type,
    )
    if not kind:
        return ""
    display = _display_title(obj, campaign_type)
    title = _overview_title_for_row_from_type(display.title, kind)
    return _physics_title_row(uid=obj.entity.uid, title=title, level=level)


def _physics_title_row(*, uid: str, title: str, level: int) -> str:
    href = "/?" + urlencode({"entity": uid})
    level_class = "tree-child" if level else "tree-root"
    return f"""
    <article class="physics-object-row {level_class}">
      <a class="overview-title" href="{href}">{_e(title)}</a>
    </article>
    """


def _overview_kind_for_row(entity: sqlite3.Row, campaign_type: str | None) -> str:
    return _overview_kind_for_object(
        entity_type=str(entity["type"]),
        raw_name=str(entity["name"]),
        metadata=metadata_from_json(entity["metadata_json"]),
        path=str(entity["path"] or ""),
        campaign_type=campaign_type,
    )


def _overview_kind_for_object(
    *,
    entity_type: str,
    raw_name: str,
    metadata: dict[str, object],
    path: str,
    campaign_type: str | None,
) -> str:
    label = display_group_for_object(
        campaign_type=campaign_type,
        entity_type=entity_type,
        raw_name=raw_name,
        metadata=metadata,
        path=path,
    )
    if label == "GX-R replay":
        return "Replay"
    if label in {"KTM / field-only models", "Tracer diagnostics"}:
        return "Kinetic Trace Model"
    if label == "Main plasma GX":
        return "Main Plasma GX"
    if label == "Main plasma XGC":
        return "Main Plasma XGC"
    return ""


def _overview_title_for_row(entity: sqlite3.Row, display_title: str, kind: str) -> str:
    if kind != "Kinetic Trace Model":
        return display_title
    return _overview_title_for_row_from_type(display_title, kind)


def _overview_title_for_row_from_type(display_title: str, kind: str) -> str:
    if kind != "Kinetic Trace Model":
        return display_title
    if display_title.startswith("Tracer diagnostic:"):
        return "Kinetic Trace Model: tracer" + display_title.removeprefix("Tracer diagnostic:")
    if display_title.startswith("Kinetic Trace Model: response of"):
        return display_title.replace("Kinetic Trace Model: response of", "Kinetic Trace Model: field response of", 1)
    return display_title


def _children_by_parent_uid(use_map: CampaignUseMap, campaign_type: str | None) -> dict[str, list[UseMapObject]]:
    children_by_parent: dict[str, list[UseMapObject]] = {}
    seen_by_parent: dict[str, set[str]] = {}
    for history in use_map.histories:
        children = [
            obj
            for group in history.use_groups
            for obj in group.objects
            if _overview_kind_for_object(
                entity_type=obj.entity.type,
                raw_name=obj.entity.name,
                metadata=obj.metadata,
                path=obj.path,
                campaign_type=campaign_type,
            )
        ]
        if not children:
            continue
        for parent in history.parents:
            seen = seen_by_parent.setdefault(parent.entity.uid, set())
            target = children_by_parent.setdefault(parent.entity.uid, [])
            for child in children:
                if child.entity.uid in seen:
                    continue
                target.append(child)
                seen.add(child.entity.uid)
    return children_by_parent


def _render_standalone_simulations(use_map: CampaignUseMap, campaign_type: str | None) -> str:
    simulations = "".join(_render_standalone_simulation(obj, campaign_type) for obj in use_map.standalone_simulations)
    if not simulations:
        simulations = '<p class="muted">No main-plasma simulations without saved histories are recorded yet.</p>'
    return f"""
    <section class="focus-panel standalone-simulation-map">
      <div class="eyebrow">Main Plasma Runs</div>
      <h2>Simulations without saved histories</h2>
      <p class="panel-help">
        These simulations do not produce a registered reusable field-history object, but they can still be
        primary physics results, convergence references, or coupled impurity runs.
      </p>
      <div class="standalone-simulation-list">{simulations}</div>
    </section>
    """


def _render_standalone_simulation(obj: UseMapObject, campaign_type: str | None) -> str:
    display = _display_title(obj, campaign_type)
    metadata = _render_metadata_chips(obj.metadata)
    raw = _render_raw_name_hint(obj, display.title)
    subtitle = f'<p class="simulation-subtitle">{_e(display.subtitle)}</p>' if display.subtitle else ""
    return f"""
    <article class="standalone-simulation-card">
      <div>
        <h3>{_object_link(obj, campaign_type)}</h3>
        {subtitle}
        <div class="history-meta">{metadata}</div>
        {raw}
      </div>
      <div class="status-stack compact-stack">
        <span class="status-badge status-{_class_token(obj.entity.status)}">{_e(obj.entity.status or 'unknown')}</span>
        <span class="status-badge scientific">{_e(obj.entity.scientific_status or 'candidate')}</span>
      </div>
    </article>
    """


def _render_data_product_map(use_map: CampaignUseMap, campaign_type: str | None) -> str:
    histories = "".join(_render_history_map(history, campaign_type) for history in use_map.histories)
    if not histories:
        histories = '<p class="muted">No reusable data products with downstream users are recorded yet.</p>'
    return f"""
    <section class="focus-panel data-product-map">
      <div class="eyebrow">Available Data</div>
      <h2>Saved histories and downstream studies</h2>
      <p class="panel-help">
        Each row starts from a reusable data product, then shows the parent simulation that produced it
        and the downstream studies that reuse it.
      </p>
      <div class="history-list">{histories}</div>
    </section>
    """


def _render_history_map(history: HistoryMap, campaign_type: str | None) -> str:
    history_link = _object_link(history.history, campaign_type)
    parent_links = _render_object_link_list(history.parents, campaign_type, empty="No parent simulation recorded.")
    metadata = _render_metadata_chips(history.history.metadata)
    users = "".join(_render_history_use_group(group, campaign_type) for group in history.use_groups)
    if not users:
        users = '<p class="muted compact">No downstream study recorded.</p>'
    return f"""
    <article class="history-card">
      <div class="history-main">
        <div>
          <h3>{history_link}</h3>
          <div class="history-meta">{metadata}</div>
        </div>
        <div class="history-parents">
          <span>Produced by</span>
          {parent_links}
        </div>
      </div>
      <div class="history-users">{users}</div>
    </article>
    """


def _render_history_use_group(group: HistoryUseGroup, campaign_type: str | None) -> str:
    label = _semantic_group_label(group, campaign_type)
    objects = "".join(_render_downstream_object(obj, campaign_type) for obj in group.objects)
    return f"""
    <section class="use-group">
      <h4>{_e(label)}</h4>
      <div class="use-object-list">{objects}</div>
    </section>
    """


def _render_downstream_object(obj: UseMapObject, campaign_type: str | None) -> str:
    display = _display_title(obj, campaign_type)
    label = display.subtitle or object_summary_label(obj)
    details = f'<span>{_e(label)}</span>' if label else ""
    raw = _render_raw_name_hint(obj, display.title)
    return f"""
    <article class="use-object">
      {_object_link(obj, campaign_type)}
      {details}
      {raw}
    </article>
    """


def _render_comparison_map(use_map: CampaignUseMap, campaign_type: str | None) -> str:
    rows = []
    for history in use_map.histories:
        downstream_count = sum(len(group.objects) for group in history.use_groups)
        if downstream_count < 2:
            continue
        group_labels = ", ".join(group.label for group in history.use_groups)
        rows.append(
            f"""
              <tr>
              <td>{_object_link(history.history, campaign_type)}</td>
              <td>{downstream_count}</td>
              <td>{_e(_semantic_group_labels(history.use_groups, campaign_type))}</td>
            </tr>
            """
        )
    table = _table(["shared data product", "downstream studies", "study groups"], rows) if rows else (
        '<p class="muted">No shared-data comparison groups are recorded yet.</p>'
    )
    return f"""
    <section class="panel comparison-map">
      <div class="eyebrow">Comparison Opportunities</div>
      <h2>Studies using the same parent data</h2>
      <p class="panel-help">
        These are the places where different models, numerical choices, or analysis methods can be compared
        on the same saved parent data.
      </p>
      {table}
    </section>
    """


def _render_review_queue(status: CampaignStatus) -> str:
    attention = "".join(_render_attention_row(item) for item in status.attention if item.count)
    if not attention:
        attention = '<p class="muted">No warnings, todos, missing receipts, or disconnected objects are recorded.</p>'
    return f"""
    <section class="focus-panel">
      <div class="eyebrow">Curation</div>
      <h2>Metadata that still needs attention</h2>
      <p class="panel-help">
        These items do not block the physics map, but they tell us where the campaign record is incomplete.
      </p>
      <div class="review-list">{attention}</div>
    </section>
    """


def _render_contents_summary(status: CampaignStatus) -> str:
    return f"""
    <section class="panel">
      <h2>Registered Objects</h2>
      <p class="panel-help">Current inventory known to Lite.</p>
      <div class="total-line"><strong>{status.entity_total}</strong> objects</div>
      <div class="chip-row">{_render_count_chips(status.counts_by_type)}</div>
    </section>
    """


def _render_status_summary(status: CampaignStatus) -> str:
    return f"""
    <section class="panel">
      <h2>Status Summary</h2>
      <details open>
        <summary>Run or availability state</summary>
        <div class="chip-row">{_render_count_chips(status.counts_by_status)}</div>
      </details>
      <details>
        <summary>Scientific confidence</summary>
        <div class="chip-row">{_render_count_chips(status.counts_by_scientific_status)}</div>
      </details>
    </section>
    """


def _object_link(obj: UseMapObject, campaign_type: str | None) -> str:
    display = _display_title(obj, campaign_type)
    href = "/?" + urlencode({"entity": obj.entity.uid})
    return f'<a href="{href}">{_e(display.title)}</a>'


def _render_object_link_list(objects: list[UseMapObject], campaign_type: str | None, *, empty: str) -> str:
    if not objects:
        return f'<p class="muted compact">{_e(empty)}</p>'
    links = " · ".join(_object_link(obj, campaign_type) for obj in objects)
    return f'<p class="entity-ref-list">{links}</p>'


def _display_title(obj: UseMapObject, campaign_type: str | None):
    return display_title_for_object(
        campaign_type=campaign_type,
        entity_type=obj.entity.type,
        raw_name=obj.entity.name,
        metadata=obj.metadata,
        path=obj.path,
    )


def _display_title_for_row(row: sqlite3.Row, campaign_type: str | None):
    return display_title_for_object(
        campaign_type=campaign_type,
        entity_type=str(row["type"]),
        raw_name=str(row["name"]),
        metadata=metadata_from_json(row["metadata_json"]),
        path=str(row["path"] or ""),
    )


def _semantic_group_label(group: HistoryUseGroup, campaign_type: str | None) -> str:
    if not group.objects:
        return group.label
    first = group.objects[0]
    return display_group_for_object(
        campaign_type=campaign_type,
        entity_type=first.entity.type,
        raw_name=first.entity.name,
        metadata=first.metadata,
        path=first.path,
    )


def _semantic_group_labels(groups: list[HistoryUseGroup], campaign_type: str | None) -> str:
    return ", ".join(dict.fromkeys(_semantic_group_label(group, campaign_type) for group in groups))


def _render_raw_name_hint(obj: UseMapObject, display_title: str) -> str:
    if obj.entity.name == display_title:
        return ""
    return f'<span class="raw-name-hint">raw: {_e(obj.entity.name)}</span>'


def _render_metadata_chips(metadata: dict[str, object]) -> str:
    items = metadata_summary(metadata)
    if not items:
        return '<span class="chip">metadata pending</span>'
    return "".join(f'<span class="chip">{_e(key)} <strong>{_e(value)}</strong></span>' for key, value in items)


def _render_count_chips(counts: dict[str, int]) -> str:
    if not counts:
        return '<span class="chip">none</span>'
    return "".join(
        f'<span class="chip">{_e(name)} <strong>{count}</strong></span>'
        for name, count in sorted(counts.items())
    )


def _render_attention_item(item: AttentionItem) -> str:
    count_class = "attention-ok" if item.count == 0 else "attention-needs-review"
    samples = _render_entity_refs(item.sample_entities)
    return f"""
    <section class="attention-item {count_class}">
      <div class="attention-count">{item.count}</div>
      <div>
        <h3>{_e(item.label)}</h3>
        {samples}
      </div>
    </section>
    """


def _render_attention_row(item: AttentionItem) -> str:
    samples = _render_entity_refs(item.sample_entities)
    return f"""
    <article class="review-row">
      <div class="review-count">{item.count}</div>
      <div>
        <h3>{_e(item.label)}</h3>
        {samples}
      </div>
    </article>
    """


def _render_entity_refs(entities: list[EntityRef]) -> str:
    if not entities:
        return '<p class="muted compact">No examples.</p>'
    links = []
    for entity in entities:
        href = "/?" + urlencode({"entity": entity.uid})
        links.append(f'<a href="{href}">{_e(entity.name)}</a>')
    return f'<p class="entity-ref-list"><span>Examples:</span> {" · ".join(links)}</p>'


def _render_recent_notes(status: CampaignStatus) -> str:
    if not status.recent_notes:
        return '<p class="muted compact">No recent warning or todo notes.</p>'
    rows = []
    for note in status.recent_notes:
        if note.entity:
            href = "/?" + urlencode({"entity": note.entity.uid})
            target = f'<a href="{href}">{_e(note.entity.name)}</a>'
        else:
            target = ""
        rows.append(
            f"""
            <tr>
              <td>{_e(note.note_type)}</td>
              <td>{target}</td>
              <td>{_e(note.markdown_text)}</td>
            </tr>
            """
        )
    return f"""
    <section class="health-band recent-notes">
      <h3>Recent notes that need attention</h3>
      {_table(["type", "entity", "note"], rows)}
    </section>
    """


def _render_entity_nav(
    entities: list[sqlite3.Row],
    selected_uid: str,
    selected_type: str,
    campaign_type: str | None,
) -> str:
    groups: dict[str, list[sqlite3.Row]] = {}
    for entity in entities:
        groups.setdefault(str(entity["type"]), []).append(entity)
    sections = []
    for type_name in sorted(groups):
        links = []
        for entity in groups[type_name]:
            href = "/?" + urlencode({"entity": entity["uid"]})
            active = " active" if entity["uid"] == selected_uid else ""
            source_label = _entity_source_label(entity)
            display = _display_title_for_row(entity, campaign_type)
            raw_name = ""
            if display.title != entity["name"]:
                raw_name = f'<span class="entity-raw-name">{_e(entity["name"])}</span>'
            links.append(
                f"""
                <a class="entity-link{active}" href="{href}">
                  <span class="entity-name">{_e(display.title)}</span>
                  {raw_name}
                  <span class="entity-meta">
                    <span class="status-dot status-{_class_token(entity['status'])}"></span>
                    {_e(entity['status'])} · {_e(entity['scientific_status'])}
                    {source_label}
                  </span>
                </a>
                """
            )
        open_attr = " open" if type_name == selected_type else ""
        sections.append(
            f"""
            <details class="nav-group"{open_attr}>
              <summary>{_e(type_name)} <span>{len(groups[type_name])}</span></summary>
              <div class="nav-links">{''.join(links)}</div>
            </details>
            """
        )
    return f"""
    <section class="panel nav-panel">
      <h2>Objects</h2>
      <p class="panel-help">Click one object to inspect its inputs, outputs, notes, metrics, and files.</p>
      {''.join(sections)}
    </section>
    """


def _entity_source_label(entity: sqlite3.Row) -> str:
    try:
        metadata = json.loads(entity["metadata_json"] or "{}")
    except json.JSONDecodeError:
        return ""
    if metadata.get("discovery_source"):
        return '<span class="source-mini">auto</span>'
    return ""


def _render_entity_header(entity: sqlite3.Row, campaign_type: str | None) -> str:
    path = entity["path"] or ""
    display = _display_title_for_row(entity, campaign_type)
    raw_name = ""
    if display.title != entity["name"]:
        raw_name = f'<div class="raw-title-row"><span>raw name</span><code>{_e(entity["name"])}</code></div>'
    subtitle = f'<p class="description">{_e(display.subtitle)}</p>' if display.subtitle else _render_description(entity['description'])
    return f"""
    <section class="entity-hero">
      <div class="entity-title-block">
        <div class="eyebrow">Selected Object</div>
        <div class="type-label">{_e(entity['type'])}</div>
        <h2>{_e(display.title)}</h2>
        <code>{_e(entity['uid'])}</code>
      </div>
      <div class="status-stack">
        <span class="status-badge status-{_class_token(entity['status'])}">{_e(entity['status'])}</span>
        <span class="status-badge scientific">{_e(entity['scientific_status'])}</span>
      </div>
      {subtitle}
      {raw_name}
      {_render_path(path)}
    </section>
    """


def _render_description(description: str) -> str:
    if not description:
        return ""
    return f'<p class="description">{_e(description)}</p>'


def _render_path(path: str) -> str:
    if not path:
        return ""
    return f'<div class="path-row"><span>path</span><code title="{_e(path)}">{_e(path)}</code></div>'


def _render_figure_preview(entity: sqlite3.Row, artifacts: list[sqlite3.Row], db_path: Path) -> str:
    if entity["type"] != "figure":
        return ""
    image_artifacts = [
        artifact
        for artifact in artifacts
        if _is_preview_image(_artifact_path_for_extension(str(artifact["path"])), artifact["format"])
    ]
    local_images = [
        artifact
        for artifact in image_artifacts
        if _resolve_existing_artifact_path(str(artifact["path"]), db_path) is not None
    ]
    if not image_artifacts:
        return """
        <section class="panel figure-preview">
          <h2>Figure Preview</h2>
          <p class="muted">No image output is recorded for this figure yet.</p>
        </section>
        """
    if not local_images:
        rows = []
        for artifact in image_artifacts:
            rows.append(
                f"""
                <tr>
                  <td>{_e(artifact['format'] or _artifact_path_for_extension(str(artifact['path'])).suffix.lstrip('.'))}</td>
                  <td><code title="{_e(artifact['path'])}">{_e(artifact['path'])}</code></td>
                </tr>
                """
            )
        return f"""
        <section class="panel figure-preview">
          <h2>Figure Preview</h2>
          <p class="muted">Image output is recorded, but no local preview file was found.</p>
          {_table(["format", "recorded path"], rows)}
        </section>
        """

    primary = _preferred_preview_artifact(local_images)
    src = "/artifact?" + urlencode({"id": str(primary["id"])})
    resolved = _resolve_existing_artifact_path(str(primary["path"]), db_path)
    other_images = [artifact for artifact in local_images if int(artifact["id"]) != int(primary["id"])]
    alternatives = _render_preview_alternatives(other_images)
    return f"""
    <section class="panel figure-preview">
      <div class="figure-preview-header">
        <h2>Figure Preview</h2>
        <span class="status-badge status-available">local image</span>
      </div>
      <figure>
        <img src="{src}" alt="{_e(entity['name'])}" loading="lazy" />
        <figcaption>
          <code title="{_e(resolved)}">{_e(primary['path'])}</code>
        </figcaption>
      </figure>
      {alternatives}
    </section>
    """


def _render_preview_alternatives(artifacts: list[sqlite3.Row]) -> str:
    if not artifacts:
        return ""
    links = []
    for artifact in artifacts:
        href = "/artifact?" + urlencode({"id": str(artifact["id"])})
        label = artifact["format"] or _artifact_path_for_extension(str(artifact["path"])).suffix.lstrip(".")
        links.append(f'<a href="{href}" target="_blank" rel="noreferrer">{_e(label)}</a>')
    return f'<p class="preview-links">Other local image outputs: {" · ".join(links)}</p>'


def _preferred_preview_artifact(artifacts: list[sqlite3.Row]) -> sqlite3.Row:
    priority = {".png": 0, ".jpg": 1, ".jpeg": 1, ".svg": 2, ".webp": 3, ".gif": 4}
    return sorted(
        artifacts,
        key=lambda artifact: (
            priority.get(_artifact_path_for_extension(str(artifact["path"])).suffix.lower(), 99),
            str(artifact["path"]),
        ),
    )[0]


def _render_relation_grid(
    entity: sqlite3.Row,
    outgoing: list[sqlite3.Row],
    incoming: list[sqlite3.Row],
) -> str:
    return f"""
    <div class="two-column relation-row">
      <section class="panel">
        <h2>Inputs Used By This Object</h2>
        {_render_relation_list(entity['uid'], outgoing, direction='outgoing')}
      </section>
      <section class="panel">
        <h2>Objects That Use This</h2>
        {_render_relation_list(entity['uid'], incoming, direction='incoming')}
      </section>
    </div>
    """


def _render_relation_list(entity_uid: str, relations: list[sqlite3.Row], *, direction: str) -> str:
    if not relations:
        return '<p class="muted">No dependency link recorded yet.</p>'
    items = []
    for rel in relations:
        if direction == "outgoing":
            other_uid = rel["target_uid"]
            other_name = rel["target_name"]
            arrow = f"uses via {_e(rel['relation_type'])}"
        else:
            other_uid = rel["source_uid"]
            other_name = rel["source_name"]
            arrow = f"uses this via {_e(rel['relation_type'])}"
        href = "/?" + urlencode({"entity": other_uid})
        note = f'<p class="relation-note">{_e(rel["note"])}</p>' if rel["note"] else ""
        items.append(
            f"""
            <li>
              <div class="relation-type">{arrow}</div>
              <a href="{href}">{_e(other_name)}</a>
              <code>{_e(other_uid)}</code>
              {note}
            </li>
            """
        )
    return f'<ul class="relation-list">{"".join(items)}</ul>'


def _render_metrics(metrics: list[sqlite3.Row]) -> str:
    rows = []
    for metric in metrics:
        value = "" if metric["value"] is None else f"{metric['value']:g}"
        unit = metric["unit"] or ""
        context = _compact_json(metric["context_json"])
        rows.append(
            f"""
            <tr>
              <td>{_e(metric['name'])}</td>
              <td>{_e(value)}</td>
              <td>{_e(unit)}</td>
              <td>{_e(context)}</td>
            </tr>
            """
        )
    table = _table(["name", "value", "unit", "context"], rows) if rows else '<p class="muted">No metrics recorded.</p>'
    return f'<section class="panel"><h2>Metrics</h2>{table}</section>'


def _render_notes(notes: list[sqlite3.Row]) -> str:
    if not notes:
        content = '<p class="muted">No notes recorded.</p>'
    else:
        entries = []
        for note in notes:
            byline = note["note_type"]
            if note["author"]:
                byline += f" by {note['author']}"
            entries.append(
                f"""
                <article class="note">
                  <div class="note-meta">{_e(byline)} · {_e(note['created_at'])}</div>
                  <p>{_e(note['markdown_text'])}</p>
                </article>
                """
            )
        content = "".join(entries)
    return f'<section class="panel"><h2>Notes</h2>{content}</section>'


def _render_artifacts(artifacts: list[sqlite3.Row]) -> str:
    rows = []
    for artifact in artifacts:
        location = _artifact_location_label(str(artifact["path"]), bool(artifact["exists_on_disk"]))
        rows.append(
            f"""
            <tr>
              <td>{_e(artifact['kind'])}</td>
              <td><code title="{_e(artifact['path'])}">{_e(artifact['path'])}</code></td>
              <td>{_e(artifact['format'] or '')}</td>
              <td>{_e(location)}</td>
              <td>{_e(artifact['description'])}</td>
            </tr>
            """
        )
    table = _table(["kind", "path", "format", "location", "description"], rows) if rows else '<p class="muted">No artifacts recorded.</p>'
    return f'<section class="panel"><h2>Artifacts</h2>{table}</section>'


def _render_metadata(entity: sqlite3.Row) -> str:
    metadata = _pretty_json(entity["metadata_json"])
    return f"""
    <section class="panel">
      <h2>Metadata</h2>
      <pre class="json-block">{_e(metadata)}</pre>
    </section>
    """


def _table(headers: list[str], rows: list[str]) -> str:
    head = "".join(f"<th>{_e(header)}</th>" for header in headers)
    return f'<div class="table-wrapper"><table><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'


def _empty_state(db_path: Path, status: CampaignStatus) -> str:
    return f"""
    {_render_topbar(db_path, "lineage")}
    <main class="empty">
      <section class="panel">
        <h2>No Entities</h2>
        <p class="muted">Import a sidecar packet to populate this registry.</p>
      </section>
    </main>
    """


def _page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{_e(title)}</title>
    <link rel="stylesheet" href="/static/lite.css" />
  </head>
  <body>
    {body}
    <script>{LITE_JS}</script>
  </body>
</html>
"""


def _pretty_json(value: str) -> str:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError:
        return value
    return json.dumps(parsed, indent=2, sort_keys=True)


def _compact_json(value: str) -> str:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError:
        return value
    if not parsed:
        return ""
    return json.dumps(parsed, sort_keys=True)


def _artifact_location_label(path: str, exists_on_disk: bool) -> str:
    if exists_on_disk:
        return "exists locally"
    if path.startswith(("/pscratch/", "/global/", "/grand/", "/flare/", "/eagle/", "/lus/")):
        return "remote path"
    return "not found locally"


def _remote_status_hint(entity: sqlite3.Row) -> dict[str, object]:
    metadata = metadata_from_json(entity["metadata_json"])
    path = _first_nonempty(
        entity["path"],
        _metadata_value(metadata, "remote_path", "run_root", "run_dir", "pool_root", "input_path", "output_path", "path"),
    )
    host_text = str(_metadata_value(metadata, "remote_host", "host", "hpc_host", "machine", "cluster") or "").lower()
    path_text = str(path or "")
    is_perlmutter = "perlmutter" in host_text or path_text.startswith(("/pscratch/", "/global/", "/global/homes/"))
    job_id = _metadata_value(metadata, "slurm_job_id", "slurm_id", "job_id", "jobid", "scheduler_job_id", "batch_job_id")
    job_name = _metadata_value(metadata, "slurm_job_name", "job_name", "batch_job_name", "scheduler_job_name")
    return {
        "available": bool(is_perlmutter or job_id or job_name),
        "host": "perlmutter" if is_perlmutter or "perlmutter" in host_text else str(host_text or ""),
        "path": str(path or ""),
        "job_id": _scalar_text(job_id),
        "job_name": _scalar_text(job_name),
        "metadata": metadata,
    }


def _metadata_value(metadata: dict[str, object], *keys: str) -> object:
    for key in keys:
        value = metadata.get(key)
        if value not in (None, "", []):
            return value
    return None


def _first_nonempty(*values: object) -> object:
    for value in values:
        if value not in (None, "", []):
            return value
    return None


def _scalar_text(value: object) -> str:
    if value in (None, "", []):
        return ""
    if isinstance(value, (list, tuple)):
        return _scalar_text(value[0]) if value else ""
    return str(value).strip()


def _cached_remote_state(entity: sqlite3.Row, hint: dict[str, object]) -> str:
    metadata = hint.get("metadata") if isinstance(hint.get("metadata"), dict) else {}
    texts = [
        entity["status"],
        _metadata_value(
            metadata,
            "slurm_state",
            "job_state",
            "scheduler_state",
            "queue_state",
            "remote_status",
            "operational_status",
        ),
    ]
    for text in texts:
        state = _normalize_remote_state(str(text or ""))
        if state in {"running", "pending", "complete", "failed"}:
            return state
    status_counts = metadata.get("status_counts")
    if isinstance(status_counts, dict):
        if _count_status(status_counts, "RUNNING", "R") > 0:
            return "running"
        if _count_status(status_counts, "PENDING", "PD", "QUEUED", "TORUN", "WAIT") > 0:
            return "pending"
    if hint.get("job_id") or hint.get("job_name") or hint.get("path"):
        return "remote"
    return "hidden"


def _count_status(status_counts: dict[object, object], *keys: str) -> float:
    total = 0.0
    for key, value in status_counts.items():
        if str(key).upper() not in keys:
            continue
        try:
            total += float(value)
        except (TypeError, ValueError):
            pass
    return total


def _normalize_remote_state(value: str) -> str:
    text = value.strip().lower()
    if text in {"r", "run", "running", "active"} or "running" in text:
        return "running"
    if text in {"pd", "pending", "queued", "queue", "submitted", "torun", "wait"} or "pending" in text:
        return "pending"
    if text in {"success", "complete", "completed", "done"} or "completed" in text:
        return "complete"
    if text in {"failed", "error", "crashed", "timeout", "cancelled", "canceled"}:
        return "failed"
    if text in {"not_found", "not found", "not-in-queue"}:
        return "not-found"
    return "unknown"


def _remote_status_short_label(state: str) -> str:
    return {
        "running": "R",
        "pending": "P",
        "complete": "OK",
        "failed": "!",
        "remote": "?",
        "checking": "...",
        "not-found": "-",
        "unavailable": "-",
        "error": "!",
    }.get(state, "?")


def _remote_status_title_for_hint(hint: dict[str, object], state: str) -> str:
    parts = ["Click to check Perlmutter Slurm status"]
    if state in {"running", "pending"}:
        parts.append(f"cached={state}")
    if hint.get("job_id"):
        parts.append(f"job={hint['job_id']}")
    if hint.get("job_name"):
        parts.append(f"name={hint['job_name']}")
    if hint.get("path"):
        parts.append(f"path={hint['path']}")
    return "; ".join(parts)


def _check_remote_status(db_path: Path, uid: str | None) -> dict[str, object]:
    if not uid:
        return _remote_status_payload("error", "Missing object uid.")
    try:
        with LiteRepository(db_path) as repo:
            entity = repo.get_entity(uid)
    except KeyError:
        return _remote_status_payload("error", f"Unknown object: {uid}")
    hint = _remote_status_hint(entity)
    if not hint["available"]:
        return _remote_status_payload("unavailable", "No remote scheduler metadata is recorded for this object.")
    if hint.get("host") != "perlmutter":
        return _remote_status_payload("unavailable", f"Live checks are only implemented for Perlmutter; host={hint.get('host') or 'unknown'}.")
    return _check_perlmutter_slurm_status(str(uid), hint)


def _check_perlmutter_slurm_status(uid: str, hint: dict[str, object]) -> dict[str, object]:
    command = (
        'squeue -u "$USER" -h -o "%i" 2>/dev/null | '
        'while read jid; do '
        'scontrol show job "$jid" 2>/dev/null | tr "\\n" " "; printf "\\n"; '
        'done'
    )
    result = _run_perlmutter_command(command)
    if result["returncode"] != 0:
        stderr = str(result.get("stderr") or "").strip()
        message = stderr or "Perlmutter Slurm status check failed."
        return _remote_status_payload("error", message, source="perlmutter:ssh", uid=uid)
    jobs = _parse_scontrol_jobs(str(result.get("stdout") or ""))
    if not jobs:
        return _remote_status_payload("not-found", "No Slurm jobs are currently listed for this Perlmutter user.", source="perlmutter:squeue", uid=uid)
    match = _match_remote_job(jobs, hint)
    if match is None:
        return _remote_status_payload(
            "not-found",
            f"No matching Slurm job found for this object. Checked {len(jobs)} current user job(s).",
            source="perlmutter:squeue+scontrol",
            uid=uid,
            checked_jobs=len(jobs),
        )
    state = _normalize_remote_state(str(match.get("JobState") or match.get("State") or ""))
    if state == "unknown":
        state = "remote"
    job_id = match.get("JobId") or match.get("ArrayJobId") or ""
    name = match.get("JobName") or ""
    runtime = match.get("RunTime") or match.get("Elapsed") or ""
    reason = match.get("Reason") or match.get("NodeList") or ""
    workdir = match.get("WorkDir") or ""
    message_parts = [f"Perlmutter {str(match.get('JobState') or state).upper()}"]
    if job_id:
        message_parts.append(f"job {job_id}")
    if name:
        message_parts.append(str(name))
    if runtime:
        message_parts.append(f"runtime {runtime}")
    if reason and reason != "None":
        message_parts.append(str(reason))
    return _remote_status_payload(
        state,
        "; ".join(message_parts),
        source="perlmutter:squeue+scontrol",
        uid=uid,
        job_id=str(job_id),
        job_name=str(name),
        runtime=str(runtime),
        reason=str(reason),
        workdir=str(workdir),
        checked_jobs=len(jobs),
    )


def _run_perlmutter_command(command: str) -> dict[str, object]:
    target = os.environ.get("DATAMAK_LITE_PERLMUTTER_TARGET")
    if not target:
        user = os.environ.get("DATAMAK_LITE_PERLMUTTER_USER") or os.environ.get("USER") or ""
        target = f"{user}@perlmutter.nersc.gov" if user else "perlmutter.nersc.gov"
    ssh_command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=20",
    ]
    key_path = Path.home() / ".ssh" / "nersc"
    cert_path = Path.home() / ".ssh" / "nersc-cert.pub"
    if key_path.exists() and cert_path.exists():
        ssh_command.extend(
            [
                "-F",
                "/dev/null",
                "-i",
                str(key_path),
                "-o",
                f"CertificateFile={cert_path}",
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "PreferredAuthentications=publickey",
            ]
        )
    ssh_command.extend([target, command])
    try:
        completed = subprocess.run(
            ssh_command,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"returncode": 124, "stdout": "", "stderr": str(exc)}
    return {"returncode": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr}


def _parse_scontrol_jobs(output: str) -> list[dict[str, str]]:
    jobs = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        fields = dict(re.findall(r"([A-Za-z][A-Za-z0-9_]*)=([^ ]*)", line))
        if fields:
            jobs.append(fields)
    return jobs


def _match_remote_job(jobs: list[dict[str, str]], hint: dict[str, object]) -> dict[str, str] | None:
    job_id = str(hint.get("job_id") or "")
    if job_id:
        for job in jobs:
            job_ids = {str(job.get("JobId") or ""), str(job.get("ArrayJobId") or "")}
            if job_id in job_ids:
                return job
    job_name = str(hint.get("job_name") or "")
    if job_name:
        for job in jobs:
            if job_name == str(job.get("JobName") or ""):
                return job
    remote_path = str(hint.get("path") or "")
    if remote_path:
        for job in jobs:
            if _remote_path_matches_job(remote_path, job):
                return job
    return None


def _remote_path_matches_job(remote_path: str, job: dict[str, str]) -> bool:
    path = remote_path.rstrip("/")
    if not path:
        return False
    for key in ("WorkDir", "Command", "StdOut", "StdErr"):
        value = str(job.get(key) or "").rstrip("/")
        if not value:
            continue
        if value == path or value.startswith(path + "/") or path.startswith(value + "/"):
            return True
    return False


def _remote_status_payload(state: str, message: str, **extra: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "state": state,
        "short_label": _remote_status_short_label(state),
        "message": message,
        "checked_at": int(time.time()),
    }
    payload.update(extra)
    return payload


def _load_artifact_for_preview(db_path: Path, artifact_id: str | None) -> sqlite3.Row:
    if artifact_id is None or not artifact_id.isdigit():
        raise ValueError("Invalid artifact id")
    with LiteRepository(db_path) as repo:
        row = repo.conn.execute(
            """
            SELECT a.*
            FROM artifact a
            JOIN entity e ON e.id=a.entity_id
            WHERE a.id=? AND e.type='figure'
            """,
            (int(artifact_id),),
        ).fetchone()
    if row is None:
        raise KeyError(f"Unknown figure artifact: {artifact_id}")
    return row


def _resolve_existing_artifact_path(path: str, db_path: Path) -> Path | None:
    raw = Path(path).expanduser()
    candidates = [raw] if raw.is_absolute() else []
    if not raw.is_absolute():
        candidates.extend(
            [
                raw,
                db_path.parent / raw,
                db_path.parent.parent / raw,
                Path.cwd() / raw,
                Path.cwd().parent / raw,
            ]
        )
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if resolved.is_file():
            return resolved
    return None


def _is_preview_image(path: Path, artifact_format: object = None) -> bool:
    suffix = path.suffix.lower()
    if suffix in PREVIEW_IMAGE_EXTENSIONS:
        return True
    fmt = str(artifact_format or "").lower().strip(".")
    return f".{fmt}" in PREVIEW_IMAGE_EXTENSIONS


def _artifact_path_for_extension(path: str) -> Path:
    return Path(path.split("?", 1)[0])


def _class_token(value: object) -> str:
    return "".join(ch if ch.isalnum() else "-" for ch in str(value or "unknown").lower()).strip("-")


def _e(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


LITE_JS = """
document.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-remote-status-uid]");
  if (!button) {
    return;
  }
  event.preventDefault();
  event.stopPropagation();
  const uid = button.getAttribute("data-remote-status-uid");
  if (!uid) {
    return;
  }
  const originalText = button.textContent;
  button.textContent = "...";
  button.classList.add("remote-status-checking");
  button.disabled = true;
  try {
    const response = await fetch("/remote-status?uid=" + encodeURIComponent(uid), {
      headers: {"Accept": "application/json"},
    });
    const payload = await response.json();
    const state = String(payload.state || "unknown").replace(/[^A-Za-z0-9_-]/g, "-").toLowerCase();
    button.className = "remote-status-badge remote-status-" + state;
    button.textContent = payload.short_label || "?";
    button.title = payload.message || "Remote status checked.";
    button.setAttribute("aria-label", button.title);
  } catch (error) {
    button.className = "remote-status-badge remote-status-error";
    button.textContent = "!";
    button.title = "Remote status check failed: " + error;
    button.setAttribute("aria-label", button.title);
  } finally {
    button.disabled = false;
    if (!button.textContent) {
      button.textContent = originalText || "?";
    }
  }
});
"""


LITE_CSS = """
:root {
  --gui-text-strong: #243b53;
  --gui-text-primary: #1f2933;
  --gui-text-muted: #52606d;
  --gui-border: #d9e2ec;
  --gui-border-strong: #cbd5e0;
  --gui-border-active: #a7c4ff;
  --gui-surface: #ffffff;
  --gui-surface-active: #e6f0ff;
  --gui-surface-hover: #f8fbff;
  --gui-accent: #0b3d91;
  --gui-danger: #a61b1b;
  --gui-warning: #a15c07;
  --gui-success: #14532d;
  --page-bg: #f7f9fc;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  background: var(--page-bg);
  color: var(--gui-text-primary);
  font-family: "Helvetica Neue", Arial, sans-serif;
  font-size: 13px;
  line-height: 1.35;
}

a {
  color: var(--gui-accent);
  text-decoration: none;
}

a:hover {
  text-decoration: underline;
}

code,
pre {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
}

.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 16px 24px;
  background: var(--gui-surface);
  border-bottom: 1px solid var(--gui-border);
  position: sticky;
  top: 0;
  z-index: 10;
}

.top-nav {
  display: flex;
  gap: 6px;
  margin-left: auto;
}

.top-nav-link,
.primary-action {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  padding: 6px 10px;
  background: #f8fafc;
  color: var(--gui-text-primary);
  font-weight: 700;
  white-space: nowrap;
}

.top-nav-link:hover,
.primary-action:hover {
  text-decoration: none;
  background: var(--gui-surface-hover);
}

.top-nav-link.active,
.primary-action {
  border-color: var(--gui-border-active);
  background: var(--gui-surface-active);
  color: var(--gui-accent);
}

.eyebrow {
  color: var(--gui-accent);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0;
  text-transform: uppercase;
}

h1,
h2 {
  margin: 0;
  color: var(--gui-text-strong);
}

h1 {
  font-size: 20px;
}

h2 {
  font-size: 15px;
  margin-bottom: 10px;
}

h3 {
  margin: 0 0 8px;
  color: var(--gui-text-strong);
  font-size: 12px;
}

h4 {
  margin: 0 0 6px;
  color: var(--gui-text-strong);
  font-size: 12px;
}

.db-pill,
.chip,
.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: 1px solid var(--gui-border);
  background: #f0f4f8;
  color: #334e68;
  border-radius: 999px;
  padding: 3px 9px;
  white-space: nowrap;
}

.layout {
  display: grid;
  grid-template-columns: minmax(260px, 340px) minmax(0, 1fr);
  gap: 16px;
  padding: 16px 24px 24px;
}

.overview-page,
.campaign-page {
  padding: 16px 24px 24px;
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.overview-hero {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  background: var(--gui-surface);
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  padding: 16px;
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
}

.overview-hero h2 {
  font-size: 20px;
  margin: 2px 0 6px;
}

.overview-hero p {
  margin: 0;
  max-width: 820px;
}

.overview-question-list {
  margin: 10px 0 0;
  padding-left: 18px;
  color: var(--gui-text-primary);
}

.overview-question-list li {
  margin-bottom: 4px;
}

.overview-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(280px, 360px);
  gap: 14px;
  align-items: start;
}

.focus-panel {
  background: var(--gui-surface);
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  padding: 14px;
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
}

.focus-panel h2 {
  font-size: 18px;
}

.physics-workflow ol {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  list-style: none;
  margin: 0;
  padding: 0;
  counter-reset: workflow-step;
}

.physics-workflow li {
  counter-increment: workflow-step;
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  background: #fbfdff;
  padding: 10px;
}

.physics-workflow li::before {
  content: counter(workflow-step);
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  margin-right: 6px;
  border-radius: 999px;
  background: var(--gui-surface-active);
  color: var(--gui-accent);
  font-weight: 800;
}

.overview-side {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.review-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.review-row {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  gap: 10px;
  border: 1px solid #fed7aa;
  border-radius: 8px;
  background: #fffaf0;
  padding: 10px;
}

.review-count {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 34px;
  height: 34px;
  border-radius: 999px;
  border: 1px solid #fed7aa;
  background: #ffedd5;
  color: #7c2d12;
  font-weight: 800;
}

.physics-object-list {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  overflow: hidden;
}

.physics-object-row {
  display: block;
  position: relative;
  padding: 8px 10px;
  background: #fbfdff;
  border-top: 1px solid var(--gui-border);
}

.physics-object-row:first-child {
  border-top: none;
}

.overview-title {
  display: block;
  font-weight: 700;
}

.physics-object-row.tree-child {
  padding-left: 34px;
}

.physics-object-row.tree-child::before {
  content: "";
  position: absolute;
  left: 18px;
  top: 0;
  bottom: 0;
  border-left: 1px solid var(--gui-border-strong);
}

.physics-object-row.tree-child::after {
  content: "";
  position: absolute;
  left: 18px;
  top: 50%;
  width: 10px;
  border-top: 1px solid var(--gui-border-strong);
}

.lineage-tree {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  overflow: hidden;
  background: #fbfdff;
}

.lineage-sort-controls {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin: 0 0 8px;
  color: var(--gui-text-muted);
  font-size: 11px;
  font-weight: 700;
}

.lineage-sort-link {
  display: inline-flex;
  align-items: center;
  border: 1px solid var(--gui-border);
  border-radius: 999px;
  padding: 2px 8px;
  background: #f8fafc;
  color: var(--gui-text-primary);
  font-weight: 800;
}

.lineage-sort-link:hover {
  background: var(--gui-surface-hover);
  text-decoration: none;
}

.lineage-sort-link.active {
  border-color: var(--gui-border-active);
  background: var(--gui-surface-active);
  color: var(--gui-accent);
}

.lineage-header,
.lineage-row {
  display: grid;
  grid-template-columns:
    minmax(170px, 0.9fr)
    minmax(130px, 0.8fr)
    minmax(130px, 0.8fr)
    minmax(80px, 0.45fr)
    minmax(105px, 0.55fr)
    minmax(120px, 0.6fr)
    minmax(110px, 0.55fr)
    50px;
  gap: 8px;
  align-items: start;
}

.lineage-header {
  padding: 7px 10px;
  background: #f0f4f8;
  border-bottom: 1px solid var(--gui-border);
  color: var(--gui-text-muted);
  font-size: 11px;
  font-weight: 800;
  text-transform: uppercase;
}

.lineage-row {
  position: relative;
  padding: 8px 10px;
  border-top: 1px solid var(--gui-border);
  background: #fbfdff;
}

.lineage-leaf-details {
  border-top: 1px solid var(--gui-border);
}

.lineage-leaf-details:first-child,
.lineage-header + .lineage-leaf-details {
  border-top: none;
}

.lineage-leaf-details > summary {
  list-style: none;
}

.lineage-leaf-details > summary::-webkit-details-marker {
  display: none;
}

.lineage-leaf-details > .lineage-row {
  border-top: none;
}

.lineage-row-clickable {
  cursor: pointer;
}

.lineage-row-clickable:hover {
  background: var(--gui-surface-hover);
}

.lineage-header + .lineage-row,
.lineage-row:first-child {
  border-top: none;
}

.lineage-leaf-list {
  border-top: 1px solid var(--gui-border);
}

.lineage-main-cell {
  min-width: 0;
}

.lineage-level-1 .lineage-main-cell {
  padding-left: 22px;
}

.lineage-level-2 .lineage-main-cell {
  padding-left: 44px;
}

.lineage-level-3 .lineage-main-cell {
  padding-left: 66px;
}

.lineage-level-4 .lineage-main-cell {
  padding-left: 88px;
}

.lineage-level-5 .lineage-main-cell,
.lineage-level-6 .lineage-main-cell {
  padding-left: 110px;
}

.lineage-level-1::before,
.lineage-level-2::before,
.lineage-level-3::before,
.lineage-level-4::before,
.lineage-level-5::before,
.lineage-level-6::before {
  content: "";
  position: absolute;
  top: 0;
  bottom: 0;
  border-left: 1px solid var(--gui-border-strong);
}

.lineage-level-1::before {
  left: 21px;
}

.lineage-level-2::before {
  left: 43px;
}

.lineage-level-3::before {
  left: 65px;
}

.lineage-level-4::before {
  left: 87px;
}

.lineage-level-5::before,
.lineage-level-6::before {
  left: 109px;
}

.lineage-title-line {
  display: flex;
  gap: 7px;
  align-items: baseline;
  min-width: 0;
  flex-wrap: wrap;
}

.lineage-title-text {
  font-weight: 800;
  overflow-wrap: anywhere;
}

.lineage-subtitle,
.lineage-history,
.lineage-data-cell {
  display: block;
  color: var(--gui-text-muted);
  font-size: 11px;
  overflow-wrap: anywhere;
}

.lineage-data-cell {
  color: var(--gui-text-primary);
}

.lineage-leaf-summary {
  grid-column: 2 / 8;
  display: block;
  color: var(--gui-text-primary);
  font-size: 11px;
  overflow-wrap: anywhere;
}

.lineage-toggle {
  position: relative;
  width: 18px;
  height: 18px;
  border: 1px solid var(--gui-border);
  border-radius: 999px;
  background: #f8fafc;
}

.lineage-actions {
  align-self: center;
  justify-self: end;
  display: inline-flex;
  align-items: center;
  gap: 5px;
}

.lineage-detail-link {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 18px;
  border: 1px solid var(--gui-border);
  border-radius: 999px;
  background: #f8fafc;
  color: var(--gui-accent);
  font-size: 11px;
  font-weight: 800;
  line-height: 1;
}

.lineage-detail-link:hover {
  background: var(--gui-surface-active);
  text-decoration: none;
}

.lineage-toggle::before {
  content: "";
  position: absolute;
  left: 6px;
  top: 4px;
  width: 6px;
  height: 6px;
  border-right: 2px solid var(--gui-text-muted);
  border-bottom: 2px solid var(--gui-text-muted);
  transform: rotate(45deg);
}

.lineage-leaf-details[open] > .lineage-row .lineage-toggle::before {
  top: 7px;
  transform: rotate(225deg);
}

.lineage-toggle-placeholder {
  visibility: hidden;
}

.lineage-role-badge {
  display: inline-flex;
  flex: 0 0 auto;
  border: 1px solid var(--gui-border);
  border-radius: 999px;
  padding: 1px 7px;
  background: #f0f4f8;
  color: #334e68;
  font-size: 10px;
  font-weight: 800;
  white-space: nowrap;
}

.lineage-role-badge-main {
  border-color: var(--gui-border-active);
  background: var(--gui-surface-active);
  color: var(--gui-accent);
}

.lineage-role-badge-branch {
  border-color: #fed7aa;
  background: #fff7ed;
  color: #9a3412;
}

.lineage-role-badge-continuation {
  border-color: #bbf7d0;
  background: #f0fdf4;
  color: #166534;
}

.lineage-role-badge-leaf {
  border-color: #ddd6fe;
  background: #f5f3ff;
  color: #5b21b6;
}

.lineage-role-badge-gxr {
  border-color: #ddd6fe;
  background: #f5f3ff;
  color: #5b21b6;
}

.lineage-role-badge-ktm {
  border-color: #bbf7d0;
  background: #f0fdf4;
  color: #166534;
}

.lineage-alias-text {
  display: inline;
  flex: 0 0 auto;
  color: #94a3b8;
  font-size: 10px;
  font-weight: 800;
  white-space: nowrap;
}

.remote-status-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex: 0 0 auto;
  width: 18px;
  height: 18px;
  border: 1px solid var(--gui-border);
  border-radius: 999px;
  background: #f8fafc;
  color: var(--gui-text-muted);
  font-size: 9px;
  font-weight: 900;
  line-height: 1;
  cursor: pointer;
  padding: 0;
}

.remote-status-badge:hover {
  background: var(--gui-surface-hover);
  border-color: var(--gui-border-active);
}

.remote-status-badge:disabled {
  cursor: wait;
  opacity: 0.75;
}

.remote-status-running {
  border-color: #86efac;
  background: #dcfce7;
  color: #166534;
}

.remote-status-pending {
  border-color: #fde68a;
  background: #fef3c7;
  color: #92400e;
}

.remote-status-complete {
  border-color: var(--gui-border);
  background: #f0f4f8;
  color: #334e68;
  font-size: 8px;
}

.remote-status-not-found,
.remote-status-unavailable {
  border-color: var(--gui-border);
  background: #f8fafc;
  color: #94a3b8;
}

.remote-status-error,
.remote-status-failed {
  border-color: #fecaca;
  background: #fef2f2;
  color: #991b1b;
}

.remote-status-checking {
  border-color: var(--gui-border-active);
  background: var(--gui-surface-active);
  color: var(--gui-accent);
}

.standalone-simulation-list {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.standalone-simulation-card {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 10px;
  align-items: start;
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  background: #fbfdff;
  padding: 10px;
}

.standalone-simulation-card h3 {
  font-size: 14px;
  margin-bottom: 5px;
}

.simulation-subtitle {
  margin: 0 0 7px;
  color: var(--gui-text-muted);
  font-size: 12px;
}

.compact-stack {
  align-items: flex-end;
}

.history-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.history-card {
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  background: #fbfdff;
  padding: 10px;
}

.history-main {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(220px, 0.4fr);
  gap: 12px;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--gui-border);
}

.history-main h3 {
  font-size: 14px;
  margin-bottom: 6px;
}

.history-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}

.history-parents span {
  display: block;
  color: var(--gui-text-muted);
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  margin-bottom: 4px;
}

.history-users {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  margin-top: 10px;
}

.use-group {
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  background: var(--gui-surface);
  padding: 8px;
}

.use-object-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.use-object {
  display: flex;
  flex-direction: column;
  gap: 2px;
  border-top: 1px solid var(--gui-border);
  padding-top: 6px;
}

.use-object:first-child {
  border-top: none;
  padding-top: 0;
}

.use-object span {
  color: var(--gui-text-muted);
  font-size: 11px;
}

.raw-name-hint,
.entity-raw-name {
  display: block;
  color: var(--gui-text-muted);
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.total-line {
  margin-bottom: 8px;
  color: var(--gui-text-muted);
}

.total-line strong {
  color: var(--gui-text-strong);
}

.lite-intro {
  display: grid;
  grid-template-columns: minmax(0, 1.2fr) minmax(280px, 0.8fr);
  gap: 16px;
  margin: 16px 24px 0;
  background: var(--gui-surface);
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  padding: 14px 16px;
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
}

.intro-copy h2 {
  font-size: 18px;
  margin-bottom: 6px;
}

.intro-copy p {
  margin: 0;
  color: var(--gui-text-primary);
  max-width: 860px;
}

.quick-start {
  margin: 0;
  padding-left: 20px;
  color: var(--gui-text-primary);
}

.quick-start li {
  margin-bottom: 6px;
}

.quick-start li:last-child {
  margin-bottom: 0;
}

.campaign-health {
  margin: 16px 24px 0;
  background: var(--gui-surface);
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  padding: 12px;
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
}

.campaign-health-header {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
  border-bottom: 1px solid var(--gui-border);
  padding-bottom: 10px;
  margin-bottom: 10px;
}

.campaign-health-header h2 {
  font-size: 18px;
  margin-bottom: 0;
}

.campaign-total {
  border: 1px solid var(--gui-border-active);
  background: var(--gui-surface-active);
  color: var(--gui-accent);
  border-radius: 999px;
  padding: 4px 10px;
  white-space: nowrap;
}

.health-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}

.health-band {
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  padding: 10px;
  background: #fbfdff;
}

.band-help,
.panel-help {
  margin: -3px 0 8px;
  color: var(--gui-text-muted);
  font-size: 12px;
}

.attention-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
  margin-top: 10px;
}

.attention-item {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  gap: 10px;
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  padding: 10px;
  background: #fbfdff;
  min-width: 0;
}

.attention-item.attention-needs-review {
  border-color: #fed7aa;
  background: #fffaf0;
}

.attention-count {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border-radius: 999px;
  border: 1px solid var(--gui-border);
  background: #f0f4f8;
  font-weight: 800;
  color: var(--gui-text-strong);
}

.attention-needs-review .attention-count {
  border-color: #fed7aa;
  background: #ffedd5;
  color: #7c2d12;
}

.entity-ref-list,
.compact {
  margin: 0;
}

.entity-ref-list {
  color: var(--gui-text-muted);
  overflow-wrap: anywhere;
}

.entity-ref-list span {
  font-weight: 700;
  color: var(--gui-text-strong);
}

.recent-notes {
  margin-top: 10px;
}

.sidebar {
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-width: 0;
}

.details {
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-width: 0;
}

.panel,
.entity-hero {
  background: var(--gui-surface);
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  padding: 12px;
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
}

.stats-panel {
  position: sticky;
  top: 70px;
  z-index: 3;
}

.nav-panel {
  max-height: calc(100vh - 148px);
  overflow: auto;
}

.chip-row {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.nav-group {
  border-top: 1px solid var(--gui-border);
  padding-top: 8px;
  margin-top: 8px;
}

.nav-group summary {
  cursor: pointer;
  display: flex;
  justify-content: space-between;
  color: var(--gui-text-strong);
  font-weight: 700;
  padding: 2px 0 6px;
}

.nav-links {
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.entity-link {
  display: block;
  border: 1px solid transparent;
  border-radius: 8px;
  padding: 7px 8px;
  color: var(--gui-text-primary);
}

.entity-link:hover {
  background: var(--gui-surface-hover);
  text-decoration: none;
}

.entity-link.active {
  background: var(--gui-surface-active);
  border-color: var(--gui-border-active);
}

.entity-name {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-weight: 600;
}

.entity-meta {
  display: flex;
  align-items: center;
  gap: 5px;
  color: var(--gui-text-muted);
  font-size: 11px;
  margin-top: 2px;
}

.source-mini {
  border: 1px solid var(--gui-border);
  border-radius: 999px;
  padding: 0 5px;
  background: #f8fafc;
  color: var(--gui-text-muted);
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #94a3b8;
  flex: 0 0 auto;
}

.status-success,
.status-available,
.status-reference {
  background: #e3fcef;
  border-color: #b7ebc6;
  color: var(--gui-success);
}

.status-partial,
.status-partially-available,
.status-candidate,
.status-prepared,
.status-pending {
  background: #fff7ed;
  border-color: #fed7aa;
  color: #7c2d12;
}

.status-running {
  background: #e0f2fe;
  border-color: #bae6fd;
  color: #0c4a6e;
}

.status-crashed,
.status-error,
.status-interrupted,
.status-suspect {
  background: #fee2e2;
  border-color: #fecaca;
  color: #7f1d1d;
}

.status-dot.status-success,
.status-dot.status-available,
.status-dot.status-reference {
  background: #22c55e;
}

.status-dot.status-running {
  background: #0284c7;
}

.status-dot.status-crashed,
.status-dot.status-error,
.status-dot.status-interrupted,
.status-dot.status-suspect {
  background: #dc2626;
}

.entity-hero {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 10px 16px;
}

.entity-title-block {
  min-width: 0;
}

.type-label {
  color: var(--gui-accent);
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  margin-top: 4px;
}

.entity-title-block h2 {
  font-size: 22px;
  margin: 2px 0 4px;
}

.entity-title-block code,
.path-row code,
td code {
  color: var(--gui-text-muted);
  overflow-wrap: anywhere;
}

.status-stack {
  display: flex;
  align-items: flex-start;
  justify-content: flex-end;
  gap: 6px;
  flex-wrap: wrap;
}

.status-badge {
  font-weight: 700;
}

.status-badge.scientific {
  background: #f0f4f8;
  color: #334e68;
}

.description,
.path-row {
  grid-column: 1 / -1;
}

.description {
  margin: 0;
  color: var(--gui-text-primary);
}

.path-row,
.raw-title-row {
  display: grid;
  grid-template-columns: 56px minmax(0, 1fr);
  gap: 8px;
  padding-top: 6px;
  border-top: 1px solid var(--gui-border);
}

.path-row span,
.raw-title-row span {
  color: var(--gui-text-muted);
  font-weight: 700;
}

.figure-preview-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
}

.figure-preview-header h2 {
  margin-bottom: 0;
}

.figure-preview figure {
  margin: 0;
}

.figure-preview img {
  display: block;
  width: 100%;
  max-height: 72vh;
  object-fit: contain;
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  background: #ffffff;
}

.figure-preview figcaption {
  margin-top: 6px;
  color: var(--gui-text-muted);
  overflow-wrap: anywhere;
}

.preview-links {
  margin: 8px 0 0;
  color: var(--gui-text-muted);
}

.two-column {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.relation-list {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.relation-list li {
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  padding: 8px;
  background: #fbfdff;
}

.relation-type,
.relation-note,
.muted,
.note-meta {
  color: var(--gui-text-muted);
}

.relation-type {
  font-size: 11px;
  margin-bottom: 3px;
}

.relation-list code {
  display: block;
  margin-top: 2px;
  color: var(--gui-text-muted);
}

.relation-note {
  margin: 5px 0 0;
}

.table-wrapper {
  max-width: 100%;
  overflow: auto;
  border: 1px solid var(--gui-border);
  border-radius: 8px;
}

table {
  border-collapse: collapse;
  width: 100%;
}

th,
td {
  border-bottom: 1px solid var(--gui-border);
  padding: 6px 8px;
  text-align: left;
  vertical-align: top;
  white-space: nowrap;
}

th {
  background: #f0f4f8;
  color: var(--gui-text-strong);
  font-weight: 700;
}

td:nth-child(2),
td:nth-child(5) {
  white-space: normal;
}

tr:last-child td {
  border-bottom: none;
}

.note {
  border-left: 3px solid var(--gui-border-active);
  padding: 0 0 0 9px;
  margin: 0 0 10px;
}

.note:last-child {
  margin-bottom: 0;
}

.note p {
  margin: 3px 0 0;
}

.json-block {
  margin: 0;
  padding: 10px;
  border: 1px solid var(--gui-border);
  border-radius: 8px;
  background: #fbfdff;
  overflow: auto;
  max-height: 260px;
  color: var(--gui-text-primary);
}

.empty {
  padding: 16px 24px;
}

@media (max-width: 900px) {
  .layout,
  .lite-intro,
  .overview-grid,
  .physics-workflow ol,
  .standalone-simulation-list,
  .standalone-simulation-card,
  .history-main,
  .history-users,
  .two-column,
  .health-grid,
  .attention-grid {
    grid-template-columns: 1fr;
  }

  .topbar,
  .overview-hero {
    align-items: flex-start;
    flex-direction: column;
  }

  .stats-panel,
  .topbar {
    position: static;
  }

  .nav-panel {
    max-height: none;
  }

  .entity-hero {
    grid-template-columns: 1fr;
  }

  .status-stack {
    justify-content: flex-start;
  }
}
"""
