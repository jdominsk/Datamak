from __future__ import annotations

import tempfile
import unittest
import json
import sqlite3
from pathlib import Path

from datamak_lite.adapters.campaign_registry import import_campaign_registry
from datamak_lite.adapters.datamak_pool_marker import build_datamak_pool_marker, write_datamak_pool_marker
from datamak_lite.adapters.figure_audit import discover_figure_audit_files, import_figure_audits
from datamak_lite.adapters.folder_inventory import import_folder_inventory, read_inventory_file
from datamak_lite.adapters.pool_packet import build_pool_packet, write_pool_packet
from datamak_lite.core.campaign_profile import format_refresh_summary, refresh_campaign
from datamak_lite.core.campaign_status import build_campaign_status, build_campaign_status_from_path
from datamak_lite.core.campaign_use_map import build_campaign_use_map
from datamak_lite.core.db import init_db
from datamak_lite.core.display_titles import display_group_for_object, display_title_for_object
from datamak_lite.core.packet import import_packet
from datamak_lite.core.report import render_entity_report
from datamak_lite.core.repository import LiteRepository
from datamak_lite.core.sync import cached_packet_path, is_remote_packet_spec, sync_and_import_packet
from datamak_lite.core.validate import has_errors, validate_packet_data
from datamak_lite.examples.demo_seed import FIGURE_UID, seed as seed_demo
from datamak_lite.gui.app import _resolve_existing_artifact_path, render_index


class DatamakLiteCoreTest(unittest.TestCase):
    def test_seed_graph_and_render_figure_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                target_uid = seed_demo(repo)
                self.assertEqual(target_uid, FIGURE_UID)

                entities = repo.list_entities()
                self.assertGreaterEqual(len(entities), 6)

                report = render_entity_report(repo, FIGURE_UID)

            self.assertIn("Demo replay pool", report)
            self.assertIn("Demo transport analysis", report)
            self.assertIn("Demo history dataset", report)
            self.assertIn("Demo source simulation", report)
            self.assertIn("--uses_input-->", report)
            self.assertIn("--produces-->", report)
            self.assertIn("decision", report)

    def test_import_sidecar_packet_idempotently(self) -> None:
        packet = Path(__file__).resolve().parents[1] / "datamak_lite/examples/generic_pool_packet.json"
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                root_uid = import_packet(repo, packet)
                second_root_uid = import_packet(repo, packet)
                report = render_entity_report(repo, root_uid)

                entity_count = repo.conn.execute("SELECT COUNT(*) AS n FROM entity").fetchone()["n"]
                artifact_count = repo.conn.execute("SELECT COUNT(*) AS n FROM artifact").fetchone()["n"]
                metric_count = repo.conn.execute("SELECT COUNT(*) AS n FROM metric").fetchone()["n"]
                note_count = repo.conn.execute("SELECT COUNT(*) AS n FROM note").fetchone()["n"]

            self.assertEqual(root_uid, "pool_demo_replay")
            self.assertEqual(second_root_uid, root_uid)
            self.assertEqual(entity_count, 3)
            self.assertEqual(artifact_count, 2)
            self.assertEqual(metric_count, 3)
            self.assertEqual(note_count, 1)
            self.assertIn("Demo replay pool", report)
            self.assertIn("--uses_input-->", report)

    def test_validate_packet_accepts_generic_pool_packet(self) -> None:
        packet = json.loads(
            (Path(__file__).resolve().parents[1] / "datamak_lite/examples/generic_pool_packet.json").read_text()
        )

        issues = validate_packet_data(packet)

        self.assertFalse(has_errors(issues))
        self.assertEqual([], issues)

    def test_validate_packet_reports_missing_relation_target(self) -> None:
        packet = {
            "schema_version": 1,
            "root_uid": "pool_test",
            "entities": [{"uid": "pool_test", "type": "pool", "name": "Pool test"}],
            "relations": [
                {
                    "source_uid": "pool_test",
                    "relation_type": "uses_input",
                    "target_uid": "missing_dataset",
                }
            ],
        }

        issues = validate_packet_data(packet)

        self.assertTrue(has_errors(issues))
        self.assertTrue(any("target_uid is not defined" in issue.message for issue in issues))

    def test_validate_packet_warns_for_weak_figure_metadata(self) -> None:
        packet = {
            "schema_version": 1,
            "root_uid": "figure_test",
            "entities": [{"uid": "figure_test", "type": "figure", "name": "Figure test"}],
        }

        issues = validate_packet_data(packet)

        self.assertFalse(has_errors(issues))
        self.assertTrue(any("no output artifact" in issue.message for issue in issues))
        self.assertTrue(any("no plots relation" in issue.message for issue in issues))

    def test_validate_packet_warns_for_large_inline_arrays(self) -> None:
        packet = {
            "schema_version": 1,
            "root_uid": "analysis_test",
            "entities": [{"uid": "analysis_test", "type": "analysis", "name": "Analysis test"}],
            "artifacts": [
                {
                    "entity_uid": "analysis_test",
                    "kind": "summary",
                    "path": "summary.json",
                    "metadata": {"values": list(range(101))},
                }
            ],
        }

        issues = validate_packet_data(packet)

        self.assertFalse(has_errors(issues))
        self.assertTrue(any("Large inline array" in issue.message for issue in issues))

    def test_remote_packet_sync_dry_run_uses_db_local_cache(self) -> None:
        remote_packet = (
            "user@example.org:"
            "/scratch/project/example_pool/datamak_lite.json"
        )
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "campaign.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                root_uid, local_packet = sync_and_import_packet(repo, remote_packet, dry_run=True)

            self.assertIsNone(root_uid)
            self.assertTrue(is_remote_packet_spec(remote_packet))
            self.assertFalse(is_remote_packet_spec("/tmp/datamak_lite.json"))
            self.assertEqual(local_packet.parent, (db_path.parent / "packets").resolve())
            self.assertTrue(local_packet.name.startswith("datamak_lite_"))
            self.assertEqual(cached_packet_path(remote_packet, db_path.parent / "packets"), local_packet)

    def test_create_pool_packet_from_pool_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "example_pool_effstride8_t10_t20"
            root.mkdir()
            (root / "pool.db").write_text("")
            (root / "pool_status.py").write_text("# status\n")
            (root / "datamak_pool.json").write_text(json.dumps({"workflow_engine": "datamak"}))
            (root / "analysis").mkdir()
            (root / "pool_manifest.json").write_text(json.dumps({"model": "es_full", "step_stride": 8}))
            dataset_path = "/example/project/datasets/history_0001.h5"

            packet = build_pool_packet(
                root,
                uid="pool_test",
                campaign_uid="campaign_demo",
                campaign_name="Demo campaign",
                uses_dataset_uid="dataset_test",
                dataset_path=dataset_path,
                relation_type="uses_input",
                note="Test pool packet.",
            )
            packet_path = write_pool_packet(packet, root)

            db_path = Path(tmp) / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                root_uid = import_packet(repo, packet_path)
                report = render_entity_report(repo, root_uid)

                pool = repo.get_entity("pool_test")
                metrics = repo.metrics_for_entity(int(pool["id"]))
                artifacts = repo.artifacts_for_entity(int(pool["id"]))

            self.assertEqual(root_uid, "pool_test")
            self.assertIn("--uses_input-->", report)
            self.assertIn("--member_of-->", report)
            self.assertIn("Pool registered by Datamak Lite", report)
            self.assertIn("dataset_test", report)
            self.assertTrue(any(metric["name"] == "effective_stride" for metric in metrics))
            self.assertTrue(any(metric["name"] == "time_start" for metric in metrics))
            self.assertTrue(any(artifact["kind"] == "datamak_pool_marker" for artifact in artifacts))

    def test_write_datamak_pool_marker_creates_readme_and_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pool"
            marker = build_datamak_pool_marker(
                root,
                pool_db="pool.db",
                status_command="python3 pool_status.py",
                interactive_launcher="./run_pool_interactive.sh",
                notes="Prepared for a smoke test.",
            )

            written = write_datamak_pool_marker(root, marker)

            self.assertEqual(written["marker"], root / "datamak_pool.json")
            self.assertEqual(written["readme"], root / "README.md")
            marker_json = json.loads((root / "datamak_pool.json").read_text())
            readme = (root / "README.md").read_text()
            self.assertEqual(marker_json["workflow_engine"], "datamak")
            self.assertIn("Datamak-Style Simulation Pool", readme)
            self.assertIn("python3 pool_status.py", readme)
            self.assertIn("datamak_lite.json", readme)

    def test_write_datamak_pool_marker_preserves_existing_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "pool"
            root.mkdir()
            (root / "README.md").write_text("custom readme")
            marker = build_datamak_pool_marker(root, notes="new")

            write_datamak_pool_marker(root, marker)

            self.assertEqual((root / "README.md").read_text(), "custom readme")
            self.assertEqual(json.loads((root / "datamak_pool.json").read_text())["workflow_engine"], "datamak")

    def test_gui_index_renders_entity_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                seed_demo(repo)

            html = render_index(db_path, selected_uid="pool_demo_replay")

            self.assertIn("Datamak Lite", html)
            self.assertIn("Browse Objects", html)
            self.assertIn("Demo replay pool", html)
            self.assertIn("Inputs Used By This Object", html)
            self.assertIn("Objects That Use This", html)
            self.assertIn("Artifacts", html)
            self.assertIn("Metrics", html)

    def test_campaign_status_reports_attention_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(uid="campaign_test", type="campaign", name="Test campaign", status="active")
                repo.upsert_entity(uid="pool_without_marker", type="pool", name="Pool without marker", status="prepared")
                repo.upsert_entity(uid="pool_with_marker", type="pool", name="Pool with marker", status="prepared")
                repo.upsert_entity(uid="figure_warning", type="figure", name="Figure with warning")
                repo.upsert_entity(
                    uid="auto_candidate",
                    type="analysis",
                    name="Auto candidate",
                    metadata={"discovery_source": "folder_inventory"},
                )
                repo.add_artifact(entity_uid="pool_with_marker", kind="datamak_pool_marker", path=str(Path(tmp) / "marker.json"))
                repo.add_relation(source_uid="pool_with_marker", relation_type="member_of", target_uid="campaign_test")
                repo.add_note(entity_uid="figure_warning", note_type="warning", markdown_text="Needs a plotting audit.")
                repo.add_note(entity_uid="pool_without_marker", note_type="todo", markdown_text="Add Datamak pool marker.")

                status = build_campaign_status(repo)

            attention = {item.key: item for item in status.attention}
            self.assertEqual(status.entity_total, 5)
            self.assertEqual(status.counts_by_type["pool"], 2)
            self.assertEqual(status.note_counts["warning"], 1)
            self.assertEqual(attention["missing_datamak_marker"].count, 1)
            self.assertEqual(attention["figure_warnings"].count, 1)
            self.assertGreaterEqual(attention["disconnected"].count, 3)
            self.assertEqual(attention["discovered_candidates"].count, 1)
            self.assertEqual(len(status.recent_notes), 2)

    def test_campaign_status_accepts_profile_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "campaign.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(uid="campaign_profile", type="campaign", name="Profile campaign")
            profile = root / "profile.json"
            profile.write_text(json.dumps({"schema_version": 1, "campaign_uid": "campaign_profile", "database": "campaign.sqlite"}))

            status = build_campaign_status_from_path(profile)

            self.assertEqual(status.campaign.uid, "campaign_profile")

    def test_gui_index_renders_campaign_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(uid="campaign_test", type="campaign", name="Test campaign", status="active")
                repo.upsert_entity(uid="pool_without_marker", type="pool", name="Pool without marker", status="prepared")
                repo.add_note(entity_uid="pool_without_marker", note_type="todo", markdown_text="Create marker.")

            html = render_index(db_path)
            campaign_html = render_index(db_path, view="campaign")

            self.assertIn("Campaign", html)
            self.assertIn("Simulations", html)
            self.assertLess(html.index(">Campaign<"), html.index(">Simulations<"))
            self.assertNotIn(">Overview<", html)
            self.assertNotIn("Overview 2", html)
            self.assertNotIn("Campaign Overview", html)
            self.assertNotIn("Physics workflow", html)
            self.assertIn("Campaign Overview", campaign_html)
            self.assertIn("Physics workflow", campaign_html)
            self.assertNotIn("Metadata that still needs attention", html)
            self.assertNotIn("Registered Objects", html)
            self.assertNotIn("Status Summary", html)
            self.assertIn("Metadata that still needs attention", campaign_html)
            self.assertIn("Registered Objects", campaign_html)
            self.assertIn("Status Summary", campaign_html)
            self.assertIn("Pools missing Datamak receipt", campaign_html)
            self.assertIn("Pool without marker", campaign_html)
            self.assertNotIn("Inputs Used By This Object", html)
            self.assertNotIn("Objects That Use This", html)

    def test_overview_maps_history_to_downstream_studies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(uid="campaign_test", type="campaign", name="Test campaign", status="active")
                repo.upsert_entity(
                    uid="sim_parent",
                    type="simulation",
                    name="Parent simulation",
                    metadata={"model": "parent", "step_record_stride": 1},
                )
                repo.upsert_entity(
                    uid="history_parent",
                    type="history_file",
                    name="Parent history",
                    metadata={"source_window": "10..20", "saved_stride": 1},
                )
                repo.upsert_entity(
                    uid="pool_downstream",
                    type="pool",
                    name="Downstream pool",
                    metadata={"category": "model-comparison", "models": ["full", "reduced"]},
                )
                repo.upsert_entity(
                    uid="sim_no_history",
                    type="simulation",
                    name="Important simulation without saved history",
                    metadata={"model": "normal GX", "nspecies": 3},
                )
                repo.add_relation(source_uid="sim_parent", relation_type="produces", target_uid="history_parent")
                repo.add_relation(source_uid="pool_downstream", relation_type="uses_history", target_uid="history_parent")

                use_map = build_campaign_use_map(repo)

            profile = root / "profile.json"
            profile.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "campaign_uid": "campaign_test",
                        "database": "datamak_lite.sqlite",
                        "overview": {"headline": "Domain-specific headline"},
                    }
                )
            )
            html = render_index(profile)
            campaign_html = render_index(profile, view="campaign")

            self.assertEqual(len(use_map.histories), 1)
            self.assertEqual(use_map.histories[0].history.entity.uid, "history_parent")
            self.assertEqual(use_map.histories[0].parents[0].entity.uid, "sim_parent")
            self.assertEqual(use_map.histories[0].use_groups[0].objects[0].entity.uid, "pool_downstream")
            self.assertEqual(use_map.standalone_simulations[0].entity.uid, "sim_no_history")
            self.assertIn("Domain-specific headline", campaign_html)
            self.assertIn("Simulations", html)
            self.assertIn("Important simulation without saved history", html)
            self.assertIn("Parent simulation", html)
            self.assertIn("t=10-20, stride=1", html)
            self.assertIn("Downstream pool", html)
            self.assertIn("lineage-role-leaf", html)

    def test_gx_impurity_campaign_type_generates_semantic_titles(self) -> None:
        history_title = display_title_for_object(
            campaign_type="gx_impurity_turbulence",
            entity_type="history_file",
            raw_name="kinetic_e_de_wmatched_t600_t700_stride1_step.step_fields_0005.nc",
            metadata={
                "source_window": "600..700",
                "saved_stride": 1,
                "fields": "Phi_step,Apar_step,Bpar_step",
            },
        )
        replay_title = display_title_for_object(
            campaign_type="gx_impurity_turbulence",
            entity_type="pool",
            raw_name="z_scan_step_replay_kinetic_e_wmatched_stride_scan",
            metadata={
                "category": "gx-r-replay-transport-scan",
                "z_values": [20, 74],
                "field_time_interpolation": "aggressive",
                "effective_stride": 8,
                "n_cases": 6,
            },
        )
        tracer_title = display_title_for_object(
            campaign_type="gx_impurity_turbulence",
            entity_type="analysis",
            raw_name="tracer_phase_factor_kinetic_e_skip2",
            metadata={"category": "tracer-diagnostic", "skip_saved_steps": 2},
        )
        ktm_title = display_title_for_object(
            campaign_type="gx_impurity_turbulence",
            entity_type="analysis",
            raw_name="ktm_kinetic_e_phi_argon_neon_miller_jacobian_20260513",
            metadata={"category": "ktm-field-response"},
        )

        self.assertIn("Saved turbulent fields", history_title.title)
        self.assertIn("kinetic e-", history_title.title)
        self.assertIn("stride=1", history_title.title)
        self.assertIn("Replay of", replay_title.title)
        self.assertIn("kinetic-e turbulence", replay_title.title)
        self.assertIn("Tracer diagnostic", tracer_title.title)
        self.assertIn("skip=2", tracer_title.title)
        self.assertIn("Kinetic Trace Model", ktm_title.title)
        self.assertEqual(
            "KTM / field-only models",
            display_group_for_object(
                campaign_type="gx_impurity_turbulence",
                entity_type="analysis",
                raw_name="ktm_kinetic_e_phi_argon_neon_miller_jacobian_20260513",
                metadata={"category": "ktm-field-response"},
            ),
        )
        self.assertEqual(
            "GX-R replay",
            display_group_for_object(
                campaign_type="gx_impurity_turbulence",
                entity_type="pool",
                raw_name="z_scan_step_replay_kinetic_e_wmatched_stride_scan",
                metadata={"category": "gx-r-replay-transport-scan"},
            ),
        )

    def test_gui_profile_uses_gx_impurity_semantic_titles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(uid="campaign_test", type="campaign", name="Test campaign", status="active")
                repo.upsert_entity(
                    uid="sim_ke",
                    type="simulation",
                    name="kinetic_e_de_wmatched_history_source",
                    metadata={"electron_model": "kinetic", "history_fields": ["Phi_step", "Apar_step", "Bpar_step"]},
                )
                repo.upsert_entity(
                    uid="history_ke",
                    type="history_file",
                    name="kinetic_e_de_wmatched_t600_t700_stride1_step.step_fields_0005.nc",
                    metadata={
                        "source_window": "600..700",
                        "saved_stride": 1,
                        "fields": "Phi_step,Apar_step,Bpar_step",
                    },
                )
                repo.upsert_entity(
                    uid="pool_replay",
                    type="pool",
                    name="z_scan_step_replay_kinetic_e_wmatched_t600_t700_stride_scan",
                    metadata={
                        "category": "gx-r-replay-transport-scan",
                        "z_values": [20, 74],
                        "effective_stride": 8,
                        "n_cases": 6,
                    },
                )
                repo.upsert_entity(
                    uid="sim_boron_no_history",
                    type="simulation",
                    name="normal_gx_aug_boron_trace_coupled",
                    metadata={
                        "electron_model": "kinetic",
                        "field_model": "full-EM",
                        "nspecies": 4,
                        "fixed_dt": False,
                    },
                )
                repo.add_relation(source_uid="sim_ke", relation_type="produces", target_uid="history_ke")
                repo.add_relation(source_uid="pool_replay", relation_type="uses_history", target_uid="history_ke")

            profile = root / "profile.json"
            profile.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "campaign_uid": "campaign_test",
                        "campaign_type": "gx_impurity_turbulence",
                        "database": "datamak_lite.sqlite",
                    }
                )
            )

            html = render_index(profile)

            self.assertNotIn("Saved turbulent fields: kinetic e-", html)
            self.assertIn("Main Plasma GX with B impurities: e-/D+ turbulence", html)
            self.assertIn("Main Plasma GX: e-/D+ turbulence", html)
            self.assertIn("Replay of W Z=20, 74 in kinetic-e turbulence", html)
            self.assertLess(
                html.index("Main Plasma GX: e-/D+ turbulence"),
                html.index("Replay of W Z=20, 74 in kinetic-e turbulence"),
            )
            self.assertIn("lineage-leaf-details", html)
            self.assertIn("lineage-level-1", html)
            self.assertNotIn("GX-R replay", html)
            self.assertNotIn("raw:", html)

    def test_gui_lineage_renders_continuations_branches_and_history_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "datamak_lite.sqlite"
            init_db(db_path)
            base_metadata = {
                "lite_alias": "G1",
                "electron_model": "kinetic e-",
                "field_model": "full-EM",
                "ntheta": 48,
                "nx": 250,
                "ny": 100,
                "nhermite": 16,
                "nlaguerre": 10,
                "fixed_dt": False,
            }
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(uid="campaign_test", type="campaign", name="Test campaign", status="active")
                repo.upsert_entity(uid="sim_parent", type="simulation", name="Parent t0 simulation", metadata=base_metadata)
                repo.upsert_entity(uid="sim_cont", type="simulation", name="Same-parameter continuation", metadata=base_metadata)
                branch_metadata = dict(base_metadata)
                branch_metadata["lite_alias"] = "G2"
                branch_metadata["nlaguerre"] = 12
                repo.upsert_entity(uid="sim_branch", type="simulation", name="Changed-resolution branch", metadata=branch_metadata)
                repo.upsert_entity(
                    uid="history_branch",
                    type="history_file",
                    name="Saved branch history object",
                    metadata={"source_window": "400..600", "saved_stride": 1},
                )
                repo.upsert_entity(
                    uid="pool_replay",
                    type="pool",
                    name="Downstream replay",
                    metadata={"category": "gx-r-replay-transport-scan", "lite_alias": "R1"},
                )
                repo.upsert_entity(
                    uid="analysis_ktm",
                    type="analysis",
                    name="KTM downstream",
                    metadata={"category": "ktm", "lite_alias": "K1"},
                )
                repo.upsert_entity(
                    uid="pool_normal_gx",
                    type="pool",
                    name="Normal GX pool should not be a replay leaf",
                    metadata={"category": "normal-gx-trace-z-scan", "normal_gx": True},
                )
                repo.add_relation(source_uid="sim_cont", relation_type="restarts_from", target_uid="sim_parent")
                repo.add_relation(source_uid="sim_branch", relation_type="restarts_from", target_uid="sim_parent")
                repo.add_relation(source_uid="sim_branch", relation_type="produces", target_uid="history_branch")
                repo.add_relation(source_uid="pool_replay", relation_type="uses_history", target_uid="history_branch")
                repo.add_relation(source_uid="analysis_ktm", relation_type="uses_history", target_uid="history_branch")

            html = render_index(db_path, view="lineage")

            self.assertIn("Simulations", html)
            self.assertIn("entity=sim_parent", html)
            self.assertIn("entity=sim_cont", html)
            self.assertIn("entity=sim_branch", html)
            self.assertIn("continuation", html)
            self.assertIn("branch", html)
            self.assertIn("GX", html)
            self.assertIn("GX-R", html)
            self.assertIn("KTM", html)
            self.assertIn("#G1", html)
            self.assertIn("G2", html)
            self.assertIn("#R1", html)
            self.assertIn("#K1", html)
            self.assertIn("lineage-alias-text", html)
            self.assertIn("lineage-role-badge-gxr", html)
            self.assertIn("lineage-role-badge-ktm", html)
            self.assertIn("t=400-600, stride=1", html)
            self.assertIn("entity=pool_replay", html)
            self.assertIn("entity=analysis_ktm", html)
            self.assertNotIn("Saved branch history object", html)
            self.assertIn("entity=pool_normal_gx", html)
            self.assertNotIn("Unplaced replay / KTM leaves", html)

    def test_gui_lineage_latest_sort_uses_downstream_child_dates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(uid="campaign_test", type="campaign", name="Test campaign", status="active")
                repo.upsert_entity(
                    uid="sim_old_root_20200101",
                    type="simulation",
                    name="Old parent with recent child",
                    metadata={"electron_model": "kinetic e-", "field_model": "full-EM"},
                )
                repo.upsert_entity(
                    uid="history_old_root",
                    type="history_file",
                    name="Old parent saved history",
                    metadata={"source_window": "10..20"},
                )
                repo.upsert_entity(
                    uid="pool_recent_replay_20260601",
                    type="pool",
                    name="Recent replay child",
                    metadata={"category": "gx-r-replay-transport-scan"},
                )
                repo.upsert_entity(
                    uid="sim_mid_root_20250501",
                    type="simulation",
                    name="Mid-date parent",
                    metadata={"electron_model": "kinetic e-", "field_model": "full-EM"},
                )
                repo.add_relation(source_uid="sim_old_root_20200101", relation_type="produces", target_uid="history_old_root")
                repo.add_relation(source_uid="pool_recent_replay_20260601", relation_type="uses_history", target_uid="history_old_root")

            default_html = render_index(db_path, view="lineage")
            latest_html = render_index(db_path, view="lineage", lineage_sort="latest")

            self.assertIn("Latest run", latest_html)
            self.assertIn("entity=sim_mid_root_20250501", default_html)
            self.assertIn("entity=sim_old_root_20200101", default_html)
            self.assertLess(latest_html.index("entity=sim_old_root_20200101"), latest_html.index("entity=sim_mid_root_20250501"))

    def test_gui_overview2_renders_main_plasma_input_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(uid="campaign_test", type="campaign", name="Test campaign", status="active")
                repo.upsert_entity(
                    uid="sim_normal_w",
                    type="pool",
                    name="normal_gx_kinelec_trace_gxrres_zscan",
                    metadata={
                        "category": "normal-gx-trace-z-scan",
                        "physics_mode": "normal_gx",
                        "normal_gx": True,
                        "electron_model": "kinetic e-",
                        "field_model": "full-EM",
                        "impurity_elements": ["W"],
                        "z_values": [20, 74],
                        "ntheta": 48,
                        "nx": 250,
                        "ny": 100,
                        "nhermite": 16,
                        "nlaguerre": 10,
                        "variants": ["dparam", "flat_ln"],
                    },
                )
                repo.upsert_entity(
                    uid="sim_kinetic_history",
                    type="simulation",
                    name="kinetic_electron_de_history",
                    metadata={
                        "electron_model": "kinetic e-",
                        "field_model": "full-EM",
                        "ntheta": 48,
                        "nx": 250,
                        "ny": 100,
                        "nhermite": 16,
                        "nlaguerre": 8,
                        "fixed_dt": True,
                        "dt": 0.0014,
                        "history_fields": ["Phi_step", "Apar_step", "Bpar_step"],
                        "step_record_stride": 15,
                    },
                )
                repo.upsert_entity(
                    uid="sim_adiabatic_history",
                    type="simulation",
                    name="source_gkinput6",
                    metadata={
                        "electron_model": "adiabatic e-",
                        "field_model": "ES",
                        "ntheta": 72,
                        "nx": 250,
                        "ny": 100,
                        "nhermite": 20,
                        "nlaguerre": 12,
                        "dt": 0.05,
                    },
                )

            profile = root / "profile.json"
            profile.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "campaign_uid": "campaign_test",
                        "campaign_type": "gx_impurity_turbulence",
                        "database": "datamak_lite.sqlite",
                    }
                )
            )

            html = render_index(profile, view="overview2")

            self.assertIn("Simulations", html)
            self.assertNotIn("Overview 2", html)
            self.assertNotIn("Normal GX / Impurity-Coupled Runs", html)
            self.assertIn("Short identity", html)
            self.assertIn("History", html)
            self.assertIn("Normal-GX trace-W Z scan", html)
            self.assertIn("kinetic e-, full-EM", html)
            self.assertIn("adiabatic e-, ES", html)

    def test_gui_renders_local_figure_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            figure_path = root / "figure.png"
            figure_path.write_bytes(b"not-a-real-png-but-local")
            db_path = root / "datamak_lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(uid="figure_preview", type="figure", name="Preview figure", status="available")
                artifact_id = repo.add_artifact(
                    entity_uid="figure_preview",
                    kind="figure_output",
                    path=str(figure_path),
                    format="png",
                    description="Preview image.",
                )

            html = render_index(db_path, selected_uid="figure_preview")

            self.assertIn("Figure Preview", html)
            self.assertIn("<img", html)
            self.assertIn(f"/artifact?id={artifact_id}", html)
            self.assertEqual(_resolve_existing_artifact_path(str(figure_path), db_path), figure_path.resolve())

    def test_import_campaign_registry_links_histories_and_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            registry = Path(tmp) / "simulation_registry.db"
            source = sqlite3.connect(registry)
            source.executescript(
                """
                CREATE TABLE campaign (
                    campaign_key TEXT PRIMARY KEY,
                    title TEXT,
                    category TEXT,
                    status TEXT,
                    purpose TEXT,
                    local_path TEXT,
                    remote_path TEXT,
                    pool_db_path TEXT,
                    source_history TEXT,
                    source_window TEXT,
                    metadata_json TEXT,
                    notes TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );
                CREATE TABLE campaign_job (
                    id INTEGER PRIMARY KEY,
                    campaign_key TEXT,
                    job_id TEXT,
                    scheduler TEXT,
                    job_name TEXT,
                    state TEXT,
                    queue TEXT,
                    nodes INTEGER,
                    time_limit TEXT,
                    elapsed TEXT,
                    exit_code TEXT,
                    submitted_at TEXT,
                    last_seen_at TEXT,
                    notes TEXT
                );
                CREATE TABLE campaign_event (
                    id INTEGER PRIMARY KEY,
                    campaign_key TEXT,
                    event_time TEXT,
                    event TEXT,
                    notes TEXT
                );
                """
            )
            source.execute(
                """
                INSERT INTO campaign VALUES (
                    'test_replay', 'Test replay pool', 'replay-transport-scan',
                    'completed', 'Purpose text.', 'local/test', '/pscratch/test_replay',
                    '/pscratch/test_replay/replay_pool.db',
                    '/example/history/fields_0001.nc', '600..700',
                    '{"effective_stride": 8, "z_values": [20, 74]}',
                    'Registry note.', NULL, NULL
                )
                """
            )
            source.execute(
                """
                INSERT INTO campaign_job VALUES (
                    1, 'test_replay', '123', 'slurm', 'gx-test', 'COMPLETED',
                    'regular', 2, '04:00:00', '00:10:00', '0:0', NULL, NULL,
                    'Job note.'
                )
                """
            )
            source.execute(
                "INSERT INTO campaign_event VALUES (1, 'test_replay', 'now', 'event text', 'event note')"
            )
            source.commit()
            source.close()

            db_path = Path(tmp) / "lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                counts = import_campaign_registry(repo, registry)
                report = render_entity_report(repo, "campaign_object_test_replay")

                entity_count = repo.conn.execute("SELECT COUNT(*) AS n FROM entity").fetchone()["n"]
                relation_count = repo.conn.execute("SELECT COUNT(*) AS n FROM relation").fetchone()["n"]

            self.assertEqual(counts["campaigns"], 1)
            self.assertEqual(counts["histories"], 1)
            self.assertEqual(counts["jobs"], 1)
            self.assertGreaterEqual(entity_count, 4)
            self.assertGreaterEqual(relation_count, 4)
            self.assertIn("--uses_history-->", report)
            self.assertIn("--has_job-->", report)
            self.assertIn("Test replay pool", report)

    def test_import_folder_inventory_skips_existing_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "lite.sqlite"
            inventory = Path(tmp) / "inventory.txt"
            existing_path = "/example/campaign/existing_pool"
            new_path = "/example/campaign/new_analysis"
            inventory.write_text(f"{existing_path}\n{new_path}\n")

            init_db(db_path)
            with LiteRepository(db_path) as repo:
                repo.upsert_entity(
                    uid="existing_pool",
                    type="pool",
                    name="Existing pool",
                    path=existing_path,
                    status="prepared",
                    scientific_status="candidate",
                )
                repo.upsert_entity(
                    uid="campaign_default",
                    type="campaign",
                    name="Default campaign",
                    status="active",
                    scientific_status="candidate",
                )
                counts = import_folder_inventory(repo, read_inventory_file(inventory))

                new_entity = repo.conn.execute("SELECT * FROM entity WHERE path=?", (new_path,)).fetchone()

            self.assertEqual(counts["added"], 1)
            self.assertEqual(counts["skipped_existing_path"], 1)
            self.assertIsNotNone(new_entity)
            self.assertEqual(new_entity["type"], "analysis")

    def test_import_figure_audit_creates_figure_and_source_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            figure_dir = root / "Paper" / "figure"
            figure_dir.mkdir(parents=True)
            output_png = figure_dir / "example_diffusion.png"
            output_pdf = figure_dir / "example_diffusion.pdf"
            output_png.write_text("png")
            output_pdf.write_text("pdf")
            script = root / "Paper" / "scripts" / "plot_example.py"
            script.parent.mkdir(parents=True)
            script.write_text("# plot\n")
            source_csv = root / "analysis" / "example_run" / "spectrum.csv"
            source_csv.parent.mkdir(parents=True)
            source_csv.write_text("ky,D\n")
            audit = figure_dir / "example_diffusion_audit.json"
            audit.write_text(
                json.dumps(
                    {
                        "script": str(script),
                        "input_csvs": [str(source_csv)],
                        "output_png": str(output_png),
                        "output_pdf": str(output_pdf),
                        "ky_spectrum": "Tracer QL used 2 * abs(D_ky) in an old test.",
                    }
                )
            )

            db_path = root / "lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                files = discover_figure_audit_files([figure_dir])
                counts = import_figure_audits(repo, files)
                figure = repo.get_entity("figure_example_diffusion")
                report = render_entity_report(repo, "figure_example_diffusion")
                notes = repo.notes_for_entity(int(figure["id"]))

            self.assertEqual(files, [audit])
            self.assertEqual(counts["figures"], 1)
            self.assertEqual(counts["outputs"], 2)
            self.assertEqual(counts["inputs"], 1)
            self.assertEqual(counts["scripts"], 1)
            self.assertEqual(counts["warnings"], 1)
            self.assertIn("--plots-->", report)
            self.assertTrue(any("2 Re[D(ky)]" in note["markdown_text"] for note in notes))

    def test_import_figure_audit_ignores_shell_command_strings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            figure_dir = root / "figure"
            figure_dir.mkdir()
            output_png = figure_dir / "shell_command_source.png"
            output_png.write_text("png")
            audit = figure_dir / "shell_command_source_audit.json"
            audit.write_text(
                json.dumps(
                    {
                        "output_png": str(output_png),
                        "h5dump_command": (
                            "module load cray-hdf5; h5dump -d /Diagnostics/phi2_t "
                            "/global/path/to/file.nc"
                        ),
                    }
                )
            )

            db_path = root / "lite.sqlite"
            init_db(db_path)
            with LiteRepository(db_path) as repo:
                counts = import_figure_audits(repo, discover_figure_audit_files([figure_dir]))
                source_count = repo.conn.execute("SELECT COUNT(*) AS n FROM entity WHERE uid LIKE 'source_%'").fetchone()[
                    "n"
                ]

            self.assertEqual(counts["inputs"], 0)
            self.assertEqual(counts["source_entities"], 0)
            self.assertEqual(source_count, 0)

    def test_refresh_campaign_profile_imports_configured_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            packets = root / "packets"
            packets.mkdir()
            packet_dir = packets / "pool_a"
            packet_dir.mkdir()
            packet = build_pool_packet(
                packet_dir,
                uid="pool_profile_test",
                campaign_uid="campaign_profile_test",
                campaign_name="Profile test campaign",
                note="Profile refresh packet.",
            )
            write_pool_packet(packet, packet_dir)

            inventory = root / "inventory.txt"
            inventory.write_text(str(root / "analysis_candidate") + "\n")

            figure_dir = root / "figures"
            figure_dir.mkdir()
            figure_png = figure_dir / "summary.png"
            figure_png.write_text("png")
            audit = figure_dir / "summary_audit.json"
            audit.write_text(
                json.dumps(
                    {
                        "output_png": str(figure_png),
                        "input_csvs": [str(root / "analysis_candidate" / "summary.csv")],
                    }
                )
            )

            profile = root / "campaign_profile.json"
            profile.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "campaign_uid": "campaign_profile_test",
                        "campaign_name": "Profile test campaign",
                        "database": "campaign.sqlite",
                        "packet_roots": ["packets"],
                        "folder_inventories": ["inventory.txt"],
                        "figure_audit_roots": ["figures"],
                    }
                )
            )

            dry_summary = refresh_campaign(profile, dry_run=True)
            summary = refresh_campaign(profile)
            text = format_refresh_summary(summary)

            self.assertEqual(dry_summary.packets, 1)
            self.assertEqual(dry_summary.folder_inventories, 1)
            self.assertEqual(dry_summary.figure_audits, 1)
            self.assertTrue(summary.ok)
            self.assertEqual(summary.packets, 1)
            self.assertEqual(summary.folder_inventories, 1)
            self.assertEqual(summary.figure_audits, 1)
            self.assertIn("pool_profile_test", text)

            with LiteRepository(root / "campaign.sqlite") as repo:
                report = render_entity_report(repo, "campaign_profile_test")

            self.assertIn("pool_profile_test", report)
            self.assertIn("summary", report.lower())


if __name__ == "__main__":
    unittest.main()
