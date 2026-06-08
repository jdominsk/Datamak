from __future__ import annotations

from pathlib import Path

from datamak_lite.core.repository import LiteRepository


FIGURE_UID = "figure_demo_transport_summary"
PROJECT_ROOT = Path("/example/project")
FIGURE_PATH = PROJECT_ROOT / "figures/transport_summary.png"


def seed(repo: LiteRepository) -> str:
    """Insert a small generic dependency graph.

    This seed data is intentionally domain-neutral.  It demonstrates how Lite
    links a campaign, a simulation, a produced dataset, an analysis, and a
    figure without encoding a specific machine, physics model, or project.
    """

    repo.upsert_entity(
        uid="campaign_demo",
        type="campaign",
        name="Demo simulation campaign",
        status="active",
        scientific_status="candidate",
        description="Domain-neutral campaign used to demonstrate Datamak Lite dependency tracking.",
        metadata={},
    )

    repo.upsert_entity(
        uid="simulation_demo_source",
        type="simulation",
        name="Demo source simulation",
        path=str(PROJECT_ROOT / "runs/source_simulation"),
        status="success",
        scientific_status="candidate",
        description="Simulation that produces the dataset used by downstream analyses.",
        metadata={"code": "ExampleCode"},
    )

    repo.upsert_entity(
        uid="dataset_demo_history",
        type="dataset",
        name="Demo history dataset",
        path=str(PROJECT_ROOT / "runs/source_simulation/history_0001.h5"),
        status="available",
        scientific_status="candidate",
        description="Compact representation of an output dataset used by downstream work.",
        metadata={"time_start": 0, "time_end": 100, "saved_stride": 1},
    )

    repo.upsert_entity(
        uid="pool_demo_replay",
        type="pool",
        name="Demo replay pool",
        path=str(PROJECT_ROOT / "runs/replay_pool"),
        status="prepared",
        scientific_status="candidate",
        description="Pool of follow-up calculations that reuse the source dataset.",
        metadata={"effective_stride": 4},
    )

    repo.upsert_entity(
        uid="analysis_demo_transport",
        type="analysis",
        name="Demo transport analysis",
        path=str(PROJECT_ROOT / "analysis/transport"),
        status="available",
        scientific_status="candidate",
        description="Analysis that compares the replay pool with the source dataset.",
        metadata={},
    )

    repo.upsert_entity(
        uid=FIGURE_UID,
        type="figure",
        name="Demo transport summary",
        path=str(FIGURE_PATH),
        status="available",
        scientific_status="candidate",
        description="Demo figure showing how Lite traces figure provenance.",
        metadata={},
    )

    for child in (
        "simulation_demo_source",
        "dataset_demo_history",
        "pool_demo_replay",
        "analysis_demo_transport",
        FIGURE_UID,
    ):
        repo.add_relation(source_uid=child, relation_type="member_of", target_uid="campaign_demo")

    repo.add_relation(
        source_uid="simulation_demo_source",
        relation_type="produces",
        target_uid="dataset_demo_history",
    )
    repo.add_relation(
        source_uid="pool_demo_replay",
        relation_type="uses_input",
        target_uid="dataset_demo_history",
    )
    repo.add_relation(
        source_uid="analysis_demo_transport",
        relation_type="analyzes",
        target_uid="pool_demo_replay",
    )
    repo.add_relation(
        source_uid=FIGURE_UID,
        relation_type="plots",
        target_uid="analysis_demo_transport",
    )

    repo.add_artifact(
        entity_uid=FIGURE_UID,
        kind="figure_png",
        path=str(FIGURE_PATH),
        format="png",
        description="Figure output used for the demo dependency report.",
    )

    repo.add_metric(
        entity_uid="dataset_demo_history",
        name="time_start",
        value=0,
        unit="time",
    )
    repo.add_metric(
        entity_uid="dataset_demo_history",
        name="time_end",
        value=100,
        unit="time",
    )
    repo.add_metric(
        entity_uid="pool_demo_replay",
        name="effective_stride",
        value=4,
        unit="step",
    )

    repo.add_note(
        entity_uid="dataset_demo_history",
        note_type="comment",
        markdown_text="This dataset is the shared upstream dependency in the demo graph.",
    )
    repo.add_note(
        entity_uid=FIGURE_UID,
        note_type="decision",
        markdown_text="Use this figure as the first Lite report target because it crosses simulation, dataset, pool, and analysis objects.",
    )

    return FIGURE_UID
