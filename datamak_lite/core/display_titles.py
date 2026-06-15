from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DisplayTitle:
    title: str
    subtitle: str = ""
    tags: list[str] = field(default_factory=list)
    confidence: str = "generic"


def display_title_for_object(
    *,
    campaign_type: str | None,
    entity_type: str,
    raw_name: str,
    metadata: dict[str, Any],
    path: str = "",
) -> DisplayTitle:
    if campaign_type == "gx_impurity_turbulence":
        from datamak_lite.campaign_types.gx_impurity_turbulence import title_for_object

        title = title_for_object(entity_type=entity_type, raw_name=raw_name, metadata=metadata, path=path)
        return DisplayTitle(
            title=title.title,
            subtitle=title.subtitle,
            tags=title.tags,
            confidence=title.confidence,
        )
    if campaign_type == "xgc_west_edge_campaign":
        from datamak_lite.campaign_types.xgc_west_edge_campaign import title_for_object

        title = title_for_object(entity_type=entity_type, raw_name=raw_name, metadata=metadata, path=path)
        return DisplayTitle(
            title=title.title,
            subtitle=title.subtitle,
            tags=title.tags,
            confidence=title.confidence,
        )
    return _generic_title(entity_type=entity_type, raw_name=raw_name, metadata=metadata)


def display_group_for_object(
    *,
    campaign_type: str | None,
    entity_type: str,
    raw_name: str,
    metadata: dict[str, Any],
    path: str = "",
) -> str:
    if campaign_type == "gx_impurity_turbulence":
        from datamak_lite.campaign_types.gx_impurity_turbulence import group_label_for_object

        return group_label_for_object(entity_type=entity_type, raw_name=raw_name, metadata=metadata, path=path)
    if campaign_type == "xgc_west_edge_campaign":
        from datamak_lite.campaign_types.xgc_west_edge_campaign import group_label_for_object

        return group_label_for_object(entity_type=entity_type, raw_name=raw_name, metadata=metadata, path=path)
    return entity_type.replace("_", " ").title()


def metadata_from_json(raw: str) -> dict[str, Any]:
    try:
        data = json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _generic_title(*, entity_type: str, raw_name: str, metadata: dict[str, Any]) -> DisplayTitle:
    explicit = metadata.get("semantic_title")
    if isinstance(explicit, str) and explicit:
        subtitle = metadata.get("semantic_subtitle")
        return DisplayTitle(
            title=explicit,
            subtitle=subtitle if isinstance(subtitle, str) else "",
            confidence="explicit",
        )
    return DisplayTitle(title=raw_name or entity_type.replace("_", " ").title())
