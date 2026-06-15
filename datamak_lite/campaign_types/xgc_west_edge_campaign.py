from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DisplayTitle:
    title: str
    subtitle: str = ""
    tags: list[str] = field(default_factory=list)
    confidence: str = "inferred"


def title_for_object(
    *,
    entity_type: str,
    raw_name: str,
    metadata: dict[str, Any],
    path: str = "",
) -> DisplayTitle:
    explicit = _explicit_title(metadata)
    if explicit:
        return explicit
    if entity_type == "simulation":
        code = str(metadata.get("code_family") or "XGC")
        machine = str(metadata.get("machine") or "").capitalize()
        subtitle = "; ".join(part for part in [machine, metadata.get("lineage_short_identity")] if part)
        return DisplayTitle(title=f"{code} simulation: {raw_name}", subtitle=subtitle, tags=["XGC", machine])
    if entity_type == "grid":
        return DisplayTitle(title=f"XGC grid: {raw_name}", tags=["XGC grid"])
    if entity_type == "equilibrium":
        return DisplayTitle(title=f"MHD equilibrium: {raw_name}", tags=["equilibrium"])
    if entity_type == "profile":
        return DisplayTitle(title=f"XGC profile: {raw_name}", tags=["profile"])
    if entity_type == "analysis":
        return DisplayTitle(title=f"Analysis: {raw_name}", tags=["analysis"])
    if entity_type == "figure":
        return DisplayTitle(title=f"Figure: {raw_name}", tags=["figure"])
    return DisplayTitle(title=raw_name or entity_type.replace("_", " ").title())


def group_label_for_object(*, entity_type: str, raw_name: str, metadata: dict[str, Any], path: str = "") -> str:
    if entity_type == "simulation":
        return "Main plasma XGC"
    if entity_type == "grid":
        return "XGC grids"
    if entity_type == "equilibrium":
        return "MHD equilibria"
    if entity_type == "profile":
        kind = str(metadata.get("profile_kind") or "").lower()
        if "density" in kind:
            return "Density profiles"
        if "temperature" in kind:
            return "Temperature profiles"
        return "XGC profiles"
    if entity_type == "analysis":
        return "Analyses"
    if entity_type == "figure":
        return "Figures"
    return entity_type.replace("_", " ").title()


def _explicit_title(metadata: dict[str, Any]) -> DisplayTitle | None:
    title = metadata.get("semantic_title")
    if not isinstance(title, str) or not title:
        return None
    subtitle = metadata.get("semantic_subtitle")
    tags = metadata.get("semantic_tags")
    return DisplayTitle(
        title=title,
        subtitle=subtitle if isinstance(subtitle, str) else "",
        tags=[str(tag) for tag in tags] if isinstance(tags, list) else [],
        confidence="explicit",
    )
