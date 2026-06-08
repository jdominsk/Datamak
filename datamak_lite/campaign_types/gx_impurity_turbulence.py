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

    text = _context_text(raw_name, path, metadata)
    if entity_type == "history_file":
        return _history_title(text, metadata)
    if entity_type == "simulation":
        return _simulation_title(text, metadata)
    if entity_type == "pool":
        return _pool_title(text, metadata)
    if entity_type == "analysis":
        return _analysis_title(text, metadata)
    if entity_type == "figure":
        return DisplayTitle(title=f"Figure: {raw_name}", subtitle="Generated figure or plot artifact.")
    return _fallback_title(entity_type, raw_name)


def group_label_for_object(*, entity_type: str, raw_name: str, metadata: dict[str, Any], path: str = "") -> str:
    text = _context_text(raw_name, path, metadata)
    category = str(metadata.get("category") or "").lower()
    if category == "metadata-curation" or metadata.get("source") == "figure_audit_input_path":
        return "Internal metadata"
    if entity_type == "pool":
        if "normal-gx" in category or "normal_gx" in text:
            return "Main plasma GX"
        if "ktm" in category or "ktm" in text:
            return "KTM / field-only models"
        if "tracer" in category or "phase_factor" in text or "phase-factor" in text or "cumulant" in text:
            return "Tracer diagnostics"
        if "replay" in category or "gxr" in text or "gx-r" in text:
            return "GX-R replay"
        return "Simulation pools"
    if entity_type == "analysis":
        if "ktm" in category or "ktm" in text:
            return "KTM / field-only models"
        if "tracer" in category or "phase" in text or "cumulant" in text:
            return "Tracer diagnostics"
        return "Analyses"
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


def _history_title(text: str, metadata: dict[str, Any]) -> DisplayTitle:
    electron = _electron_model(text, metadata)
    field = _field_model(text, metadata)
    fields = _history_fields(text, metadata)
    window = _time_window(text, metadata)
    stride = _stride(metadata)
    parts = [electron, field]
    if fields:
        parts.append(fields)
    if window:
        parts.append(window)
    if stride:
        parts.append(stride)
    subtitle = "; ".join(_metadata_details(metadata, ["source_window", "history_stride", "saved_stride", "fields"]))
    return DisplayTitle(
        title="Saved turbulent fields: " + ", ".join(part for part in parts if part),
        subtitle=subtitle,
        tags=["saved history", electron, field],
    )


def _simulation_title(text: str, metadata: dict[str, Any]) -> DisplayTitle:
    electron = _electron_model(text, metadata)
    field = _field_model(text, metadata)
    suffixes = _main_plasma_suffixes(text, metadata)
    impurity = _impurity_phrase(text, metadata)
    if impurity:
        title = f"Main Plasma GX with {impurity}: e-/D+ turbulence"
    else:
        title = "Main Plasma GX: e-/D+ turbulence"
    details = [electron, field] + suffixes
    if details:
        title += ", " + ", ".join(detail for detail in details if detail)
    subtitle = "; ".join(_metadata_details(metadata, ["category", "nhermite", "nlaguerre", "nspecies", "dt", "cfl"]))
    return DisplayTitle(title=title, subtitle=subtitle, tags=["main plasma", electron, field])


def _pool_title(text: str, metadata: dict[str, Any]) -> DisplayTitle:
    category = str(metadata.get("category") or "").lower()
    if "normal-gx" in category or "normal_gx" in text:
        return _simulation_title(text, metadata)
    if "ktm" in category or "ktm" in text:
        return _ktm_title(text, metadata)
    if "tracer" in category or "phase_factor" in text or "phase-factor" in text or "cumulant" in text:
        return _tracer_title(text, metadata)
    if "replay" in category or "gxr" in text or "gx-r" in text or "z_scan" in text or "m_scan" in text:
        return _replay_title(text, metadata)
    return _fallback_title("pool", metadata.get("name") or "")


def _analysis_title(text: str, metadata: dict[str, Any]) -> DisplayTitle:
    category = str(metadata.get("category") or "").lower()
    if "ktm" in category or "ktm" in text:
        return _ktm_title(text, metadata)
    if "tracer" in category or "phase" in text or "cumulant" in text:
        return _tracer_title(text, metadata)
    return _fallback_title("analysis", "")


def _replay_title(text: str, metadata: dict[str, Any]) -> DisplayTitle:
    species = _species_phrase(text, metadata)
    parent = _parent_turbulence_phrase(text, metadata)
    suffix = _model_suffix(text, metadata)
    title = f"Replay of {species} in {parent}"
    if suffix:
        title += f" -- {suffix}"
    subtitle_bits = []
    variants = _gradient_variants(text, metadata)
    if variants:
        subtitle_bits.append(f"gradient variants: {variants}")
    subtitle_bits.extend(_metadata_details(metadata, ["field_time_interpolation", "effective_stride", "n_cases", "nhermite", "nlaguerre"]))
    return DisplayTitle(
        title=title,
        subtitle="; ".join(subtitle_bits),
        tags=["GX-R replay", species, parent],
    )


def _tracer_title(text: str, metadata: dict[str, Any]) -> DisplayTitle:
    parent = _parent_turbulence_phrase(text, metadata)
    diagnostic = "phase-factor NL/QL"
    if "cumul" in text:
        diagnostic = "orbit cumulants"
    elif "kernel" in text:
        diagnostic = "NL/QL kernels"
    elif "phase" in text:
        diagnostic = "phase-factor NL/QL"
    suffix = _skip_or_stride(text, metadata)
    title = f"Tracer diagnostic: {diagnostic} in {parent}"
    if suffix:
        title += f", {suffix}"
    subtitle = "; ".join(_metadata_details(metadata, ["category", "tau_window", "particles_per_theta", "skip_saved_steps"]))
    return DisplayTitle(title=title, subtitle=subtitle, tags=["tracer", parent])


def _ktm_title(text: str, metadata: dict[str, Any]) -> DisplayTitle:
    species = _species_phrase(text, metadata)
    parent = _parent_turbulence_phrase(text, metadata)
    title = f"Kinetic Trace Model: response of {species} in {parent}"
    suffix = _model_suffix(text, metadata)
    if suffix:
        title += f" -- {suffix}"
    subtitle = "; ".join(_metadata_details(metadata, ["category", "models", "n_cases"]))
    return DisplayTitle(title=title, subtitle=subtitle, tags=["KTM", species, parent])


def _fallback_title(entity_type: str, raw_name: object) -> DisplayTitle:
    kind = str(entity_type or "object").replace("_", " ")
    suffix = f" ({raw_name})" if raw_name else ""
    return DisplayTitle(
        title=f"Uncurated {kind}: needs semantic title metadata",
        subtitle=f"Raw name{suffix}" if suffix else "Raw name unavailable.",
        tags=["needs curation"],
        confidence="fallback",
    )


def _electron_model(text: str, metadata: dict[str, Any]) -> str:
    value = _metadata_text(metadata, "electron_model")
    if value:
        return _nice_electron(value)
    if "adiabatic" in text:
        return "adiabatic e-"
    if "kinetic" in text or "kin_e" in text or "kinetic_e" in text or "kinelec" in text:
        return "kinetic e-"
    return "electron model unknown"


def _field_model(text: str, metadata: dict[str, Any]) -> str:
    value = _metadata_text(metadata, "field_model")
    if value:
        return value
    fields = _history_fields(text, metadata).lower()
    if "apar" in fields or "bpar" in fields or "full-em" in text or "full_em" in text:
        return "full-EM"
    if "fapar" in metadata and "fbpar" in metadata:
        if _is_zero(metadata.get("fapar")) and _is_zero(metadata.get("fbpar")):
            return "ES"
        return "EM"
    if "es_" in text or "_es" in text or " electrostatic" in text:
        return "ES"
    if "em_" in text or "_em" in text:
        return "EM"
    return "field model unknown"


def _history_fields(text: str, metadata: dict[str, Any]) -> str:
    fields = metadata.get("history_fields") or metadata.get("fields")
    if isinstance(fields, list):
        return "/".join(str(field) for field in fields)
    if isinstance(fields, str) and fields:
        normalized = fields.replace("_step", "").replace(",", "/")
        normalized = normalized.replace("Phi", "phi").replace("phi/Apar/Bpar", "phi/Apar/Bpar")
        return normalized
    detected = []
    if "phi" in text:
        detected.append("phi")
    if "apar" in text:
        detected.append("Apar")
    if "bpar" in text:
        detected.append("Bpar")
    return "/".join(detected)


def _time_window(text: str, metadata: dict[str, Any]) -> str:
    explicit = metadata.get("history_time_window") or metadata.get("source_window")
    if isinstance(explicit, str) and explicit:
        return f"t={explicit}"
    start = metadata.get("time_start")
    end = metadata.get("time_end")
    if start is not None and end is not None:
        return f"t={start}-{end}"
    return ""


def _stride(metadata: dict[str, Any]) -> str:
    for key in ("history_stride", "saved_stride", "step_record_stride", "effective_stride"):
        value = metadata.get(key)
        if value not in (None, "", []):
            return f"stride={value}"
    return ""


def _main_plasma_suffixes(text: str, metadata: dict[str, Any]) -> list[str]:
    suffixes: list[str] = []
    if "alti819" in text or "alti8" in text:
        suffixes.append("high a/LTi=8.19")
    if "titeq" in text or "ti=te" in text:
        suffixes.append("Ti=Te")
    if metadata.get("fixed_dt") is True:
        suffixes.append("fixed dt")
    elif metadata.get("fixed_dt") is False:
        suffixes.append("adaptive dt")
    return suffixes


def _impurity_phrase(text: str, metadata: dict[str, Any]) -> str:
    species = metadata.get("species")
    z = metadata.get("Z")
    if species and z:
        return f"{species} Z={z}"
    if "boron" in text or "_b" in text:
        return "B impurities"
    if "w20" in text:
        return "W Z=20 impurities"
    nspecies = metadata.get("nspecies")
    if isinstance(nspecies, (int, float)) and nspecies > 2:
        return "impurities"
    return ""


def _species_phrase(text: str, metadata: dict[str, Any]) -> str:
    species = metadata.get("species")
    z = metadata.get("Z")
    if species and z:
        return f"{species} Z={z}"
    if species:
        return str(species)
    z_values = metadata.get("z_values")
    if isinstance(z_values, list) and z_values:
        prefix = "W" if _looks_like_tungsten(text, metadata) else "impurities"
        return f"{prefix} Z={_range_or_list(z_values)}"
    if "fully_stripped" in text or "fully stripped" in text:
        return "fully stripped impurities"
    if "argon" in text or "neon" in text or "ar_ne" in text:
        return "W/Ar/Ne"
    if "wself" in text or "wtotal" in text or "tungsten" in text or "_w" in text:
        return "W"
    if "boron" in text:
        return "B"
    if "mass scan" in text or "m_scan" in text:
        return "impurity mass scan"
    return "impurities"


def _parent_turbulence_phrase(text: str, metadata: dict[str, Any]) -> str:
    electron = _electron_model(text, metadata)
    if "titeq" in text or "equal-gradient" in text or "equal_gradient" in text:
        return f"{electron} equal-gradient turbulence"
    if "adiabatic" in text:
        return "adiabatic-e turbulence"
    if "kinetic" in text or "kin_e" in text or "kinetic_e" in text:
        return "kinetic-e turbulence"
    return "parent turbulence"


def _model_suffix(text: str, metadata: dict[str, Any]) -> str:
    model_terms = metadata.get("model_terms")
    models = metadata.get("models")
    model = str(metadata.get("model") or "")
    candidates = []
    if isinstance(model_terms, list):
        candidates.extend(str(item) for item in model_terms)
    if isinstance(models, list):
        candidates.extend(str(item) for item in models)
    if model:
        candidates.append(model)
    joined = " ".join(candidates).lower() + " " + text
    suffixes = []
    if "no_vparallel" in joined or "no parallel" in joined or "no_vpar" in joined:
        suffixes.append("no v_parallel")
    if "no_magnetic_drift" in joined or "no magnetic" in joined or "no_vd" in joined:
        suffixes.append("no magnetic drift")
    if "no_ve" in joined or "without ve" in joined or "w/o ve" in joined or "no_vexb" in joined:
        suffixes.append("w/o turbulent vE.grad g")
    if "stride scan" in joined or "stride_scan" in joined:
        suffixes.append("stride scan")
    if "collision" in joined or "coll" in joined:
        suffixes.append("collision scan")
    if "double" in joined and "gradient" in joined:
        suffixes.append("doubled impurity temperature gradient")
    if "with turbulent v" in joined or "with turbulent v_e" in joined:
        suffixes.append("with turbulent vE")
    if not suffixes and "full" in joined:
        suffixes.append("full model")
    return ", ".join(dict.fromkeys(suffixes))


def _gradient_variants(text: str, metadata: dict[str, Any]) -> str:
    variants = metadata.get("gradient_variants")
    if isinstance(variants, list) and variants:
        return ", ".join(str(item) for item in variants)
    found = []
    if "dparam" in text or "reference" in text:
        found.append("reference")
    if "flat_ln" in text:
        found.append("flat a/Ln")
    if "flat_lt" in text:
        found.append("flat a/LT")
    if found:
        return ", ".join(dict.fromkeys(found))
    n_cases = metadata.get("n_cases")
    if isinstance(n_cases, (int, float)) and n_cases >= 3:
        return "variants recorded in pool"
    return ""


def _skip_or_stride(text: str, metadata: dict[str, Any]) -> str:
    skip = metadata.get("skip_saved_steps")
    if skip not in (None, "", []):
        return f"skip={skip}"
    for token in ("skip2", "skip-2", "skip_2"):
        if token in text:
            return "skip=2"
    stride = metadata.get("effective_stride") or metadata.get("history_stride")
    if stride not in (None, "", []):
        return f"stride={stride}"
    return ""


def _metadata_details(metadata: dict[str, Any], keys: list[str]) -> list[str]:
    details = []
    for key in keys:
        value = metadata.get(key)
        if value not in (None, "", []):
            details.append(f"{key}={_compact(value)}")
    return details


def _metadata_text(metadata: dict[str, Any], key: str) -> str:
    value = metadata.get(key)
    return str(value).strip() if value not in (None, "", []) else ""


def _context_text(raw_name: str, path: str, metadata: dict[str, Any]) -> str:
    pieces = [raw_name, path]
    for key in ("category", "code", "model", "models", "source_window", "fields", "species"):
        value = metadata.get(key)
        if value not in (None, "", []):
            pieces.append(_compact(value, max_len=200))
    return " ".join(pieces).replace("-", "_").lower()


def _nice_electron(value: str) -> str:
    text = value.lower().replace("_", " ")
    if "adiabatic" in text:
        return "adiabatic e-"
    if "kinetic" in text:
        return "kinetic e-"
    return value


def _looks_like_tungsten(text: str, metadata: dict[str, Any]) -> bool:
    if "tungsten" in text or " w " in text or "_w" in text or "wself" in text or "wtotal" in text:
        return True
    mass = metadata.get("m_Z_over_m_D") or metadata.get("m_z_over_m_d")
    return bool(isinstance(mass, (int, float)) and mass > 50)


def _range_or_list(values: list[Any]) -> str:
    compact = [str(value) for value in values]
    if len(compact) > 6:
        return f"{compact[0]}-{compact[-1]}"
    return ", ".join(compact)


def _is_zero(value: Any) -> bool:
    try:
        return abs(float(value)) < 1e-15
    except (TypeError, ValueError):
        return False


def _compact(value: Any, *, max_len: int = 80) -> str:
    if isinstance(value, list):
        text = ", ".join(str(item) for item in value[:8])
        if len(value) > 8:
            text += ", ..."
    elif isinstance(value, dict):
        text = ", ".join(f"{key}={value[key]}" for key in list(value)[:5])
        if len(value) > 5:
            text += ", ..."
    else:
        text = str(value)
    text = " ".join(text.split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "..."
