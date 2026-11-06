# GX/GX-R/KTM Impurity-Turbulence Campaign Type

This campaign type is a reusable Datamak Lite template for studies where a
main-plasma turbulence simulation is reused to study impurity dynamics.

It is intentionally domain-specific, but not campaign-specific.  It can be
reused for different tokamaks, shots, equilibria, or plasma regimes.

## Scope

Use this campaign type when the campaign has one or more of these workflows:

- GX or GX-like main-plasma simulations generating turbulence;
- saved turbulent field histories used as reusable data products;
- GX-R replay scans using saved turbulence;
- tracer-particle or phase-factor diagnostics using saved turbulence;
- KTM or other field-only impurity-response models;
- comparisons between nonlinear, quasi-linear, reduced, no-vE, no-parallel, or
  no-drift models on the same parent turbulence.

Do not use this template for every Lite campaign.  Lite core remains
code-agnostic; this template is a reusable domain layer.

## Files

- `title_rules.md`: semantic display-title rules for the GUI and agents.
- `metadata_keys.json`: recommended metadata keys and controlled vocabulary.

## Campaign Profile Usage

A campaign can reference this template in its profile:

```json
{
  "campaign_type": "gx_impurity_turbulence",
  "campaign_type_template": "../../Datamak_lite/templates/campaign_types/gx_impurity_turbulence",
  "display_title_rules": "local_title_rules.md"
}
```

The template provides default rules.  The local `display_title_rules` file can
override or refine them for a specific shot, machine, or project.

## Design Principle

In the GUI overview, show semantic physics titles first.  Raw folder names,
input filenames, and remote paths should remain available in object details,
but they should not be the primary titles.
