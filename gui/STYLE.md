# Datamak GUI Style Guide

This file documents the current visual and interaction rules for the Datamak GUI.

It is not a full design system yet. The current app still keeps a large amount
of CSS in [gui/templates/index.html](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/templates/index.html),
but shared control styles have started moving into
[gui/static/app.css](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/static/app.css).
This file defines the intended style contract so future work stays coherent.

## Scope

- Main Flask GUI under [gui/app.py](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/app.py)
- Main template under [gui/templates/index.html](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/templates/index.html)
- Shared GUI partials under [gui/templates](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/templates)
- Shared extracted control CSS under [gui/static/app.css](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/static/app.css)

## Design Principles

- Prefer clarity over decoration.
- Keep controls compact and dense enough for scientific workflow use.
- Use white controls instead of default gray OS-style widgets when building custom selectors.
- Use color to encode workflow identity and state, not as ornament.
- Keep important actions visually calm; reserve red for error and recovery situations.
- Favor immediate visual feedback before page reload when a control selection changes.

## Typography

- Base UI font: `"Helvetica Neue", Arial, sans-serif`
- Default table/control text: about `13px`
- Section headings:
  - use `.equilibria-heading`
  - generally `16px`, bold
- Chart or sub-block titles:
  - use `.chart-title`
  - generally `14px`
- Support text / explanatory notes:
  - use `.panel-note`
  - generally `13px`

## Color Rules

- Base text:
  - dark text: `#1f2933` or `#243b53`
  - secondary text: `#52606d` or `#7b8794`
- Neutral borders:
  - default border: `#d9e2ec`
  - stronger control border: `#cbd5e0`
- Active/selected scientific workflow emphasis:
  - background: `#eef6ff`
  - accent line: `#0b3d91`
- Recovery / missing database:
  - heading text: `#a61b1b`
  - border: `#f5c6c6`

## Layout Rules

- Use rounded boxes with light borders for major sections.
- Use sticky top tabs for panel switching.
- Keep the page visually flat: avoid heavy shadows or dark chrome.
- Prefer stacked content over side-by-side only when the content is dense and easier to scan vertically.
- Panels with workflow meaning should use consistent spacing and titles:
  - `Origin Registry`
  - `AI advisor`
  - `Scan Equilibria & Generate Inputs`
  - `Launch Batch of Simulations`
  - `Batch monitoring`

## Controls

### General

- Keep controls compact.
- Avoid bulky vertical padding.
- Default white control surface for custom controls.
- Buttons should feel lightweight, not dashboard-heavy.

### Origin Picker

Current custom origin picker lives in:
- [gui/templates/index.html](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/templates/index.html)
  markup
- [gui/static/app.css](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/static/app.css)
  shared control styling
  selectors:
  - `.origin-picker`
  - `.origin-picker summary`
  - `.origin-picker-panel`
  - `.origin-picker-option`

Rules:
- The closed control is white.
- The selected origin shows a colored square before the label.
- The picker should be compact:
  - currently around `28px` minimum height
  - vertical padding should remain very small
- This control is now the reference style for ordinary app buttons and button-like links.
- Opening the picker should overlay the selected value, not duplicate it visually underneath.
- Clicking a new origin should:
  1. update the visible label immediately
  2. update the swatch immediately
  3. close the picker immediately
  4. submit shortly after

### Standard Buttons

- Standard buttons should follow the same control surface language as the origin picker:
  - white background
  - compact height
  - `#cbd5e0` border
  - rounded corners
  - dark text
  - light blue hover background
- This applies to ordinary action buttons throughout panels.
- Exceptions:
  - top-level tabs
  - subtabs
  - floating drawer/fab buttons
  - origin-picker option rows inside the dropdown

### Standard Selects and Inputs

- Standard `<select>` controls should also use the origin-picker control surface:
  - white background
  - compact height
  - dark text
  - rounded border
  - custom subtle caret instead of the heavy native gray widget look
- Standard text and number inputs should use the same border radius, border color, and compact spacing.

### Native Select Replacement

When replacing a native select with a custom control:
- preserve the same semantic parameter name in the form
- preserve keyboard-close behavior where feasible
- close on outside click and `Escape`
- do not introduce extra vertical bulk

## Color Swatches

- Use a small square swatch before origin names when identity matters.
- Current swatch helper class:
  - `.color-swatch`
- Origin color should be shown in:
  - origin rows
  - workflow headers tied to a selected origin
  - custom origin pickers

## Error and Recovery UI

- Error blocks can use the existing `.error` style.
- Recovery blocks should be visually distinct from ordinary panels.
- `Database recovery` is the reference example:
  - red heading
  - red border
  - clear action ordering:
    1. use detected local DB first
    2. download only if needed

## Tabs and Drawers

- Top-level tabs should remain calm and light.
- Active tab uses pale blue background and subtle emphasis.
- Floating drawers such as `HPC Login` and `Workflow Suggestions` must have:
  - visible close button
  - consistent header styling
  - enough width to avoid horizontal scrolling where practical

## Tables

- Tables are still the main scientific data surface.
- Keep:
  - sticky headers
  - ellipsis for long content
  - light borders
- Avoid over-decorating tables.
- Schema summary tables should appear above row content when useful.

## Workflow State Styling

- Use chips/badges for machine state.
- State color should map to meaning:
  - blue: active / running
  - green: ready / done / synced
  - gray: empty / pending / neutral
  - orange-red: failed / mixed / warning

## Interaction Rules

- Prefer fast local feedback before reload.
- Avoid unnecessary modal interruptions.
- Avoid UI elements that claim certainty about remote SSH/Duo prompts unless the app really has that certainty.
- When a browser action depends on external authenticated state, prefer:
  - opening the browser
  - then detecting the resulting file locally
instead of trying to do the authenticated download invisibly in Flask.

## Current Technical Debt

- Most CSS is still inline in [gui/templates/index.html](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/templates/index.html).
- This style guide is the contract; the implementation is not yet centralized.

## Recommended Next Step

Continue moving shared GUI CSS into:
- [gui/static/app.css](/Users/jdominsk/Documents/Projects/AIML_database/Datamak/gui/static/app.css)

If that happens:
- keep this file as the human-readable style contract
- keep component class names stable where possible
