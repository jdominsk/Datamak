# Datamak Tutorial

This folder contains a short PPPL-style LaTeX beamer quickstart deck for new users working with the lightweight demo database.

Main artifacts:
- `datamak_quickstart_demo_light.tex`
- `datamak_quickstart_demo_light.pdf`

## What the deck covers

- how to load `gyrokinetic_simulations_demo_light.db`
- what the main Datamak screens are for
- how to inspect workflow status, results, and statistics
- how to report a failure with the support-bundle flow

## Build

From the repository root:

```bash
pdflatex -interaction=nonstopmode -halt-on-error -output-directory tutorial tutorial/datamak_quickstart_demo_light.tex
pdflatex -interaction=nonstopmode -halt-on-error -output-directory tutorial tutorial/datamak_quickstart_demo_light.tex
```

## Style source

The deck reuses the PPPL-style beamer structure already used in:

- `docs/AIMLWorkflowManagement/aiml_workflow_management.tex`
