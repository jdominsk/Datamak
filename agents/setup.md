# Set up Datamak Lite with an AI assistant

This guide is for Codex, Claude Code, and other assistants helping a new user
adopt Datamak Lite. The goal is a working local provenance tool, a reusable
skill, and an explicit suggestion for persistent instructions. Do the setup
the user requested; ask only for missing choices that affect the workspace
or existing data.

## 1. Find the local installation and research workspace

Read this checkout's `AGENTS.md`, `docs/lite_overview.md`, and
`agents/maintenance_protocol.md`. Use the checkout the user downloaded, not
paths copied from the original author's environment. Distinguish the Datamak
source checkout from the user's research workspace.

Inspect the existing assistant skills and instructions before creating new
ones. Reuse an applicable Datamak skill instead of installing a duplicate or
replacing user-maintained guidance.

Choose an available Python interpreter and run this from the checkout:

```bash
python3 -m datamak_lite.cli --help
```

Use the actual interpreter path in the installed skill. The local Lite CLI
and read-only GUI use the Python standard library; the full repository's
`requirements.txt`, Pyrokinetics, HPC access, and scheduler tests are not
prerequisites for this local setup. Install additional dependencies only for
features the user requests. Do not change shell startup files just to make
the module importable: commands can run from the checkout.

## 2. Connect an existing campaign, or start a small local one

Check the machine-level index before searching research directories:

```bash
python3 -m datamak_lite.cli list-campaigns
python3 -m datamak_lite.cli resolve-campaign CAMPAIGN_UID --field database
```

The index lives at `$DATAMAK_HOME/campaigns.json`, or
`~/.datamak/campaigns.json` when `DATAMAK_HOME` is unset. If several campaigns
match, resolve the relevant one before importing anything. Preserve existing
profiles, databases, sidecars, and identifiers.

If no campaign exists and the user requested a new workspace setup, create a
minimal campaign profile inside that workspace, for example
`.datamak_lite/campaign_profile.json`. Give it a project-specific UID and name:

```json
{
  "schema_version": 1,
  "campaign_uid": "campaign_example",
  "campaign_name": "Example research campaign",
  "database": "campaign.sqlite",
  "packet_roots": []
}
```

Paths in a profile are relative to its directory. Add only known metadata
sources; do not copy the example profile's demonstration remote paths. Then:

```bash
python3 -m datamak_lite.cli refresh-campaign /absolute/path/to/campaign_profile.json
```

This initializes the campaign database and registers the profile in the user
index. Reuse this profile for later refreshes. For existing campaigns, inspect
the configured sources before refreshing, especially remote sources. Avoid
broad filesystem scans or raw simulation-data imports as an installation step.

## 3. Create or adapt the reusable skill

Use `skills/datamak-lite/SKILL.md` as the instruction-only starting point.
Create the local skill in a location supported by the user's assistant:

| Assistant | Project skill | Personal skill |
| --- | --- | --- |
| Codex | `.agents/skills/datamak-lite/SKILL.md` | `~/.agents/skills/datamak-lite/SKILL.md` |
| Claude Code | `.claude/skills/datamak-lite/SKILL.md` | `~/.claude/skills/datamak-lite/SKILL.md` |

Honor the user's chosen scope and any configured skill roots. For another
assistant or a different version, check its supported convention rather than
assuming these paths apply. Do not install into both scopes by default.

Adapt the skill to the environment, replacing its **Local installation** section
with the verified checkout and Python paths and links to the checkout's
maintenance and figure-metadata guides. Keep campaign discovery through the
user index; do not embed one campaign as the skill's permanent target. Preserve
an existing registry default unless the user requests a change (the first
registered campaign becomes the default automatically). Preserve the skill's
normal automatic discovery.
If the assistant has no skill mechanism, propose the same concise workflow
in its supported project instructions instead.

## 4. Suggest persistent instructions

Explain that the skill supplies the workflow, while a short persistent rule
helps the assistant consult it before falling back to file searches. Suggest
this exact text for the user's personalization or agent instructions:

> Use Datamak Lite first for simulation provenance, status, produced files, restart/history availability, case lists, and diagnostic inputs when a campaign database, registry, sidecar, or pool database exists.

Also recommend the maintenance counterpart:

> When Datamak Lite metadata is missing or stale, inspect the necessary files, record what you verified in the relevant sidecar or campaign database, and report remaining uncertainty. Preserve existing identifiers and update only unambiguous mappings within the authorized task.

For Codex, propose the applicable project `AGENTS.md` or the effective
user-level instruction file under `$CODEX_HOME` (normally
`~/.codex/AGENTS.md`); check for an active `AGENTS.override.md` first. For
Claude Code, propose the applicable project `CLAUDE.md` or personal
`~/.claude/CLAUDE.md`. For a personalization UI, provide text to paste.
Show the target and proposed addition before changing persistent settings,
unless the user already explicitly authorized that change. Append without
duplicating or replacing unrelated instructions. Do not claim to have changed
UI settings that the assistant cannot access.

These are workflow instructions, not a guarantee that an assistant will
always select the tool. They do not expand permissions to launch jobs, change
research data, or import ambiguous provenance.

## 5. Verify and hand off

Use the smallest local check: list the registered campaign, resolve its
database, and inspect one existing entity report if available. Report an
empty new campaign as ready for its first records; do not insert demo data
into a research database. Confirm that the assistant discovers the installed
skill, or explain how to reload its skill list. No simulation or scheduler
job is needed.

Tell the user the checkout, interpreter, skill location, campaign profile and
database, what was verified, and whether persistent instructions were only
suggested or actually added with authorization. A useful next prompt is:

> Use Datamak Lite to find what produced this figure, and record any verified
> provenance gaps you resolve.

## Assistant documentation

- [Codex skills](https://developers.openai.com/codex/skills)
- [Codex instructions](https://developers.openai.com/codex/guides/agents-md)
- [Claude Code skills](https://code.claude.com/docs/en/skills)
- [Claude Code memory](https://code.claude.com/docs/en/memory)
