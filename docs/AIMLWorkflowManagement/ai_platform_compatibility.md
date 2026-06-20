# AI Platform Compatibility for Datamak

## Recommendation

The best choice is to make Datamak itself a stable typed tool server, and only
later expose it to an external AI platform through a thin adapter.

This means Datamak should not be tightly bound to one vendor SDK, one chat UI,
or one prompt format. The workflow logic should live in Datamak, while the
future platform integration should remain a replaceable outer layer.

## Target Architecture

### 1. Core tool layer

Datamak should define a small set of typed workflow tools with stable JSON
input/output contracts. These tools should be pure application logic, not tied
to Flask routes or prompt text.

Examples:
- `get_origin_workflow_state(origin_id)`
- `list_allowed_actions(origin_id)`
- `check_flux_status(origin_id)`
- `check_simulations(origin_id)`
- later, approval-gated write tools such as:
  - `run_on_flux(origin_id)`
  - `sync_back_from_flux(origin_id)`

### 2. Policy layer

The tool surface should explicitly separate:
- read-only tools, which may be called more freely
- mutating or remote tools, which should remain approval-gated

This keeps the system auditable and avoids giving models raw shell or SQL
access.

### 3. Adapter layer

Once the internal tool contracts are stable, Datamak can expose them through a
thin adapter. The preferred future-compatible option is MCP, because it is
becoming a standard way for AI platforms to connect to external tools and data.

Practical implication:
- the same Datamak tool can be used:
  - directly by the GUI
  - directly in tests
  - later through an MCP server
  - or through a small REST wrapper if needed

### 4. AI platform layer

The external AI platform should call Datamak through the adapter, not through
raw Flask routes, raw SQL access, or free-form shell generation.

This keeps Datamak in control of:
- state semantics
- allowed actions
- approval rules
- auditability

## Why This Is the Best Choice

- It avoids locking Datamak to one future platform.
- It keeps workflow logic testable without any model.
- It matches the current direction of the Datamak phase-2 supervisor work.
- It allows the current advisor and tool-calling supervisor to evolve into a
  platform integration without redesign.

## Current Audit Conclusion

Datamak is compatible with this architecture in direction, but not yet in full
implementation.

Current assessment:
- The repository already has a good foundation for AIML integration.
- It is not yet fully AI-platform compatible in the intended four-layer sense.
- The missing work is mostly architectural extraction, not a full redesign.

What is already in place:
- origin-aware workflow state is explicit in the application
- the phase-2 workflow advisor already uses typed read-only workflow helpers
- the GUI action registry already provides a strong basis for future write-tool
  execution

Main gaps:
- the current workflow tools still live inside the Workflow panel module instead
  of a standalone reusable core tool module
- policy is not yet a first-class layer with explicit metadata for approval and
  side effects
- tool contracts are still ad hoc dictionaries rather than shared, reusable
  schemas
- no external adapter layer exists yet (for example MCP or REST)
- current tests validate the Workflow UI path more than a standalone reusable
  tool API

Practical verdict:
- AI-compatible foundation: yes
- externally AI-platform ready: not yet
- best next step: extract the current workflow tools and policy into a
  standalone Datamak module, then add a thin adapter later

This means the project is already on the right path, but it still needs one
important refactor before a future external AI platform can integrate cleanly.

## Development Rules

Future AIML-related development should follow these rules:

- Put workflow semantics in the lowest reasonable layer.
- If the GUI and a future AI platform both need a capability, factor it into a
  reusable typed tool first.
- Keep policy decisions separate from the tool implementation itself.
- Keep adapters thin; they should translate, not own business logic.
- Keep platform-specific code outside the workflow core.
- Do not expose raw SQL, raw shell, or ad hoc Flask route behavior as the model
  interface.
- Treat provider-specific SDK code as replaceable outer-layer code, not as the
  place where Datamak workflow rules live.

## Immediate Next Step

The next structural step is not to add platform-specific code. It is to
formalize the current workflow supervisor tools into a standalone Datamak tool
module with stable schemas. Once that exists, adding an MCP adapter becomes a
thin integration task instead of an architectural rewrite.

## Reference

OpenAI official MCP guide:
- https://developers.openai.com/api/docs/mcp/
