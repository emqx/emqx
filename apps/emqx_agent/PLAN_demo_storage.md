# Demo Storage Plan

## Goal

Demonstrate EMQX pipelines governing storage-facility devices (tetris-inspired shapes)
with a web demo and pipeline-focused CT tests.

## Naming

- Use `demo-storage` naming across API/UI/tests.
- Keep Agent prefix in routes: `/agent/demo-storage/ui`.

## Scope

1. Add a new static UI endpoint served from `emqx_agent` priv files:
   - `GET /agent/demo-storage/ui`
   - Serve `apps/emqx_agent/priv/demo-storage.html` the same way as `/agent/ui`.
2. Keep the existing dashboard SPA flow unchanged.
3. Add/adjust API schema and i18n entries for the new endpoint.
4. Add API CT coverage that verifies the endpoint returns HTML.
5. Add a new pipeline-focused CT suite for demo-storage behavior:
   - Hard-require Fireworks availability and `FIREWORKS_API_KEY`.
   - Use Fireworks Kimi 2.5 model for `llm_loop` session config.
   - Test only pipeline outcomes and decisions, not realm emulator internals.

## Pipeline Tests to Implement

1. Field update pipeline parks and persists occupancy.
2. Field update pipeline no-ops when already parked.
3. Core pipeline breaks early when telemetry reports parked.
4. Core pipeline emits exactly one legal next command.
5. Core pipeline emits park when device is tightly packed.

## LLM Requirement

- No skip/fallback behavior for LLM-dependent cases.
- Suite fails if Fireworks key is missing or provider/model is unreachable.
- API key source: `FIREWORKS_API_KEY`.

## Prompt Constraints

- Field-update prompt:
  - Only decide whether to no-op or execute the two DB actions in order.
  - No extra tools, no reordered actions.
- Core-admin prompt:
  - Exactly one action from `move|rotate|park|none`.
  - Never issue illegal movement.
  - If parked => `none`; if tightly packed and legal => `park`.
