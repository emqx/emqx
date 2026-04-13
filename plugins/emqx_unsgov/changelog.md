# EMQX Unified Namespace Governance Changelog

## 0.1.2

### Backend

- Fixed delete model API returning HTTP 500.
- Metrics -- `topic_nomatch` excluded from `messages_dropped` aggregation; displayed independently in stats and Prometheus output.

###  UI Fixes & Improvements

- Pie chart label overflow fix — Labels no longer overflow outside the chart container.
- Endpoint type editor — Added endpoint node type to the tree editor with a payload type reference dropdown.
  The dropdown dynamically shows/hides based on node type (visible for endpoint, hidden for namespace/variable).
- No-match and exempt counters displayed separately -- `topic_nomatch` is no longer lumped into total drops.
  Both "No Match" and "Exempt" are shown as independent global counters with a "(global)" hint when a model is scoped.
- New button creates empty model -- Clicking "New" now starts with a blank model instead of loading a default template.
- Model deletion from UI -- Added a "Delete" button on the model list. API returns 200 with confirmation body.
- Nested object support in payload schema editor -- Properties of type object can now have sub-properties,
  with recursive rendering, add/delete, and additionalProperties toggle at each
  nesting level.
- Enum rows are hidden for non-string types, and type changes dynamically expand/collapse sub-widgets.

## 0.1.3

- Fixed a UI render issue when topic segment is `variable`, it was rendered as `namespace`.
