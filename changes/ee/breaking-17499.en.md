Tightened read access for namespaced dashboard users (administrators and viewers in `ns:<namespace>::<role>`):

- Namespaced administrators can no longer use `GET` to read arbitrary global endpoints. They retain access to the namespace-scoped APIs (Connectors, Actions, Sources, Rules, Trace, Data Backup) and to their own namespace configuration and account.
- The `/clients*` and `/subscriptions` endpoints now reject every non-global request (administrator, viewer, or API key) with `403 FORBIDDEN`, so namespaced users can no longer observe sessions or subscriptions in other namespaces or in the global scope. Mutating these endpoints was already denied before this change.
- The trace log download endpoints `GET /trace/:name/download` and `GET /trace/:name/log` now reject non-global requests with `403 FORBIDDEN`. The trace lookup is global, so without this gate a namespaced user that knows or guesses a trace name could pull log content captured outside their namespace.

Backup file downloads (`GET /data/files/:filename`) were already gated to the global administrator role.

Other global read-only endpoints (`/nodes`, `/stats`, `/metrics`, `/listeners`, `/banned`, …) remain readable by namespaced *viewers* via the generic viewer-GET rule, which may change in future versions.
