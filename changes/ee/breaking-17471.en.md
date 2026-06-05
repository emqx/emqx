Tightened access for namespaced dashboard users (administrators and viewers in `ns:<namespace>::<role>`):

- Namespaced administrators can no longer use `GET` to read arbitrary global endpoints. They retain access to the namespace-scoped APIs (Connectors, Actions, Sources, Rules, Trace, Data Backup) and to their own namespace configuration and account.
- The `/clients*` and `/subscriptions` endpoints now reject all non-global requests with `403 FORBIDDEN`, regardless of the requester's role (administrator, viewer, API key). This prevents namespaced users from observing or mutating sessions and subscriptions in other namespaces or in the global scope.

Other global read-only endpoints (`/nodes`, `/stats`, `/metrics`, `/listeners`, `/banned`, …) remain readable by namespaced *viewers* via the generic viewer-GET rule. Tightening those is tracked as a follow-up.
