The Dashboard schema endpoints `/api/v5/schemas/hotconf`, `/api/v5/schemas/actions`, and `/api/v5/schemas/connectors` now require authentication.

This is an intentional breaking change for the bundled Dashboard. The Dashboard schema loader must send the logged-in Bearer token when fetching these endpoints. Update the Dashboard package together with EMQX; older Dashboard bundles will receive HTTP 401 and schema-driven configuration forms will not load.
