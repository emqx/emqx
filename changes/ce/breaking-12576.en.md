Starting from 5.6, the "Configuration Manual" document will no longer include the `bridges` config root.

A `bridge` is now either `action` + `connector` for egress data integration, or `source` + `connector` for ingress data integration.
Please note that the `bridges` config (in `cluster.hocon`) and the REST API path `api/v5/bridges` still works, but considered deprecated.
