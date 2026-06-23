Dropped support for the JSON output format in the Prometheus REST API.

The endpoints under `/api/v5/prometheus` (`stats`, `auth`, `data_integration`, `schema_validation`, `message_transformation`) now only produce the Prometheus text format. Requests sending `Accept: application/json` are rejected with `400 Bad Request` ("only prometheus format is supported"); previously they returned a JSON representation of the metrics.
