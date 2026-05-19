Removed the bundled Swagger UI assets from the EMQX release package, reducing tarball size by approximately 11 MB.

`/api-docs/swagger.json` continues to serve the full OpenAPI 3 JSON spec, so external Swagger UI deployments that load it by URL keep working. The legacy `/api-docs` URL now responds with HTTP 308 redirect to `/api-spec.html`, the new in-tree spec explorer introduced in 6.3.0. Other `/api-docs/*` subpaths (the embedded Swagger UI assets) are no longer served and return 404.
