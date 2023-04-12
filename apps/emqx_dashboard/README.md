# EMQX Dashboard

This application provides access to the EMQX Dashboard as well as the actual,
underlying REST API itself and provides authorization to protect against
unauthorized access. Furthermore it connects middleware adding CORS headers.
Last but not least it exposes the `/status` endpoint needed for healtcheck
monitoring.

## Implementation details

This implementation is based on `minirest`, and relies on `hoconsc` to provide an
OpenAPI spec for `swagger`.

Note, at this point EMQX Dashboard itself is an independent frontend project and
is integrated through a static file handler. This code here is responsible to
provide an HTTP(S) server to give access to it and its underlying API calls.
This includes user management and login for the frontend.
