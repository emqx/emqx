Added `EMQX_SECURITY_PROFILE` environment variable support to provide graceful transiton from legacy defaults to more secure defaults in EMQX 7.
With `EMQX_SECURITY_PROFILE` set to `legacy` (default in EMQX 6 and earlier), the behavior is unchanged.
With `EMQX_SECURITY_PROFILE` set to `hardened`, the following changes are made:

- Reject insecure Erlang cookies.
- Bind MQTT and WebSocket listeners to loopback by default or when bind host is ommitted.
- Bind the default HTTP Dashboard listener to loopback by default or when bind host is ommitted.
- Reject Dashboard login with the default `public` password.
- Deny MQTT anonimous access when no authentication backends are configured.

In EMQX 7, the default value of `EMQX_SECURITY_PROFILE` will be `hardened`. To achieve previous default behavior, setting `EMQX_SECURITY_PROFILE` to `legacy` explicitly will be required.
