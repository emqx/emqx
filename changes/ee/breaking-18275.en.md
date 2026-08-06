Changed `authorization.include_mountpoint` to default to `true` under the hardened security profile. I.e., by default, the final topics including the mountpoint prefix are authorized.

If mounting with default settings is used, either update the authorization rules or explicitly change settings to legacy behavior.

To update the authorization rules and records, add mountpoint prefixes to rules' topics. For example, with mountpoint `org1/`, change a rule for `sensors/#` to `org1/sensors/#`.

To keep the existing rules and records working without changes, do one of the following:

- Set `authorization.include_mountpoint = false` explicitly.
- Do not set `authorization.include_mountpoint` and run EMQX with the legacy security profile (`EMQX_SECURITY_PROFILE=legacy`).
