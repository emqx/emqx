Changed the default security profile from `legacy` to `hardened`. The profile sets the default of a group of security behaviours at once; each behaviour still has its own configuration option, and an explicitly configured value always wins.

Listeners:

- Default MQTT and Dashboard listeners bind to loopback instead of all interfaces. Set `node.default_listener_address` to serve remote clients without leaving the profile. The official Docker image already sets it to `all`.

Authentication:

- Clients are denied when no authenticator is configured.
- Clients are denied when an authentication backend fails, instead of the failure being ignored.
- A JWT authenticator denies clients that present no token.
- Dashboard login is denied while the default admin password is unchanged.
- Built-in database authentication auto-generates user passwords, hashes manually set passwords with pbkdf2, and rejects weak hash algorithms in new authenticator configurations.
- A namespaced built-in database user is rejected when a global user has the same user ID.

Authorization:

- Clients are denied when an authorization backend fails, instead of the failure being ignored.
- Authorization runs on the topic with the mountpoint prefix included. See the `authorization.include_mountpoint` change for how to migrate existing rules.
- Subscriptions made on a client's behalf (REST API, CLI, auto-subscribe) are authorized like client subscriptions.
- Delayed messages are re-authorized when they are published.
- Authentication and authorization are interrupted when a hook callback crashes, instead of the crash being ignored.

Other:

- Outbound TLS verifies peer certificates, for example the JWKS endpoint of JWT authentication.
- An ExHook failure denies the hooked action, and an unreachable ExHook server denies it rather than applying the configured `failed_action`.
- Pre-approving a plugin installation requires a SHA-256 checksum, which the plugin package is verified against.
- The node refuses to start with the default Erlang cookie.

Set `EMQX_SECURITY_PROFILE=legacy` to retain the previous behaviour during migration.

Existing built-in database authentication configurations with a manually set password hash algorithm remain valid after the upgrade.
