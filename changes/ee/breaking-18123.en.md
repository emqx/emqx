Added a new configuration `multi_tenancy.deny_namespaces` holding namespace names that cannot be used as a namespace identifier — neither as an admin namespace (dashboard roles, API keys, multi-tenancy management API) nor as a per-client `client_attrs.tns`; a client whose `client_attrs.tns` resolves to a denied name is rejected.

This is a breaking change: the default value `["global", "undefined", "null", "none"]` denies names that were previously accepted. These names collide with internal sentinels and would produce ambiguous log lines and dashboard output. Existing namespaces with these names are not migrated; rename them before upgrading, or set `multi_tenancy.deny_namespaces` to an empty list to lift the restriction.

Additionally, when `multi_tenancy.post_auth_tns_expression` is configured and evaluates to an empty value or fails to evaluate, a client whose pre-authentication `client_attrs.tns` is a denied namespace name is now also rejected, consistent with the handling when the expression evaluates to a non-empty value.

Relevant PRs: [#17626](https://github.com/emqx/emqx/pull/17626), [#18123](https://github.com/emqx/emqx/pull/18123).
