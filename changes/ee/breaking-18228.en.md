The default authorization rules file (`acl.conf`) no longer grants clients connecting from `127.0.0.1` blanket publish/subscribe access to all topics (including `$SYS/#` and `#`).

Clients connecting from localhost are now authorized by the same rules as any other client, and ultimately by the `authorization.no_match` setting. In particular, subscriptions to `$SYS/#` and the wildcard filters `#` and `+/#` are now denied for localhost clients by the default rules, regardless of the security profile.

Deployments that relied on the built-in localhost allowance must add an explicit rule to `acl.conf`. The previous rule is retained in the file as a comment for easy re-enabling:

```erlang
%% {allow, {ipaddr, "127.0.0.1"}, all, ["$SYS/#", "#"]}.
```

Note: this applies to new installations and deployments that have not customized `acl.conf`; existing customized `acl.conf` files are not modified by upgrades.
