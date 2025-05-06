# Authenticate clients with connection information

This application implements an extended authentication for EMQX Enterprise edition.

Client-info (of type `cinfo`) authentication is a lightweight authentication mechanism which checks client properties and attributes against user defined rules.
The rules make use of the Variform expression to define match conditions, and the authentication result when match is found.
For example, to quickly fencing off clients without a username, the match condition can be `is_empty_val(username)` associated with a attributes result `deny`.

The new authenticator config look is like below.

```
authentication = [
  {
    mechanism = cinfo
    checks = [
      # allow clients with username starts with 'super-'
      {
        is_match = "regex_match(username, '^super-.+$')"
        result = allow
      },
      # deny clients with empty username and client ID starts with 'v1-'
      {
        # when is_match is an array, it yields 'true' if all individual checks yield 'true'
        is_match = ["is_empty_val(username)", "str_eq(nth(1,tokens(clientid,'-')), 'v1')"]
        result = deny
      }
      # if all checks are exhausted without an 'allow' or a 'deny' result, continue to the next authentication
    ]
  },
  # ... more authentications ...
  # ...
  # if all authenticators are exhausted without an 'allow' or a 'deny' result, the client is not rejected
]
```

More match expression examples:

- TLS certificate common name is the same as username: `str_eq(cert_common_name, username)`
- Password is the `sha1` hash of environment variable `EMQXVAR_SECRET` concatenated to client ID: `str_eq(password, hash(sha1, concat([clientid, getenv('SECRET')])))`
- Client attributes `client_attrs.group` is not 'g0': `str_neq(client_attrs.group, 'g0')`
- Client ID starts with zone name: `regex_match(clientid, concat(['^', zone, '.+$']))`

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
