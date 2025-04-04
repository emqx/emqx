# EMQX HTTP Broker Bridge

This application enables EMQX to connect to any HTTP API, conforming to the
HTTP standard. The connection is established via the [HTTP][1] bridge abstraction,
which facilitates the unidirectional flow of data from EMQX to the HTTP API
(egress).

Users can define a rule and efficiently transfer data to a remote HTTP API
utilizing [EMQX Rules][2].

# Documentation

- For instructions on how to use the EMQX dashboard to set up an egress bridge,
  refer to [Bridge Data into HTTP API][3].

- To understand the EMQX rules engine, please refer to [EMQX Rules][2].

# HTTP APIs

We provide a range of APIs for bridge management. For more detailed
information, refer to [API Docs -Bridges][4].

# Contributing

For those interested in contributing, please consult our
[contributing guide](../../CONTRIBUTING.md).

# Refs

[1]: https://tools.ietf.org/html/rfc2616
[2]: https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html
[3]: https://www.emqx.io/docs/en/v5.0/data-integration/data-bridge-webhook.html
[4]: https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges
