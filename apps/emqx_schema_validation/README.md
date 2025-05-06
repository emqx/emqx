# EMQX Schema Validation

This application encapsulates the functionality to validate incoming or internally
triggered published payloads and take an action upon failure, which can be to just drop
the message without further processing, or to disconnect the offending client as well.

# Documentation

Refer to [Schema
Validation](https://docs.emqx.com/en/enterprise/latest/data-integration/schema-validation.html)
for more information about the semantics and checks available.

# HTTP APIs

APIs are provided for validation management, which includes creating,
updating, looking up, deleting, listing validations.

Refer to [API Docs -
Bridges](https://docs.emqx.com/en/enterprise/latest/admin/api-docs.html#tag/Schema-Validation)
for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
