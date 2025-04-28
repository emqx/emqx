# EMQX Message Transformation

This application encapsulates the functionality to transform incoming or internally
triggered published payloads and take an action upon failure, which can be to just drop
the message without further processing, or to disconnect the offending client as well.

# Documentation

Refer to [Message
Transformation](https://docs.emqx.com/en/enterprise/latest/data-integration/message-transformation.html)
for more information about the semantics.

# HTTP APIs

APIs are provided for transformation management, which includes creating,
updating, looking up, deleting, listing transformations.

Refer to [API Docs -
Bridges](https://docs.emqx.com/en/enterprise/latest/admin/api-docs.html#tag/Message-Transformation)
for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
