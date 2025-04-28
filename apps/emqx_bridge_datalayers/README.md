# EMQX Datalayers Bridge

[Datalayers](https://docs.datalayers.cn/datalayers/latest/) is a multimodal,
hyper-converged database for the Industrial IoT, Telematics, Energy and other industries.

Datalayers is designed to be fast, efficient, and scalable, and it has a SQL-like
query language that makes it easy to extract insights from time-series data.

The application is used to connect EMQX and Datalayers. User can create a rule and
easily ingest IoT data into Datalayers by leveraging
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).


# Documentation

- Refer to [Ingest Data into Datalayers](https://docs.emqx.com/en/enterprise/v5.0/data-integration/data-bridge-datalayers.html)
  for how to use EMQX dashboard to ingest IoT data into Datalayers.

- Refer to [EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for the EMQX rules engine introduction.


# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges) for more detailed information.

- [Create bridge API doc](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges/paths/~1bridges/post)
  list required parameters for creating a Datalayers bridge.
  - `server`: The IPv4 or IPv6 address or the hostname to connect to.
  - `database`: Datalayers database name
  - `write_syntax`: Conf of Datalayers line protocol to write data points. It is a text-based format that provides the measurement, tag set, field set, and timestamp of a data point, and placeholder supported.

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
