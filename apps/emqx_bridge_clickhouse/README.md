# EMQX ClickHouse Bridge

[ClickHouse](https://github.com/ClickHouse/ClickHouse) is an open-source, column-based
database management system. It is designed for real-time processing of large volumes of
data and is known for its high performance and scalability.

The application is used to connect EMQX and ClickHouse.
User can create a rule and easily ingest IoT data into ClickHouse by leveraging
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).


# Documentation

- Refer to [Ingest data into ClickHouse](https://docs.emqx.com/en/enterprise/v5.0/data-integration/data-bridge-clickhouse.html)
  for how to use EMQX dashboard to ingest IoT data into ClickHouse.

- Refer to [EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for the EMQX rules engine introduction.


# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

- Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges)
  for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
