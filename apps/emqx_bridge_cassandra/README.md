# EMQX Cassandra Bridge

[Apache Cassandra](https://github.com/apache/cassandra) is an open-source, distributed
NoSQL database management system that is designed to manage large amounts of structured
and semi-structured data across many commodity servers, providing high availability
with no single point of failure.
It is commonly used in web and mobile applications, IoT, and other systems that
require storing, querying, and analyzing large amounts of data.

The application is used to connect EMQX and Cassandra. User can create a rule
and easily ingest IoT data into Cassandra by leveraging
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).


# Documentation

- Refer to [Ingest data into Cassandra](https://docs.emqx.com/en/enterprise/v5.0/data-integration/data-bridge-cassa.html)
  for how to use EMQX dashboard to ingest IoT data into Cassandra.
- Refer to [EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for the EMQX rules engine introduction.


# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges) for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
