# EMQX TimescaleDB Bridge

[TimescaleDB](https://github.com/timescaleDB/timescaleDB) is an open-source database
designed to make SQL scalable for time-series data.
It is engineered up from PostgreSQL and packaged as a PostgreSQL extension,
providing automatic partitioning across time and space (partitioning key), as well as full SQL support.

The application is used to connect EMQX and TimescaleDB.
User can create a rule and easily ingest IoT data into TimescaleDB by leveraging
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).

<!---

# Documentation

- Refer to [Ingest data into TimescaleDB](https://docs.emqx.com/en/enterprise/v5.0/data-integration/data-bridge-timescaledb.html)
  for how to use EMQX dashboard to ingest IoT data into TimescaleDB.

- Refer to [EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for the EMQX rules engine introduction.

--->

# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges)
  for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
