# EMQX InfluxDB Bridge

[InfluxDB](https://github.com/influxdata/influxdb) is an open-source time-series
database that is optimized for storing, retrieving, and querying large volumes of
time-stamped data.
It is commonly used for monitoring and analysis of metrics, events, and real-time
analytics.
InfluxDB is designed to be fast, efficient, and scalable, and it has a SQL-like
query language that makes it easy to extract insights from time-series data.

The application is used to connect EMQX and InfluxDB. User can create a rule and
easily ingest IoT data into InfluxDB by leveraging
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).


# Documentation

- Refer to [Ingest Data into InfluxDB](https://docs.emqx.com/en/enterprise/v5.0/data-integration/data-bridge-influxdb.html)
  for how to use EMQX dashboard to ingest IoT data into InfluxDB.

- Refer to [EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html)
  for the EMQX rules engine introduction.


# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges) for more detailed information.

- [Create bridge API doc](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges/paths/~1bridges/post)
  list required parameters for creating a InfluxDB bridge.
  There are two types of InfluxDB API (`v1` and `v2`), please select the right
  version of InfluxDB. Below are several important parameters for `v1`,
  - `server`: The IPv4 or IPv6 address or the hostname to connect to.
  - `database`: InfluxDB database name
  - `write_syntax`: Conf of InfluxDB line protocol to write data points. It is a text-based format that provides the measurement, tag set, field set, and timestamp of a data point, and placeholder supported.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
