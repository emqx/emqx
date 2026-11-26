# emqx_bridge_greptimedb
This application houses the GreptimeDB data integration to EMQX.
It provides the means to connect to GreptimeDB and publish messages to it.

It implements connection management and interaction without the need for a
 separate connector app, since it's not used for authentication and authorization
 applications.

## Docs

For more information about GreptimeDB, please refer to [official
 document](https://docs.greptime.com/).

## Configurations

Just like the InfluxDB data bridge but have some different parameters. Below are several important parameters:
  - `server`: The IPv4 or IPv6 address or the hostname to connect to.
  - `dbname`: The GreptimeDB database name.
  - `write`: Structured table row definitions. Each point defines a template-based measurement (table name), tags, typed fields, and an optional timestamp with its source precision. Multiple point definitions write multiple rows for each input message.
  - `write_syntax`: Legacy line protocol configuration. It provides the measurement, tag set, field set, and timestamp of a point, and supports placeholders. This field is mutually exclusive with `write`.
  - `ttl`: The time-to-live setting for automatically created tables. For more information about TTL, please read [the documentation](https://docs.greptime.com/reference/sql/create#create-a-table-with-ttl).

# Contributing - [Mandatory]
Please see our [contributing.md](../../CONTRIBUTING.md).
