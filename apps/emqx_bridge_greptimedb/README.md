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
  - `write_syntax`: Like the `write_syntax` in `InfluxDB` conf, it's the conf of InfluxDB line protocol to write data points. It is a text-based format that provides the measurement, tag set, field set, and timestamp of a data point, and placeholder supported.


# Contributing - [Mandatory]
Please see our [contributing.md](../../CONTRIBUTING.md).
