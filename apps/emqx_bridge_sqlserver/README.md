# EMQX SQL Server Bridge

[Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server) is a relational database management system (RDBMS) that is developed and owned by Microsoft.
Microsoft SQL Server offers a wide range of features, including support for high availability and disaster recovery,
integration with other Microsoft products and services, and advanced security and encryption options.
It also provides tools for data warehousing, business intelligence, and analytics, making it a versatile and powerful database platform.

The application is used to connect EMQX and Microsoft SQL Server.
User can create a rule and easily ingest IoT data into Microsoft SQL Server by leveraging
[EMQX Rules](https://docs.emqx.com/en/enterprise/v5.0/data-integration/rules.html).


## Documentation links

For more information about Microsoft SQL Server, please see the [official site](https://learn.microsoft.com/en-us/sql/sql-server/?view=sql-server-ver16)

# Configurations

Please see [Ingest Data into SQL Server](https://www.emqx.io/docs/en/v5.0/data-integration/data-bridge-sqlserver.html) for more detailed information.

## UPDATE templates

Set `resource_opts.batch_size = 1` to use UPDATE; the default is `100`.
INSERT continues to support batching.

```sql
UPDATE dbo.device_points
SET status = ${payload.status}
WHERE device_id = ${payload.device_id} AND point_id = ${payload.point_id}
```

Placeholders represent values; table and column names must be static.
UPDATE supports SET expressions and an optional WHERE condition, but not JOIN, FROM, OUTPUT or CTEs.
Without WHERE, UPDATE affects every row. No matching row still returns success.

Missing variables follow `undefined_vars_as_null`; use `IS NULL` to test SQL NULL.

Connection retries can repeat an update, so prefer assignments over increments.
Concurrent workers can apply messages out of order; use a timestamp or sequence condition in WHERE
when the row must retain the newest state.

# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges) for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
