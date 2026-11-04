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

SQL Server actions support UPDATE templates when `resource_opts.batch_size` is `1`.
The default batch size is `100`, so set this option explicitly for an UPDATE action.

```sql
UPDATE dbo.device_points
SET status = ${payload.status}
WHERE device_id = ${payload.device_id} AND point_id = ${payload.point_id}
```

Placeholders represent values. The action quotes and escapes them. A bare placeholder such as
`${payload.status}` preserves valid UTF-8 Unicode text, including Emoji, by using an NVARCHAR value.
ASCII values keep the existing VARCHAR representation. Use `N'prefix ${payload.status}'` for an
explicit Unicode string template. The destination column must support the characters, for example
an NVARCHAR column. A quoted string template follows SQL string semantics: a missing value inside
`N'${payload.status}'` becomes text such as `N'null'`, while a bare missing value can become SQL NULL.
Table and column names must be static.

Match the template's string type to indexed key columns. Comparing an NVARCHAR value with a
VARCHAR key can cause an implicit column conversion and an index scan under some collations.
Prefer NVARCHAR columns for Unicode keys. For a VARCHAR column whose code page represents all
intended key values, use an explicit VARCHAR template such as `WHERE device_id = '${payload.device_id}'`.
VARCHAR conversion can replace unrepresentable characters, so it must not be used to narrow
arbitrary Unicode identity values.

Unicode and backslash/line-ending values use UTF-16 hexadecimal expressions with bounded string
types when they fit. VARCHAR conversion follows the configured database's code page. Ordinary
identifiers keep the statement on the direct execution path regardless of the value's characters.
Both execution paths read all query results, including errors after trigger results.
Static string text in templates follows SQL Server line-continuation semantics, including when
retaining an older compiled action plan during code loading.

Configured Unicode table, column or function names still require `sys.sp_executesql`. This path
has a different `@@NESTLEVEL`, independent of the value's characters. For trigger recursion guards,
use `TRIGGER_NESTLEVEL()` to count trigger invocations rather than all procedure invocations.
String values must be valid UTF-8. Invalid UTF-8 returns a non-retryable rendering error;
if any row in an INSERT batch contains an invalid string, the entire batch fails without
being sent to the database. For binary data, use a bare placeholder with a `0x...` hexadecimal value;
all characters after the lowercase prefix must be hexadecimal digits. Use a quoted template to
store hexadecimal-looking values as text. Printable integer lists follow the template engine's
character-list convention; use `json_encode` in the rule when an array should be stored as JSON.
The template supports SET expressions and an optional WHERE condition. It does not support
UPDATE with JOIN, FROM, OUTPUT, or a common table expression.
INSERT templates continue to support batching.
For heterogeneous numeric and text values in one destination column, explicitly convert each
value in the template, for example `CONVERT(NVARCHAR(MAX), ${payload.value})`. SQL Server otherwise
infers one type for that column across the VALUES rows and can reject a batch that succeeds as
individual inserts.

UPDATE returns success when no row matches the WHERE condition. Without WHERE, the statement
updates every row in the table. Missing variables follow the `undefined_vars_as_null` setting;
use `IS NULL` to test SQL NULL values.

The action can retry a request after a connection failure. Use assignments such as
`SET status = ${payload.status}` when retries must have the same result. An expression such as
`SET count = count + 1` can run more than once if the connection fails after the database
commits the update. Concurrent action workers or connections can apply messages out of order;
use a timestamp or sequence condition in WHERE when the row must keep the newest device state.

# HTTP APIs

- Several APIs are provided for bridge management, which includes create bridge,
  update bridge, get bridge, stop or restart bridge and list bridges etc.

  Refer to [API Docs - Bridges](https://docs.emqx.com/en/enterprise/v5.0/admin/api-docs.html#tag/Bridges) for more detailed information.


# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
