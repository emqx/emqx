# EMQX Bridge Dameng

Bridge/connector for connecting EMQX to the Dameng DM8 database via ODBC.

This app implements an `emqx_connector` / `emqx_action` of type `dameng` and
reuses the shared low-level ODBC wrapper in `emqx_odbc`.

## Configuration

Connector parameters (see `emqx_bridge_dameng:fields("config_connector")`):

| Field | Description |
|------|-------------|
| `server` | DM host (`host`, `host:port` or an IPv6 address). Required unless `dsn` is set |
| `port` | DM port, defaults to `5236` (the DM default). It is only used when `server` does not contain a port, and is ignored when `dsn` is set |
| `username` | DM user. Defaults to `SYSDBA` when no `dsn` is set; with a `dsn`, leave it empty to use the user from the DSN entry |
| `password` | DM password. With a `dsn`, leave it empty to use the password from the DSN entry |
| `driver` | ODBC driver name (e.g. `DM8 ODBC DRIVER`) or absolute path to the driver library. Ignored when `dsn` is set |
| `dsn` | Optional DSN for DSN-based connection; when set, `server`, `port`, `driver` and `charset` are ignored |
| `charset` | Optional `Charset` connection attribute. Ignored when `dsn` is set |
| `pool_size` | Connection pool size, defaults to `8` |

There is intentionally no `database` field: DM8 locates the target instance by
`host`/`port`, and its ODBC driver ignores the `Database` connection attribute.

Action parameters:

| Field | Description |
|------|-------------|
| `sql` | SQL template. `INSERT` statements must list the target columns explicitly |
| `undefined_vars_as_null` | Write `null` for variables missing from the message instead of failing the request, defaults to `false` |

## Supported column types

`INSERT` column types are resolved with `describe` when the action is created
and rows are written with `odbc:param_query`. Character, numeric, boolean and
timestamp columns are supported, including the types that `odbc:param_query`
has no native binding for (`BIGINT`, `DATE`, `TIME`), which are bound as their
string representation and coerced by the DM8 driver.

Values are validated before they are bound:

* a value that does not fit the declared column size is rejected with
  `{invalid_value, {value_too_long, ...}}` instead of being silently truncated;
* a character value containing a NUL byte is rejected (`{invalid_value,
  {nul_byte_in_string, ...}}`), because `odbc:param_query` binds character
  parameters as NUL terminated strings.

Binary (`BINARY`, `VARBINARY`, `LONGVARBINARY`), large object
(`LONGVARCHAR`, `WLONGVARCHAR`/NCLOB) and interval columns are rejected when the
action is created: `odbc:param_query` cannot bind them without corrupting the
data. Store such values as an escaped string instead.

## Note on ODBC driver configuration

The connection is established through the OTP `odbc` application whose
`odbcserver` port program reads the ODBC configuration from `/etc/`. For the
`DSN=`/registered driver name forms to work, make sure `odbc.ini` and
`odbcinst.ini` are placed in or symlinked to `/etc/`. Otherwise, use the
absolute driver path for the `driver` field.

## Testing

`test/emqx_bridge_dameng_SUITE.erl` runs against a real DM8 instance; it is
skipped unless the ODBC driver and a DM8 server are reachable, and it is
executed in CI through the docker testbed defined by
`.ci/docker-compose-file/docker-compose-dameng.yaml` (see
`apps/emqx_bridge_dameng/docker-ct`). The testbed runs the
`xuxuclassmate/dameng` image, whose DM8 instance listens on the DM default port
`5236` with the credentials `SYSDBA`/`SYSDBA001`; `DM_HOST`, `DM_PORT`,
`DM_USER` and `DM_PASSWORD` can be used to point the suite at another instance.
The DM8 client libraries are copied from that image into the emqx container by
`scripts/ct/run.sh`, because the vendor download is not reachable from
GitHub-hosted runners.
