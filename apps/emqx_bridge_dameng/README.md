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
| `ssl_path` | Optional directory holding the DM8 client SSL certificate files. Sent as the `SSL_PATH` connection attribute; only needed when the DM8 server has `ENABLE_ENCRYPT` enabled. Ignored unless set |
| `ssl_pwd` | Optional password of the client private key in `ssl_path`. Sent as the `SSL_PWD` connection attribute; only needed when that key is encrypted. Ignored unless set |
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

## TLS

DM8 encrypts client connections at the transport layer when `ENABLE_ENCRYPT` is
enabled in the server `dm.ini`. The TLS handshake is performed by the DM8 ODBC
driver, not by EMQX: the connector only passes the `SSL_PATH` and `SSL_PWD`
connection attributes from the `ssl_path` and `ssl_pwd` fields. The certificate
files therefore have to be present on the EMQX node (mounted into the container
when applicable), and the DM8 `dependencies`/OpenSSL libraries must be loadable
by the driver.

The certificate requirements depend on the server mode:

| `ENABLE_ENCRYPT` | Meaning | `ssl_path` must contain |
|---|---|---|
| `0` | no encryption (default) | nothing; the SSL attributes are ignored |
| `4` | encryption, no certificate verification | nothing |
| `5` | client verifies the server + encryption | `ca-cert.pem` |
| `1` | two-way authentication + encryption | `ca-cert.pem`, `client-cert.pem`, `client-key.pem` |
| `2` | two-way authentication only | `ca-cert.pem`, `client-cert.pem`, `client-key.pem` |

`ssl_pwd` is only needed when `client-key.pem` is encrypted, and a blank value is
omitted from the connection string. Whether the server certificate is verified
is decided by the DM8 server mode and by the files in `ssl_path`; EMQX does not
verify it. The GmSSL (`3`) and TLCP (`6`) modes are not covered. When the server
runs with `ENABLE_ENCRYPT=0` these attributes have no effect, so setting them is
safe even before the server is switched over.

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
