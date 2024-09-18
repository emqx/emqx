## Initialize Snowflake ODBC driver

### Linux

Run `scripts/install-snowflake-driver.sh` to install the Snowflake ODBC driver and configure `odbc.ini`.

### macOS

- Install unixODBC (e.g. `brew install unixodbc`)
- [Download and install iODBC](https://github.com/openlink/iODBC/releases/download/v3.52.16/iODBC-SDK-3.52.16-macOS11.dmg)
- [Download and install the Snowflake ODBC driver](https://sfc-repo.snowflakecomputing.com/odbc/macuniversal/3.3.2/snowflake_odbc_mac_64universal-3.3.2.dmg)
- Refer to [Installing and configuring the ODBC Driver for macOS](https://docs.snowflake.com/en/developer-guide/odbc/odbc-mac) for more information.
- Update `~/.odbc.ini` and `/opt/snowflake/snowflakeodbc/lib/universal/simba.snowflake.ini`:

```sh
chown $(id -u):$(id -g) /opt/snowflake/snowflakeodbc/lib/universal/simba.snowflake.ini
echo 'ODBCInstLib=libiodbcinst.dylib' >> /opt/snowflake/snowflakeodbc/lib/universal/simba.snowflake.ini

cat < EOF > ~/.odbc.ini
[ODBC]
Trace=no
TraceFile=

[ODBC Drivers]
Snowflake = Installed

[ODBC Data Sources]
snowflake = Snowflake

[Snowflake]
Driver = /opt/snowflake/snowflakeodbc/lib/universal/libSnowflake.dylib
EOF
```

## Create user account and database

After completing the steps below, the following credentials shall be used in connector in action:

| Field                  | Value                                            |
|------------------------|--------------------------------------------------|
| Data Source Name(DSN)  | `snowflake`                                      |
| Username               | `snowpipeuser`                                   |
| Password               | `Snowpipeuser99`                                 |
| Database Name          | `testdatabase`                                   |
| Schema                 | `public`                                         |
| Stage                  | `emqx`                                           |
| Pipe                   | `emqx`                                           |
| Pipe User              | `snowpipeuser`                                   |
| Private Key            | `file://<path to snowflake_rsa_key.private.pem>` |

### Generate RSA key pair

```sh
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_rsa_key.private.pem -nocrypt
openssl rsa -in snowflake_rsa_key.private.pem -pubout -out snowflake_rsa_key.public.pem
```

### Snowflake SQL Worksheet (+ Create --> SQL Worksheet)
    
```sql
USE ROLE accountadmin;

CREATE DATABASE IF NOT EXISTS testdatabase;

CREATE OR REPLACE TABLE testdatabase.public.emqx (
    clientid STRING,
    topic STRING,
    payload STRING,
    publish_received_at TIMESTAMP_LTZ
);

CREATE STAGE IF NOT EXISTS testdatabase.public.emqx
FILE_FORMAT = (TYPE = CSV PARSE_HEADER = TRUE)
COPY_OPTIONS = (ON_ERROR = CONTINUE PURGE = TRUE);

CREATE PIPE IF NOT EXISTS testdatabase.public.emqx AS
COPY INTO testdatabase.public.emqx
FROM @testdatabase.public.emqx
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE USER IF NOT EXISTS snowpipeuser
    PASSWORD = 'Snowpipeuser99'
    MUST_CHANGE_PASSWORD = FALSE;

-- Set the RSA public key for 'snowpipeuser'
-- Note: Remove the '-----BEGIN PUBLIC KEY-----' and '-----END PUBLIC KEY-----' lines from your PEM file,
-- and include the remaining content below, preserving line breaks.

ALTER USER snowpipeuser SET RSA_PUBLIC_KEY = '
<YOUR_PUBLIC_KEY_CONTENTS_LINE_1>
<YOUR_PUBLIC_KEY_CONTENTS_LINE_2>
<YOUR_PUBLIC_KEY_CONTENTS_LINE_3>
<YOUR_PUBLIC_KEY_CONTENTS_LINE_4>
';

CREATE OR REPLACE ROLE snowpipe;

GRANT USAGE ON DATABASE testdatabase TO ROLE snowpipe;
GRANT USAGE ON SCHEMA testdatabase.public TO ROLE snowpipe;
GRANT INSERT, SELECT ON testdatabase.public.emqx TO ROLE snowpipe;
GRANT READ, WRITE ON STAGE testdatabase.public.emqx TO ROLE snowpipe;
GRANT OPERATE, MONITOR ON PIPE testdatabase.public.emqx TO ROLE snowpipe;
GRANT ROLE snowpipe TO USER snowpipeuser;
ALTER USER snowpipeuser SET DEFAULT_ROLE = snowpipe;
```

## Rule SQL

```
SELECT
  clientid,
  unix_ts_to_rfc3339(publish_received_at, 'millisecond') as publish_received_at,
  topic,
  payload
FROM
  "t/#"
```
