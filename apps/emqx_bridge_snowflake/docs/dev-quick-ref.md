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

## Generate RSA key pair for user accounts

```sh
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_rsa_key.private.pem -nocrypt
openssl rsa -in snowflake_rsa_key.private.pem -pubout -out snowflake_rsa_key.public.pem
```

## SQL setup cheat sheet

```sql
CREATE USER IF NOT EXISTS testuser
    PASSWORD = 'TestUser99'
    MUST_CHANGE_PASSWORD = FALSE;

-- Set the RSA public key for 'testuser'
-- Note: Remove the '-----BEGIN PUBLIC KEY-----' and '-----END PUBLIC KEY-----' lines from your PEM file,
-- and include the remaining content below, preserving line breaks.

ALTER USER testuser SET RSA_PUBLIC_KEY = '
<YOUR_PUBLIC_KEY_CONTENTS_LINE_1>
<YOUR_PUBLIC_KEY_CONTENTS_LINE_2>
<YOUR_PUBLIC_KEY_CONTENTS_LINE_3>
<YOUR_PUBLIC_KEY_CONTENTS_LINE_4>
';


create or replace role testrole;

create warehouse testwarehouse;

CREATE OR REPLACE TABLE testdatabase.public.test0 (
    clientid STRING,
    topic STRING,
    payload BINARY,
    publish_received_at TIMESTAMP_LTZ
);

CREATE STAGE IF NOT EXISTS testdatabase.public.teststage0
FILE_FORMAT = (TYPE = CSV PARSE_HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"')
COPY_OPTIONS = (ON_ERROR = CONTINUE PURGE = TRUE);

CREATE PIPE IF NOT EXISTS testdatabase.public.testpipe0 AS
COPY INTO testdatabase.public.test0
FROM @testdatabase.public.teststage0
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE PIPE IF NOT EXISTS testdatabase.public.emqxstreaming AS
COPY INTO testdatabase.public.emqx FROM (
  SELECT $1:clientid, $1:topic, $1:payload, $1:publish_received_at
  FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


-- Grant the USAGE privilege on the database and schema that contain the pipe object.
grant usage on database testdatabase to role testrole;
grant usage on schema testdatabase.public to role testrole;
-- Grant the USAGE privilege on the warehouse (only needed for test account)
grant usage on warehouse testwarehouse to role testrole;
-- Grant the INSERT, SELECT, TRUNCATE and DELETE privileges on the target table
-- for cleaning up after tests
grant insert, select, truncate, delete on testdatabase.public.test0 to role testrole;
grant insert, select, truncate, delete on testdatabase.public.emqx to role testrole;
-- Grant the READ and WRITE privilege on the internal stage.
grant read, write on stage testdatabase.public.teststage0 to role testrole;
-- Grant the OPERATE and MONITOR privileges on the pipe object.
grant operate, monitor on pipe testdatabase.public.testpipe0 to role testrole;
grant operate, monitor on pipe testdatabase.public.emqxstreaming to role testrole;
-- Grant the role to a user
grant role testrole to user testuser;
-- Set the role as the default role for the user
alter user testuser set default_role = testrole;

-- Create a role for the Snowpipe privileges.
create or replace role snowpipe;
-- Grant the USAGE privilege on the database and schema that contain the pipe object.
grant usage on database testdatabase to role snowpipe;
grant usage on schema testdatabase.public to role snowpipe;
-- Grant the INSERT and SELECT privileges on the target table.
grant insert, select on testdatabase.public.test0 to role snowpipe;
grant insert, select on testdatabase.public.emqx to role snowpipe;
-- Grant the READ and WRITE privilege on the internal stage.
grant read, write on stage testdatabase.public.teststage0 to role snowpipe;
-- Grant the OPERATE and MONITOR privileges on the pipe object.
grant operate, monitor on pipe testdatabase.public.testpipe0 to role snowpipe;
grant operate, monitor on pipe testdatabase.public.emqxstreaming to role snowpipe;
-- Grant the role to a user
grant role snowpipe to user snowpipeuser;
-- Set the role as the default role for the user
alter user snowpipeuser set default_role = snowpipe;

---- OPTIONAL
-- not required, but helps gather JWT failure reasons like skewed time
grant monitor on account to role snowpipe;


-- Create a role for the Snowpipe privileges, but missing write permissions to
-- stage, so health check can happen but staging can't.

CREATE USER IF NOT EXISTS snowpipe_ro_user
    PASSWORD = 'TestUser99'
    MUST_CHANGE_PASSWORD = FALSE;

-- Set the RSA public key for 'testuser'
-- Note: Remove the '-----BEGIN PUBLIC KEY-----' and '-----END PUBLIC KEY-----' lines from your PEM file,
-- and include the remaining content below, preserving line breaks.

ALTER USER snowpipe_ro_user SET RSA_PUBLIC_KEY = '
<YOUR_PUBLIC_KEY_CONTENTS_LINE_1>
<YOUR_PUBLIC_KEY_CONTENTS_LINE_2>
<YOUR_PUBLIC_KEY_CONTENTS_LINE_3>
<YOUR_PUBLIC_KEY_CONTENTS_LINE_4>
';


create or replace role snowpipe_ro;
-- Grant the USAGE privilege on the database and schema that contain the pipe object.
grant usage on database testdatabase to role snowpipe_ro;
grant usage on schema testdatabase.public to role snowpipe_ro;
-- Grant the SELECT privileges on the target table.
grant  select on testdatabase.public.test0 to role snowpipe_ro;
-- Grant the READ privilege on the internal stage.
grant read on stage testdatabase.public.teststage0 to role snowpipe_ro;
-- Grant the MONITOR privileges on the pipe object.
grant monitor on pipe testdatabase.public.testpipe0 to role snowpipe_ro;
-- Grant the role to a user
grant role snowpipe_ro to user snowpipe_ro_user;
-- Set the role as the default role for the user
alter user snowpipe_ro_user set default_role = snowpipe_ro;

---- OPTIONAL
-- not required, but helps gather JWT failure reasons like skewed time
grant monitor on account to role snowpipe_ro;

```

## Basic helper functions

### Elixir

```elixir
Application.ensure_all_started(:odbc)
user = "your_admin_user"
pass = System.fetch_env!("SNOWFLAKE_PASSWORD")
account = "orgid-accountid"
server = "#{account}.snowflakecomputing.com"
dsn = "snowflake"

{:ok, conn} = (
  "dsn=#{dsn};uid=#{user};pwd=#{pass};server=#{server};account=#{account};"
  |> to_charlist()
  |> :odbc.connect([])
)

query = fn conn, sql -> :odbc.sql_query(conn, sql |> to_charlist()) end
```

Or, if you have already set up a connector:

```elixir
conn_res_id = "connector:snowflake:name"
query = fn sql -> conn_res_id |> :ecpool.pick_and_do(fn conn -> :odbc.sql_query(conn, sql |> to_charlist()) end, :handover) end
```

### Erlang

```erlang
application:ensure_all_started(odbc).
User = "your_admin_user".
Pass = os:getenv("SNOWFLAKE_PASSWORD").
OrgID = "orgid".
Account = "accountid".
Server = lists:flatten([OrgID, "-", Account, ".snowflakecomputing.com"]).
DSN = "snowflake".
{ok, Conn} = odbc:connect(["dsn=snowflake;uid=", User, ";pwd=", Pass, ";server=", Server, ";account=", Account], []).
Query = fun(Conn, Sql) -> odbc:sql_query(Conn, Sql) end.
```

Or, if you have already set up a connector:

```Erlang
ConnResId = <<"connector:snowflake:name">>.
Query = fun(Sql) -> ecpool:pick_and_do(ConnResId, fun(Conn) -> odbc:sql_query(Conn, Sql) end, handover) end.
```

## Initialize Database and user accounts

### Elixir

```elixir
database = "testdatabase"
schema = "public"
table = "test0"
stage = "teststage0"
pipe = "testpipe0"
warehouse = "testwarehouse"
fqn_table = "#{database}.#{schema}.#{table}"
fqn_stage = "#{database}.#{schema}.#{stage}"
fqn_pipe = "#{database}.#{schema}.#{pipe}"

query.(conn, "use role accountadmin")

# create database, table, stage, pipe, warehouse
query.(conn, "create database if not exists #{database}")
query.(conn, "create or replace table #{fqn_table} (clientid string, topic string, payload binary, publish_received_at timestamp_ltz)")
query.(conn, "create stage if not exists #{fqn_stage} file_format = (type = csv parse_header = true) copy_options = (on_error = continue purge = true)")
query.(conn, "create pipe if not exists #{fqn_pipe} as copy into #{fqn_table} from @#{fqn_stage} match_by_column_name = case_insensitive")
query.(conn, "create or replace warehouse #{warehouse}")

# read public key contents
public_pem_contents_trimmed = File.read!("snowflake_rsa_key.public.pem") |> String.trim() |> String.split("\n") |> Enum.drop(1) |> Enum.drop(-1) |> Enum.join("\n")

# create user account for running local tests
test_role = "testrole"
test_user = "testuser"

query.(conn, "create user #{test_user} password = 'TestUser99' must_change_password = false rsa_public_key = '#{public_pem_contents_trimmed}'")
# {:updated, :undefined}

# Create a role for the Snowpipe privileges.
query.(conn, "create or replace role #{test_role}")
# Grant the USAGE privilege on the database and schema that contain the pipe object.
query.(conn, "grant usage on database #{database} to role #{test_role}")
query.(conn, "grant usage on schema #{database}.#{schema} to role #{test_role}")
# Grant the USAGE privilege on the warehouse (only needed for test account)
query.(conn, "grant usage on warehouse #{warehouse} to role #{test_role}")
# Grant the INSERT, SELECT, TRUNCATE and DELETE privileges on the target table
# for cleaning up after tests
query.(conn, "grant insert, select, truncate, delete on #{fqn_table} to role #{test_role}")
# Grant the READ and WRITE privilege on the internal stage.
query.(conn, "grant read, write on stage #{fqn_stage} to role #{test_role}")
# Grant the OPERATE and MONITOR privileges on the pipe object.
query.(conn, "grant operate, monitor on pipe #{fqn_pipe} to role #{test_role}")
# Grant the role to a user
query.(conn, "grant role #{test_role} to user #{test_user}")
# Set the role as the default role for the user
query.(conn, "alter user testuser set default_role = #{test_role}")

# create user account for connector and action
snowpipe_role = "snowpipe"
snowpipe_user = "snowpipeuser"

query.(conn, "create user if not exists #{snowpipe_user} password = 'TestUser99' must_change_password = false rsa_public_key = '#{public_pem_contents_trimmed}'")

# Create a role for the Snowpipe privileges.
query.(conn, "create or replace role #{snowpipe_role}")
# Grant the USAGE privilege on the database and schema that contain the pipe object.
query.(conn, "grant usage on database #{database} to role #{snowpipe_role}")
query.(conn, "grant usage on schema #{database}.#{schema} to role #{snowpipe_role}")
# Grant the INSERT and SELECT privileges on the target table.
query.(conn, "grant insert, select on #{fqn_table} to role #{snowpipe_role}")
# Grant the READ and WRITE privilege on the internal stage.
query.(conn, "grant read, write on stage #{fqn_stage} to role #{snowpipe_role}")
# Grant the OPERATE and MONITOR privileges on the pipe object.
query.(conn, "grant operate, monitor on pipe #{fqn_pipe} to role #{snowpipe_role}")
# Grant the role to a user
query.(conn, "grant role #{snowpipe_role} to user #{snowpipe_user}")
# Set the role as the default role for the user
query.(conn, "alter user #{snowpipe_user} set default_role = #{snowpipe_role}")

## OPTIONAL
# not required, but helps gather JWT failure reasons like skewed time
query.(conn, "grant monitor on account to role #{snowpipe_role}")
```

### Erlang

```erlang
Database = "testdatabase".
Schema = "public".
Table = "test0".
Stage = "teststage0".
Pipe = "testpipe0".
Warehouse = "testwarehouse".
TestRole = "testrole".
FqnTable = [Database, ".", Schema, ".", Table].
FqnStage = [Database, ".", Schema, ".", Stage].
FqnPipe = [Database, ".", Schema, ".", Pipe].

Query(Conn, "use role accountadmin").

% Create database, table, stage, pipe, warehouse
Query(Conn, ["create database if not exists ", Database]).
Query(Conn, ["create or replace table ", FqnTable, " (clientid string, topic string, payload binary, publish_received_at timestamp_ltz)"]).
Query(Conn, ["create stage if not exists ", FqnStage, " file_format = (type = csv parse_header = true) copy_options = (on_error = continue purge = true)"]).
Query(Conn, ["create pipe if not exists ", FqnPipe, " as copy into ", FqnTable, " from @", FqnStage, " match_by_column_name = case_insensitive"]).
Query(Conn, ["create or replace warehouse ", Warehouse]).

% Read public key contents
{ok, Bin} = file:read_file("snowflake_rsa_key.public.pem").
Pem = binary_to_list(Bin).
[_ | Lines] = string:split(string:trim(Pem), "\n", all).
PublicPemContentsTrimmed = lists:join("\n", lists:droplast(Lines)).

% Create user account for running local tests
TestUser = "testuser".
Query(Conn, ["create user ", TestUser, " password = 'TestUser99' must_change_password = false rsa_public_key = '", PublicPemContentsTrimmed, "'"]).
% {updated,undefined}

% Create a role for the Snowpipe privileges.
Query(Conn, ["create or replace role ", TestRole]).

% Grant the USAGE privilege on the database and schema that contain the pipe object.
Query(Conn, ["grant usage on database ", Database, " to role ", TestRole]).
Query(Conn, ["grant usage on schema ", Database, ".", Schema, " to role ", TestRole]).
% Grant the USAGE privilege on the warehouse (only needed for test account)
Query(Conn, ["grant usage on warehouse ", Warehouse, " to role ", TestRole]).

% Grant the INSERT, SELECT, TRUNCATE and DELETE privileges on the target table
% for cleaning up after tests
Query(Conn, ["grant insert, select, truncate, delete on ", FqnTable, " to role ", TestRole]).

% Grant the READ and WRITE privilege on the internal stage
Query(Conn, ["grant read, write on stage ", FqnStage, " to role ", TestRole]).

% Grant the OPERATE and MONITOR privileges on the pipe object.
Query(Conn, ["grant operate, monitor on pipe ", FqnPipe, " to role ", TestRole]).

% Grant the role to a user
Query(Conn, ["grant role ", TestRole, " to user ", TestUser]).

% Set the role as the default role for the user
Query(Conn, ["alter user testuser set default_role = ", TestRole]).

% Create user account for connector and action
SnowpipeRole = "snowpipe".
SnowpipeUser = "snowpipeuser".

Query(Conn, ["create user if not exists ", SnowpipeUser, " password = 'TestUser99' must_change_password = false rsa_public_key = '", PublicPemContentsTrimmed, "'"]).

% Create a role for the Snowpipe privileges.
Query(Conn, ["create or replace role ", SnowpipeRole]).

% Grant the USAGE privilege on the database and schema that contain the pipe object.
Query(Conn, ["grant usage on database ", Database, " to role ", SnowpipeRole]).
Query(Conn, ["grant usage on schema ", Database, ".", Schema, " to role ", SnowpipeRole]).

% Grant the INSERT and SELECT privileges on the target table.
Query(Conn, ["grant insert, select on ", FqnTable, " to role ", SnowpipeRole]).

% Grant the READ and WRITE privilege on the internal stage.
Query(Conn, ["grant read, write on stage ", FqnStage, " to role ", SnowpipeRole]).

% Grant the OPERATE and MONITOR privileges on the pipe object.
Query(Conn, ["grant operate, monitor on pipe ", FqnPipe, " to role ", SnowpipeRole]).

% Grant the role to a user
Query(Conn, ["grant role ", SnowpipeRole, " to user ", SnowpipeUser]).

% Set the role as the default role for the user
Query(Conn, ["alter user ", SnowpipeUser, " set default_role = ", SnowpipeRole]).

%%  OPTIONAL
% not required, but helps gather JWT failure reasons like skewed time
Query(Conn, ["grant monitor on account to role ", SnowpipeRole]).
```
