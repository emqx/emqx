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

## Create user

### Shell

```sh
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_rsa_key.private.pem -nocrypt
openssl rsa -in snowflake_rsa_key.private.pem -pubout -out snowflake_rsa_key.public.pem
```

### Elixir

```elixir
test_user = "testuser"
query.(conn, "create user #{test_user} password = 'TestUser99' must_change_password = false")
# {:updated, :undefined}

public_pem_contents_trimmed = File.read!("snowflake_rsa_key.public.pem") |> String.trim() |> String.split("\n") |> Enum.drop(1) |> Enum.drop(-1) |> Enum.join("\n")

query.(conn, "alter user #{test_user} set rsa_public_key = '#{public_pem_contents_trimmed}'")
# {:updated, :undefined}
```

### Erlang

```erlang
TestUser = "testuser".
Query(Conn, ["create user ", TestUser, " password = 'TestUser99' must_change_password = false"]).
# {updated,undefined}

{ok, Bin} = file:read_file("snowflake_rsa_key.public.pem").
Pem = binary_to_list(Bin).
[_ | Lines] = string:split(string:trim(Pem), "\n", all).
PublicPemContentsTrimmed = lists:join("\n", lists:droplast(Lines)).

Query(Conn, ["alter user ", TestUser, " set rsa_public_key = '", PublicPemContentsTrimmed, "'"]).
# {updated,undefined}
```

## Create database objects

### Elixir

```elixir
database = "testdatabase"
schema = "public"
table = "test1"
stage = "teststage0"
pipe = "testpipe0"
warehouse = "testwarehouse"
snowpipe_role = "snowpipe1"
snowpipe_user = "snowpipeuser"
test_role = "testrole"
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

# Create a role for the Snowpipe privileges.
query.(conn, "create or replace role #{snowpipe_role}")
query.(conn, "create or replace role #{test_role}")
# Grant the USAGE privilege on the database and schema that contain the pipe object.
query.(conn, "grant usage on database #{database} to role #{snowpipe_role}")
query.(conn, "grant usage on database #{database} to role #{test_role}")
query.(conn, "grant usage on schema #{database}.#{schema} to role #{snowpipe_role}")
query.(conn, "grant usage on schema #{database}.#{schema} to role #{test_role}")
# Grant the INSERT and SELECT privileges on the target table.
query.(conn, "grant insert, select on #{fqn_table} to role #{snowpipe_role}")
# for cleaning up table after tests
query.(conn, "grant insert, select, truncate, delete on #{fqn_table} to role #{test_role}")
# Grant the USAGE privilege on the external stage.
# must use read/write for internal stage
# query.(conn, "grant usage on stage #{fqn_stage} to role #{snowpipe_role}")
query.(conn, "grant read, write on stage #{fqn_stage} to role #{snowpipe_role}")
# for cleaning up table after tests
query.(conn, "grant read, write on stage #{fqn_stage} to role #{test_role}")
# Grant the OPERATE and MONITOR privileges on the pipe object.
query.(conn, "grant operate, monitor on pipe #{fqn_pipe} to role #{snowpipe_role}")
# Grant the role to a user
query.(conn, "create user if not exists #{snowpipe_user} password = 'TestUser99' must_change_password = false rsa_public_key = '#{public_pem_contents_trimmed}'")

query.(conn, "grant usage on warehouse #{warehouse} to role #{test_role}")

query.(conn, "grant role #{snowpipe_role} to user #{snowpipe_user}")
query.(conn, "grant role #{snowpipe_role} to user #{test_user}")
query.(conn, "grant role #{test_role} to user #{test_user}")
# Set the role as the default role for the user
query.(conn, "alter user #{snowpipe_user} set default_role = #{snowpipe_role}")
query.(conn, "alter user testuser set default_role = #{test_role}")
```

### Erlang

```erlang
Database = "testdatabase",
Schema = "public",
Table = "test1",
Stage = "teststage0",
Pipe = "testpipe0",
Warehouse = "testwarehouse",
SnowpipeRole = "snowpipe1",
SnowpipeUser = "snowpipeuser",
TestRole = "testrole",
FqnTable = [Database, ".", Schema, ".", Table],
FqnStage = [Database, ".", Schema, ".", Stage],
FqnPipe = [Database, ".", Schema, ".", Pipe],

Query(Conn, "use role accountadmin"),

% Create database, table, stage, pipe, warehouse
Query(Conn, ["create database if not exists ", Database]),
Query(Conn, ["create or replace table ", FqnTable, " (clientid string, topic string, payload binary, publish_received_at timestamp_ltz)"]),
Query(Conn, ["create stage if not exists ", FqnStage, " file_format = (type = csv parse_header = true) copy_options = (on_error = continue purge = true)"]),
Query(Conn, ["create pipe if not exists ", FqnPipe, " as copy into ", FqnTable, " from @", FqnStage, " match_by_column_name = case_insensitive"]),
Query(Conn, ["create or replace warehouse ", Warehouse]),

% Create a role for the Snowpipe privileges.
Query(Conn, ["create or replace role ", SnowpipeRole]),
Query(Conn, ["create or replace role ", TestRole]),

% Grant the USAGE privilege on the database and schema that contain the pipe object.
Query(Conn, ["grant usage on database ", Database, " to role ", SnowpipeRole]),
Query(Conn, ["grant usage on database ", Database, " to role ", TestRole]),
Query(Conn, ["grant usage on schema ", Database, ".", Schema, " to role ", SnowpipeRole]),
Query(Conn, ["grant usage on schema ", Database, ".", Schema, " to role ", TestRole]),

% Grant the INSERT and SELECT privileges on the target table.
Query(Conn, ["grant insert, select on ", FqnTable, " to role ", SnowpipeRole]),
% For cleaning up table after tests
Query(Conn, ["grant insert, select, truncate, delete on ", FqnTable, " to role ", TestRole]),

% Grant the USAGE privilege on the external stage.
% Must use read/write for internal stage
% Query(Conn, ["grant usage on stage ", FqnStage, " to role ", SnowpipeRole]),
Query(Conn, ["grant read, write on stage ", FqnStage, " to role ", SnowpipeRole]),
% For cleaning up table after tests
Query(Conn, ["grant read, write on stage ", FqnStage, " to role ", TestRole]),

% Grant the OPERATE and MONITOR privileges on the pipe object.
Query(Conn, ["grant operate, monitor on pipe ", FqnPipe, " to role ", SnowpipeRole]),

% Grant the role to a user
Query(Conn, ["create user if not exists ", SnowpipeUser, " password = 'TestUser99' must_change_password = false rsa_public_key = '", PublicPemContentsTrimmed, "'"]),

Query(Conn, ["grant usage on warehouse ", Warehouse, " to role ", TestRole]),

Query(Conn, ["grant role ", SnowpipeRole, " to user ", SnowpipeUser]),
Query(Conn, ["grant role ", SnowpipeRole, " to user ", TestUser]),
Query(Conn, ["grant role ", TestRole, " to user ", TestUser]),

% Set the role as the default role for the user
Query(Conn, ["alter user ", SnowpipeUser, " set default_role = ", SnowpipeRole]),
Query(Conn, ["alter user testuser set default_role = ", TestRole]).
```
