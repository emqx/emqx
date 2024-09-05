## Basic helper functions

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

## Create user

```sh
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_rsa_key.private.pem -nocrypt
openssl rsa -in snowflake_rsa_key.private.pem -pubout -out snowflake_rsa_key.public.pem
```

```elixir
test_user = "testuser"
query.(conn, "create user #{test_user} password = 'TestUser99' must_change_password = false")
# {:updated, :undefined}

public_pem_contents_trimmed = File.read!("snowflake_rsa_key.public.pem") |> String.trim() |> String.split("\n") |> Enum.drop(1) |> Enum.drop(-1) |> Enum.join("\n")

query.(conn, "alter user #{test_user} set rsa_public_key = '#{public_pem_contents_trimmed}'")
# {:updated, :undefined}
```

## Create database objects

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
