%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pgsql).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_postgresql/include/emqx_postgresql.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).
%% for sharing with other actions
-export([fields/3]).

%% Examples
-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1
]).

%% Exported for timescale and matrix bridges
-export([
    values/1,
    values_conn_bridge_examples/2
]).

-define(ACTION_TYPE, pgsql).

%% Hocon Schema Definitions
namespace() -> "bridge_pgsql".

roots() ->
    [].

fields("config_connector") ->
    emqx_postgresql_connector_schema:fields("config_connector");
fields(config) ->
    fields("config_connector") ++
        fields(action);
fields(action) ->
    {pgsql,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_pgsql, pgsql_action)),
            #{
                desc => <<"PostgreSQL Action Config">>,
                required => false
            }
        )};
fields(action_parameters) ->
    [
        {sql,
            hoconsc:mk(
                emqx_schema:template(),
                #{desc => ?DESC("sql_template"), default => default_sql(), format => <<"sql">>}
            )}
    ];
fields(pgsql_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("action_parameters")
            }
        )
    );
fields("put_bridge_v2") ->
    fields(pgsql_action);
fields("get_bridge_v2") ->
    fields(pgsql_action);
fields("post_bridge_v2") ->
    fields("post", pgsql, pgsql_action);
fields("config") ->
    %% Bridge v1 config for all postgres-based bridges (pgsql, matrix, timescale)
    [
        {enable, hoconsc:mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {sql,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("sql_template"), default => default_sql(), format => <<"sql">>}
            )},
        {local_topic,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("local_topic"), default => undefined}
            )}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        proplists:delete(
            disable_prepared_statements,
            emqx_postgresql:fields(config) --
                emqx_connector_schema_lib:prepare_statement_fields()
        );
fields("post") ->
    fields("post", ?ACTION_TYPE, "config");
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

fields("post", Type, StructName) ->
    [type_field(Type), name_field() | fields(StructName)].

type_field(Type) ->
    {type, hoconsc:mk(hoconsc:enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, hoconsc:mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for PostgreSQL using `", string:to_upper(Method), "` method."];
desc(pgsql_action) ->
    ?DESC("pgsql_action");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc("config_connector") ->
    ?DESC(emqx_postgresql_connector_schema, "config_connector");
desc(_) ->
    undefined.

default_sql() ->
    <<
        "insert into t_mqtt_msg(msgid, topic, qos, payload, arrived) "
        "values (${id}, ${topic}, ${qos}, ${payload}, TO_TIMESTAMP((${timestamp} :: bigint)/1000))"
    >>.

%% Examples

bridge_v2_examples(Method) ->
    [
        #{
            <<"pgsql">> => #{
                summary => <<"PostgreSQL Action">>,
                value => values({Method, pgsql})
            }
        }
    ].

conn_bridge_examples(Method) ->
    [
        #{
            <<"pgsql">> => #{
                summary => <<"PostgreSQL Bridge">>,
                value => values_conn_bridge_examples(Method, pgsql)
            }
        }
    ].

values({get, PostgreSQLType}) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        },
        values({put, PostgreSQLType})
    );
values({post, PostgreSQLType}) ->
    values({put, PostgreSQLType});
values({put, PostgreSQLType}) ->
    maps:merge(
        #{
            name => <<"my_action">>,
            type => PostgreSQLType,
            enable => true,
            connector => <<"my_connector">>,
            resource_opts => #{
                batch_size => 1,
                batch_time => <<"50ms">>,
                inflight_window => 100,
                max_buffer_bytes => <<"256MB">>,
                request_ttl => <<"45s">>,
                worker_pool_size => 16
            }
        },
        values(parameters)
    );
values(parameters) ->
    #{
        <<"parameters">> => #{
            <<"sql">> =>
                <<
                    "INSERT INTO client_events(clientid, event, created_at)"
                    "VALUES (\n"
                    "  ${clientid},\n"
                    "  ${event},\n"
                    "  TO_TIMESTAMP((${timestamp} :: bigint))\n"
                    ")"
                >>
        }
    }.

values_conn_bridge_examples(get, Type) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        },
        values_conn_bridge_examples(post, Type)
    );
values_conn_bridge_examples(_Method, Type) ->
    #{
        enable => true,
        type => Type,
        name => <<"foo">>,
        server => <<"127.0.0.1:5432">>,
        database => <<"mqtt">>,
        pool_size => 8,
        username => <<"root">>,
        password => <<"******">>,
        sql => default_sql(),
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 8,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => async,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.
