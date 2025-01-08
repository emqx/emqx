%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_clickhouse).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

%% Examples
-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(DEFAULT_SQL, <<"INSERT INTO messages(data, arrived) VALUES ('${payload}', ${timestamp})">>).
-define(DEFAULT_BATCH_VALUE_SEPARATOR, <<", ">>).
-define(CONNECTOR_TYPE, clickhouse).
-define(ACTION_TYPE, clickhouse).
%% -------------------------------------------------------------------------------------------------
%% Callback used by HTTP API
%% -------------------------------------------------------------------------------------------------

conn_bridge_examples(Method) ->
    [
        #{
            <<"clickhouse">> => #{
                summary => <<"Clickhouse Bridge">>,
                value => values(Method, "clickhouse")
            }
        }
    ].

bridge_v2_examples(Method) ->
    ParamsExample = #{
        parameters => #{
            batch_value_separator => ?DEFAULT_BATCH_VALUE_SEPARATOR,
            sql => ?DEFAULT_SQL
        }
    },
    [
        #{
            <<"clickhouse">> => #{
                summary => <<"ClickHouse Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method, clickhouse, clickhouse, ParamsExample
                )
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"clickhouse">> => #{
                summary => <<"ClickHouse Connector">>,
                value => emqx_connector_schema:connector_values(
                    Method, clickhouse, #{
                        url => <<"http://localhost:8123">>,
                        database => <<"mqtt">>,
                        pool_size => 8,
                        username => <<"default">>,
                        password => <<"******">>
                    }
                )
            }
        }
    ].

values(_Method, Type) ->
    #{
        enable => true,
        type => Type,
        name => <<"foo">>,
        url => <<"http://127.0.0.1:8123">>,
        database => <<"mqtt">>,
        pool_size => 8,
        username => <<"default">>,
        password => <<"******">>,
        sql => ?DEFAULT_SQL,
        batch_value_separator => ?DEFAULT_BATCH_VALUE_SEPARATOR,
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

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
%% -------------------------------------------------------------------------------------------------

namespace() -> "bridge_clickhouse".

roots() -> [].

fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        emqx_bridge_clickhouse_connector:fields(config) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(action) ->
    {clickhouse,
        mk(
            hoconsc:map(name, ref(?MODULE, clickhouse_action)),
            #{desc => <<"ClickHouse Action Config">>, required => false}
        )};
fields(clickhouse_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(ref(?MODULE, action_parameters), #{
            required => true, desc => ?DESC(action_parameters)
        })
    );
fields(action_parameters) ->
    [
        sql_field(),
        emqx_bridge_v2_schema:undefined_as_null_field(),
        batch_value_separator_field()
    ];
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        emqx_bridge_clickhouse_connector:fields(config) ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(clickhouse_action));
fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        sql_field(),
        batch_value_separator_field(),
        {local_topic,
            mk(
                binary(),
                #{desc => ?DESC("local_topic"), default => undefined}
            )},
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                }
            )}
    ] ++
        emqx_bridge_clickhouse_connector:fields(config);
fields("creation_opts") ->
    emqx_resource_schema:fields("creation_opts");
fields("post") ->
    fields("post", clickhouse);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

sql_field() ->
    {sql,
        mk(
            emqx_schema:template(),
            #{
                desc => ?DESC("sql_template"),
                default => ?DEFAULT_SQL,
                format => <<"sql">>
            }
        )}.

batch_value_separator_field() ->
    {batch_value_separator,
        mk(
            binary(),
            #{desc => ?DESC("batch_value_separator"), default => ?DEFAULT_BATCH_VALUE_SEPARATOR}
        )}.

desc(clickhouse_action) ->
    ?DESC(clickhouse_action);
desc(action_parameters) ->
    ?DESC(action_parameters);
desc("config_connector") ->
    ?DESC("desc_config");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Clickhouse using `", string:to_upper(Method), "` method."];
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------
%% internal
%% -------------------------------------------------------------------------------------------------

type_field(Type) ->
    {type, mk(enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
