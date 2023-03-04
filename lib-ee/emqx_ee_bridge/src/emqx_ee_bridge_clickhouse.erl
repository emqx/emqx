%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_clickhouse).

-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    conn_bridge_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(DEFAULT_SQL,
    <<"INSERT INTO mqtt_test(payload, arrived) VALUES ('${payload}', ${timestamp})">>
).

-define(DEFAULT_BATCH_VALUE_SEPARATOR, <<", ">>).

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

values(get, Type) ->
    maps:merge(values(post, Type), ?METRICS_EXAMPLE);
values(post, Type) ->
    #{
        enable => true,
        type => Type,
        name => <<"foo">>,
        server => <<"127.0.0.1:8123">>,
        database => <<"mqtt">>,
        pool_size => 8,
        username => <<"default">>,
        password => <<"public">>,
        sql => ?DEFAULT_SQL,
        batch_value_separator => ?DEFAULT_BATCH_VALUE_SEPARATOR,
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 8,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            auto_restart_interval => ?AUTO_RESTART_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => async,
            max_queue_bytes => ?DEFAULT_QUEUE_SIZE
        }
    };
values(put, Type) ->
    values(post, Type).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
%% -------------------------------------------------------------------------------------------------

namespace() -> "bridge_clickhouse".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {sql,
            mk(
                binary(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        {batch_value_separator,
            mk(
                binary(),
                #{desc => ?DESC("batch_value_separator"), default => ?DEFAULT_BATCH_VALUE_SEPARATOR}
            )},
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
        emqx_ee_connector_clickhouse:fields(config);
fields("creation_opts") ->
    Opts = emqx_resource_schema:fields("creation_opts"),
    [O || {Field, _} = O <- Opts, not is_hidden_opts(Field)];
fields("post") ->
    fields("post", clickhouse);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

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
is_hidden_opts(Field) ->
    lists:member(Field, [
        async_inflight_window
    ]).

type_field(Type) ->
    {type, mk(enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
