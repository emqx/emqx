%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_tdengine).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    conn_bridge_examples/1,
    values/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(DEFAULT_SQL, <<
    "insert into t_mqtt_msg(ts, msgid, mqtt_topic, qos, payload, arrived) "
    "values (${ts}, ${id}, ${topic}, ${qos}, ${payload}, ${timestamp})"
>>).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"tdengine">> => #{
                summary => <<"TDengine Bridge">>,
                value => values(Method)
            }
        }
    ].

values(_Method) ->
    #{
        enable => true,
        type => tdengine,
        name => <<"foo">>,
        server => <<"127.0.0.1:6041">>,
        database => <<"mqtt">>,
        pool_size => 8,
        username => <<"root">>,
        password => <<"******">>,
        sql => ?DEFAULT_SQL,
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 8,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            auto_restart_interval => ?AUTO_RESTART_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => sync,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_tdengine".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {sql,
            mk(
                binary(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        {local_topic,
            mk(
                binary(),
                #{desc => ?DESC("local_topic"), default => undefined}
            )}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        emqx_bridge_tdengine_connector:fields(config);
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for TDengine using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field() ->
    {type, mk(enum([tdengine]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
