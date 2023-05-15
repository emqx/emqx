%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_oracle).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-export([
    conn_bridge_examples/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(DEFAULT_SQL, <<
    "insert into t_mqtt_msg(msgid, topic, qos, payload)"
    "values (${id}, ${topic}, ${qos}, ${payload})"
>>).

conn_bridge_examples(Method) ->
    [
        #{
            <<"oracle">> => #{
                summary => <<"Oracle Database Bridge">>,
                value => values(Method)
            }
        }
    ].

values(_Method) ->
    #{
        enable => true,
        type => oracle,
        name => <<"foo">>,
        server => <<"127.0.0.1:1521">>,
        pool_size => 8,
        database => <<"ORCL">>,
        sid => <<"ORCL">>,
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
            query_mode => async,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions

namespace() -> "bridge_oracle".

roots() -> [].

fields("config") ->
    [
        {enable,
            hoconsc:mk(
                boolean(),
                #{desc => ?DESC("config_enable"), default => true}
            )},
        {sql,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("sql_template"), default => ?DEFAULT_SQL, format => <<"sql">>}
            )},
        {local_topic,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("local_topic"), default => undefined}
            )}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        (emqx_oracle_schema:fields(config) --
            emqx_connector_schema_lib:prepare_statement_fields());
fields("post") ->
    fields("post", oracle);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

desc("config") ->
    ?DESC("desc_config");
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field(Type) ->
    {type, hoconsc:mk(hoconsc:enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, hoconsc:mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
