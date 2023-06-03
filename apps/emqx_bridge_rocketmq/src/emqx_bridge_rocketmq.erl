%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rocketmq).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
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

-define(DEFAULT_TEMPLATE, <<>>).
-define(DEFFAULT_REQ_TIMEOUT, <<"15s">>).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"rocketmq">> => #{
                summary => <<"RocketMQ Bridge">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    values(post);
values(post) ->
    #{
        enable => true,
        type => rocketmq,
        name => <<"foo">>,
        server => <<"127.0.0.1:9876">>,
        topic => <<"TopicTest">>,
        template => ?DEFAULT_TEMPLATE,
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 1,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => sync,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    };
values(put) ->
    values(post).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_rocketmq".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {template,
            mk(
                binary(),
                #{desc => ?DESC("template"), default => ?DEFAULT_TEMPLATE}
            )},
        {local_topic,
            mk(
                binary(),
                #{desc => ?DESC("local_topic"), required => false}
            )}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        (emqx_bridge_rocketmq_connector:fields(config) --
            emqx_connector_schema_lib:prepare_statement_fields());
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for RocketMQ using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field() ->
    {type, mk(enum([rocketmq]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
