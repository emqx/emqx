%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_syskeeper).

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

%% -------------------------------------------------------------------------------------------------
%% api
conn_bridge_examples(Method) ->
    [
        #{
            <<"syskeeper">> => #{
                summary => <<"Syskeeper Bridge">>,
                value => values(Method)
            }
        }
    ].

values(_Method) ->
    #{
        enable => true,
        type => syskeeper,
        name => <<"foo">>,
        server => <<"127.0.0.1:9092">>,
        ack_mode => <<"no_ack">>,
        ack_timeout => <<"10s">>,
        pool_size => 16,
        target_topic => <<"${topic}">>,
        target_qos => <<"-1">>,
        template => <<"${payload}">>,
        resource_opts => #{
            worker_pool_size => 16,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => sync,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_syskeeper".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {target_topic,
            mk(
                binary(),
                #{desc => ?DESC("target_topic"), default => <<"${topic}">>}
            )},
        {target_qos,
            mk(
                range(-1, 2),
                #{desc => ?DESC("target_qos"), default => -1}
            )},
        {template,
            mk(
                binary(),
                #{desc => ?DESC("template"), default => <<"${payload}">>}
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
    ] ++ emqx_bridge_syskeeper_connector:fields(config);
fields("creation_opts") ->
    emqx_resource_schema:create_opts([{request_ttl, #{default => infinity}}]);
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Syskeeper using `", string:to_upper(Method), "` method."];
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field() ->
    {type, mk(enum([syskeeper]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
