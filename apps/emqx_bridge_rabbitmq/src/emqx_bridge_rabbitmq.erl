%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq).

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

%% -------------------------------------------------------------------------------------------------
%% Callback used by HTTP API v1
%% -------------------------------------------------------------------------------------------------

conn_bridge_examples(Method) ->
    [
        #{
            <<"rabbitmq">> => #{
                summary => <<"RabbitMQ Bridge">>,
                value => values(Method, "rabbitmq")
            }
        }
    ].

values(_Method, Type) ->
    #{
        enable => true,
        type => Type,
        name => <<"foo">>,
        server => <<"localhost">>,
        port => 5672,
        username => <<"guest">>,
        password => <<"******">>,
        pool_size => 8,
        timeout => 5,
        virtual_host => <<"/">>,
        heartbeat => <<"30s">>,
        auto_reconnect => <<"2s">>,
        exchange => <<"messages">>,
        exchange_type => <<"topic">>,
        routing_key => <<"my_routing_key">>,
        durable => false,
        payload_template => <<"">>,
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

namespace() -> "bridge_rabbitmq".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {local_topic,
            mk(
                binary(),
                #{desc => ?DESC("local_topic")}
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
        emqx_bridge_rabbitmq_connector:fields(config);
fields("creation_opts") ->
    emqx_resource_schema:fields("creation_opts");
fields("post") ->
    fields("post", rabbitmq);
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

fields("post", Type) ->
    [type_field(Type), name_field() | fields("config")].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for RabbitMQ using `", string:to_upper(Method), "` method."];
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
