%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_syskeeper_proxy).

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

-define(SYSKEEPER_HOST_OPTIONS, #{
    default_port => 9092
}).

%% -------------------------------------------------------------------------------------------------
%% api
conn_bridge_examples(Method) ->
    [
        #{
            <<"syskeeper_proxy">> => #{
                summary => <<"Syskeeper Bridge Proxy">>,
                value => values(Method)
            }
        }
    ].

values(_Method) ->
    #{
        enable => true,
        type => syskeeper_proxy,
        name => <<"foo">>,
        listen => <<"127.0.0.1:9092">>,
        acceptors => 16,
        handshake_timeout => <<"16s">>
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_syskeeper_proxy".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {listen, listen()},
        {acceptors,
            mk(
                non_neg_integer(),
                #{desc => ?DESC("acceptors"), default => 16}
            )},
        {handshake_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{desc => ?DESC(handshake_timeout), default => <<"10s">>}
            )}
    ];
fields("creation_opts") ->
    emqx_resource_schema:create_opts([{worker_pool_size, #{default => 1}}]);
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Syskeeper Proxy using `", string:to_upper(Method), "` method."];
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc(_) ->
    undefined.

listen() ->
    Meta = #{desc => ?DESC("listen")},
    emqx_schema:servers_sc(Meta, ?SYSKEEPER_HOST_OPTIONS).

%% -------------------------------------------------------------------------------------------------

type_field() ->
    {type, mk(enum([syskeeper_proxy]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
