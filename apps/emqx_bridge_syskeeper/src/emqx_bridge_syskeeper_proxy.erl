%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_syskeeper_proxy).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    connector_examples/1,
    values/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, syskeeper_proxy).

-define(SYSKEEPER_HOST_OPTIONS, #{
    default_port => 9092
}).

%% -------------------------------------------------------------------------------------------------
%% api
connector_examples(Method) ->
    [
        #{
            <<"syskeeper_proxy">> => #{
                summary => <<"Syskeeper Proxy Connector">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ],
            actions => [<<"my_action">>]
        },
        values(post)
    );
values(post) ->
    maps:merge(
        #{
            name => <<"syskeeper_proxy">>,
            type => <<"syskeeper_proxy">>
        },
        values(put)
    );
values(put) ->
    #{
        enable => true,
        listen => <<"127.0.0.1:9092">>,
        acceptors => 16,
        handshake_timeout => <<"16s">>
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "connector_syskeeper_proxy".

roots() -> [].

fields(config) ->
    emqx_connector_schema:common_fields() ++
        fields("connection_fields") ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields("connection_fields") ->
    [
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
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields(Field) when
    Field == "get";
    Field == "post";
    Field == "put"
->
    Fields =
        fields("connection_fields") ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(
        Field ++ "_connector", ?CONNECTOR_TYPE, Fields
    ).

desc(config) ->
    ?DESC("desc_config");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Syskeeper Proxy using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

listen() ->
    Meta = #{desc => ?DESC("listen")},
    emqx_schema:servers_sc(Meta, ?SYSKEEPER_HOST_OPTIONS).
