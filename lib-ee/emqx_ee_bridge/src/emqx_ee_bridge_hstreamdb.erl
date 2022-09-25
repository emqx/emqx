%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_hstreamdb).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").

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
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"hstreamdb">> => #{
                summary => <<"HStreamDB Bridge">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    maps:merge(values(post), ?METRICS_EXAMPLE);
values(post) ->
    #{
        type => hstreamdb,
        name => <<"demo">>,
        connector => <<"hstreamdb:connector">>,
        enable => true,
        direction => egress,
        local_topic => <<"local/topic/#">>,
        payload => <<"${payload}">>
    };
values(put) ->
    values(post).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_hstreamdb".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {direction, mk(egress, #{desc => ?DESC("config_direction"), default => egress})},
        {local_topic, mk(binary(), #{desc => ?DESC("local_topic")})},
        {payload, mk(binary(), #{default => <<"${payload}">>, desc => ?DESC("payload")})},
        {connector, field(connector)}
    ];
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post").

field(connector) ->
    mk(
        hoconsc:union([binary(), ref(emqx_ee_connector_hstreamdb, config)]),
        #{
            required => true,
            example => <<"hstreamdb:demo">>,
            desc => ?DESC("desc_connector")
        }
    ).

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for HStream using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------
%% internal
type_field() ->
    {type, mk(enum([hstreamdb]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
