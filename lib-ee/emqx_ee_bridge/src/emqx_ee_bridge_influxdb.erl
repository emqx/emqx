%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_influxdb).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_ee_bridge.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    conn_bridge_example/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_example(Method) ->
    #{
        <<"influxdb">> => #{
            summary => <<"InfluxDB Bridge">>,
            value => values(Method)
        }
    }.

values(get) ->
    maps:merge(values(post), ?METRICS_EXAMPLE);
values(post) ->
    #{
        type => influxdb,
        name => <<"demo">>,
        connector => <<"influxdb:api_v2_connector">>,
        enable => true,
        direction => egress,
        local_topic => <<"local/topic/#">>,
        payload => <<"${payload}">>
    };
values(put) ->
    values(post).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge".

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
    ConnectorConfigRef =
        [
            ref(emqx_ee_connector_influxdb, udp),
            ref(emqx_ee_connector_influxdb, api_v1),
            ref(emqx_ee_connector_influxdb, api_v2)
        ],
    mk(
        hoconsc:union([binary() | ConnectorConfigRef]),
        #{
            required => true,
            example => <<"influxdb:demo">>,
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
    {type, mk(enum([influxdb]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
