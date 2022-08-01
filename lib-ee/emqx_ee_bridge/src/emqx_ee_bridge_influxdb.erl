%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_influxdb).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_ee_bridge.hrl").

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
            <<"influxdb_udp">> => #{
                summary => <<"InfluxDB UDP Bridge">>,
                value => values("influxdb_udp", Method)
            }
        },
        #{
            <<"influxdb_api_v1">> => #{
                summary => <<"InfluxDB HTTP API V1 Bridge">>,
                value => values("influxdb_api_v1", Method)
            }
        },
        #{
            <<"influxdb_api_v2">> => #{
                summary => <<"InfluxDB HTTP API V2 Bridge">>,
                value => values("influxdb_api_v2", Method)
            }
        }
    ].

values(Protocol, get) ->
    maps:merge(values(Protocol, post), ?METRICS_EXAMPLE);
values(Protocol, post) ->
    #{
        type => list_to_atom(Protocol),
        name => <<"demo">>,
        connector => list_to_binary(Protocol ++ ":connector"),
        enable => true,
        direction => egress,
        local_topic => <<"local/topic/#">>,
        measurement => <<"${topic}">>,
        tags => #{<<"clientid">> => <<"${clientid}">>},
        fields => #{
            <<"payload">> => <<"${payload}">>,
            <<"int_value">> => [int, <<"${payload.int_key}">>],
            <<"uint_value">> => [uint, <<"${payload.uint_key}">>]
        }
    };
values(Protocol, put) ->
    values(Protocol, post).

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge".

roots() -> [].

fields(basic) ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {direction, mk(egress, #{desc => ?DESC("config_direction"), default => egress})},
        {local_topic, mk(binary(), #{desc => ?DESC("local_topic")})},
        {measurement, mk(binary(), #{desc => ?DESC("measurement"), required => true})},
        {timestamp,
            mk(binary(), #{
                desc => ?DESC("timestamp"), default => <<"${timestamp}">>, required => false
            })},
        {tags, mk(map(), #{desc => ?DESC("tags"), required => false})},
        {fields, mk(map(), #{desc => ?DESC("fields"), required => true})}
    ];
fields("post_udp") ->
    method_fileds(post, influxdb_udp);
fields("post_api_v1") ->
    method_fileds(post, influxdb_api_v1);
fields("post_api_v2") ->
    method_fileds(post, influxdb_api_v2);
fields("put_udp") ->
    method_fileds(put, influxdb_udp);
fields("put_api_v1") ->
    method_fileds(put, influxdb_api_v1);
fields("put_api_v2") ->
    method_fileds(put, influxdb_api_v2);
fields("get_udp") ->
    method_fileds(get, influxdb_udp);
fields("get_api_v1") ->
    method_fileds(get, influxdb_api_v1);
fields("get_api_v2") ->
    method_fileds(get, influxdb_api_v2);
fields(Name) when
    Name == influxdb_udp orelse Name == influxdb_api_v1 orelse Name == influxdb_api_v2
->
    fields(basic) ++ connector_field(Name).

method_fileds(post, ConnectorType) ->
    fields(basic) ++ connector_field(ConnectorType) ++ type_name_field(ConnectorType);
method_fileds(get, ConnectorType) ->
    fields(basic) ++
        emqx_bridge_schema:metrics_status_fields() ++
        connector_field(ConnectorType) ++ type_name_field(ConnectorType);
method_fileds(put, ConnectorType) ->
    fields(basic) ++ connector_field(ConnectorType).

connector_field(Type) ->
    [
        {connector,
            mk(
                hoconsc:union([binary(), ref(emqx_ee_connector_influxdb, Type)]),
                #{
                    required => true,
                    example => list_to_binary(atom_to_list(Type) ++ ":connector"),
                    desc => ?DESC(<<"desc_connector">>)
                }
            )}
    ].

type_name_field(Type) ->
    [
        {type, mk(Type, #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for HStream using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.
