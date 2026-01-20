%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_iotdb_connector_info).

-behaviour(emqx_connector_info).

-include("emqx_bridge_iotdb.hrl").

-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_transform_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

-define(CONNECTOR, emqx_bridge_iotdb_connector).
-define(DRIVER_REST, "restapi").
-define(DRIVER_THRIFT, "thrift").

type_name() ->
    iotdb.

bridge_types() ->
    [iotdb].

resource_callback_module() ->
    emqx_bridge_iotdb_connector.

config_transform_module() ->
    emqx_bridge_iotdb_connector.

config_schema() ->
    {iotdb,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:union(fun driver_union_selector/1)),
            #{
                desc => <<"IoTDB Connector Config">>,
                required => false,
                validator => fun validate_iotdb_vsn/1
            }
        )}.

schema_module() ->
    emqx_bridge_iotdb_connector.

api_schema(Method) ->
    {<<"iotdb">>, hoconsc:union(mk_api_union_selector(Method))}.

driver_union_selector(all_union_members) ->
    [
        ref(?DRIVER_REST, "config"),
        ref(?DRIVER_THRIFT, "config")
    ];
driver_union_selector({value, Value}) ->
    case Value of
        #{<<"driver">> := <<"thrift">>} ->
            [ref(?DRIVER_THRIFT, "config")];
        #{<<"driver">> := thrift} ->
            [ref(?DRIVER_THRIFT, "config")];
        _ ->
            [ref(?DRIVER_REST, "config")]
    end.

mk_api_union_selector(Method) ->
    fun
        (all_union_members) ->
            [
                ref(?DRIVER_REST, Method),
                ref(?DRIVER_THRIFT, Method)
            ];
        ({value, Value}) ->
            case Value of
                #{<<"driver">> := <<"thrift">>} ->
                    [ref(?DRIVER_THRIFT, Method)];
                _ ->
                    [ref(?DRIVER_REST, Method)]
            end
    end.

ref(Driver, Field) ->
    Name = Field ++ "_" ++ Driver,
    hoconsc:ref(?CONNECTOR, Name).

validate_iotdb_vsn(Config) ->
    do_validate_iotdb_vsn(emqx_utils_maps:binary_key_map(Config)).

do_validate_iotdb_vsn(#{<<"driver">> := restapi, <<"iotdb_version">> := Version} = Config) ->
    case Version of
        ?VSN_2_0_X ->
            ok;
        ?VSN_1_3_X ->
            case maps:get(<<"sql">>, Config, undefined) of
                #{<<"dialect">> := table} ->
                    {error, <<"Table model only supports IoTDB 2.0.x and later">>};
                _ ->
                    ok
            end;
        _ ->
            {error, <<"REST API only supports IoTDB 1.3.x and later">>}
    end;
do_validate_iotdb_vsn(#{<<"driver">> := thrift, <<"protocol_version">> := Version}) ->
    case Version of
        ?PROTOCOL_V3 ->
            ok;
        _ ->
            {error, <<"Thrift protocol version 1 or 2 is deprecated">>}
    end;
do_validate_iotdb_vsn(_) ->
    ok.
