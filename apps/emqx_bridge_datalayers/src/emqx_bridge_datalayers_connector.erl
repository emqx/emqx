%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_datalayers_connector).

-include("emqx_bridge_datalayers.hrl").

-include_lib("emqx_connector/include/emqx_connector.hrl").

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,
    on_get_channels/1,
    on_query/3,
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4,
    on_get_status/2
]).

-export([
    roots/0,
    namespace/0,
    fields/1,
    desc/1
]).

-export([precision_field/0]).

-define(DEFAULT_POOL_SIZE, 8).

-define(influx_driver, #{driver_type := ?DATALAYERS_DRIVER_TYPE_INFLUX}).
-define(arrow_flight_driver, #{driver_type := ?DATALAYERS_DRIVER_TYPE_ARROW_FLIGHT}).

%%--------------------------------------------------------------------
%% resource callback

resource_type() -> datalayers.

callback_mode() -> async_if_possible.

on_start(InstId, Config) ->
    case Config of
        #{parameters := ?influx_driver} ->
            case emqx_bridge_influxdb_connector:on_start(InstId, enrich_config(Config)) of
                {ok, State} ->
                    %% Start the InfluxDB client
                    {ok, State#{driver_type => ?DATALAYERS_DRIVER_TYPE_INFLUX}};
                Err ->
                    Err
            end;
        #{parameters := ?arrow_flight_driver} ->
            case emqx_bridge_datalayers_arrow_flight_connector:on_start(InstId, Config) of
                {ok, State} ->
                    %% Start the Arrow Flight client
                    {ok, State#{driver_type => ?DATALAYERS_DRIVER_TYPE_ARROW_FLIGHT}};
                Err ->
                    Err
            end
    end.

enrich_config(
    Config = #{parameters := Params = #{driver_type := ?DATALAYERS_DRIVER_TYPE_INFLUX}}
) ->
    Config#{parameters := Params#{influxdb_type => influxdb_api_v1}}.

on_stop(InstId, State = ?influx_driver) ->
    emqx_bridge_influxdb_connector:on_stop(InstId, State);
on_stop(InstId, State = ?arrow_flight_driver) ->
    emqx_bridge_datalayers_arrow_flight_connector:on_stop(InstId, State).

on_add_channel(
    InstId,
    OldState = ?influx_driver,
    ChannelId,
    ChannelConf
) ->
    emqx_bridge_influxdb_connector:on_add_channel(
        InstId,
        OldState,
        ChannelId,
        ChannelConf
    );
on_add_channel(
    InstId,
    OldState = ?arrow_flight_driver,
    ChannelId,
    ChannelConf
) ->
    emqx_bridge_datalayers_arrow_flight_connector:on_add_channel(
        InstId,
        OldState,
        ChannelId,
        ChannelConf
    ).

on_remove_channel(InstId, State = ?influx_driver, ChannelId) ->
    emqx_bridge_influxdb_connector:on_remove_channel(InstId, State, ChannelId);
on_remove_channel(InstId, State = ?arrow_flight_driver, ChannelId) ->
    emqx_bridge_datalayers_arrow_flight_connector:on_remove_channel(InstId, State, ChannelId).

on_get_channel_status(InstId, ChannelId, State = ?influx_driver) ->
    emqx_bridge_influxdb_connector:on_get_channel_status(InstId, ChannelId, State);
on_get_channel_status(InstId, ChannelId, State = ?arrow_flight_driver) ->
    emqx_bridge_datalayers_arrow_flight_connector:on_get_channel_status(InstId, ChannelId, State).

on_get_channels(InstId) ->
    emqx_bridge_v2:get_channels_for_connector(InstId).

on_query(
    InstId,
    {Channel, Message},
    State = ?influx_driver
) ->
    emqx_bridge_influxdb_connector:on_query(InstId, {Channel, Message}, State);
on_query(
    InstId,
    {Channel, Message},
    State = ?arrow_flight_driver
) ->
    emqx_bridge_datalayers_arrow_flight_connector:on_query(InstId, {Channel, Message}, State).

on_batch_query(
    InstId,
    BatchData,
    State = ?influx_driver
) ->
    emqx_bridge_influxdb_connector:on_batch_query(InstId, BatchData, State);
on_batch_query(
    InstId,
    BatchData,
    State = ?arrow_flight_driver
) ->
    emqx_bridge_datalayers_arrow_flight_connector:on_batch_query(InstId, BatchData, State).

on_query_async(
    InstId,
    {Channel, Message},
    {ReplyFun, Args},
    State = ?influx_driver
) ->
    emqx_bridge_influxdb_connector:on_query_async(
        InstId, {Channel, Message}, {ReplyFun, Args}, State
    );
on_query_async(
    InstId,
    {Channel, Message},
    {ReplyFun, Args},
    State = ?arrow_flight_driver
) ->
    emqx_bridge_datalayers_arrow_flight_connector:on_query_async(
        InstId, {Channel, Message}, {ReplyFun, Args}, State
    ).

on_batch_query_async(
    InstId,
    BatchData,
    {ReplyFun, Args},
    State = ?influx_driver
) ->
    emqx_bridge_influxdb_connector:on_batch_query_async(
        InstId,
        BatchData,
        {ReplyFun, Args},
        State
    );
on_batch_query_async(
    InstId,
    BatchData,
    {ReplyFun, Args},
    State = ?arrow_flight_driver
) ->
    emqx_bridge_datalayers_arrow_flight_connector:on_batch_query_async(
        InstId,
        BatchData,
        {ReplyFun, Args},
        State
    ).

on_get_status(InstId, State = ?influx_driver) ->
    emqx_bridge_influxdb_connector:on_get_status(InstId, State);
on_get_status(InstId, State = ?arrow_flight_driver) ->
    emqx_bridge_datalayers_arrow_flight_connector:on_get_status(InstId, State).

%%--------------------------------------------------------------------
%% schema

namespace() -> connector_datalayers.

roots() ->
    [
        {config, #{
            type => hoconsc:ref(?MODULE, "connector")
        }}
    ].

fields("connector") ->
    [
        {server, server()},
        emqx_connector_schema:ehttpc_max_inactive_sc(),
        {pool_size,
            mk(
                integer(),
                #{
                    required => false,
                    default => ?DEFAULT_POOL_SIZE,
                    desc => ?DESC("pool_size")
                }
            )},
        {parameters,
            mk(
                hoconsc:union([ref(?MODULE, "datalayers_parameters")]),
                #{required => true, desc => ?DESC("datalayers_parameters")}
            )}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("datalayers_parameters") ->
    [
        {driver_type,
            mk(enum([?DATALAYERS_DRIVER_TYPE_INFLUX, ?DATALAYERS_DRIVER_TYPE_ARROW_FLIGHT]), #{
                required => false,
                default => ?DATALAYERS_DRIVER_TYPE_INFLUX,
                desc => ?DESC("driver_type")
            })},
        {database, mk(binary(), #{required => true, desc => ?DESC("database")})},
        {username, mk(binary(), #{desc => ?DESC("username")})},
        {password, emqx_schema_secret:mk(#{desc => ?DESC("password")})},
        {enable_prepared, fun enable_prepared/1}
    ].

precision_field() ->
    {precision,
        %% The influxdb only supports these 4 precision:
        %% See "https://github.com/influxdata/influxdb/blob/
        %% 6b607288439a991261307518913eb6d4e280e0a7/models/points.go#L487" for
        %% more information.
        mk(enum([ns, us, ms, s]), #{
            required => false, default => ms, desc => ?DESC("precision")
        })}.

server() ->
    Meta = #{
        required => false,
        default => <<"127.0.0.1:8361">>,
        desc => ?DESC("server"),
        converter => fun convert_server/2
    },
    emqx_schema:servers_sc(Meta, ?DATALAYERS_HOST_OPTIONS).

desc(common) ->
    ?DESC("common");
desc(parameters) ->
    ?DESC("dayalayers_parameters");
desc("datalayers_parameters") ->
    ?DESC("datalayers_parameters");
desc(datalayers_api) ->
    ?DESC("datalayers_api");
desc("connector") ->
    ?DESC("connector").

enable_prepared(type) ->
    boolean();
enable_prepared(required) ->
    false;
enable_prepared(desc) ->
    ?DESC("enable_prepared");
enable_prepared(default) ->
    true;
enable_prepared(_) ->
    undefined.

%%--------------------------------------------------------------------
%% internal functions

convert_server(<<"http://", Server/binary>>, HoconOpts) ->
    convert_server(Server, HoconOpts);
convert_server(<<"https://", Server/binary>>, HoconOpts) ->
    convert_server(Server, HoconOpts);
convert_server(Server0, HoconOpts) ->
    Server = string:trim(Server0, trailing, "/"),
    emqx_schema:convert_servers(Server, HoconOpts).

%%===================================================================
%% eunit tests
%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% for coverage
desc_test_() ->
    [
        ?_assertMatch(
            {desc, _, _},
            desc(common)
        ),
        ?_assertMatch(
            {desc, _, _},
            desc(datalayers_api)
        ),
        ?_assertMatch(
            {desc, _, _},
            hocon_schema:field_schema(server(), desc)
        ),
        ?_assertMatch(
            connector_datalayers,
            namespace()
        )
    ].
-endif.
