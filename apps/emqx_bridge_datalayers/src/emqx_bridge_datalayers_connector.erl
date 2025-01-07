%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_datalayers_connector).

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

-define(DATALAYERS_DEFAULT_PORT, 8361).

%% datalayers servers don't need parse
-define(DATALAYERS_HOST_OPTIONS, #{
    default_port => ?DATALAYERS_DEFAULT_PORT
}).

-define(DEFAULT_POOL_SIZE, 8).

%%--------------------------------------------------------------------
%% resource callback

resource_type() -> datalayers.

callback_mode() -> async_if_possible.

on_add_channel(
    InstId,
    OldState,
    ChannelId,
    ChannelConf
) ->
    emqx_bridge_influxdb_connector:on_add_channel(
        InstId,
        OldState,
        ChannelId,
        ChannelConf
    ).

on_remove_channel(InstId, State, ChannelId) ->
    emqx_bridge_influxdb_connector:on_remove_channel(InstId, State, ChannelId).

on_get_channel_status(InstId, ChannelId, State) ->
    emqx_bridge_influxdb_connector:on_get_channel_status(InstId, ChannelId, State).

on_get_channels(InstId) ->
    emqx_bridge_influxdb_connector:on_get_channels(InstId).

on_start(InstId, Config) ->
    case driver_type(Config) of
        influxdb_v1 ->
            Config1 = convert_config_to_influxdb(Config),
            emqx_bridge_influxdb_connector:on_start(InstId, Config1)
    end.

driver_type(#{parameters := #{driver_type := influxdb_v1}}) ->
    influxdb_v1.

convert_config_to_influxdb(Config = #{parameters := Params = #{driver_type := influxdb_v1}}) ->
    Config#{
        parameters := maps:without([driver_type], Params#{influxdb_type => influxdb_api_v1})
    }.

on_stop(InstId, State) ->
    emqx_bridge_influxdb_connector:on_stop(InstId, State).

on_query(InstId, {Channel, Message}, State) ->
    emqx_bridge_influxdb_connector:on_query(InstId, {Channel, Message}, State).

on_batch_query(InstId, BatchData, State) ->
    emqx_bridge_influxdb_connector:on_batch_query(InstId, BatchData, State).

on_query_async(
    InstId,
    {Channel, Message},
    {ReplyFun, Args},
    State
) ->
    emqx_bridge_influxdb_connector:on_query_async(
        InstId,
        {Channel, Message},
        {ReplyFun, Args},
        State
    ).

on_batch_query_async(
    InstId,
    BatchData,
    {ReplyFun, Args},
    State
) ->
    emqx_bridge_influxdb_connector:on_batch_query_async(
        InstId,
        BatchData,
        {ReplyFun, Args},
        State
    ).

on_get_status(InstId, State) ->
    emqx_bridge_influxdb_connector:on_get_status(InstId, State).

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
                hoconsc:union([
                    ref(?MODULE, "datalayers_influxdb_v1_parameters")
                ]),
                #{required => true, desc => ?DESC("datalayers_parameters")}
            )}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("datalayers_influxdb_v1_parameters") ->
    datalayers_parameters_fields().

precision_field() ->
    {precision,
        %% The influxdb only supports these 4 precision:
        %% See "https://github.com/influxdata/influxdb/blob/
        %% 6b607288439a991261307518913eb6d4e280e0a7/models/points.go#L487" for
        %% more information.
        mk(enum([ns, us, ms, s]), #{
            required => false, default => ms, desc => ?DESC("precision")
        })}.

datalayers_parameters_fields() ->
    [
        {driver_type,
            mk(enum([influxdb_v1]), #{
                required => false, default => influxdb_v1, desc => ?DESC("driver_type")
            })},
        {database, mk(binary(), #{required => true, desc => ?DESC("database")})},
        {username, mk(binary(), #{desc => ?DESC("username")})},
        {password, emqx_schema_secret:mk(#{desc => ?DESC("password")})}
    ].

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
desc("datalayers_influxdb_v1_parameters") ->
    ?DESC("datalayers_parameters");
desc(datalayers_api) ->
    ?DESC("datalayers_api");
desc("connector") ->
    ?DESC("connector").

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
