%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_redis_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_config_to_action_config/2,
    connector_action_config_to_bridge_v1_config/2,
    bridge_v1_config_to_connector_config/1,
    bridge_v1_type_name_fun/1
]).

-import(emqx_utils_conv, [bin/1]).

-define(SCHEMA_MODULE, emqx_bridge_redis_schema).
-import(hoconsc, [mk/2, enum/1, ref/1, ref/2]).

action_type_name() -> redis.

connector_type_name() -> redis.

schema_module() -> ?SCHEMA_MODULE.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    fix_v1_type(
        maps:merge(
            maps:without(
                [<<"connector">>],
                map_unindent(<<"parameters">>, ActionConfig)
            ),
            map_unindent(<<"parameters">>, ConnectorConfig)
        )
    ).

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    ActionTopLevelKeys = schema_keys(?SCHEMA_MODULE:fields(redis_action)),
    ActionParametersKeys = schema_keys(emqx_bridge_redis:fields(action_parameters)),
    ActionKeys = ActionTopLevelKeys ++ ActionParametersKeys,
    ActionConfig = make_config_map(ActionKeys, ActionParametersKeys, BridgeV1Config),
    ActionConfig#{<<"connector">> => ConnectorName}.

bridge_v1_config_to_connector_config(BridgeV1Config) ->
    ActionTopLevelKeys = schema_keys(?SCHEMA_MODULE:fields(redis_action)),
    ActionParametersKeys = schema_keys(emqx_bridge_redis:fields(action_parameters)),
    ActionKeys = ActionTopLevelKeys ++ ActionParametersKeys,
    ConnectorTopLevelKeys = schema_keys(?SCHEMA_MODULE:fields("config_connector")),
    %% Need put redis_type into parameter.
    %% cluster need type to filter resource_opts
    ConnectorKeys =
        (maps:keys(BridgeV1Config) -- (ActionKeys -- ConnectorTopLevelKeys)) ++
            [<<"redis_type">>],
    ConnectorParametersKeys = ConnectorKeys -- ConnectorTopLevelKeys,
    make_config_map(ConnectorKeys, ConnectorParametersKeys, BridgeV1Config).

%%------------------------------------------------------------------------------------------
%% Internal helper fns
%%------------------------------------------------------------------------------------------

bridge_v1_type_name() ->
    {fun ?MODULE:bridge_v1_type_name_fun/1, bridge_v1_type_names()}.
bridge_v1_type_name_fun({#{<<"parameters">> := #{<<"redis_type">> := Type}}, _}) ->
    v1_type(Type).

fix_v1_type(#{<<"redis_type">> := RedisType} = Conf) ->
    Conf#{<<"type">> => v1_type(RedisType)}.

v1_type(<<"single">>) -> redis_single;
v1_type(<<"sentinel">>) -> redis_sentinel;
v1_type(<<"cluster">>) -> redis_cluster.

bridge_v1_type_names() -> [redis_single, redis_sentinel, redis_cluster].

map_unindent(Key, Map) ->
    maps:merge(
        maps:get(Key, Map),
        maps:remove(Key, Map)
    ).

map_indent(IndentKey, PickKeys, Map) ->
    maps:put(
        IndentKey,
        maps:with(PickKeys, Map),
        maps:without(PickKeys, Map)
    ).

schema_keys(Schema) ->
    [bin(Key) || {Key, _} <- Schema].

make_config_map(PickKeys, IndentKeys, Config) ->
    Conf0 = maps:with(PickKeys, Config),
    map_indent(<<"parameters">>, IndentKeys, Conf0).
