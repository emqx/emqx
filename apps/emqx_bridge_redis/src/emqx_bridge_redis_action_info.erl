%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_redis_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    connector_action_config_to_bridge_v1_config/2,
    bridge_v1_config_to_action_config/2,
    bridge_v1_config_to_connector_config/1,
    bridge_v1_type_name_fun/1,
    action_convert_from_connector/2
]).

-import(emqx_utils_conv, [bin/1]).

-define(SCHEMA_MODULE, emqx_bridge_redis_schema).
-import(hoconsc, [mk/2, enum/1, ref/1, ref/2]).

action_type_name() -> redis.

connector_type_name() -> redis.

schema_module() -> ?SCHEMA_MODULE.

%% redis_cluster don't have batch options
connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    Config0 = emqx_utils_maps:deep_merge(
        maps:without(
            [<<"connector">>, <<"last_modified_at">>],
            emqx_utils_maps:unindent(<<"parameters">>, ActionConfig)
        ),
        emqx_utils_maps:unindent(<<"parameters">>, ConnectorConfig)
    ),
    Config1 =
        case Config0 of
            #{<<"resource_opts">> := ResOpts0, <<"redis_type">> := Type} ->
                Schema = emqx_bridge_redis:fields("creation_opts_redis_" ++ binary_to_list(Type)),
                ResOpts = maps:with(schema_keys(Schema), ResOpts0),
                Config0#{<<"resource_opts">> => ResOpts};
            _ ->
                Config0
        end,
    maps:without([<<"description">>], Config1).

action_convert_from_connector(ConnectorConfig, ActionConfig) ->
    case ConnectorConfig of
        #{<<"parameters">> := #{<<"redis_type">> := <<"redis_cluster">>}} ->
            emqx_utils_maps:update_if_present(
                <<"resource_opts">>,
                fun(Opts) ->
                    Opts#{
                        <<"batch_size">> => 1,
                        <<"batch_time">> => <<"0ms">>
                    }
                end,
                ActionConfig
            );
        _ ->
            ActionConfig
    end.

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    ActionTopLevelKeys = schema_keys(?SCHEMA_MODULE:fields(redis_action)),
    ActionParametersKeys = schema_keys(emqx_bridge_redis:fields(action_parameters)),
    ActionKeys = ActionTopLevelKeys ++ ActionParametersKeys,
    ActionConfig0 = make_config_map(ActionKeys, ActionParametersKeys, BridgeV1Config),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_bridge_v2_schema:project_to_actions_resource_opts/1,
        ActionConfig0#{<<"connector">> => ConnectorName}
    ).

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
    ConnectorConfig0 = make_config_map(ConnectorKeys, ConnectorParametersKeys, BridgeV1Config),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_connector_schema:project_to_connector_resource_opts/1,
        ConnectorConfig0
    ).

%%------------------------------------------------------------------------------------------
%% Internal helper fns
%%------------------------------------------------------------------------------------------

bridge_v1_type_name() ->
    {fun ?MODULE:bridge_v1_type_name_fun/1, bridge_v1_type_names()}.
bridge_v1_type_name_fun({#{<<"parameters">> := #{<<"redis_type">> := Type}}, _}) ->
    v1_type(Type).

v1_type(<<"single">>) -> redis_single;
v1_type(<<"sentinel">>) -> redis_sentinel;
v1_type(<<"cluster">>) -> redis_cluster.

bridge_v1_type_names() -> [redis_single, redis_sentinel, redis_cluster].

schema_keys(Schema) ->
    [bin(Key) || {Key, _} <- Schema].

make_config_map(PickKeys, IndentKeys, Config) ->
    Conf0 = maps:with(PickKeys, Config),
    emqx_utils_maps:indent(<<"parameters">>, IndentKeys, Conf0).
