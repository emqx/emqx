%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc The module which knows everything about actions.

%% NOTE: it does not cover the V1 bridges.

-module(emqx_action_info).

-export([
    action_type_to_connector_type/1,
    action_type_to_bridge_v1_type/2,
    bridge_v1_type_to_action_type/1,
    is_action_type/1,
    registered_schema_modules/0,
    connector_action_config_to_bridge_v1_config/3,
    has_custom_connector_action_config_to_bridge_v1_config/1,
    bridge_v1_config_to_connector_config/2,
    has_custom_bridge_v1_config_to_connector_config/1,
    bridge_v1_config_to_action_config/3,
    has_custom_bridge_v1_config_to_action_config/1,
    transform_bridge_v1_config_to_action_config/4
]).

-callback bridge_v1_type_name() ->
    atom()
    | {
        fun(({ActionConfig :: map(), ConnectorConfig :: map()}) -> Type :: atom()),
        TypeList :: [atom()]
    }.
-callback action_type_name() -> atom().
-callback connector_type_name() -> atom().
-callback schema_module() -> atom().
%% Define this if the automatic config downgrade is not enough for the bridge.
-callback connector_action_config_to_bridge_v1_config(
    ConnectorConfig :: map(), ActionConfig :: map()
) -> map().
%% Define this if the automatic config upgrade is not enough for the connector.
-callback bridge_v1_config_to_connector_config(BridgeV1Config :: map()) -> map().
%% Define this if the automatic config upgrade is not enough for the bridge.
%% If you want to make use of the automatic config upgrade, you can call
%% emqx_action_info:transform_bridge_v1_config_to_action_config/4 in your
%% implementation and do some adjustments on the result.
-callback bridge_v1_config_to_action_config(BridgeV1Config :: map(), ConnectorName :: binary()) ->
    map().

-optional_callbacks([
    bridge_v1_type_name/0,
    connector_action_config_to_bridge_v1_config/2,
    bridge_v1_config_to_connector_config/1,
    bridge_v1_config_to_action_config/2
]).

%% ====================================================================
%% Hadcoded list of info modules for actions
%% TODO: Remove this list once we have made sure that all relevants
%% apps are loaded before this module is called.
%% ====================================================================

-if(?EMQX_RELEASE_EDITION == ee).
hard_coded_action_info_modules_ee() ->
    [
        emqx_bridge_azure_event_hub_action_info,
        emqx_bridge_confluent_producer_action_info,
        emqx_bridge_gcp_pubsub_producer_action_info,
        emqx_bridge_kafka_action_info,
        emqx_bridge_matrix_action_info,
        emqx_bridge_mongodb_action_info,
        emqx_bridge_pgsql_action_info,
        emqx_bridge_syskeeper_action_info,
        emqx_bridge_timescale_action_info
    ].
-else.
hard_coded_action_info_modules_ee() ->
    [].
-endif.

hard_coded_action_info_modules_common() ->
    [emqx_bridge_http_action_info].

hard_coded_action_info_modules() ->
    hard_coded_action_info_modules_common() ++ hard_coded_action_info_modules_ee().

%% ====================================================================
%% API
%% ====================================================================

action_type_to_connector_type(Type) when not is_atom(Type) ->
    action_type_to_connector_type(binary_to_existing_atom(iolist_to_binary(Type)));
action_type_to_connector_type(Type) ->
    ActionInfoMap = info_map(),
    ActionTypeToConnectorTypeMap = maps:get(action_type_to_connector_type, ActionInfoMap),
    case maps:get(Type, ActionTypeToConnectorTypeMap, undefined) of
        undefined -> Type;
        ConnectorType -> ConnectorType
    end.

bridge_v1_type_to_action_type(Bin) when is_binary(Bin) ->
    bridge_v1_type_to_action_type(binary_to_existing_atom(Bin));
bridge_v1_type_to_action_type(Type) ->
    ActionInfoMap = info_map(),
    BridgeV1TypeToActionType = maps:get(bridge_v1_type_to_action_type, ActionInfoMap),
    case maps:get(Type, BridgeV1TypeToActionType, undefined) of
        undefined -> Type;
        ActionType -> ActionType
    end.

action_type_to_bridge_v1_type(Bin, Conf) when is_binary(Bin) ->
    action_type_to_bridge_v1_type(binary_to_existing_atom(Bin), Conf);
action_type_to_bridge_v1_type(ActionType, ActionConf) ->
    ActionInfoMap = info_map(),
    ActionTypeToBridgeV1Type = maps:get(action_type_to_bridge_v1_type, ActionInfoMap),
    case maps:get(ActionType, ActionTypeToBridgeV1Type, undefined) of
        undefined ->
            ActionType;
        BridgeV1TypeFun when is_function(BridgeV1TypeFun) ->
            case get_confs(ActionType, ActionConf) of
                {ConnectorConfig, ActionConfig} -> BridgeV1TypeFun({ConnectorConfig, ActionConfig});
                undefined -> ActionType
            end;
        BridgeV1Type ->
            BridgeV1Type
    end.

get_confs(ActionType, #{<<"connector">> := ConnectorName} = ActionConfig) ->
    ConnectorType = action_type_to_connector_type(ActionType),
    ConnectorConfig = emqx_conf:get_raw([connectors, ConnectorType, ConnectorName]),
    {ConnectorConfig, ActionConfig};
get_confs(_, _) ->
    undefined.

%% This function should return true for all inputs that are bridge V1 types for
%% bridges that have been refactored to bridge V2s, and for all all bridge V2
%% types. For everything else the function should return false.
is_action_type(Bin) when is_binary(Bin) ->
    is_action_type(binary_to_existing_atom(Bin));
is_action_type(Type) ->
    ActionInfoMap = info_map(),
    ActionTypes = maps:get(action_type_names, ActionInfoMap),
    case maps:get(Type, ActionTypes, undefined) of
        undefined -> false;
        _ -> true
    end.

registered_schema_modules() ->
    InfoMap = info_map(),
    Schemas = maps:get(action_type_to_schema_module, InfoMap),
    maps:to_list(Schemas).

has_custom_connector_action_config_to_bridge_v1_config(ActionOrBridgeType) ->
    Module = get_action_info_module(ActionOrBridgeType),
    erlang:function_exported(Module, connector_action_config_to_bridge_v1_config, 2).

connector_action_config_to_bridge_v1_config(ActionOrBridgeType, ConnectorConfig, ActionConfig) ->
    Module = get_action_info_module(ActionOrBridgeType),
    %% should only be called if defined
    Module:connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig).

has_custom_bridge_v1_config_to_connector_config(ActionOrBridgeType) ->
    Module = get_action_info_module(ActionOrBridgeType),
    erlang:function_exported(Module, bridge_v1_config_to_connector_config, 1).

bridge_v1_config_to_connector_config(ActionOrBridgeType, BridgeV1Config) ->
    Module = get_action_info_module(ActionOrBridgeType),
    %% should only be called if defined
    Module:bridge_v1_config_to_connector_config(BridgeV1Config).

has_custom_bridge_v1_config_to_action_config(ActionOrBridgeType) ->
    Module = get_action_info_module(ActionOrBridgeType),
    erlang:function_exported(Module, bridge_v1_config_to_action_config, 2).

bridge_v1_config_to_action_config(ActionOrBridgeType, BridgeV1Config, ConnectorName) ->
    Module = get_action_info_module(ActionOrBridgeType),
    %% should only be called if defined
    Module:bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName).

transform_bridge_v1_config_to_action_config(
    BridgeV1Conf, ConnectorName, ConnectorConfSchemaMod, ConnectorConfSchemaName
) ->
    emqx_connector_schema:transform_bridge_v1_config_to_action_config(
        BridgeV1Conf, ConnectorName, ConnectorConfSchemaMod, ConnectorConfSchemaName
    ).

%% ====================================================================
%% Internal functions for building the info map and accessing it
%% ====================================================================

get_action_info_module(ActionOrBridgeType) ->
    InfoMap = info_map(),
    ActionInfoModuleMap = maps:get(action_type_to_info_module, InfoMap),
    maps:get(ActionOrBridgeType, ActionInfoModuleMap, undefined).

internal_emqx_action_persistent_term_info_key() ->
    ?FUNCTION_NAME.

info_map() ->
    case persistent_term:get(internal_emqx_action_persistent_term_info_key(), not_found) of
        not_found ->
            build_cache();
        ActionInfoMap ->
            ActionInfoMap
    end.

build_cache() ->
    ActionInfoModules = action_info_modules(),
    ActionInfoMap =
        lists:foldl(
            fun(Module, InfoMapSoFar) ->
                ModuleInfoMap = get_info_map(Module),
                emqx_utils_maps:deep_merge(InfoMapSoFar, ModuleInfoMap)
            end,
            initial_info_map(),
            ActionInfoModules
        ),
    %% Update the persistent term with the new info map
    persistent_term:put(internal_emqx_action_persistent_term_info_key(), ActionInfoMap),
    ActionInfoMap.

action_info_modules() ->
    ActionInfoModules = [
        action_info_modules(App)
     || {App, _, _} <- application:loaded_applications()
    ],
    lists:usort(lists:flatten(ActionInfoModules) ++ hard_coded_action_info_modules()).

action_info_modules(App) ->
    case application:get_env(App, emqx_action_info_modules) of
        {ok, Modules} ->
            Modules;
        _ ->
            []
    end.

initial_info_map() ->
    #{
        action_type_names => #{},
        bridge_v1_type_to_action_type => #{},
        action_type_to_bridge_v1_type => #{},
        action_type_to_connector_type => #{},
        action_type_to_schema_module => #{},
        action_type_to_info_module => #{}
    }.

get_info_map(Module) ->
    %% Force the module to get loaded
    _ = code:ensure_loaded(Module),
    ActionType = Module:action_type_name(),
    {BridgeV1TypeOrFun, BridgeV1Types} =
        case erlang:function_exported(Module, bridge_v1_type_name, 0) of
            true ->
                case Module:bridge_v1_type_name() of
                    {_BridgeV1TypeFun, _BridgeV1Types} = BridgeV1TypeTuple ->
                        BridgeV1TypeTuple;
                    BridgeV1Type0 ->
                        {BridgeV1Type0, [BridgeV1Type0]}
                end;
            false ->
                {ActionType, [ActionType]}
        end,
    #{
        action_type_names =>
            lists:foldl(
                fun(BridgeV1Type, M) ->
                    M#{BridgeV1Type => true}
                end,
                #{ActionType => true},
                BridgeV1Types
            ),
        bridge_v1_type_to_action_type =>
            lists:foldl(
                fun(BridgeV1Type, M) ->
                    %% Alias the bridge V1 type to the action type
                    M#{BridgeV1Type => ActionType}
                end,
                #{ActionType => ActionType},
                BridgeV1Types
            ),
        action_type_to_bridge_v1_type => #{
            ActionType => BridgeV1TypeOrFun
        },
        action_type_to_connector_type =>
            lists:foldl(
                fun(BridgeV1Type, M) ->
                    M#{BridgeV1Type => Module:connector_type_name()}
                end,
                #{ActionType => Module:connector_type_name()},
                BridgeV1Types
            ),
        action_type_to_schema_module => #{
            ActionType => Module:schema_module()
        },
        action_type_to_info_module =>
            lists:foldl(
                fun(BridgeV1Type, M) ->
                    M#{BridgeV1Type => Module}
                end,
                #{ActionType => Module},
                BridgeV1Types
            )
    }.
