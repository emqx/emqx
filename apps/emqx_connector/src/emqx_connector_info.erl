%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The module which knows everything about connectors.

-module(emqx_connector_info).

%% Public API
-export([
    connector_types/0,
    bridge_types/1,
    resource_callback_module/1,
    schema_module/1,
    config_schema/1,
    api_schema/2,
    config_transform_module/1
]).

-export([clean_cache/0]).

-callback type_name() -> atom().
-callback bridge_types() -> [atom()].
-callback resource_callback_module() -> atom().
-callback schema_module() -> atom().
-callback config_schema() -> term().
-callback api_schema([char()]) -> term().
%% Optional callback that should return a module with an exported
%% connector_config/2 function. If present this function will be used to
%% transfrom the connector configuration. See the callback connector_config/2
%% in emqx_connector_resource for more information.
-callback config_transform_module() -> atom().

-optional_callbacks([
    config_transform_module/0
]).

%% ====================================================================
%% HardCoded list of info modules for connectors
%% TODO: Remove this list once we have made sure that all relevants
%% apps are loaded before this module is called.
%% ====================================================================

-if(?EMQX_RELEASE_EDITION == ee).
hard_coded_connector_info_modules_ee() ->
    [
        emqx_bridge_dynamo_connector_info,
        emqx_bridge_azure_event_hub_connector_info,
        emqx_bridge_confluent_producer_connector_info,
        emqx_bridge_gcp_pubsub_consumer_connector_info,
        emqx_bridge_gcp_pubsub_producer_connector_info,
        emqx_bridge_hstreamdb_connector_info,
        emqx_bridge_kafka_consumer_connector_info,
        emqx_bridge_kafka_connector_info
    ].
-else.
hard_coded_connector_info_modules_ee() ->
    [].
-endif.

hard_coded_connector_info_modules_common() ->
    [emqx_bridge_http_connector_info].

hard_coded_connector_info_modules() ->
    hard_coded_connector_info_modules_common() ++ hard_coded_connector_info_modules_ee().

%% --------------------------------------------------------------------
%% Atom macros to avoid typos
%% --------------------------------------------------------------------

-define(emqx_connector_info_modules, emqx_connector_info_modules).
-define(connector_type_to_info_module, connector_type_to_info_module).
-define(connector_type_names, connector_type_names).
-define(connector_type_to_bridge_types, connector_type_to_bridge_types).
-define(connector_type_to_resource_callback_module, connector_type_to_resource_callback_module).
-define(connector_type_to_schema_module, connector_type_to_schema_module).
-define(connector_type_to_config_schema, connector_type_to_config_schema).
-define(connector_type_to_config_transform_module, connector_type_to_config_transform_module).

%% ====================================================================
%% API
%% ====================================================================

connector_types() ->
    InfoMap = info_map(),
    TypeNamesMap = maps:get(?connector_type_names, InfoMap),
    maps:keys(TypeNamesMap).

bridge_types(ConnectorType) ->
    InfoMap = info_map(),
    ConToBridges = maps:get(?connector_type_to_bridge_types, InfoMap),
    maps:get(ConnectorType, ConToBridges).

resource_callback_module(ConnectorType) ->
    InfoMap = info_map(),
    ConToCallbackMod = maps:get(?connector_type_to_resource_callback_module, InfoMap),
    maps:get(ConnectorType, ConToCallbackMod).

schema_module(ConnectorType) ->
    InfoMap = info_map(),
    ConToSchemaMod = maps:get(?connector_type_to_schema_module, InfoMap),
    maps:get(ConnectorType, ConToSchemaMod).

config_schema(ConnectorType) ->
    InfoMap = info_map(),
    ConToConfSchema = maps:get(?connector_type_to_config_schema, InfoMap),
    maps:get(ConnectorType, ConToConfSchema).

api_schema(ConnectorType, Method) ->
    InfoMod = info_module(ConnectorType),
    InfoMod:api_schema(Method).

config_transform_module(ConnectorType) ->
    InfoMap = info_map(),
    ConToConfTransMod = maps:get(?connector_type_to_config_transform_module, InfoMap),
    maps:get(ConnectorType, ConToConfTransMod, undefined).

%% ====================================================================
%% Internal functions for building the info map and accessing it
%% ====================================================================

info_module(ConnectorType) ->
    InfoMap = info_map(),
    ConToInfoMod = maps:get(?connector_type_to_info_module, InfoMap),
    maps:get(ConnectorType, ConToInfoMod).

internal_emqx_connector_persistent_term_info_key() ->
    ?FUNCTION_NAME.

info_map() ->
    case persistent_term:get(internal_emqx_connector_persistent_term_info_key(), not_found) of
        not_found ->
            build_cache();
        InfoMap ->
            InfoMap
    end.

build_cache() ->
    InfoModules = connector_info_modules(),
    InfoMap =
        lists:foldl(
            fun(Module, InfoMapSoFar) ->
                ModuleInfoMap = get_info_map(Module),
                emqx_utils_maps:deep_merge(InfoMapSoFar, ModuleInfoMap)
            end,
            initial_info_map(),
            InfoModules
        ),
    %% Update the persistent term with the new info map
    persistent_term:put(internal_emqx_connector_persistent_term_info_key(), InfoMap),
    InfoMap.

clean_cache() ->
    persistent_term:erase(internal_emqx_connector_persistent_term_info_key()).

connector_info_modules() ->
    InfoModules = [
        connector_info_modules(App)
     || {App, _, _} <- application:loaded_applications()
    ],
    lists:usort(lists:flatten(InfoModules) ++ hard_coded_connector_info_modules()).

connector_info_modules(App) ->
    case application:get_env(App, ?emqx_connector_info_modules) of
        {ok, Modules} ->
            Modules;
        _ ->
            []
    end.

initial_info_map() ->
    #{
        ?connector_type_names => #{},
        ?connector_type_to_info_module => #{},
        ?connector_type_to_bridge_types => #{},
        ?connector_type_to_resource_callback_module => #{},
        ?connector_type_to_schema_module => #{},
        ?connector_type_to_config_schema => #{},
        ?connector_type_to_config_transform_module => #{}
    }.

get_info_map(Module) ->
    %% Force the module to get loaded
    _ = code:ensure_loaded(Module),
    Type = Module:type_name(),
    ConfigTransformModule =
        case erlang:function_exported(Module, config_transform_module, 0) of
            true ->
                Module:config_transform_module();
            false ->
                undefined
        end,
    #{
        ?connector_type_names => #{
            Type => true
        },
        ?connector_type_to_info_module => #{
            Type => Module
        },
        ?connector_type_to_bridge_types => #{
            Type => Module:bridge_types()
        },
        ?connector_type_to_resource_callback_module => #{
            Type => Module:resource_callback_module()
        },
        ?connector_type_to_schema_module => #{
            Type => Module:schema_module()
        },
        ?connector_type_to_config_schema => #{
            Type => Module:config_schema()
        },
        ?connector_type_to_config_transform_module => #{
            Type => ConfigTransformModule
        }
    }.
