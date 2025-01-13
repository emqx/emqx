%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_http_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    connector_action_config_to_bridge_v1_config/2,
    bridge_v1_config_to_action_config/2,
    bridge_v1_config_to_connector_config/1
]).

-define(REMOVED_KEYS, [<<"direction">>]).
-define(ACTION_KEYS, [<<"local_topic">>, <<"resource_opts">>]).
-define(PARAMETER_KEYS, [<<"body">>, <<"max_retries">>, <<"method">>, <<"request_timeout">>]).

bridge_v1_type_name() -> webhook.

action_type_name() -> http.

connector_type_name() -> http.

schema_module() -> emqx_bridge_http_schema.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    BridgeV1Config1 = maps:without([<<"connector">>, <<"last_modified_at">>], ActionConfig),
    %% Move parameters to the top level
    ParametersMap1 = maps:get(<<"parameters">>, BridgeV1Config1, #{}),
    ParametersMap2 = maps:without([<<"path">>, <<"headers">>], ParametersMap1),
    BridgeV1Config2 = maps:remove(<<"parameters">>, BridgeV1Config1),
    BridgeV1Config3 = emqx_utils_maps:deep_merge(BridgeV1Config2, ParametersMap2),
    BridgeV1Config4 = emqx_utils_maps:deep_merge(ConnectorConfig, BridgeV1Config3),

    Url = maps:get(<<"url">>, ConnectorConfig),
    Path = maps:get(<<"path">>, ParametersMap1, <<>>),

    Headers1 = maps:get(<<"headers">>, ConnectorConfig, #{}),
    Headers2 = maps:get(<<"headers">>, ParametersMap1, #{}),

    Url1 =
        case Path of
            <<>> -> Url;
            _ -> iolist_to_binary(emqx_bridge_http_connector:join_paths(Url, Path))
        end,

    BridgeV1Config4#{
        <<"headers">> => maps:merge(Headers1, Headers2),
        <<"url">> => Url1
    }.

bridge_v1_config_to_connector_config(BridgeV1Conf) ->
    %% To satisfy the emqx_bridge_api_SUITE:t_http_crud_apis/1
    ok = validate_webhook_url(maps:get(<<"url">>, BridgeV1Conf, undefined)),
    ConnectorConfig0 = maps:without(?REMOVED_KEYS ++ ?ACTION_KEYS ++ ?PARAMETER_KEYS, BridgeV1Conf),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_connector_schema:project_to_connector_resource_opts/1,
        ConnectorConfig0
    ).

bridge_v1_config_to_action_config(BridgeV1Conf, ConnectorName) ->
    Parameters = maps:with(?PARAMETER_KEYS, BridgeV1Conf),
    Parameters1 = Parameters#{<<"path">> => <<>>, <<"headers">> => #{}},
    CommonKeys = [<<"enable">>, <<"description">>],
    ActionConfig0 = maps:with(?ACTION_KEYS ++ CommonKeys, BridgeV1Conf),
    ActionConfig = emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_bridge_v2_schema:project_to_actions_resource_opts/1,
        ActionConfig0
    ),
    ActionConfig#{<<"parameters">> => Parameters1, <<"connector">> => ConnectorName}.

%%--------------------------------------------------------------------
%% helpers

validate_webhook_url(undefined) ->
    throw(#{
        kind => validation_error,
        reason => required_field,
        required_field => <<"url">>
    });
validate_webhook_url(Url) ->
    %% parse_url throws if the URL is invalid
    {_RequestBase, _Path} = emqx_connector_resource:parse_url(Url),
    ok.
