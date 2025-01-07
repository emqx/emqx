%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_rabbitmq_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_config_to_connector_config/1,
    bridge_v1_config_to_action_config/2,
    is_source/0,
    is_action/0
]).

-define(SCHEMA_MODULE, emqx_bridge_rabbitmq_pubsub_schema).
-import(emqx_utils_conv, [bin/1]).

bridge_v1_type_name() -> rabbitmq.

action_type_name() -> rabbitmq.

connector_type_name() -> rabbitmq.

schema_module() -> ?SCHEMA_MODULE.

is_source() -> true.
is_action() -> true.

bridge_v1_config_to_connector_config(BridgeV1Config) ->
    ActionTopLevelKeys = schema_keys(?SCHEMA_MODULE:fields(publisher_action)),
    ActionParametersKeys = schema_keys(?SCHEMA_MODULE:fields(action_parameters)),
    ActionKeys = ActionTopLevelKeys ++ ActionParametersKeys,
    ConnectorTopLevelKeys = schema_keys(
        emqx_bridge_rabbitmq_connector_schema:fields("config_connector")
    ),
    ConnectorKeys = (maps:keys(BridgeV1Config) -- (ActionKeys -- ConnectorTopLevelKeys)),
    ConnectorConfig0 = maps:with(ConnectorKeys, BridgeV1Config),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_connector_schema:project_to_connector_resource_opts/1,
        ConnectorConfig0
    ).

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    ActionTopLevelKeys = schema_keys(?SCHEMA_MODULE:fields(publisher_action)),
    ActionParametersKeys = schema_keys(?SCHEMA_MODULE:fields(action_parameters)),
    ActionKeys = ActionTopLevelKeys ++ ActionParametersKeys,
    ActionConfig0 = make_config_map(ActionKeys, ActionParametersKeys, BridgeV1Config),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_bridge_v2_schema:project_to_actions_resource_opts/1,
        ActionConfig0#{<<"connector">> => ConnectorName}
    ).

schema_keys(Schema) ->
    [bin(Key) || {Key, _} <- Schema].

make_config_map(PickKeys, IndentKeys, Config) ->
    Conf0 = maps:with(PickKeys, Config),
    emqx_utils_maps:indent(<<"parameters">>, IndentKeys, Conf0).
