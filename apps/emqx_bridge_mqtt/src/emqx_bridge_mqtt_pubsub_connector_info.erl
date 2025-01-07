%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_pubsub_connector_info).

-behaviour(emqx_connector_info).

-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

type_name() ->
    mqtt.

bridge_types() ->
    [mqtt].

resource_callback_module() ->
    emqx_bridge_mqtt_connector.

config_schema() ->
    {mqtt,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_mqtt_connector_schema, "config_connector")),
            #{
                desc => <<"MQTT Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_mqtt_connector_schema.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_mqtt_connector_schema, <<"mqtt">>, Method ++ "_connector"
    ).
