%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_mqtt_pubsub_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0]).

-export([
    bridge_v2_examples/1,
    source_examples/1,
    conn_bridge_examples/1
]).

-define(ACTION_TYPE, mqtt).
-define(SOURCE_TYPE, mqtt).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> "bridge_mqtt_publisher".

roots() -> [].

fields(action) ->
    {mqtt,
        mk(
            hoconsc:map(name, ref(?MODULE, "mqtt_publisher_action")),
            #{
                desc => <<"MQTT Publisher Action Config">>,
                required => false
            }
        )};
fields("mqtt_publisher_action") ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("action_parameters")
            }
        ),
        #{resource_opts_ref => ref(?MODULE, action_resource_opts)}
    );
fields(action_parameters) ->
    [
        %% for backwards compatibility
        {local,
            mk(
                ref(emqx_bridge_mqtt_connector_schema, "egress_local"),
                #{
                    default => #{},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
        | emqx_bridge_mqtt_connector_schema:fields("egress_remote")
    ];
fields(source) ->
    {mqtt,
        mk(
            hoconsc:map(name, ref(?MODULE, "mqtt_subscriber_source")),
            #{
                desc => <<"MQTT Subscriber Source Config">>,
                required => false
            }
        )};
fields("mqtt_subscriber_source") ->
    emqx_bridge_v2_schema:make_consumer_action_schema(
        mk(
            ref(?MODULE, ingress_parameters),
            #{
                required => true,
                desc => ?DESC("source_parameters")
            }
        ),
        #{resource_opts_ref => ref(?MODULE, source_resource_opts)}
    );
fields(ingress_parameters) ->
    [
        %% for backwards compatibility
        {local,
            mk(
                ref(emqx_bridge_mqtt_connector_schema, "ingress_local"),
                #{
                    default => #{},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {no_local,
            mk(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC("source_no_local")
                }
            )}
        | emqx_bridge_mqtt_connector_schema:fields("ingress_remote")
    ];
fields(action_resource_opts) ->
    UnsupportedOpts = [enable_batch, batch_size, batch_time],
    lists:filter(
        fun({K, _V}) -> not lists:member(K, UnsupportedOpts) end,
        emqx_bridge_v2_schema:action_resource_opts_fields()
    );
fields(source_resource_opts) ->
    emqx_bridge_v2_schema:source_resource_opts_fields();
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields("mqtt_publisher_action"));
fields(Field) when
    Field == "get_source";
    Field == "post_source";
    Field == "put_source"
->
    emqx_bridge_v2_schema:api_fields(Field, ?SOURCE_TYPE, fields("mqtt_subscriber_source"));
fields(What) ->
    error({?MODULE, missing_field_handler, What}).
%% v2: api schema
%% The parameter equls to
%%   `get_bridge_v2`, `post_bridge_v2`, `put_bridge_v2` from emqx_bridge_v2_schema:api_schema/1
%%   `get_connector`, `post_connector`, `put_connector` from emqx_connector_schema:api_schema/1
%%--------------------------------------------------------------------
%% v1/v2

desc("config") ->
    ?DESC("desc_config");
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(source_resource_opts) ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(action_parameters) ->
    ?DESC(action_parameters);
desc(ingress_parameters) ->
    ?DESC(ingress_parameters);
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for WebHook using `", string:to_upper(Method), "` method."];
desc("http_action") ->
    ?DESC("desc_config");
desc("parameters_opts") ->
    ?DESC("config_parameters_opts");
desc("mqtt_publisher_action") ->
    ?DESC("mqtt_publisher_action");
desc("mqtt_subscriber_source") ->
    ?DESC("mqtt_subscriber_source");
desc(_) ->
    undefined.

bridge_v2_examples(Method) ->
    [
        #{
            <<"mqtt">> => #{
                summary => <<"MQTT Producer Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method,
                    _ActionType = mqtt,
                    _ConnectorType = mqtt,
                    #{
                        parameters => #{
                            topic => <<"remote/topic">>,
                            qos => 2,
                            retain => false,
                            payload => <<"${.payload}">>
                        }
                    }
                )
            }
        }
    ].

source_examples(Method) ->
    [
        #{
            <<"mqtt">> => #{
                summary => <<"MQTT Subscriber Source">>,
                value => emqx_bridge_v2_schema:source_values(
                    Method,
                    _SourceType = mqtt,
                    _ConnectorType = mqtt,
                    #{
                        parameters => #{
                            topic => <<"remote/topic">>,
                            qos => 1
                        }
                    }
                )
            }
        }
    ].

conn_bridge_examples(Method) ->
    [
        #{
            <<"mqtt">> => #{
                summary => <<"MQTT Producer Action">>,
                value => emqx_bridge_api:mqtt_v1_example(Method)
            }
        }
    ].
