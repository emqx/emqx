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
-module(emqx_bridge_rabbitmq_pubsub_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([roots/0, fields/1, desc/1, namespace/0]).

-export([
    bridge_v2_examples/1,
    source_examples/1
]).

-define(ACTION_TYPE, rabbitmq).
-define(SOURCE_TYPE, rabbitmq).
-define(CONNECTOR_SCHEMA, emqx_bridge_rabbitmq_connector_schema).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> "bridge_rabbitmq".

roots() -> [].

fields(action) ->
    {rabbitmq,
        ?HOCON(
            ?MAP(name, ?R_REF(publisher_action)),
            #{
                desc => <<"RabbitMQ Action Config">>,
                required => false
            }
        )};
fields(publisher_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        ?HOCON(
            ?R_REF(action_parameters),
            #{
                required => true,
                desc => ?DESC(action_parameters)
            }
        ),
        #{resource_opts_ref => ?R_REF(action_resource_opts)}
    );
fields(action_parameters) ->
    [
        {wait_for_publish_confirmations,
            hoconsc:mk(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(?CONNECTOR_SCHEMA, "wait_for_publish_confirmations")
                }
            )},
        {publish_confirmation_timeout,
            hoconsc:mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC(?CONNECTOR_SCHEMA, "timeout")
                }
            )},
        {exchange,
            hoconsc:mk(
                emqx_schema:template(),
                #{
                    required => true,
                    desc => ?DESC(?CONNECTOR_SCHEMA, "exchange")
                }
            )},
        {routing_key,
            hoconsc:mk(
                emqx_schema:template(),
                #{
                    required => true,
                    desc => ?DESC(?CONNECTOR_SCHEMA, "routing_key")
                }
            )},
        {delivery_mode,
            hoconsc:mk(
                hoconsc:enum([non_persistent, persistent]),
                #{
                    default => non_persistent,
                    desc => ?DESC(?CONNECTOR_SCHEMA, "delivery_mode")
                }
            )},
        {payload_template,
            hoconsc:mk(
                emqx_schema:template(),
                #{
                    default => <<"">>,
                    desc => ?DESC(?CONNECTOR_SCHEMA, "payload_template")
                }
            )}
    ];
fields(source) ->
    {rabbitmq,
        ?HOCON(
            hoconsc:map(name, ?R_REF(subscriber_source)),
            #{
                desc => <<"MQTT Subscriber Source Config">>,
                required => false
            }
        )};
fields(subscriber_source) ->
    emqx_bridge_v2_schema:make_consumer_action_schema(
        ?HOCON(
            ?R_REF(source_parameters),
            #{
                required => true,
                desc => ?DESC("source_parameters")
            }
        )
    );
fields(source_parameters) ->
    [
        {queue,
            ?HOCON(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("source_queue")
                }
            )},
        {wait_for_publish_confirmations,
            hoconsc:mk(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(?CONNECTOR_SCHEMA, "wait_for_publish_confirmations")
                }
            )},
        {no_ack,
            ?HOCON(
                boolean(),
                #{
                    required => false,
                    default => true,
                    desc => ?DESC("source_no_ack")
                }
            )}
    ];
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields();
fields(source_resource_opts) ->
    emqx_bridge_v2_schema:source_resource_opts_fields();
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(publisher_action));
fields(Field) when
    Field == "get_source";
    Field == "post_source";
    Field == "put_source"
->
    emqx_bridge_v2_schema:api_fields(Field, ?SOURCE_TYPE, fields(subscriber_source));
fields(What) ->
    error({?MODULE, missing_field_handler, What}).
%% v2: api schema
%% The parameter equals to
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
desc(source_parameters) ->
    ?DESC(source_parameters);
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for WebHook using `", string:to_upper(Method), "` method."];
desc("http_action") ->
    ?DESC("desc_config");
desc("parameters_opts") ->
    ?DESC("config_parameters_opts");
desc(publisher_action) ->
    ?DESC(publisher_action);
desc(subscriber_source) ->
    ?DESC(subscriber_source);
desc(_) ->
    undefined.

bridge_v2_examples(Method) ->
    [
        #{
            <<"rabbitmq">> => #{
                summary => <<"RabbitMQ Producer Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method,
                    _ActionType = ?ACTION_TYPE,
                    _ConnectorType = rabbitmq,
                    #{
                        parameters => #{
                            wait_for_publish_confirmations => true,
                            publish_confirmation_timeout => <<"30s">>,
                            exchange => <<"test_exchange">>,
                            routing_key => <<"/">>,
                            delivery_mode => <<"non_persistent">>,
                            payload_template => <<"${.payload}">>
                        }
                    }
                )
            }
        }
    ].

source_examples(Method) ->
    [
        #{
            <<"rabbitmq">> => #{
                summary => <<"RabbitMQ Subscriber Source">>,
                value => emqx_bridge_v2_schema:source_values(
                    Method,
                    _SourceType = ?SOURCE_TYPE,
                    _ConnectorType = rabbitmq,
                    #{
                        parameters => #{
                            queue => <<"test_queue">>,
                            no_ack => true
                        }
                    }
                )
            }
        }
    ].
