%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_ee_schema).

-if(?EMQX_RELEASE_EDITION == ee).

-export([
    resource_type/1,
    connector_impl_module/1
]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    api_schemas/1,
    fields/1,
    examples/1
]).

resource_type(Type) when is_binary(Type) ->
    resource_type(binary_to_atom(Type, utf8));
%% We use AEH's Kafka interface.
resource_type(azure_event_hub_producer) ->
    emqx_bridge_kafka_impl_producer;
resource_type(confluent_producer) ->
    emqx_bridge_kafka_impl_producer;
resource_type(gcp_pubsub_producer) ->
    emqx_bridge_gcp_pubsub_impl_producer;
resource_type(kafka_producer) ->
    emqx_bridge_kafka_impl_producer;
resource_type(syskeeper_forwarder) ->
    emqx_bridge_syskeeper_connector;
resource_type(syskeeper_proxy) ->
    emqx_bridge_syskeeper_proxy_server;
resource_type(Type) ->
    error({unknown_connector_type, Type}).

%% For connectors that need to override connector configurations.
connector_impl_module(ConnectorType) when is_binary(ConnectorType) ->
    connector_impl_module(binary_to_atom(ConnectorType, utf8));
connector_impl_module(azure_event_hub_producer) ->
    emqx_bridge_azure_event_hub;
connector_impl_module(confluent_producer) ->
    emqx_bridge_confluent_producer;
connector_impl_module(_ConnectorType) ->
    undefined.

fields(connectors) ->
    connector_structs().

connector_structs() ->
    [
        {azure_event_hub_producer,
            mk(
                hoconsc:map(name, ref(emqx_bridge_azure_event_hub, "config_connector")),
                #{
                    desc => <<"Azure Event Hub Connector Config">>,
                    required => false
                }
            )},
        {confluent_producer,
            mk(
                hoconsc:map(name, ref(emqx_bridge_confluent_producer, "config_connector")),
                #{
                    desc => <<"Confluent Connector Config">>,
                    required => false
                }
            )},
        {gcp_pubsub_producer,
            mk(
                hoconsc:map(name, ref(emqx_bridge_gcp_pubsub_producer_schema, "config_connector")),
                #{
                    desc => <<"GCP PubSub Producer Connector Config">>,
                    required => false
                }
            )},
        {kafka_producer,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kafka, "config_connector")),
                #{
                    desc => <<"Kafka Connector Config">>,
                    required => false
                }
            )},
        {syskeeper_forwarder,
            mk(
                hoconsc:map(name, ref(emqx_bridge_syskeeper_connector, config)),
                #{
                    desc => <<"Syskeeper Connector Config">>,
                    required => false
                }
            )},
        {syskeeper_proxy,
            mk(
                hoconsc:map(name, ref(emqx_bridge_syskeeper_proxy, config)),
                #{
                    desc => <<"Syskeeper Proxy Connector Config">>,
                    required => false
                }
            )}
    ].

examples(Method) ->
    MergeFun =
        fun(Example, Examples) ->
            maps:merge(Examples, Example)
        end,
    Fun =
        fun(Module, Examples) ->
            ConnectorExamples = erlang:apply(Module, connector_examples, [Method]),
            lists:foldl(MergeFun, Examples, ConnectorExamples)
        end,
    lists:foldl(Fun, #{}, schema_modules()).

schema_modules() ->
    [
        emqx_bridge_azure_event_hub,
        emqx_bridge_confluent_producer,
        emqx_bridge_gcp_pubsub_producer_schema,
        emqx_bridge_kafka,
        emqx_bridge_syskeeper_connector,
        emqx_bridge_syskeeper_proxy
    ].

api_schemas(Method) ->
    [
        %% We need to map the `type' field of a request (binary) to a
        %% connector schema module.
        api_ref(
            emqx_bridge_azure_event_hub, <<"azure_event_hub_producer">>, Method ++ "_connector"
        ),
        api_ref(
            emqx_bridge_confluent_producer, <<"confluent_producer">>, Method ++ "_connector"
        ),
        api_ref(emqx_bridge_kafka, <<"kafka_producer">>, Method ++ "_connector"),
        api_ref(
            emqx_bridge_gcp_pubsub_producer_schema,
            <<"gcp_pubsub_producer">>,
            Method ++ "_connector"
        ),
        api_ref(emqx_bridge_syskeeper_connector, <<"syskeeper_forwarder">>, Method),
        api_ref(emqx_bridge_syskeeper_proxy, <<"syskeeper_proxy">>, Method)
    ].

api_ref(Module, Type, Method) ->
    {Type, ref(Module, Method)}.

-else.

-endif.
