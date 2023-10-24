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

resource_type(Type) when is_binary(Type) -> resource_type(binary_to_atom(Type, utf8));
resource_type(kafka) -> emqx_bridge_kafka_impl_producer;
%% We use AEH's Kafka interface.
resource_type(azure_event_hub) -> emqx_bridge_kafka_impl_producer.

%% For connectors that need to override connector configurations.
connector_impl_module(ConnectorType) when is_binary(ConnectorType) ->
    connector_impl_module(binary_to_atom(ConnectorType, utf8));
connector_impl_module(azure_event_hub) ->
    emqx_bridge_azure_event_hub;
connector_impl_module(_ConnectorType) ->
    undefined.

fields(connectors) ->
    connector_structs().

connector_structs() ->
    [
        {kafka,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kafka, "config")),
                #{
                    desc => <<"Kafka Connector Config">>,
                    required => false
                }
            )},
        {azure_event_hub,
            mk(
                hoconsc:map(name, ref(emqx_bridge_azure_event_hub, "config_connector")),
                #{
                    desc => <<"Azure Event Hub Connector Config">>,
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
        emqx_bridge_kafka,
        emqx_bridge_azure_event_hub
    ].

api_schemas(Method) ->
    [
        %% We need to map the `type' field of a request (binary) to a
        %% connector schema module.
        api_ref(emqx_bridge_kafka, <<"kafka">>, Method ++ "_connector"),
        api_ref(emqx_bridge_azure_event_hub, <<"azure_event_hub">>, Method ++ "_connector")
    ].

api_ref(Module, Type, Method) ->
    {Type, ref(Module, Method)}.

-else.

-endif.
