%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_v2_enterprise).

-if(?EMQX_RELEASE_EDITION == ee).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    api_schemas/1,
    examples/1,
    fields/1
]).

examples(Method) ->
    MergeFun =
        fun(Example, Examples) ->
            maps:merge(Examples, Example)
        end,
    Fun =
        fun(Module, Examples) ->
            ConnectorExamples = erlang:apply(Module, bridge_v2_examples, [Method]),
            lists:foldl(MergeFun, Examples, ConnectorExamples)
        end,
    lists:foldl(Fun, #{}, schema_modules()).

schema_modules() ->
    [
        emqx_bridge_kafka,
        emqx_bridge_azure_event_hub
    ].

fields(bridges_v2) ->
    bridge_v2_structs().

bridge_v2_structs() ->
    [
        {kafka,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kafka, kafka_producer_action)),
                #{
                    desc => <<"Kafka Producer Bridge V2 Config">>,
                    required => false
                }
            )},
        {azure_event_hub,
            mk(
                hoconsc:map(name, ref(emqx_bridge_azure_event_hub, bridge_v2)),
                #{
                    desc => <<"Azure Event Hub Bridge V2 Config">>,
                    required => false
                }
            )}
    ].

api_schemas(Method) ->
    [
        %% We need to map the `type' field of a request (binary) to a
        %% connector schema module.
        %% TODO: rename this to `kafka_producer' after alias support is added
        %% to hocon; keeping this as just `kafka' for backwards compatibility.
        api_ref(emqx_bridge_kafka, <<"kafka">>, Method ++ "_bridge_v2"),
        api_ref(emqx_bridge_azure_event_hub, <<"azure_event_hub">>, Method ++ "_bridge_v2")
    ].

api_ref(Module, Type, Method) ->
    {Type, ref(Module, Method)}.

-else.

-endif.
