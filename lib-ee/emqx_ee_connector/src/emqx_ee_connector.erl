%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_connector).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    fields/1,
    connector_examples/1,
    api_schemas/1
]).

api_schemas(Method) ->
    [
        ref(emqx_ee_connector_hstream, Method),
        ref(emqx_ee_connector_influxdb, Method ++ "_udp"),
        ref(emqx_ee_connector_influxdb, Method ++ "_api_v1"),
        ref(emqx_ee_connector_influxdb, Method ++ "_api_v2")
    ].

fields(connectors) ->
    [
        {hstreamdb,
            mk(
                hoconsc:map(name, ref(emqx_ee_connector_hstream, config)),
                #{desc => <<"EMQX Enterprise Config">>}
            )}
    ];
% ] ++ fields(influxdb);
fields(influxdb) ->
    [
        {
            influxdb,
            mk(hoconsc:map(name, ref(emqx_ee_connector_influxdb, influxdb_udp)), #{
                desc => <<"EMQX Enterprise Config">>
            })
        }
        %  || Protocol <- [influxdb_udp, influxdb_api_v1, influxdb_api_v2]
    ].

connector_examples(Method) ->
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
    [emqx_ee_connector_hstream, emqx_ee_connector_influxdb].
