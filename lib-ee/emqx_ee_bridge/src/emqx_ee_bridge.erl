%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    api_schemas/1,
    conn_bridge_examples/1,
    resource_type/1,
    fields/1
]).

api_schemas(Method) ->
    [
        ref(emqx_ee_bridge_hstream, Method),
        ref(emqx_ee_bridge_mysql, Method),
        ref(emqx_ee_bridge_influxdb, Method ++ "_udp"),
        ref(emqx_ee_bridge_influxdb, Method ++ "_api_v1"),
        ref(emqx_ee_bridge_influxdb, Method ++ "_api_v2")
    ].

schema_modules() ->
    [
        emqx_ee_bridge_hstream,
        emqx_ee_bridge_influxdb,
        emqx_ee_bridge_mysql
    ].

conn_bridge_examples(Method) ->
    MergeFun =
        fun(Example, Examples) ->
            maps:merge(Examples, Example)
        end,
    Fun =
        fun(Module, Examples) ->
            ConnectorExamples = erlang:apply(Module, conn_bridge_examples, [Method]),
            lists:foldl(MergeFun, Examples, ConnectorExamples)
        end,
    lists:foldl(Fun, #{}, schema_modules()).

resource_type(Type) when is_binary(Type) -> resource_type(binary_to_atom(Type, utf8));
resource_type(hstreamdb) -> emqx_ee_connector_hstream;
resource_type(mysql) -> emqx_ee_connector_mysql;
resource_type(influxdb_udp) -> emqx_ee_connector_influxdb;
resource_type(influxdb_api_v1) -> emqx_ee_connector_influxdb;
resource_type(influxdb_api_v2) -> emqx_ee_connector_influxdb.

fields(bridges) ->
    [
        {hstreamdb,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_hstream, "config")),
                #{desc => <<"EMQX Enterprise Config">>}
            )},
        {mysql,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_mysql, "config")),
                #{desc => <<"EMQX Enterprise Config">>}
            )}
    ] ++ fields(influxdb);
fields(influxdb) ->
    [
        {Protocol,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_influxdb, Protocol)),
                #{desc => <<"EMQX Enterprise Config">>}
            )}
     || Protocol <- [influxdb_udp, influxdb_api_v1, influxdb_api_v2]
    ].
