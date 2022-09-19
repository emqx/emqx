%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    api_schemas/1,
    examples/1,
    resource_type/1,
    fields/1
]).

api_schemas(Method) ->
    [
        ref(emqx_ee_bridge_mysql, Method),
        ref(emqx_ee_bridge_mongodb, Method ++ "_rs"),
        ref(emqx_ee_bridge_mongodb, Method ++ "_sharded"),
        ref(emqx_ee_bridge_mongodb, Method ++ "_single"),
        ref(emqx_ee_bridge_hstreamdb, Method),
        %% ref(emqx_ee_bridge_influxdb, Method ++ "_udp"),
        ref(emqx_ee_bridge_influxdb, Method ++ "_api_v1"),
        ref(emqx_ee_bridge_influxdb, Method ++ "_api_v2")
    ].

schema_modules() ->
    [
        emqx_ee_bridge_hstreamdb,
        emqx_ee_bridge_influxdb,
        emqx_ee_bridge_mongodb,
        emqx_ee_bridge_mysql
    ].

examples(Method) ->
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
resource_type(hstreamdb) -> emqx_ee_connector_hstreamdb;
resource_type(mongodb_rs) -> emqx_connector_mongo;
resource_type(mongodb_sharded) -> emqx_connector_mongo;
resource_type(mongodb_single) -> emqx_connector_mongo;
resource_type(mysql) -> emqx_connector_mysql;
resource_type(influxdb_udp) -> emqx_ee_connector_influxdb;
resource_type(influxdb_api_v1) -> emqx_ee_connector_influxdb;
resource_type(influxdb_api_v2) -> emqx_ee_connector_influxdb.

fields(bridges) ->
    [
        {hstreamdb,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_hstreamdb, "config")),
                #{desc => <<"EMQX Enterprise Config">>}
            )},
        {mysql,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_mysql, "config")),
                #{desc => <<"EMQX Enterprise Config">>}
            )}
    ] ++ fields(mongodb) ++ fields(influxdb);
fields(mongodb) ->
    [
        {Type,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_mongodb, Type)),
                #{desc => <<"EMQX Enterprise Config">>}
            )}
     || Type <- [mongodb_rs, mongodb_sharded, mongodb_single]
    ];
fields(influxdb) ->
    [
        {Protocol,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_influxdb, Protocol)),
                #{desc => <<"EMQX Enterprise Config">>}
            )}
     || Protocol <- [
            %% influxdb_udp,
            influxdb_api_v1,
            influxdb_api_v2
        ]
    ].
