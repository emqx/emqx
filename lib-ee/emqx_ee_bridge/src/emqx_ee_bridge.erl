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
        ref(emqx_ee_bridge_gcp_pubsub, Method),
        ref(emqx_ee_bridge_kafka, Method),
        ref(emqx_ee_bridge_mysql, Method),
        ref(emqx_ee_bridge_mongodb, Method ++ "_rs"),
        ref(emqx_ee_bridge_mongodb, Method ++ "_sharded"),
        ref(emqx_ee_bridge_mongodb, Method ++ "_single"),
        ref(emqx_ee_bridge_hstreamdb, Method),
        ref(emqx_ee_bridge_influxdb, Method ++ "_api_v1"),
        ref(emqx_ee_bridge_influxdb, Method ++ "_api_v2")
    ].

schema_modules() ->
    [
        emqx_ee_bridge_kafka,
        emqx_ee_bridge_hstreamdb,
        emqx_ee_bridge_gcp_pubsub,
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
resource_type(kafka) -> emqx_bridge_impl_kafka;
resource_type(hstreamdb) -> emqx_ee_connector_hstreamdb;
resource_type(gcp_pubsub) -> emqx_ee_bridge_gcp_pubsub_resource;
resource_type(mongodb_rs) -> emqx_connector_mongo;
resource_type(mongodb_sharded) -> emqx_connector_mongo;
resource_type(mongodb_single) -> emqx_connector_mongo;
resource_type(mysql) -> emqx_connector_mysql;
resource_type(influxdb_api_v1) -> emqx_ee_connector_influxdb;
resource_type(influxdb_api_v2) -> emqx_ee_connector_influxdb.

fields(bridges) ->
    [
        {kafka,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_kafka, "config")),
                #{
                    desc => <<"Kafka Bridge Config">>,
                    required => false
                }
            )},
        {hstreamdb,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_hstreamdb, "config")),
                #{
                    desc => <<"HStreamDB Bridge Config">>,
                    required => false
                }
            )},
        {gcp_pubsub,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_gcp_pubsub, "config")),
                #{
                    desc => <<"EMQX Enterprise Config">>,
                    required => false
                }
            )},
        {mysql,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_mysql, "config")),
                #{
                    desc => <<"MySQL Bridge Config">>,
                    required => false
                }
            )}
    ] ++ mongodb_structs() ++ influxdb_structs().

mongodb_structs() ->
    [
        {Type,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_mongodb, Type)),
                #{
                    desc => <<"MongoDB Bridge Config">>,
                    required => false
                }
            )}
     || Type <- [mongodb_rs, mongodb_sharded, mongodb_single]
    ].

influxdb_structs() ->
    [
        {Protocol,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_influxdb, Protocol)),
                #{
                    desc => <<"InfluxDB Bridge Config">>,
                    required => false
                }
            )}
     || Protocol <- [
            influxdb_api_v1,
            influxdb_api_v2
        ]
    ].
