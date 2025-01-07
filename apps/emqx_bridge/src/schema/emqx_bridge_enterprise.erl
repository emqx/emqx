%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_enterprise).

-if(?EMQX_RELEASE_EDITION == ee).

-include_lib("hocon/include/hoconsc.hrl").
-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    api_schemas/1,
    examples/1,
    resource_type/1,
    bridge_impl_module/1,
    fields/1,
    namespace/0
]).

api_schemas(Method) ->
    [
        %% We need to map the `type' field of a request (binary) to a
        %% bridge schema module.
        api_ref(emqx_bridge_gcp_pubsub, <<"gcp_pubsub">>, Method ++ "_producer"),
        api_ref(emqx_bridge_gcp_pubsub, <<"gcp_pubsub_consumer">>, Method ++ "_consumer"),
        api_ref(emqx_bridge_kafka, <<"kafka_consumer">>, Method ++ "_consumer"),
        api_ref(emqx_bridge_kafka, <<"kafka">>, Method ++ "_producer"),
        api_ref(emqx_bridge_cassandra, <<"cassandra">>, Method),
        api_ref(emqx_bridge_mysql, <<"mysql">>, Method),
        api_ref(emqx_bridge_pgsql, <<"pgsql">>, Method),
        api_ref(emqx_bridge_mongodb, <<"mongodb_rs">>, Method ++ "_rs"),
        api_ref(emqx_bridge_mongodb, <<"mongodb_sharded">>, Method ++ "_sharded"),
        api_ref(emqx_bridge_mongodb, <<"mongodb_single">>, Method ++ "_single"),
        api_ref(emqx_bridge_hstreamdb, <<"hstreamdb">>, Method),
        api_ref(emqx_bridge_influxdb, <<"influxdb_api_v1">>, Method ++ "_api_v1"),
        api_ref(emqx_bridge_influxdb, <<"influxdb_api_v2">>, Method ++ "_api_v2"),
        api_ref(emqx_bridge_redis, <<"redis_single">>, Method ++ "_single"),
        api_ref(emqx_bridge_redis, <<"redis_sentinel">>, Method ++ "_sentinel"),
        api_ref(emqx_bridge_redis, <<"redis_cluster">>, Method ++ "_cluster"),
        api_ref(emqx_bridge_timescale, <<"timescale">>, Method),
        api_ref(emqx_bridge_matrix, <<"matrix">>, Method),
        api_ref(emqx_bridge_tdengine, <<"tdengine">>, Method),
        api_ref(emqx_bridge_clickhouse, <<"clickhouse">>, Method),
        api_ref(emqx_bridge_dynamo, <<"dynamo">>, Method),
        api_ref(emqx_bridge_rocketmq, <<"rocketmq">>, Method),
        api_ref(emqx_bridge_sqlserver, <<"sqlserver">>, Method),
        api_ref(emqx_bridge_opents, <<"opents">>, Method),
        api_ref(emqx_bridge_pulsar, <<"pulsar_producer">>, Method ++ "_producer"),
        api_ref(emqx_bridge_oracle, <<"oracle">>, Method),
        api_ref(emqx_bridge_iotdb, <<"iotdb">>, Method),
        api_ref(emqx_bridge_rabbitmq, <<"rabbitmq">>, Method),
        api_ref(emqx_bridge_kinesis, <<"kinesis_producer">>, Method ++ "_producer"),
        api_ref(emqx_bridge_greptimedb, <<"greptimedb">>, Method ++ "_grpc_v1"),
        api_ref(emqx_bridge_azure_event_hub, <<"azure_event_hub_producer">>, Method ++ "_producer")
    ].

schema_modules() ->
    [
        emqx_bridge_kafka,
        emqx_bridge_cassandra,
        emqx_bridge_hstreamdb,
        emqx_bridge_gcp_pubsub,
        emqx_bridge_influxdb,
        emqx_bridge_mongodb,
        emqx_bridge_mysql,
        emqx_bridge_redis,
        emqx_bridge_pgsql,
        emqx_bridge_timescale,
        emqx_bridge_matrix,
        emqx_bridge_tdengine,
        emqx_bridge_clickhouse,
        emqx_bridge_dynamo,
        emqx_bridge_rocketmq,
        emqx_bridge_sqlserver,
        emqx_bridge_opents,
        emqx_bridge_pulsar,
        emqx_bridge_oracle,
        emqx_bridge_iotdb,
        emqx_bridge_rabbitmq,
        emqx_bridge_kinesis,
        emqx_bridge_greptimedb,
        emqx_bridge_azure_event_hub
    ].

examples(Method) ->
    registered_examples(Method).

registered_examples(Method) ->
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

%% TODO: existing atom
resource_type(Type) when is_binary(Type) -> resource_type(binary_to_atom(Type, utf8));
resource_type(kafka_consumer) -> emqx_bridge_kafka_impl_consumer;
resource_type(kafka_producer) -> emqx_bridge_kafka_impl_producer;
resource_type(cassandra) -> emqx_bridge_cassandra_connector;
resource_type(hstreamdb) -> emqx_bridge_hstreamdb_connector;
resource_type(gcp_pubsub) -> emqx_bridge_gcp_pubsub_impl_producer;
resource_type(gcp_pubsub_consumer) -> emqx_bridge_gcp_pubsub_impl_consumer;
resource_type(mongodb_rs) -> emqx_bridge_mongodb_connector;
resource_type(mongodb_sharded) -> emqx_bridge_mongodb_connector;
resource_type(mongodb_single) -> emqx_bridge_mongodb_connector;
resource_type(mysql) -> emqx_mysql;
resource_type(influxdb_api_v1) -> emqx_bridge_influxdb_connector;
resource_type(influxdb_api_v2) -> emqx_bridge_influxdb_connector;
resource_type(redis_single) -> emqx_bridge_redis_connector;
resource_type(redis_sentinel) -> emqx_bridge_redis_connector;
resource_type(redis_cluster) -> emqx_bridge_redis_connector;
resource_type(pgsql) -> emqx_postgresql;
resource_type(timescale) -> emqx_postgresql;
resource_type(matrix) -> emqx_postgresql;
resource_type(tdengine) -> emqx_bridge_tdengine_connector;
resource_type(clickhouse) -> emqx_bridge_clickhouse_connector;
resource_type(dynamo) -> emqx_bridge_dynamo_connector;
resource_type(rocketmq) -> emqx_bridge_rocketmq_connector;
resource_type(sqlserver) -> emqx_bridge_sqlserver_connector;
resource_type(opents) -> emqx_bridge_opents_connector;
resource_type(pulsar_producer) -> emqx_bridge_pulsar_connector;
resource_type(oracle) -> emqx_oracle;
resource_type(iotdb) -> emqx_bridge_iotdb_connector;
resource_type(rabbitmq) -> emqx_bridge_rabbitmq_connector;
resource_type(kinesis_producer) -> emqx_bridge_kinesis_impl_producer;
resource_type(greptimedb) -> emqx_bridge_greptimedb_connector;
%% We use AEH's Kafka interface.
resource_type(azure_event_hub_producer) -> emqx_bridge_kafka_impl_producer.

%% For bridges that need to override connector configurations.
bridge_impl_module(BridgeType) when is_binary(BridgeType) ->
    bridge_impl_module(binary_to_atom(BridgeType, utf8));
bridge_impl_module(azure_event_hub_producer) ->
    emqx_bridge_azure_event_hub;
bridge_impl_module(_BridgeType) ->
    undefined.

namespace() -> undefined.

fields(bridges) ->
    [
        {hstreamdb,
            mk(
                hoconsc:map(name, ref(emqx_bridge_hstreamdb, "config")),
                #{
                    desc => <<"HStreamDB Bridge Config">>,
                    required => false
                }
            )},
        {mysql,
            mk(
                hoconsc:map(name, ref(emqx_bridge_mysql, "config")),
                #{
                    desc => <<"MySQL Bridge Config">>,
                    required => false
                }
            )},
        {tdengine,
            mk(
                hoconsc:map(name, ref(emqx_bridge_tdengine, "config")),
                #{
                    desc => <<"TDengine Bridge Config">>,
                    required => false
                }
            )},
        {dynamo,
            mk(
                hoconsc:map(name, ref(emqx_bridge_dynamo, "config")),
                #{
                    desc => <<"Dynamo Bridge Config">>,
                    required => false
                }
            )},
        {rocketmq,
            mk(
                hoconsc:map(name, ref(emqx_bridge_rocketmq, "config")),
                #{
                    desc => <<"RocketMQ Bridge Config">>,
                    required => false
                }
            )},
        {cassandra,
            mk(
                hoconsc:map(name, ref(emqx_bridge_cassandra, "config")),
                #{
                    desc => <<"Cassandra Bridge Config">>,
                    required => false
                }
            )},
        {opents,
            mk(
                hoconsc:map(name, ref(emqx_bridge_opents, "config")),
                #{
                    desc => <<"OpenTSDB Bridge Config">>,
                    required => false
                }
            )},
        {oracle,
            mk(
                hoconsc:map(name, ref(emqx_bridge_oracle, "config")),
                #{
                    desc => <<"Oracle Bridge Config">>,
                    validator => fun emqx_bridge_oracle:config_validator/1,
                    required => false
                }
            )},
        {iotdb,
            mk(
                hoconsc:map(name, ref(emqx_bridge_iotdb, "config")),
                #{
                    desc => <<"Apache IoTDB Bridge Config">>,
                    required => false
                }
            )}
    ] ++ kafka_structs() ++ pulsar_structs() ++ gcp_pubsub_structs() ++ mongodb_structs() ++
        influxdb_structs() ++
        redis_structs() ++
        pgsql_structs() ++ clickhouse_structs() ++ sqlserver_structs() ++ rabbitmq_structs() ++
        kinesis_structs() ++ greptimedb_structs() ++ azure_event_hub_structs().

mongodb_structs() ->
    [
        {Type,
            mk(
                hoconsc:map(name, ref(emqx_bridge_mongodb, Type)),
                #{
                    desc => <<"MongoDB Bridge Config">>,
                    required => false
                }
            )}
     || Type <- [mongodb_rs, mongodb_sharded, mongodb_single]
    ].

kafka_structs() ->
    [
        {kafka,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kafka, kafka_producer)),
                #{
                    desc => <<"Kafka Producer Bridge Config">>,
                    required => false,
                    converter => fun kafka_producer_converter/2
                }
            )},
        {kafka_consumer,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kafka, kafka_consumer)),
                #{desc => <<"Kafka Consumer Bridge Config">>, required => false}
            )}
    ].

pulsar_structs() ->
    [
        {pulsar_producer,
            mk(
                hoconsc:map(name, ref(emqx_bridge_pulsar, pulsar_producer)),
                #{
                    desc => <<"Pulsar Producer Bridge Config">>,
                    required => false,
                    validator => fun emqx_bridge_pulsar:producer_strategy_key_validator/1
                }
            )}
    ].

gcp_pubsub_structs() ->
    [
        {gcp_pubsub,
            mk(
                hoconsc:map(name, ref(emqx_bridge_gcp_pubsub, "config_producer")),
                #{
                    desc => <<"EMQX Enterprise Config">>,
                    required => false
                }
            )},
        {gcp_pubsub_consumer,
            mk(
                hoconsc:map(name, ref(emqx_bridge_gcp_pubsub, "config_consumer")),
                #{
                    desc => <<"EMQX Enterprise Config">>,
                    required => false
                }
            )}
    ].

influxdb_structs() ->
    [
        {Protocol,
            mk(
                hoconsc:map(name, ref(emqx_bridge_influxdb, Protocol)),
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

greptimedb_structs() ->
    [
        {Protocol,
            mk(
                hoconsc:map(name, ref(emqx_bridge_greptimedb, Protocol)),
                #{
                    desc => <<"GreptimeDB Bridge Config">>,
                    required => false
                }
            )}
     || Protocol <- [
            greptimedb
        ]
    ].

redis_structs() ->
    [
        {Type,
            mk(
                hoconsc:map(name, ref(emqx_bridge_redis, Type)),
                #{
                    desc => <<"Redis Bridge Config">>,
                    required => false
                }
            )}
     || Type <- [
            redis_single,
            redis_sentinel,
            redis_cluster
        ]
    ].

pgsql_structs() ->
    [
        {Type,
            mk(
                hoconsc:map(name, ref(emqx_bridge_pgsql, "config")),
                #{
                    desc => <<Name/binary, " Bridge Config">>,
                    required => false
                }
            )}
     || {Type, Name} <- [
            {pgsql, <<"PostgreSQL">>},
            {timescale, <<"Timescale">>},
            {matrix, <<"Matrix">>}
        ]
    ].

clickhouse_structs() ->
    [
        {clickhouse,
            mk(
                hoconsc:map(name, ref(emqx_bridge_clickhouse, "config")),
                #{
                    desc => <<"Clickhouse Bridge Config">>,
                    required => false
                }
            )}
    ].

sqlserver_structs() ->
    [
        {sqlserver,
            mk(
                hoconsc:map(name, ref(emqx_bridge_sqlserver, "config")),
                #{
                    desc => <<"Microsoft SQL Server Bridge Config">>,
                    required => false
                }
            )}
    ].

kafka_producer_converter(undefined, _) ->
    undefined;
kafka_producer_converter(Map, Opts) ->
    maps:map(
        fun(_Name, Config) ->
            emqx_bridge_kafka:kafka_producer_converter(Config, Opts)
        end,
        Map
    ).

azure_event_hub_producer_converter(undefined, _) ->
    undefined;
azure_event_hub_producer_converter(Map, Opts) ->
    maps:map(
        fun(_Name, Config) ->
            emqx_bridge_azure_event_hub:producer_converter(Config, Opts)
        end,
        Map
    ).

rabbitmq_structs() ->
    [
        {rabbitmq,
            mk(
                hoconsc:map(name, ref(emqx_bridge_rabbitmq, "config")),
                #{
                    desc => <<"RabbitMQ Bridge Config">>,
                    required => false
                }
            )}
    ].

kinesis_structs() ->
    [
        {kinesis_producer,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kinesis, "config_producer")),
                #{
                    desc => <<"Amazon Kinesis Producer Bridge Config">>,
                    required => false
                }
            )}
    ].

azure_event_hub_structs() ->
    [
        {azure_event_hub_producer,
            mk(
                hoconsc:map(name, ref(emqx_bridge_azure_event_hub, "config_producer")),
                #{
                    desc => <<"EMQX Enterprise Config">>,
                    converter => fun azure_event_hub_producer_converter/2,
                    required => false
                }
            )}
    ].

api_ref(Module, Type, Method) ->
    {Type, ref(Module, Method)}.

-else.

-endif.
