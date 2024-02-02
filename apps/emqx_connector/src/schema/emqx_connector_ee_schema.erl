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
    %%examples/1
    schema_modules/0
]).

resource_type(Type) when is_binary(Type) ->
    resource_type(binary_to_atom(Type, utf8));
resource_type(azure_event_hub_producer) ->
    %% We use AEH's Kafka interface.
    emqx_bridge_kafka_impl_producer;
resource_type(confluent_producer) ->
    emqx_bridge_kafka_impl_producer;
resource_type(gcp_pubsub_producer) ->
    emqx_bridge_gcp_pubsub_impl_producer;
resource_type(kafka_producer) ->
    emqx_bridge_kafka_impl_producer;
resource_type(kinesis) ->
    emqx_bridge_kinesis_impl_producer;
resource_type(matrix) ->
    emqx_postgresql;
resource_type(mongodb) ->
    emqx_bridge_mongodb_connector;
resource_type(oracle) ->
    emqx_oracle;
resource_type(influxdb) ->
    emqx_bridge_influxdb_connector;
resource_type(cassandra) ->
    emqx_bridge_cassandra_connector;
resource_type(mysql) ->
    emqx_bridge_mysql_connector;
resource_type(pgsql) ->
    emqx_postgresql;
resource_type(syskeeper_forwarder) ->
    emqx_bridge_syskeeper_connector;
resource_type(syskeeper_proxy) ->
    emqx_bridge_syskeeper_proxy_server;
resource_type(timescale) ->
    emqx_postgresql;
resource_type(redis) ->
    emqx_bridge_redis_connector;
resource_type(iotdb) ->
    emqx_bridge_iotdb_connector;
resource_type(elasticsearch) ->
    emqx_bridge_es_connector;
resource_type(opents) ->
    emqx_bridge_opents_connector;
resource_type(greptimedb) ->
    emqx_bridge_greptimedb_connector;
resource_type(tdengine) ->
    emqx_bridge_tdengine_connector;
resource_type(Type) ->
    error({unknown_connector_type, Type}).

%% For connectors that need to override connector configurations.
connector_impl_module(ConnectorType) when is_binary(ConnectorType) ->
    connector_impl_module(binary_to_atom(ConnectorType, utf8));
connector_impl_module(azure_event_hub_producer) ->
    emqx_bridge_azure_event_hub;
connector_impl_module(confluent_producer) ->
    emqx_bridge_confluent_producer;
connector_impl_module(iotdb) ->
    emqx_bridge_iotdb_connector;
connector_impl_module(elasticsearch) ->
    emqx_bridge_es_connector;
connector_impl_module(opents) ->
    emqx_bridge_opents_connector;
connector_impl_module(tdengine) ->
    emqx_bridge_tdengine_connector;
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
        {kinesis,
            mk(
                hoconsc:map(name, ref(emqx_bridge_kinesis, "config_connector")),
                #{
                    desc => <<"Kinesis Connector Config">>,
                    required => false
                }
            )},
        {matrix,
            mk(
                hoconsc:map(name, ref(emqx_bridge_matrix, "config_connector")),
                #{
                    desc => <<"Matrix Connector Config">>,
                    required => false
                }
            )},
        {mongodb,
            mk(
                hoconsc:map(name, ref(emqx_bridge_mongodb, "config_connector")),
                #{
                    desc => <<"MongoDB Connector Config">>,
                    required => false
                }
            )},
        {oracle,
            mk(
                hoconsc:map(name, ref(emqx_bridge_oracle, "config_connector")),
                #{
                    desc => <<"Oracle Connector Config">>,
                    required => false,
                    validator => fun emqx_bridge_oracle:config_validator/1
                }
            )},
        {influxdb,
            mk(
                hoconsc:map(name, ref(emqx_bridge_influxdb, "config_connector")),
                #{
                    desc => <<"InfluxDB Connector Config">>,
                    required => false
                }
            )},
        {cassandra,
            mk(
                hoconsc:map(name, ref(emqx_bridge_cassandra, "config_connector")),
                #{
                    desc => <<"Cassandra Connector Config">>,
                    required => false
                }
            )},
        {mysql,
            mk(
                hoconsc:map(name, ref(emqx_bridge_mysql, "config_connector")),
                #{
                    desc => <<"MySQL Connector Config">>,
                    required => false
                }
            )},
        {pgsql,
            mk(
                hoconsc:map(name, ref(emqx_bridge_pgsql, "config_connector")),
                #{
                    desc => <<"PostgreSQL Connector Config">>,
                    required => false
                }
            )},
        {redis,
            mk(
                hoconsc:map(name, ref(emqx_bridge_redis_schema, "config_connector")),
                #{
                    desc => <<"Redis Connector Config">>,
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
            )},
        {timescale,
            mk(
                hoconsc:map(name, ref(emqx_bridge_timescale, "config_connector")),
                #{
                    desc => <<"Timescale Connector Config">>,
                    required => false
                }
            )},
        {iotdb,
            mk(
                hoconsc:map(name, ref(emqx_bridge_iotdb_connector, config)),
                #{
                    desc => <<"IoTDB Connector Config">>,
                    required => false
                }
            )},
        {elasticsearch,
            mk(
                hoconsc:map(name, ref(emqx_bridge_es_connector, config)),
                #{
                    desc => <<"ElasticSearch Connector Config">>,
                    required => false
                }
            )},
        {opents,
            mk(
                hoconsc:map(name, ref(emqx_bridge_opents_connector, "config_connector")),
                #{
                    desc => <<"OpenTSDB Connector Config">>,
                    required => false
                }
            )},
        {greptimedb,
            mk(
                hoconsc:map(name, ref(emqx_bridge_greptimedb, "config_connector")),
                #{
                    desc => <<"GreptimeDB Connector Config">>,
                    required => false
                }
            )},
        {tdengine,
            mk(
                hoconsc:map(name, ref(emqx_bridge_tdengine_connector, "config_connector")),
                #{
                    desc => <<"TDengine Connector Config">>,
                    required => false
                }
            )}
    ].

schema_modules() ->
    [
        emqx_bridge_azure_event_hub,
        emqx_bridge_confluent_producer,
        emqx_bridge_gcp_pubsub_producer_schema,
        emqx_bridge_kafka,
        emqx_bridge_kinesis,
        emqx_bridge_matrix,
        emqx_bridge_mongodb,
        emqx_bridge_oracle,
        emqx_bridge_influxdb,
        emqx_bridge_cassandra,
        emqx_bridge_mysql,
        emqx_bridge_syskeeper_connector,
        emqx_bridge_syskeeper_proxy,
        emqx_bridge_timescale,
        emqx_postgresql_connector_schema,
        emqx_bridge_redis_schema,
        emqx_bridge_iotdb_connector,
        emqx_bridge_es_connector,
        emqx_bridge_opents_connector,
        emqx_bridge_greptimedb,
        emqx_bridge_tdengine_connector
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
        api_ref(
            emqx_bridge_gcp_pubsub_producer_schema,
            <<"gcp_pubsub_producer">>,
            Method ++ "_connector"
        ),
        api_ref(emqx_bridge_kafka, <<"kafka_producer">>, Method ++ "_connector"),
        api_ref(emqx_bridge_kinesis, <<"kinesis">>, Method ++ "_connector"),
        api_ref(emqx_bridge_matrix, <<"matrix">>, Method ++ "_connector"),
        api_ref(emqx_bridge_mongodb, <<"mongodb">>, Method ++ "_connector"),
        api_ref(emqx_bridge_oracle, <<"oracle">>, Method ++ "_connector"),
        api_ref(emqx_bridge_influxdb, <<"influxdb">>, Method ++ "_connector"),
        api_ref(emqx_bridge_cassandra, <<"cassandra">>, Method ++ "_connector"),
        api_ref(emqx_bridge_mysql, <<"mysql">>, Method ++ "_connector"),
        api_ref(emqx_bridge_syskeeper_connector, <<"syskeeper_forwarder">>, Method),
        api_ref(emqx_bridge_syskeeper_proxy, <<"syskeeper_proxy">>, Method),
        api_ref(emqx_bridge_timescale, <<"timescale">>, Method ++ "_connector"),
        api_ref(emqx_postgresql_connector_schema, <<"pgsql">>, Method ++ "_connector"),
        api_ref(emqx_bridge_redis_schema, <<"redis">>, Method ++ "_connector"),
        api_ref(emqx_bridge_iotdb_connector, <<"iotdb">>, Method),
        api_ref(emqx_bridge_es_connector, <<"elasticsearch">>, Method),
        api_ref(emqx_bridge_opents_connector, <<"opents">>, Method),
        api_ref(emqx_bridge_greptimedb, <<"greptimedb">>, Method ++ "_connector"),
        api_ref(emqx_bridge_tdengine_connector, <<"tdengine">>, Method)
    ].

api_ref(Module, Type, Method) ->
    {Type, ref(Module, Method)}.

-else.

-endif.
