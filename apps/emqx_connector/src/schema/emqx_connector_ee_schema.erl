%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    schema_modules/0,
    namespace/0
]).

resource_type(Type) when is_binary(Type) ->
    resource_type(binary_to_atom(Type, utf8));
resource_type(influxdb) ->
    emqx_bridge_influxdb_connector;
resource_type(cassandra) ->
    emqx_bridge_cassandra_connector;
resource_type(clickhouse) ->
    emqx_bridge_clickhouse_connector;
resource_type(mysql) ->
    emqx_bridge_mysql_connector;
resource_type(syskeeper_forwarder) ->
    emqx_bridge_syskeeper_connector;
resource_type(syskeeper_proxy) ->
    emqx_bridge_syskeeper_proxy_server;
resource_type(sqlserver) ->
    emqx_bridge_sqlserver_connector;
resource_type(redis) ->
    emqx_bridge_redis_connector;
resource_type(rocketmq) ->
    emqx_bridge_rocketmq_connector;
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
resource_type(pulsar) ->
    emqx_bridge_pulsar_connector;
resource_type(rabbitmq) ->
    emqx_bridge_rabbitmq_connector;
resource_type(s3) ->
    emqx_bridge_s3_connector;
resource_type(Type) ->
    error({unknown_connector_type, Type}).

%% For connectors that need to override connector configurations.
connector_impl_module(ConnectorType) when is_binary(ConnectorType) ->
    connector_impl_module(binary_to_atom(ConnectorType, utf8));
connector_impl_module(iotdb) ->
    emqx_bridge_iotdb_connector;
connector_impl_module(elasticsearch) ->
    emqx_bridge_es_connector;
connector_impl_module(opents) ->
    emqx_bridge_opents_connector;
connector_impl_module(pulsar) ->
    emqx_bridge_pulsar_connector;
connector_impl_module(tdengine) ->
    emqx_bridge_tdengine_connector;
connector_impl_module(rabbitmq) ->
    emqx_bridge_rabbitmq_connector;
connector_impl_module(_ConnectorType) ->
    undefined.

namespace() -> undefined.

fields(connectors) ->
    connector_structs().

connector_structs() ->
    [
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
        {clickhouse,
            mk(
                hoconsc:map(name, ref(emqx_bridge_clickhouse, "config_connector")),
                #{
                    desc => <<"ClickHouse Connector Config">>,
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
        {redis,
            mk(
                hoconsc:map(name, ref(emqx_bridge_redis_schema, "config_connector")),
                #{
                    desc => <<"Redis Connector Config">>,
                    required => false
                }
            )},
        {rocketmq,
            mk(
                hoconsc:map(name, ref(emqx_bridge_rocketmq, "config_connector")),
                #{
                    desc => <<"RocketMQ Connector Config">>,
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
        {sqlserver,
            mk(
                hoconsc:map(name, ref(emqx_bridge_sqlserver, "config_connector")),
                #{
                    desc => <<"Microsoft SQL Server Connector Config">>,
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
            )},
        {pulsar,
            mk(
                hoconsc:map(name, ref(emqx_bridge_pulsar_connector_schema, "config_connector")),
                #{
                    desc => <<"Pulsar Connector Config">>,
                    required => false
                }
            )},
        {rabbitmq,
            mk(
                hoconsc:map(name, ref(emqx_bridge_rabbitmq_connector_schema, "config_connector")),
                #{
                    desc => <<"RabbitMQ Connector Config">>,
                    required => false
                }
            )},
        {s3,
            mk(
                hoconsc:map(name, ref(emqx_bridge_s3, "config_connector")),
                #{
                    desc => <<"S3 Connector Config">>,
                    required => false
                }
            )}
    ].

schema_modules() ->
    [
        emqx_bridge_influxdb,
        emqx_bridge_cassandra,
        emqx_bridge_clickhouse,
        emqx_bridge_mysql,
        emqx_bridge_syskeeper_connector,
        emqx_bridge_syskeeper_proxy,
        emqx_bridge_sqlserver,
        emqx_postgresql_connector_schema,
        emqx_bridge_redis_schema,
        emqx_bridge_rocketmq,
        emqx_bridge_iotdb_connector,
        emqx_bridge_es_connector,
        emqx_bridge_rabbitmq_connector_schema,
        emqx_bridge_pulsar_connector_schema,
        emqx_bridge_opents_connector,
        emqx_bridge_greptimedb,
        emqx_bridge_tdengine_connector,
        emqx_bridge_s3
    ].

api_schemas(Method) ->
    [
        %% We need to map the `type' field of a request (binary) to a
        %% connector schema module.
        api_ref(emqx_bridge_influxdb, <<"influxdb">>, Method ++ "_connector"),
        api_ref(emqx_bridge_cassandra, <<"cassandra">>, Method ++ "_connector"),
        api_ref(emqx_bridge_clickhouse, <<"clickhouse">>, Method ++ "_connector"),
        api_ref(emqx_bridge_mysql, <<"mysql">>, Method ++ "_connector"),
        api_ref(emqx_bridge_syskeeper_connector, <<"syskeeper_forwarder">>, Method),
        api_ref(emqx_bridge_syskeeper_proxy, <<"syskeeper_proxy">>, Method),
        api_ref(emqx_bridge_sqlserver, <<"sqlserver">>, Method ++ "_connector"),
        api_ref(emqx_bridge_redis_schema, <<"redis">>, Method ++ "_connector"),
        api_ref(emqx_bridge_rocketmq, <<"rocketmq">>, Method ++ "_connector"),
        api_ref(emqx_bridge_iotdb_connector, <<"iotdb">>, Method),
        api_ref(emqx_bridge_es_connector, <<"elasticsearch">>, Method),
        api_ref(emqx_bridge_opents_connector, <<"opents">>, Method),
        api_ref(emqx_bridge_rabbitmq_connector_schema, <<"rabbitmq">>, Method),
        api_ref(emqx_bridge_pulsar_connector_schema, <<"pulsar">>, Method),
        api_ref(emqx_bridge_greptimedb, <<"greptimedb">>, Method ++ "_connector"),
        api_ref(emqx_bridge_tdengine_connector, <<"tdengine">>, Method),
        api_ref(emqx_bridge_s3, <<"s3">>, Method ++ "_connector")
    ].

api_ref(Module, Type, Method) ->
    {Type, ref(Module, Method)}.

-else.

-endif.
