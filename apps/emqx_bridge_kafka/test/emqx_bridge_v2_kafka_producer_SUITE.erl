%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_kafka_producer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("brod/include/brod.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    ok = emqx_common_test_helpers:start_apps(apps_to_start_and_stop()),
    application:ensure_all_started(telemetry),
    application:ensure_all_started(wolff),
    application:ensure_all_started(brod),
    emqx_bridge_kafka_impl_producer_SUITE:wait_until_kafka_is_up(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps(apps_to_start_and_stop()).

apps_to_start_and_stop() ->
    [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge,
        emqx_rule_engine
    ].

t_create_remove_list(_) ->
    [] = emqx_bridge_v2:list(),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_connector:create(kafka, test_connector, ConnectorConfig),
    Config = bridge_v2_config(<<"test_connector">>),
    {ok, _Config} = emqx_bridge_v2:create(kafka, test_bridge_v2, Config),
    [BridgeV2Info] = emqx_bridge_v2:list(),
    #{
        name := <<"test_bridge_v2">>,
        type := <<"kafka">>,
        raw_config := _RawConfig
    } = BridgeV2Info,
    {ok, _Config2} = emqx_bridge_v2:create(kafka, test_bridge_v2_2, Config),
    2 = length(emqx_bridge_v2:list()),
    {ok, _} = emqx_bridge_v2:remove(kafka, test_bridge_v2),
    1 = length(emqx_bridge_v2:list()),
    {ok, _} = emqx_bridge_v2:remove(kafka, test_bridge_v2_2),
    [] = emqx_bridge_v2:list(),
    emqx_connector:remove(kafka, test_connector),
    ok.

%% Test sending a message to a bridge V2
t_send_message(_) ->
    BridgeV2Config = bridge_v2_config(<<"test_connector2">>),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_connector:create(kafka, test_connector2, ConnectorConfig),
    {ok, _} = emqx_bridge_v2:create(kafka, test_bridge_v2_1, BridgeV2Config),
    %% Use the bridge to send a message
    check_send_message_with_bridge(test_bridge_v2_1),
    %% Create a few more bridges with the same connector and test them
    BridgeNames1 = [
        list_to_atom("test_bridge_v2_" ++ integer_to_list(I))
     || I <- lists:seq(2, 10)
    ],
    lists:foreach(
        fun(BridgeName) ->
            {ok, _} = emqx_bridge_v2:create(kafka, BridgeName, BridgeV2Config),
            check_send_message_with_bridge(BridgeName)
        end,
        BridgeNames1
    ),
    BridgeNames = [test_bridge_v2_1 | BridgeNames1],
    %% Send more messages to the bridges
    lists:foreach(
        fun(BridgeName) ->
            lists:foreach(
                fun(_) ->
                    check_send_message_with_bridge(BridgeName)
                end,
                lists:seq(1, 10)
            )
        end,
        BridgeNames
    ),
    %% Remove all the bridges
    lists:foreach(
        fun(BridgeName) ->
            {ok, _} = emqx_bridge_v2:remove(kafka, BridgeName)
        end,
        BridgeNames
    ),
    emqx_connector:remove(kafka, test_connector2),
    ok.

%% Test that we can get the status of the bridge V2
t_health_check(_) ->
    BridgeV2Config = bridge_v2_config(<<"test_connector3">>),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_connector:create(kafka, test_connector3, ConnectorConfig),
    {ok, _} = emqx_bridge_v2:create(kafka, test_bridge_v2, BridgeV2Config),
    connected = emqx_bridge_v2:health_check(kafka, test_bridge_v2),
    {ok, _} = emqx_bridge_v2:remove(kafka, test_bridge_v2),
    %% Check behaviour when bridge does not exist
    {error, bridge_not_found} = emqx_bridge_v2:health_check(kafka, test_bridge_v2),
    {ok, _} = emqx_connector:remove(kafka, test_connector3),
    ok.

t_local_topic(_) ->
    BridgeV2Config = bridge_v2_config(<<"test_connector">>),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_connector:create(kafka, test_connector, ConnectorConfig),
    {ok, _} = emqx_bridge_v2:create(kafka, test_bridge, BridgeV2Config),
    %% Send a message to the local topic
    Payload = <<"local_topic_payload">>,
    Offset = resolve_kafka_offset(),
    emqx:publish(emqx_message:make(<<"kafka_t/hej">>, Payload)),
    check_kafka_message_payload(Offset, Payload),
    {ok, _} = emqx_bridge_v2:remove(kafka, test_bridge),
    {ok, _} = emqx_connector:remove(kafka, test_connector),
    ok.

check_send_message_with_bridge(BridgeName) ->
    %% ######################################
    %% Create Kafka message
    %% ######################################
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Payload = list_to_binary("payload" ++ integer_to_list(Time)),
    Msg = #{
        clientid => BinTime,
        payload => Payload,
        timestamp => Time
    },
    Offset = resolve_kafka_offset(),
    %% ######################################
    %% Send message
    %% ######################################
    emqx_bridge_v2:send_message(kafka, BridgeName, Msg, #{}),
    %% ######################################
    %% Check if message is sent to Kafka
    %% ######################################
    check_kafka_message_payload(Offset, Payload).

resolve_kafka_offset() ->
    KafkaTopic = emqx_bridge_kafka_impl_producer_SUITE:test_topic_one_partition(),
    Partition = 0,
    Hosts = emqx_bridge_kafka_impl_producer_SUITE:kafka_hosts(),
    {ok, Offset0} = emqx_bridge_kafka_impl_producer_SUITE:resolve_kafka_offset(
        Hosts, KafkaTopic, Partition
    ),
    Offset0.

check_kafka_message_payload(Offset, ExpectedPayload) ->
    KafkaTopic = emqx_bridge_kafka_impl_producer_SUITE:test_topic_one_partition(),
    Partition = 0,
    Hosts = emqx_bridge_kafka_impl_producer_SUITE:kafka_hosts(),
    {ok, {_, [KafkaMsg0]}} = brod:fetch(Hosts, KafkaTopic, Partition, Offset),
    ?assertMatch(#kafka_message{value = ExpectedPayload}, KafkaMsg0).

bridge_v2_config(ConnectorName) ->
    #{
        <<"connector">> => ConnectorName,
        <<"enable">> => true,
        <<"kafka">> => #{
            <<"buffer">> => #{
                <<"memory_overload_protection">> => false,
                <<"mode">> => <<"memory">>,
                <<"per_partition_limit">> => <<"2GB">>,
                <<"segment_bytes">> => <<"100MB">>
            },
            <<"compression">> => <<"no_compression">>,
            <<"kafka_header_value_encode_mode">> => <<"none">>,
            <<"max_batch_bytes">> => <<"896KB">>,
            <<"max_inflight">> => 10,
            <<"message">> => #{
                <<"key">> => <<"${.clientid}">>,
                <<"timestamp">> => <<"${.timestamp}">>,
                <<"value">> => <<"${.payload}">>
            },
            <<"partition_count_refresh_interval">> => <<"60s">>,
            <<"partition_strategy">> => <<"random">>,
            <<"query_mode">> => <<"sync">>,
            <<"required_acks">> => <<"all_isr">>,
            <<"sync_query_timeout">> => <<"5s">>,
            <<"topic">> => emqx_bridge_kafka_impl_producer_SUITE:test_topic_one_partition()
        },
        <<"local_topic">> => <<"kafka_t/#">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>
        }
    }.

connector_config() ->
    #{
        <<"authentication">> => <<"none">>,
        <<"bootstrap_hosts">> => iolist_to_binary(kafka_hosts_string()),
        <<"connect_timeout">> => <<"5s">>,
        <<"enable">> => true,
        <<"metadata_request_timeout">> => <<"5s">>,
        <<"min_metadata_refresh_interval">> => <<"3s">>,
        <<"socket_opts">> =>
            #{
                <<"recbuf">> => <<"1024KB">>,
                <<"sndbuf">> => <<"1024KB">>,
                <<"tcp_keepalive">> => <<"none">>
            },
        <<"ssl">> =>
            #{
                <<"ciphers">> => [],
                <<"depth">> => 10,
                <<"enable">> => false,
                <<"hibernate_after">> => <<"5s">>,
                <<"log_level">> => <<"notice">>,
                <<"reuse_sessions">> => true,
                <<"secure_renegotiate">> => true,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.3">>, <<"tlsv1.2">>]
            }
    }.

kafka_hosts_string() ->
    KafkaHost = os:getenv("KAFKA_PLAIN_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_PLAIN_PORT", "9092"),
    KafkaHost ++ ":" ++ KafkaPort.
