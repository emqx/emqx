%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_bridge_kafka_tests).

-include_lib("eunit/include/eunit.hrl").

%%===========================================================================
%% Test cases
%%===========================================================================

kafka_producer_test() ->
    Conf1 = parse(kafka_producer_old_hocon(_WithLocalTopic0 = false)),
    Conf2 = parse(kafka_producer_old_hocon(_WithLocalTopic1 = true)),
    Conf3 = parse(kafka_producer_new_hocon()),

    ?assertMatch(
        #{
            <<"bridges">> :=
                #{
                    <<"kafka">> :=
                        #{
                            <<"myproducer">> :=
                                #{<<"kafka">> := #{}}
                        }
                }
        },
        check(Conf1)
    ),
    ?assertNotMatch(
        #{
            <<"bridges">> :=
                #{
                    <<"kafka">> :=
                        #{
                            <<"myproducer">> :=
                                #{<<"local_topic">> := _}
                        }
                }
        },
        check(Conf1)
    ),
    ?assertMatch(
        #{
            <<"bridges">> :=
                #{
                    <<"kafka">> :=
                        #{
                            <<"myproducer">> :=
                                #{
                                    <<"kafka">> := #{},
                                    <<"local_topic">> := <<"mqtt/local">>
                                }
                        }
                }
        },
        check(Conf2)
    ),
    ?assertMatch(
        #{
            <<"bridges">> :=
                #{
                    <<"kafka">> :=
                        #{
                            <<"myproducer">> :=
                                #{
                                    <<"kafka">> := #{},
                                    <<"local_topic">> := <<"mqtt/local">>
                                }
                        }
                }
        },
        check(Conf3)
    ),

    ok.

kafka_consumer_test() ->
    Conf1 = parse(kafka_consumer_hocon()),
    ?assertMatch(
        #{
            <<"bridges">> :=
                #{
                    <<"kafka_consumer">> :=
                        #{
                            <<"my_consumer">> := _
                        }
                }
        },
        check(Conf1)
    ),

    %% Bad: can't repeat kafka topics.
    BadConf1 = emqx_map_lib:deep_put(
        [<<"bridges">>, <<"kafka_consumer">>, <<"my_consumer">>, <<"topic_mapping">>],
        Conf1,
        [
            #{
                <<"kafka_topic">> => <<"t1">>,
                <<"mqtt_topic">> => <<"mqtt/t1">>,
                <<"qos">> => 1,
                <<"payload_template">> => <<"${.}">>
            },
            #{
                <<"kafka_topic">> => <<"t1">>,
                <<"mqtt_topic">> => <<"mqtt/t2">>,
                <<"qos">> => 2,
                <<"payload_template">> => <<"v = ${.value}">>
            }
        ]
    ),
    ?assertThrow(
        {_, [
            #{
                path := "bridges.kafka_consumer.my_consumer.topic_mapping",
                reason := "Kafka topics must not be repeated in a bridge"
            }
        ]},
        check(BadConf1)
    ),

    %% Bad: there must be at least 1 mapping.
    BadConf2 = emqx_map_lib:deep_put(
        [<<"bridges">>, <<"kafka_consumer">>, <<"my_consumer">>, <<"topic_mapping">>],
        Conf1,
        []
    ),
    ?assertThrow(
        {_, [
            #{
                path := "bridges.kafka_consumer.my_consumer.topic_mapping",
                reason := "There must be at least one Kafka-MQTT topic mapping"
            }
        ]},
        check(BadConf2)
    ),

    ok.

%%===========================================================================
%% Helper functions
%%===========================================================================

parse(Hocon) ->
    {ok, Conf} = hocon:binary(Hocon),
    Conf.

check(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_bridge_schema, Conf).

%%===========================================================================
%% Data section
%%===========================================================================

%% erlfmt-ignore
kafka_producer_old_hocon(_WithLocalTopic = true) ->
    kafka_producer_old_hocon("mqtt {topic = \"mqtt/local\"}\n");
kafka_producer_old_hocon(_WithLocalTopic = false) ->
    kafka_producer_old_hocon("mqtt {}\n");
kafka_producer_old_hocon(MQTTConfig) when is_list(MQTTConfig) ->
"""
bridges.kafka {
  myproducer {
    authentication = \"none\"
    bootstrap_hosts = \"toxiproxy:9292\"
    connect_timeout = \"5s\"
    metadata_request_timeout = \"5s\"
    min_metadata_refresh_interval = \"3s\"
    producer {
      kafka {
        buffer {
          memory_overload_protection = false
          mode = \"memory\"
          per_partition_limit = \"2GB\"
          segment_bytes = \"100MB\"
        }
        compression = \"no_compression\"
        max_batch_bytes = \"896KB\"
        max_inflight = 10
        message {
          key = \"${.clientid}\"
          timestamp = \"${.timestamp}\"
          value = \"${.}\"
        }
        partition_count_refresh_interval = \"60s\"
        partition_strategy = \"random\"
        required_acks = \"all_isr\"
        topic = \"test-topic-two-partitions\"
      }
""" ++ MQTTConfig ++
"""
    }
    socket_opts {
      nodelay = true
      recbuf = \"1024KB\"
      sndbuf = \"1024KB\"
    }
    ssl {enable = false, verify = \"verify_peer\"}
  }
}
""".

kafka_producer_new_hocon() ->
    ""
    "\n"
    "bridges.kafka {\n"
    "  myproducer {\n"
    "    authentication = \"none\"\n"
    "    bootstrap_hosts = \"toxiproxy:9292\"\n"
    "    connect_timeout = \"5s\"\n"
    "    metadata_request_timeout = \"5s\"\n"
    "    min_metadata_refresh_interval = \"3s\"\n"
    "    kafka {\n"
    "      buffer {\n"
    "        memory_overload_protection = false\n"
    "        mode = \"memory\"\n"
    "        per_partition_limit = \"2GB\"\n"
    "        segment_bytes = \"100MB\"\n"
    "      }\n"
    "      compression = \"no_compression\"\n"
    "      max_batch_bytes = \"896KB\"\n"
    "      max_inflight = 10\n"
    "      message {\n"
    "        key = \"${.clientid}\"\n"
    "        timestamp = \"${.timestamp}\"\n"
    "        value = \"${.}\"\n"
    "      }\n"
    "      partition_count_refresh_interval = \"60s\"\n"
    "      partition_strategy = \"random\"\n"
    "      required_acks = \"all_isr\"\n"
    "      topic = \"test-topic-two-partitions\"\n"
    "    }\n"
    "    local_topic = \"mqtt/local\"\n"
    "    socket_opts {\n"
    "      nodelay = true\n"
    "      recbuf = \"1024KB\"\n"
    "      sndbuf = \"1024KB\"\n"
    "    }\n"
    "    ssl {enable = false, verify = \"verify_peer\"}\n"
    "  }\n"
    "}\n"
    "".

%% erlfmt-ignore
kafka_consumer_hocon() ->
"""
bridges.kafka_consumer.my_consumer {
  enable = true
  bootstrap_hosts = \"kafka-1.emqx.net:9292\"
  connect_timeout = 5s
  min_metadata_refresh_interval = 3s
  metadata_request_timeout = 5s
  authentication = {
    mechanism = plain
    username = emqxuser
    password = password
  }
  kafka {
    max_batch_bytes = 896KB
    max_rejoin_attempts = 5
    offset_commit_interval_seconds = 3
    offset_reset_policy = latest
  }
  topic_mapping = [
    {
      kafka_topic = \"kafka-topic-1\"
      mqtt_topic = \"mqtt/topic/1\"
      qos = 1
      payload_template = \"${.}\"
    },
    {
      kafka_topic = \"kafka-topic-2\"
      mqtt_topic = \"mqtt/topic/2\"
      qos = 2
      payload_template = \"v = ${.value}\"
    }
  ]
  key_encoding_mode = none
  value_encoding_mode = none
  ssl {
    enable = false
    verify = verify_none
    server_name_indication = \"auto\"
  }
}
""".
