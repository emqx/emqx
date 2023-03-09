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
