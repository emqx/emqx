%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_event_hub_tests).

-include_lib("eunit/include/eunit.hrl").

%%===========================================================================
%% Data Section
%%===========================================================================

%% erlfmt-ignore
aeh_producer_hocon() ->
"
bridges.azure_event_hub_producer.my_producer {
  enable = true
  authentication {
    password = \"Endpoint=...\"
  }
  bootstrap_hosts = \"emqx.servicebus.windows.net:9093\"
  connect_timeout = 5s
  kafka {
    buffer {
      memory_overload_protection = false
      mode = memory
      per_partition_limit = 2GB
      segment_bytes = 100MB
    }
    compression = no_compression
    kafka_header_value_encode_mode = none
    max_batch_bytes = 896KB
    max_inflight = 10
    message {
      key = \"${.clientid}\"
      value = \"${.}\"
    }
    partition_count_refresh_interval = 60s
    partition_strategy = random
    query_mode = async
    required_acks = all_isr
    sync_query_timeout = 5s
    topic = test
  }
  local_topic = \"t/aeh\"
  metadata_request_timeout = 5s
  min_metadata_refresh_interval = 3s
  socket_opts {
    recbuf = 1024KB
    sndbuf = 1024KB
    tcp_keepalive = none
  }
  ssl {
    ciphers = []
    depth = 10
    hibernate_after = 5s
    log_level = notice
    reuse_sessions = true
    secure_renegotiate = true
    verify = verify_none
    versions = [tlsv1.3, tlsv1.2]
    server_name_indication = auto
  }
}
".

%%===========================================================================
%% Helper functions
%%===========================================================================

parse(Hocon) ->
    {ok, Conf} = hocon:binary(Hocon),
    Conf.

check(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_bridge_schema, Conf).

-define(validation_error(Reason, Value),
    {emqx_bridge_schema, [
        #{
            kind := validation_error,
            reason := Reason,
            value := Value
        }
    ]}
).

-define(ok_config(Cfg), #{
    <<"bridges">> :=
        #{
            <<"azure_event_hub_producer">> :=
                #{
                    <<"my_producer">> :=
                        Cfg
                }
        }
}).

%%===========================================================================
%% Test cases
%%===========================================================================

aeh_producer_test_() ->
    %% ensure this module is loaded when testing only this file
    _ = emqx_bridge_enterprise:module_info(),
    BaseConf = parse(aeh_producer_hocon()),
    Override = fun(Cfg) ->
        emqx_utils_maps:deep_merge(
            BaseConf,
            #{
                <<"bridges">> =>
                    #{
                        <<"azure_event_hub_producer">> =>
                            #{<<"my_producer">> => Cfg}
                    }
            }
        )
    end,
    [
        {"base config",
            ?_assertMatch(
                ?ok_config(
                    #{
                        <<"authentication">> := #{
                            <<"username">> := <<"$ConnectionString">>,
                            <<"mechanism">> := plain
                        },
                        <<"ssl">> := #{
                            <<"enable">> := true,
                            <<"server_name_indication">> := "servicebus.windows.net"
                        }
                    }
                ),
                check(BaseConf)
            )},
        {"sni disabled",
            ?_assertMatch(
                ?ok_config(
                    #{<<"ssl">> := #{<<"server_name_indication">> := "disable"}}
                ),
                check(Override(#{<<"ssl">> => #{<<"server_name_indication">> => <<"disable">>}}))
            )},
        {"custom sni",
            ?_assertMatch(
                ?ok_config(
                    #{
                        <<"ssl">> := #{
                            <<"server_name_indication">> := "custom.servicebus.windows.net"
                        }
                    }
                ),
                check(
                    Override(#{
                        <<"ssl">> => #{
                            <<"server_name_indication">> => <<"custom.servicebus.windows.net">>
                        }
                    })
                )
            )},
        {"ssl disabled",
            ?_assertThrow(
                ?validation_error("Expected: true" ++ _, <<"false">>),
                check(Override(#{<<"ssl">> => #{<<"enable">> => <<"false">>}}))
            )},
        {"bad authn mechanism: scram sha256",
            ?_assertThrow(
                ?validation_error("Expected: plain" ++ _, <<"scram_sha_256">>),
                check(
                    Override(#{<<"authentication">> => #{<<"mechanism">> => <<"scram_sha_256">>}})
                )
            )},
        {"bad authn mechanism: scram sha512",
            ?_assertThrow(
                ?validation_error("Expected: plain" ++ _, <<"scram_sha_512">>),
                check(
                    Override(#{<<"authentication">> => #{<<"mechanism">> => <<"scram_sha_512">>}})
                )
            )},
        {"bad required acks: none",
            ?_assertThrow(
                ?validation_error(not_a_enum_symbol, none),
                check(Override(#{<<"kafka">> => #{<<"required_acks">> => <<"none">>}}))
            )},
        {"bad compression: snappy",
            ?_assertThrow(
                ?validation_error("Expected: no_compression" ++ _, <<"snappy">>),
                check(Override(#{<<"kafka">> => #{<<"compression">> => <<"snappy">>}}))
            )},
        {"bad compression: gzip",
            ?_assertThrow(
                ?validation_error("Expected: no_compression" ++ _, <<"gzip">>),
                check(Override(#{<<"kafka">> => #{<<"compression">> => <<"gzip">>}}))
            )}
    ].
