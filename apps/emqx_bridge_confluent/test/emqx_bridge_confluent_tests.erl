%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_confluent_tests).

-include_lib("eunit/include/eunit.hrl").

%%===========================================================================
%% Data Section
%%===========================================================================

%% erlfmt-ignore
confluent_producer_action_hocon() ->
"
actions.confluent_producer.my_producer {
  enable = true
  connector = my_connector
  parameters {
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
  local_topic = \"t/confluent\"
}
".

confluent_producer_connector_hocon() ->
    ""
    "\n"
    "connectors.confluent_producer.my_producer {\n"
    "  enable = true\n"
    "  authentication {\n"
    "    username = \"user\"\n"
    "    password = \"xxx\"\n"
    "  }\n"
    "  bootstrap_hosts = \"xyz.sa-east1.gcp.confluent.cloud:9092\"\n"
    "  connect_timeout = 5s\n"
    "  metadata_request_timeout = 5s\n"
    "  min_metadata_refresh_interval = 3s\n"
    "  socket_opts {\n"
    "    recbuf = 1024KB\n"
    "    sndbuf = 1024KB\n"
    "    tcp_keepalive = none\n"
    "  }\n"
    "}\n"
    "".

%%===========================================================================
%% Helper functions
%%===========================================================================

parse(Hocon) ->
    {ok, Conf} = hocon:binary(Hocon),
    Conf.

check(SchemaMod, Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(SchemaMod, Conf, #{required => false}).

check_action(Conf) when is_map(Conf) ->
    check(emqx_bridge_v2_schema, Conf).

check_connector(Conf) when is_map(Conf) ->
    check(emqx_connector_schema, Conf).

-define(validation_error(SchemaMod, Reason, Value),
    {SchemaMod, [
        #{
            kind := validation_error,
            reason := Reason,
            value := Value
        }
    ]}
).

-define(connector_validation_error(Reason, Value),
    ?validation_error(emqx_connector_schema, Reason, Value)
).

-define(ok_config(RootKey, Cfg), #{
    RootKey :=
        #{
            <<"confluent_producer">> :=
                #{
                    <<"my_producer">> :=
                        Cfg
                }
        }
}).
-define(ok_connector_config(Cfg), ?ok_config(<<"connectors">>, Cfg)).
-define(ok_action_config(Cfg), ?ok_config(<<"actions">>, Cfg)).

%%===========================================================================
%% Test cases
%%===========================================================================

confluent_producer_connector_test_() ->
    %% ensure this module is loaded when testing only this file
    _ = emqx_bridge_enterprise:module_info(),
    BaseConf = parse(confluent_producer_connector_hocon()),
    Override = fun(Cfg) ->
        emqx_utils_maps:deep_merge(
            BaseConf,
            #{
                <<"connectors">> =>
                    #{
                        <<"confluent_producer">> =>
                            #{<<"my_producer">> => Cfg}
                    }
            }
        )
    end,
    [
        {"base config",
            ?_assertMatch(
                ?ok_connector_config(
                    #{
                        <<"authentication">> := #{
                            <<"mechanism">> := plain
                        },
                        <<"ssl">> := #{
                            <<"enable">> := true,
                            <<"verify">> := verify_none
                        }
                    }
                ),
                check_connector(BaseConf)
            )},
        {"ssl disabled",
            ?_assertThrow(
                ?connector_validation_error("Expected: true" ++ _, <<"false">>),
                check_connector(Override(#{<<"ssl">> => #{<<"enable">> => <<"false">>}}))
            )},
        {"bad authn mechanism: scram sha256",
            ?_assertThrow(
                ?connector_validation_error("Expected: plain" ++ _, <<"scram_sha_256">>),
                check_connector(
                    Override(#{<<"authentication">> => #{<<"mechanism">> => <<"scram_sha_256">>}})
                )
            )},
        {"bad authn mechanism: scram sha512",
            ?_assertThrow(
                ?connector_validation_error("Expected: plain" ++ _, <<"scram_sha_512">>),
                check_connector(
                    Override(#{<<"authentication">> => #{<<"mechanism">> => <<"scram_sha_512">>}})
                )
            )}
    ].

confluent_producer_action_test_() ->
    %% ensure this module is loaded when testing only this file
    _ = emqx_bridge_enterprise:module_info(),
    BaseConf = parse(confluent_producer_action_hocon()),
    [
        {"base config",
            ?_assertMatch(
                ?ok_action_config(_),
                check_action(BaseConf)
            )}
    ].
