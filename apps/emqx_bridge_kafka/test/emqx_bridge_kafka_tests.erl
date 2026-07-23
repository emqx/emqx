%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_tests).

-include_lib("eunit/include/eunit.hrl").

-export([atoms/0]).

%% ensure atoms exist
atoms() -> [myproducer, my_consumer].

%%===========================================================================
%% Test cases
%%===========================================================================

message_key_dispatch_validations_test() ->
    ?assertThrow(
        {_, [
            #{
                path := "actions.kafka_producer.x.parameters",
                reason := "Message key cannot be empty when `key_dispatch` strategy is used"
            }
        ]},
        emqx_bridge_kafka_testlib:action_config(#{
            <<"parameters">> => #{
                <<"message">> => #{<<"key">> => <<"">>},
                <<"partition_strategy">> => <<"key_dispatch">>
            }
        })
    ),
    ok.

tcp_keepalive_validation_test_() ->
    ProducerConf = emqx_bridge_kafka_testlib:action_connector_config(#{}),
    ConsumerConf = emqx_bridge_kafka_testlib:source_connector_config(#{}),
    test_keepalive_validation(action, ProducerConf) ++
        test_keepalive_validation(source, ConsumerConf).

test_keepalive_validation(Kind, Conf) ->
    Check = fun(X) ->
        case Kind of
            action -> check_action_connector(X);
            source -> check_source_connector(X)
        end
    end,
    Path = [<<"socket_opts">>, <<"tcp_keepalive">>],
    Conf1 = emqx_utils_maps:deep_force_put(Path, Conf, <<"5,6,7">>),
    Conf2 = emqx_utils_maps:deep_force_put(Path, Conf, <<"none">>),
    ValidConfs = [Conf, Conf1, Conf2],
    InvalidConf = emqx_utils_maps:deep_force_put(Path, Conf, <<"invalid">>),
    InvalidConf1 = emqx_utils_maps:deep_force_put(Path, Conf, <<"5,6">>),
    InvalidConf2 = emqx_utils_maps:deep_force_put(Path, Conf, <<"5,6,1000">>),
    InvalidConfs = [InvalidConf, InvalidConf1, InvalidConf2],
    [?_assertMatch(#{}, Check(C)) || C <- ValidConfs] ++
        [?_assertThrow(_, Check(C)) || C <- InvalidConfs].

%% `max_batch_age' and `max_retries' accept `infinity' (the default) or a
%% duration / non-negative integer, and are rejected otherwise.
producer_max_batch_age_max_retries_schema_test_() ->
    Check = fun(Overrides) ->
        emqx_bridge_kafka_testlib:action_config(#{<<"parameters">> => Overrides})
    end,
    [
        {"defaults are infinity",
            ?_assertMatch(
                #{
                    <<"parameters">> := #{
                        <<"max_batch_age">> := <<"infinity">>,
                        <<"max_retries">> := <<"infinity">>
                    }
                },
                Check(#{})
            )},
        {"duration and integer accepted",
            ?_assertMatch(
                #{
                    <<"parameters">> := #{
                        <<"max_batch_age">> := <<"500ms">>,
                        <<"max_retries">> := 3
                    }
                },
                Check(#{<<"max_batch_age">> => <<"500ms">>, <<"max_retries">> => 3})
            )},
        {"zero retries accepted",
            ?_assertMatch(
                #{<<"parameters">> := #{<<"max_retries">> := 0}},
                Check(#{<<"max_retries">> => 0})
            )},
        {"negative retries rejected", ?_assertThrow(_, Check(#{<<"max_retries">> => -1}))},
        {"bad duration rejected",
            ?_assertThrow(_, Check(#{<<"max_batch_age">> => <<"not_a_duration">>}))}
    ].

%% The wolff ack callback for the wolff 4.2.0 drop reasons: `message_expired'
%% is reported as `request_expired' (concludes the rule action without bumping
%% resource metrics; `dropped'/`dropped.expired' are bumped via the
%% [wolff, dropped_expired] telemetry handler), while `max_retry_exceeded' is
%% reported as a regular error (counted as `failed' by the reply path).
on_kafka_ack_drop_reasons_test_() ->
    AckResult = fun(Reason) ->
        Ref = make_ref(),
        Self = self(),
        ReplyFn = {fun(R) -> Self ! {Ref, R} end, []},
        ok = emqx_bridge_kafka_impl_producer:on_kafka_ack(0, Reason, ReplyFn),
        receive
            {Ref, Result} -> Result
        after 1_000 -> timeout
        end
    end,
    [
        {"message_expired -> request_expired",
            ?_assertEqual({error, request_expired}, AckResult(message_expired))},
        {"max_retry_exceeded -> max_retry_exceeded",
            ?_assertEqual({error, max_retry_exceeded}, AckResult(max_retry_exceeded))}
    ].

custom_group_id_test() ->
    BadSourceConfig = emqx_bridge_kafka_testlib:source_config(#{
        <<"parameters">> =>
            #{<<"group_id">> => <<"">>}
    }),
    %% Empty strings will be treated as absent by the connector.
    ?assertMatch(
        #{<<"parameters">> := #{<<"group_id">> := <<"">>}},
        check_source(BadSourceConfig)
    ),
    CustomId = <<"custom_id">>,
    OkSourceConfig = emqx_bridge_kafka_testlib:source_config(#{
        <<"parameters">> =>
            #{<<"group_id">> => CustomId}
    }),
    ?assertMatch(
        #{<<"parameters">> := #{<<"group_id">> := CustomId}},
        check_source(OkSourceConfig)
    ),
    ok.

%%===========================================================================
%% Helper functions
%%===========================================================================

check_source(Conf) ->
    emqx_bridge_v2_testlib:parse_and_check(source, kafka_consumer, <<"x">>, Conf).

check_action_connector(Conf) ->
    emqx_bridge_v2_testlib:parse_and_check_connector(kafka_producer, <<"x">>, Conf).

check_source_connector(Conf) ->
    emqx_bridge_v2_testlib:parse_and_check_connector(kafka_consumer, <<"x">>, Conf).

%%===========================================================================
%% Data section
%%===========================================================================
