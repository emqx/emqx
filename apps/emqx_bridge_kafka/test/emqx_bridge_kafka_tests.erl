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
