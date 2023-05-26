%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pulsar_tests).

-include_lib("eunit/include/eunit.hrl").

%%===========================================================================
%% Test cases
%%===========================================================================

pulsar_producer_validations_test() ->
    Conf0 = pulsar_producer_hocon(),
    Conf1 =
        Conf0 ++
            "\n"
            "bridges.pulsar_producer.my_producer.strategy = key_dispatch"
            "\n"
            "bridges.pulsar_producer.my_producer.message.key = \"\"",
    Conf = parse(Conf1),
    ?assertMatch(
        #{
            <<"strategy">> := <<"key_dispatch">>,
            <<"message">> := #{<<"key">> := <<>>}
        },
        emqx_utils_maps:deep_get([<<"bridges">>, <<"pulsar_producer">>, <<"my_producer">>], Conf)
    ),
    ?assertThrow(
        {_, [
            #{
                path := "bridges.pulsar_producer.my_producer",
                reason := "Message key cannot be empty when `key_dispatch` strategy is used"
            }
        ]},
        check(Conf)
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
pulsar_producer_hocon() ->
"""
bridges.pulsar_producer.my_producer {
  enable = true
  servers = \"localhost:6650\"
  pulsar_topic = pulsar_topic
  strategy = random
  message {
    key = \"${.clientid}\"
    value = \"${.}\"
  }
  authentication = none
  ssl {
    enable = false
    verify = verify_none
    server_name_indication = \"auto\"
  }
}
""".
