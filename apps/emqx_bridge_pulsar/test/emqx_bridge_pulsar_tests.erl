%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pulsar_tests).

-include_lib("eunit/include/eunit.hrl").

%%===========================================================================
%% Test cases
%%===========================================================================

atoms() ->
    [my_producer].

pulsar_producer_validations_test() ->
    Name = hd(atoms()),
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
        emqx_utils_maps:deep_get([<<"bridges">>, <<"pulsar_producer">>, atom_to_binary(Name)], Conf)
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
    ?assertThrow(
        {_, [
            #{
                path := "bridges.pulsar_producer.my_producer",
                reason := "Message key cannot be empty when `key_dispatch` strategy is used"
            }
        ]},
        check_atom_key(Conf)
    ),

    ok.

%%===========================================================================
%% Helper functions
%%===========================================================================

parse(Hocon) ->
    {ok, Conf} = hocon:binary(Hocon),
    Conf.

%% what bridge creation does
check(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_bridge_schema, Conf).

%% what bridge probe does
check_atom_key(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_bridge_schema, Conf, #{atom_key => true, required => false}).

%%===========================================================================
%% Data section
%%===========================================================================

%% erlfmt-ignore
pulsar_producer_hocon() ->
"
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
".
