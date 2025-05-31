%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_rule_raw_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    Config.
end_per_testcase(_TestCase, _Config) ->
    ok.

t_parse_ok(_Config) ->
    lists:foreach(
        fun({Expected, RuleRaw}) ->
            ct:pal("Raw rule: ~p~nExpected: ~p~n", [RuleRaw, Expected]),
            ?assertEqual({ok, Expected}, emqx_authz_rule_raw:parse_rule(RuleRaw))
        end,
        ok_cases()
    ).

t_composite_who(_Config) ->
    {ok, {allow, {'and', Who}, {all, [{qos, [0, 1, 2]}, {retain, all}]}, []}} =
        emqx_authz_rule_raw:parse_rule(
            #{
                <<"permission">> => <<"allow">>,
                <<"topics">> => [],
                <<"action">> => <<"all">>,
                <<"clientid_re">> => <<"^x+$">>,
                <<"username_re">> => <<"^x+$">>,
                <<"ipaddr">> => <<"192.168.1.0/24">>
            }
        ),

    ?assertEqual(
        [
            {clientid, {re, <<"^x+$">>}},
            {ipaddr, "192.168.1.0/24"},
            {username, {re, <<"^x+$">>}}
        ],
        lists:sort(Who)
    ).

t_parse_error(_Config) ->
    lists:foreach(
        fun(RuleRaw) ->
            ?assertMatch(
                {error, _},
                emqx_authz_rule_raw:parse_rule(RuleRaw)
            )
        end,
        error_cases() ++ error_rich_action_cases()
    ).

t_format(_Config) ->
    ?assertEqual(
        #{
            action => subscribe,
            permission => allow,
            qos => [1, 2],
            retain => true,
            topic => [<<"a/b/c">>]
        },
        emqx_authz_rule_raw:format_rule(
            {allow, all, {subscribe, [{qos, [1, 2]}, {retain, true}]}, [<<"a/b/c">>]}
        )
    ),
    ?assertEqual(
        #{
            action => publish,
            permission => allow,
            topic => [<<"a/b/c">>]
        },
        emqx_authz_rule_raw:format_rule(
            {allow, all, publish, [<<"a/b/c">>]}
        )
    ),
    ?assertEqual(
        #{
            action => all,
            permission => allow,
            topic => [],
            ipaddr => <<"192.168.1.0/24">>,
            username_re => <<"^u+$">>,
            clientid_re => <<"^c+$">>
        },
        emqx_authz_rule_raw:format_rule(
            {
                allow,
                {'and', [
                    {clientid, {re, <<"^c+$">>}},
                    {ipaddr, "192.168.1.0/24"},
                    {username, {re, <<"^u+$">>}}
                ]},
                all,
                []
            }
        )
    ),
    ?assertEqual(
        #{
            action => all,
            permission => allow,
            topic => [],
            username_re => <<"^u+$">>
        },
        emqx_authz_rule_raw:format_rule(
            {
                allow,
                {username, {re, <<"^u+$">>}},
                all,
                []
            }
        )
    ),
    %% Legacy rule (without `who' field)
    ?assertEqual(
        #{
            action => subscribe,
            permission => allow,
            qos => [1, 2],
            retain => true,
            topic => [<<"a/b/c">>]
        },
        emqx_authz_rule_raw:format_rule(
            {allow, {subscribe, [{qos, [1, 2]}, {retain, true}]}, [<<"a/b/c">>]}
        )
    ),
    ?assertEqual(
        #{
            action => all,
            permission => allow,
            topic => [],
            zone => <<"zone1">>
        },
        emqx_authz_rule_raw:format_rule(
            {
                allow,
                {zone, <<"zone1">>},
                all,
                []
            }
        )
    ),
    ?assertEqual(
        #{
            action => all,
            permission => allow,
            topic => [],
            zone_re => <<"^zone-[0-9]+$">>
        },
        emqx_authz_rule_raw:format_rule(
            {
                allow,
                {zone, {re, <<"^zone-[0-9]+$">>}},
                all,
                []
            }
        )
    ),
    ?assertEqual(
        #{
            action => all,
            permission => allow,
            topic => [],
            listener => <<"tcp:default">>
        },
        emqx_authz_rule_raw:format_rule(
            {
                allow,
                {listener, <<"tcp:default">>},
                all,
                []
            }
        )
    ),
    ?assertEqual(
        #{
            action => all,
            permission => allow,
            topic => [],
            listener_re => <<"^tcp:.*$">>
        },
        emqx_authz_rule_raw:format_rule(
            {
                allow,
                {listener, {re, <<"^tcp:.*$">>}},
                all,
                []
            }
        )
    ),
    ok.

t_invalid_regex_rules(_Config) ->
    Assert = fun(Rule, Invalid) ->
        ?assertMatch({error, #{reason := Invalid}}, emqx_authz_rule_raw:parse_rule(Rule))
    end,
    Assert(
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"all">>,
            <<"username_re">> => <<"(unmatched">>
        },
        invalid_username_re
    ),
    Assert(
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"all">>,
            <<"clientid_re">> => <<"a{invalid)">>
        },
        invalid_clientid_re
    ),
    Assert(
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"all">>,
            <<"zone_re">> => <<"[invalid">>
        },
        invalid_zone_re
    ),
    Assert(
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"all">>,
            <<"listener_re">> => <<"">>
        },
        invalid_listener_re
    ).

t_invalid_string_rules(_Config) ->
    Assert = fun(Rule, Invalid) ->
        ?assertMatch({error, #{reason := Invalid}}, emqx_authz_rule_raw:parse_rule(Rule))
    end,
    Assert(
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"all">>,
            <<"zone">> => <<"">>
        },
        invalid_zone
    ),
    Assert(
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"all">>,
            <<"listener">> => <<"">>
        },
        invalid_listener
    ).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

ok_cases() ->
    [
        {
            {allow, all, {publish, [{qos, [0, 1, 2]}, {retain, all}]}, [<<"a/b/c">>]},
            #{
                <<"permission">> => <<"allow">>,
                <<"topic">> => <<"a/b/c">>,
                <<"action">> => <<"publish">>
            }
        },
        {
            {deny, all, {subscribe, [{qos, [1, 2]}]}, [{eq, <<"a/b/c">>}]},
            #{
                <<"permission">> => <<"deny">>,
                <<"topic">> => <<"eq a/b/c">>,
                <<"action">> => <<"subscribe">>,
                <<"retain">> => <<"true">>,
                <<"qos">> => <<"1,2">>
            }
        },
        {
            {allow, all, {publish, [{qos, [0, 1, 2]}, {retain, all}]}, [<<"a">>, <<"b">>]},
            #{
                <<"permission">> => <<"allow">>,
                <<"topics">> => [<<"a">>, <<"b">>],
                <<"action">> => <<"publish">>
            }
        },
        {
            {allow, all, {all, [{qos, [0, 1, 2]}, {retain, all}]}, []},
            #{
                <<"permission">> => <<"allow">>,
                <<"topics">> => [],
                <<"action">> => <<"all">>
            }
        },
        {
            {allow, {ipaddr, "192.168.1.0/24"}, {all, [{qos, [0, 1, 2]}, {retain, all}]}, []},
            #{
                <<"permission">> => <<"allow">>,
                <<"topics">> => [],
                <<"action">> => <<"all">>,
                <<"ipaddr">> => <<"192.168.1.0/24">>
            }
        },
        {
            {allow, {username, {re, <<"^x+$">>}}, {all, [{qos, [0, 1, 2]}, {retain, all}]}, []},
            #{
                <<"permission">> => <<"allow">>,
                <<"topics">> => [],
                <<"action">> => <<"all">>,
                <<"username_re">> => <<"^x+$">>
            }
        },
        {
            {allow, {clientid, {re, <<"^x+$">>}}, {all, [{qos, [0, 1, 2]}, {retain, all}]}, []},
            #{
                <<"permission">> => <<"allow">>,
                <<"topics">> => [],
                <<"action">> => <<"all">>,
                <<"clientid_re">> => <<"^x+$">>
            }
        },
        %% Retain
        {
            expected_rule_with_qos_retain([0, 1, 2], true),
            rule_with_raw_qos_retain(#{<<"retain">> => <<"true">>})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], true),
            rule_with_raw_qos_retain(#{<<"retain">> => true})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], false),
            rule_with_raw_qos_retain(#{<<"retain">> => false})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], false),
            rule_with_raw_qos_retain(#{<<"retain">> => <<"false">>})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], all),
            rule_with_raw_qos_retain(#{<<"retain">> => <<"all">>})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], all),
            rule_with_raw_qos_retain(#{<<"retain">> => undefined})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], all),
            rule_with_raw_qos_retain(#{<<"retain">> => null})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], all),
            rule_with_raw_qos_retain(#{})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], true),
            rule_with_raw_qos_retain(#{<<"retain">> => <<"1">>})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], false),
            rule_with_raw_qos_retain(#{<<"retain">> => <<"0">>})
        },
        %% Qos
        {
            expected_rule_with_qos_retain([2], all),
            rule_with_raw_qos_retain(#{<<"qos">> => <<"2">>})
        },
        {
            expected_rule_with_qos_retain([2], all),
            rule_with_raw_qos_retain(#{<<"qos">> => [<<"2">>]})
        },
        {
            expected_rule_with_qos_retain([1, 2], all),
            rule_with_raw_qos_retain(#{<<"qos">> => <<"1,2">>})
        },
        {
            expected_rule_with_qos_retain([1, 2], all),
            rule_with_raw_qos_retain(#{<<"qos">> => [<<"1">>, <<"2">>]})
        },
        {
            expected_rule_with_qos_retain([1, 2], all),
            rule_with_raw_qos_retain(#{<<"qos">> => [1, 2]})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], all),
            rule_with_raw_qos_retain(#{<<"qos">> => undefined})
        },
        {
            expected_rule_with_qos_retain([0, 1, 2], all),
            rule_with_raw_qos_retain(#{<<"qos">> => null})
        }
    ].

error_cases() ->
    [
        #{
            <<"permission">> => <<"allo">>,
            <<"topic">> => <<"a/b/c">>,
            <<"action">> => <<"publish">>
        },
        #{
            <<"permission">> => <<"allow">>,
            <<"topic">> => <<"a/b/c">>,
            <<"action">> => <<"publis">>
        },
        #{
            <<"permission">> => <<"allow">>,
            <<"topic">> => #{},
            <<"action">> => <<"publish">>
        },
        #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"publish">>
        }
    ].

error_rich_action_cases() ->
    [
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"publish">>,
            <<"qos">> => 3
        },
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"publish">>,
            <<"qos">> => <<"three">>
        },
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"publish">>,
            <<"retain">> => 3
        },
        #{
            <<"permission">> => <<"allow">>,
            <<"topics">> => [],
            <<"action">> => <<"publish">>,
            <<"qos">> => [<<"3">>]
        }
    ].

expected_rule_with_qos_retain(QoS, Retain) ->
    {allow, all, {publish, [{qos, QoS}, {retain, Retain}]}, []}.

rule_with_raw_qos_retain(Overrides) ->
    maps:merge(base_raw_rule(), Overrides).

base_raw_rule() ->
    #{
        <<"permission">> => <<"allow">>,
        <<"topics">> => [],
        <<"action">> => <<"publish">>
    }.

simple_rule({Pemission, Who, {Action, _Opts}, Topics}) ->
    {Pemission, Who, Action, Topics}.
