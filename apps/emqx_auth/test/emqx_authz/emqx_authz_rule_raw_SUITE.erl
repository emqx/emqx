%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    _ = emqx_authz:set_feature_available(rich_actions, true),
    ok.

t_parse_ok(_Config) ->
    lists:foreach(
        fun({Expected, RuleRaw}) ->
            _ = emqx_authz:set_feature_available(rich_actions, true),
            ct:pal("Raw rule: ~p~nExpected: ~p~n", [RuleRaw, Expected]),
            ?assertEqual({ok, Expected}, emqx_authz_rule_raw:parse_rule(RuleRaw)),
            _ = emqx_authz:set_feature_available(rich_actions, false),
            ?assertEqual({ok, simple_rule(Expected)}, emqx_authz_rule_raw:parse_rule(RuleRaw))
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
    emqx_authz:set_feature_available(rich_actions, true),
    lists:foreach(
        fun(RuleRaw) ->
            ?assertMatch(
                {error, _},
                emqx_authz_rule_raw:parse_rule(RuleRaw)
            )
        end,
        error_cases() ++ error_rich_action_cases()
    ),

    %% without rich actions some fields are not parsed, so they are not errors when invalid
    _ = emqx_authz:set_feature_available(rich_actions, false),
    lists:foreach(
        fun(RuleRaw) ->
            ?assertMatch(
                {error, _},
                emqx_authz_rule_raw:parse_rule(RuleRaw)
            )
        end,
        error_cases()
    ),
    lists:foreach(
        fun(RuleRaw) ->
            ?assertMatch(
                {ok, _},
                emqx_authz_rule_raw:parse_rule(RuleRaw)
            )
        end,
        error_rich_action_cases()
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
    ).

t_format_no_rich_action(_Config) ->
    _ = emqx_authz:set_feature_available(rich_actions, false),

    Rule = {allow, all, {subscribe, [{qos, [1, 2]}, {retain, true}]}, [<<"a/b/c">>]},

    ?assertEqual(
        #{action => subscribe, permission => allow, topic => [<<"a/b/c">>]},
        emqx_authz_rule_raw:format_rule(Rule)
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
