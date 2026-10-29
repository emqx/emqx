%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_rule_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(CLIENT_INFO_BASE, #{
    clientid => <<"test">>,
    username => <<"test">>,
    peerhost => {127, 0, 0, 1},
    zone => default,
    listener => 'tcp:default'
}).

-define(PROFILE_ENV_VAR, "EMQX_SECURITY_PROFILE").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, #{
                config => #{
                    authorization =>
                        #{
                            cache => #{enable => false},
                            no_match => deny,
                            sources => []
                        }
                }
            }},
            emqx_auth,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.
end_per_testcase(_TestCase, _Config) ->
    ok.

t_compile(_) ->
    % NOTE
    % Some of the following testcase are relying on the internal representation of
    % `emqx_template:t()`. If the internal representation is changed, these testcases
    % may fail.
    ?assertEqual({deny, all, all, ['ALL_TOPICS']}, emqx_authz_rule:compile({deny, all})),
    ?assertEqual({deny, all, all, ['ALL_TOPICS']}, emqx_authz_rule:compile({deny, all, all, all})),
    ?assertEqual(
        {deny, all, subscribe, ['ALL_TOPICS']}, emqx_authz_rule:compile({deny, all, subscribe, all})
    ),

    ?assertEqual(
        {allow, {ipaddr, {{127, 0, 0, 1}, {127, 0, 0, 1}, 32}}, all, [{eq, <<"#">>}, {eq, <<"+">>}]},
        emqx_authz_rule:compile({allow, {ipaddr, "127.0.0.1"}, all, [{eq, "#"}, {eq, "+"}]})
    ),

    %% A filter with a wildcard is split, a filter without one stays a binary.
    ?assertEqual(
        {allow, all, all, [[<<"a">>, '+', <<"b">>], [<<"a">>, '#'], <<"a/b">>, {eq, <<"a/#">>}]},
        emqx_authz_rule:compile({allow, all, all, ["a/+/b", "a/#", "a/b", {eq, "a/#"}]})
    ),

    ?assertMatch(
        {allow,
            {ipaddrs, [
                {{127, 0, 0, 1}, {127, 0, 0, 1}, 32},
                {{192, 168, 1, 0}, {192, 168, 1, 255}, 24}
            ]},
            subscribe, [{pattern, [{var, "clientid", [_]}]}]},
        emqx_authz_rule:compile(
            {allow, {ipaddrs, ["127.0.0.1", "192.168.1.0/24"]}, subscribe, [?PH_S_CLIENTID]}
        )
    ),

    ?assertEqual(
        {allow, {'and', [{clientid, {eq, <<"test">>}}, {username, {eq, <<"test">>}}]}, publish, [
            <<"topic/test">>
        ]},
        emqx_authz_rule:compile(
            {allow, {'and', [{client, "test"}, {user, "test"}]}, publish, ["topic/test"]}
        )
    ),

    ?assertMatch(
        {allow,
            {'or', [
                {username, {re_pattern, _, _, _, _}},
                {clientid, {re_pattern, _, _, _, _}}
            ]},
            publish, [
                {pattern, [{var, "username", [_]}]}, {pattern, [{var, "clientid", [_]}]}
            ]},
        emqx_authz_rule:compile(
            {allow,
                {'or', [
                    {username, {re, "^test"}},
                    {clientid, {re, "test?"}}
                ]},
                publish, [?PH_S_USERNAME, ?PH_S_CLIENTID]}
        )
    ),

    ?assertMatch(
        {allow, {username, {eq, <<"test">>}}, publish, [
            {pattern, [<<"t/foo">>, {var, "username", [_]}, <<"boo">>]}
        ]},
        emqx_authz_rule:compile({allow, {username, "test"}, publish, ["t/foo${username}boo"]})
    ),

    ?assertEqual(
        {allow, {username, {eq, <<"test">>}},
            #{action_type => publish, qos => [0, 1, 2], retain => all}, [<<"topic/test">>]},
        emqx_authz_rule:compile(
            {allow, {username, "test"}, {publish, [{retain, all}]}, ["topic/test"]}
        )
    ),

    ?assertEqual(
        {allow, {username, {eq, <<"test">>}}, #{action_type => publish, qos => [1], retain => true},
            [
                <<"topic/test">>
            ]},
        emqx_authz_rule:compile(
            {allow, {username, "test"}, {publish, [{qos, 1}, {retain, true}]}, ["topic/test"]}
        )
    ),

    ?assertEqual(
        {allow, {username, {eq, <<"test">>}}, #{action_type => subscribe, qos => [1, 2]}, [
            <<"topic/test">>
        ]},
        emqx_authz_rule:compile(
            {allow, {username, "test"}, {subscribe, [{qos, 1}, {qos, 2}]}, ["topic/test"]}
        )
    ),

    ?assertEqual(
        {allow, {username, {eq, <<"test">>}}, #{action_type => subscribe, qos => [1]}, [
            <<"topic/test">>
        ]},
        emqx_authz_rule:compile(
            {allow, {username, "test"}, {subscribe, [{qos, 1}]}, ["topic/test"]}
        )
    ),

    ?assertEqual(
        {allow, {username, {eq, <<"test">>}}, #{action_type => all, qos => [2], retain => true}, [
            <<"topic/test">>
        ]},
        emqx_authz_rule:compile(
            {allow, {username, "test"}, {all, [{qos, 2}, {retain, true}]}, ["topic/test"]}
        )
    ),

    ?assertEqual(
        {allow, {client_attr, <<"a1">>, {eq, <<"v1">>}}, all, [<<"topic/test">>]},
        emqx_authz_rule:compile(
            {allow, {client_attr, "a1", "v1"}, all, ["topic/test"]}
        )
    ),

    ?assertMatch(
        {allow, {client_attr, <<"a2">>, {re_pattern, _, _, _, _}}, all, [<<"topic/test">>]},
        emqx_authz_rule:compile(
            {allow, {client_attr, "a2", {re, "v2.*"}}, all, ["topic/test"]}
        )
    ),

    ?assertMatch(
        {allow, {client_attr, <<"a2">>, {re_pattern, _, _, _, _}}, all, ['ALL_TOPICS']},
        emqx_authz_rule:compile(
            {allow, {client_attr, "a2", {re, "v2.*"}}, all, all}
        )
    ),

    ?assertEqual(
        {allow, {security_profile, legacy}, all, ['ALL_TOPICS']},
        emqx_authz_rule:compile({allow, {security_profile, legacy}})
    ),

    ?assertEqual(
        {allow, {security_profile, hardened}, all, ['ALL_TOPICS']},
        emqx_authz_rule:compile({allow, {security_profile, hardened}, all, all})
    ),
    ok.

t_match(_) ->
    ?assertEqual(
        {matched, deny},
        emqx_authz_rule:match(
            client_info(),
            #{action_type => subscribe, qos => 0},
            <<"#">>,
            emqx_authz_rule:compile({deny, all})
        )
    ),

    ?assertEqual(
        {matched, deny},
        emqx_authz_rule:match(
            client_info(#{peerhost => {192, 168, 1, 10}}),
            #{action_type => subscribe, qos => 0},
            <<"+">>,
            emqx_authz_rule:compile({deny, all})
        )
    ),

    ?assertEqual(
        {matched, deny},
        emqx_authz_rule:match(
            client_info(#{username => <<"fake">>}),
            #{action_type => subscribe, qos => 0},
            <<"topic/test">>,
            emqx_authz_rule:compile({deny, all})
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(),
            #{action_type => subscribe, qos => 0},
            <<"#">>,
            emqx_authz_rule:compile({allow, {ipaddr, "127.0.0.1"}, all, [{eq, "#"}, {eq, "+"}]})
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(),
            #{action_type => subscribe, qos => 0},
            <<"topic/test">>,
            emqx_authz_rule:compile({allow, {ipaddr, "127.0.0.1"}, all, [{eq, "#"}, {eq, "+"}]})
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{peerhost => {192, 168, 1, 10}}),
            #{action_type => subscribe, qos => 0},
            <<"#">>,
            emqx_authz_rule:compile({allow, {ipaddr, "127.0.0.1"}, all, [{eq, "#"}, {eq, "+"}]})
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(),
            #{action_type => subscribe, qos => 0},
            <<"test">>,
            emqx_authz_rule:compile(
                {allow, {ipaddrs, ["127.0.0.1", "192.168.1.0/24"]}, subscribe, [?PH_S_CLIENTID]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{peerhost => {192, 168, 1, 10}}),
            #{action_type => subscribe, qos => 0},
            <<"test">>,
            emqx_authz_rule:compile(
                {allow, {ipaddrs, ["127.0.0.1", "192.168.1.0/24"]}, subscribe, [?PH_S_CLIENTID]}
            )
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{peerhost => {192, 168, 1, 10}}),
            #{action_type => subscribe, qos => 0},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {ipaddrs, ["127.0.0.1", "192.168.1.0/24"]}, subscribe, [?PH_S_CLIENTID]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(),
            #{action_type => publish, qos => 0, retain => false},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {'and', [{client, "test"}, {user, "test"}]}, publish, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{peerhost => {192, 168, 1, 10}}),
            #{action_type => publish, qos => 0, retain => false},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {'and', [{client, "test"}, {user, "test"}]}, publish, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{username => <<"fake">>}),
            #{action_type => publish, qos => 0, retain => false},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {'and', [{client, "test"}, {user, "test"}]}, publish, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 0, retain => false},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {'and', [{client, "test"}, {user, "test"}]}, publish, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(),
            #{action_type => publish, qos => 0, retain => false},
            <<"test">>,
            emqx_authz_rule:compile(
                {allow,
                    {'or', [
                        {username, {re, "^test"}},
                        {clientid, {re, "test?"}}
                    ]},
                    publish, [?PH_S_USERNAME, ?PH_S_CLIENTID]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{peerhost => {192, 168, 1, 10}}),
            #{action_type => publish, qos => 0, retain => false},
            <<"test">>,
            emqx_authz_rule:compile(
                {allow,
                    {'or', [
                        {username, {re, "^test"}},
                        {clientid, {re, "test?"}}
                    ]},
                    publish, [?PH_S_USERNAME, ?PH_S_CLIENTID]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{username => <<"fake">>}),
            #{action_type => publish, qos => 0, retain => false},
            <<"test">>,
            emqx_authz_rule:compile(
                {allow,
                    {'or', [
                        {username, {re, "^test"}},
                        {clientid, {re, "test?"}}
                    ]},
                    publish, [?PH_S_USERNAME, ?PH_S_CLIENTID]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{username => <<"fake">>}),
            #{action_type => publish, qos => 0, retain => false},
            <<"fake">>,
            emqx_authz_rule:compile(
                {allow,
                    {'or', [
                        {username, {re, "^test"}},
                        {clientid, {re, "test?"}}
                    ]},
                    publish, [?PH_S_USERNAME, ?PH_S_CLIENTID]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 0, retain => false},
            <<"test">>,
            emqx_authz_rule:compile(
                {allow,
                    {'or', [
                        {username, {re, "^test"}},
                        {clientid, {re, "test?"}}
                    ]},
                    publish, [?PH_S_USERNAME, ?PH_S_CLIENTID]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 0, retain => false},
            <<"fake">>,
            emqx_authz_rule:compile(
                {allow,
                    {'or', [
                        {username, {re, "^test"}},
                        {clientid, {re, "test?"}}
                    ]},
                    publish, [?PH_S_USERNAME, ?PH_S_CLIENTID]}
            )
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(),
            #{action_type => publish, qos => 0, retain => false},
            <<"t/foo${username}boo">>,
            emqx_authz_rule:compile({allow, {username, "test"}, publish, ["t/foo${username}boo"]})
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 0, retain => false},
            <<"t/footestboo">>,
            emqx_authz_rule:compile({allow, {username, "test"}, publish, ["t/foo${username}boo"]})
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 1, retain => false},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {publish, [{retain, all}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 0, retain => true},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {publish, [{retain, all}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 1, retain => true},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {publish, [{qos, 1}, {retain, true}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 0, retain => true},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {publish, [{qos, 1}, {retain, true}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 1, retain => false},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {publish, [{qos, 1}, {retain, true}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => subscribe, qos => 0},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {subscribe, []}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => subscribe, qos => 2},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {subscribe, []}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => subscribe, qos => 1},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {subscribe, [{qos, 1}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => subscribe, qos => 0},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {subscribe, [{qos, 1}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => subscribe, qos => 2},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {all, [{qos, 2}, {retain, true}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => subscribe, qos => 0},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {all, [{qos, 2}, {retain, true}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 1, retain => true},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {all, [{qos, 2}, {retain, true}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 2, retain => true},
            <<"topic/test">>,
            emqx_authz_rule:compile(
                {allow, {username, "test"}, {all, [{qos, 2}, {retain, true}]}, ["topic/test"]}
            )
        )
    ),

    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 2, retain => true},
            <<"topic/test">>,
            emqx_authz_rule:compile({allow, all, publish, ["#"]})
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => subscribe, qos => 2},
            <<"topic/test">>,
            emqx_authz_rule:compile({allow, all, publish, ["#"]})
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{username => undefined, peerhost => undefined}),
            #{action_type => subscribe, qos => 2},
            <<"topic/test">>,
            emqx_authz_rule:compile({allow, {username, "user"}, all, ["#"]})
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{username => undefined, peerhost => undefined}),
            #{action_type => subscribe, qos => 2},
            <<"topic/test">>,
            emqx_authz_rule:compile({allow, {ipaddr, "127.0.0.1"}, all, ["#"]})
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{username => undefined, peerhost => undefined}),
            #{action_type => subscribe, qos => 2},
            <<"topic/test">>,
            emqx_authz_rule:compile({allow, {ipaddrs, []}, all, ["#"]})
        )
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:match(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => subscribe, qos => 2},
            <<"topic/test">>,
            emqx_authz_rule:compile({allow, {clientid, {re, "^test"}}, all, ["#"]})
        )
    ),

    ?assertEqual(
        {matched, deny},
        emqx_authz_rule:match(
            client_info(),
            #{action_type => subscribe, qos => 0},
            <<"$topic/test">>,
            emqx_authz_rule:compile({deny, all})
        )
    ),
    ok.

t_security_profile_condition(_) ->
    Rule = emqx_authz_rule:compile({allow, {security_profile, legacy}, all, all}),
    emqx_common_test_helpers:with_security_profile("legacy", fun() ->
        ?assertEqual(
            {matched, allow},
            emqx_authz_rule:match(
                client_info(),
                #{action_type => publish, qos => 0, retain => false},
                <<"topic/test">>,
                Rule
            )
        )
    end),
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        ?assertEqual(
            nomatch,
            emqx_authz_rule:match(
                client_info(),
                #{action_type => publish, qos => 0, retain => false},
                <<"topic/test">>,
                Rule
            )
        )
    end).

t_topic_template_allow_security_profile(_) ->
    Default = emqx_config:get([authorization, topic_template_allow]),
    AllowAll = #{plus => true, hash => true, slash => true},
    try
        emqx_common_test_helpers:with_security_profile("legacy", fun() ->
            assert_topic_template_injection_result(nomatch)
        end),
        emqx_common_test_helpers:with_security_profile("hardened", fun() ->
            assert_topic_template_injection_result({matched, deny})
        end),
        emqx_config:put([authorization, topic_template_allow], AllowAll),
        emqx_common_test_helpers:with_security_profile("hardened", fun() ->
            assert_topic_template_injection_result({matched, allow})
        end)
    after
        emqx_config:put([authorization, topic_template_allow], Default)
    end.

t_invalid_rule(_) ->
    ?assertThrow(
        #{reason := invalid_authorization_permission},
        emqx_authz_rule:compile({allawww, all, all, ["topic/test"]})
    ),

    ?assertThrow(
        #{reason := invalid_authorization_rule},
        emqx_authz_rule:compile(ooops)
    ),

    ?assertThrow(
        #{reason := invalid_authorization_qos},
        emqx_authz_rule:compile({allow, {username, "test"}, {publish, [{qos, 3}]}, ["topic/test"]})
    ),

    ?assertThrow(
        #{reason := invalid_authorization_retain},
        emqx_authz_rule:compile(
            {allow, {username, "test"}, {publish, [{retain, 'FALSE'}]}, ["topic/test"]}
        )
    ),

    ?assertThrow(
        #{reason := invalid_authorization_action},
        emqx_authz_rule:compile({allow, all, unsubscribe, ["topic/test"]})
    ),

    ?assertThrow(
        #{reason := invalid_client_match_condition},
        emqx_authz_rule:compile({allow, who, all, ["topic/test"]})
    ),

    ?assertThrow(
        #{reason := invalid_client_match_condition},
        emqx_authz_rule:compile({allow, {security_profile, unsupported}, all, ["topic/test"]})
    ),

    ?assertThrow(
        #{reason := invalid_re_pattern, type := clientid},
        emqx_authz_rule:compile({allow, {clientid, {re, "["}}, all, ["topic/test"]})
    ),

    ?assertThrow(
        #{reason := invalid_re_pattern, type := username},
        emqx_authz_rule:compile({allow, {username, {re, "["}}, all, ["topic/test"]})
    ),

    ?assertThrow(
        #{reason := invalid_re_pattern, type := zone},
        emqx_authz_rule:compile({allow, {zone, {re, "["}}, all, ["topic/test"]})
    ),

    ?assertThrow(
        #{reason := invalid_re_pattern, type := listener},
        emqx_authz_rule:compile({allow, {listener, {re, "["}}, all, ["topic/test"]})
    ),

    ?assertThrow(
        #{reason := invalid_re_pattern, type := {client_attr, "a"}},
        emqx_authz_rule:compile({allow, {client_attr, "a", {re, "["}}, all, ["topic/test"]})
    ),

    ok.

t_match_client_attr(_) ->
    Topic = <<"test/topic">>,
    RuleFn = fun(AttrName, AttrValue) ->
        emqx_authz_rule:compile(
            {allow, {'and', [{client, "c1"}, {client_attr, AttrName, AttrValue}]}, publish, [Topic]}
        )
    end,
    ClientInfoFn = fun(AttrName, AttrValue) ->
        client_info(#{clientid => <<"c1">>, client_attrs => #{bin(AttrName) => bin(AttrValue)}})
    end,
    Action = #{action_type => publish, qos => 0, retain => false},
    MatchFn = fun(RuleAttrName, RuleAttrValue, AttrName, AttrValue) ->
        ClientInfo = ClientInfoFn(AttrName, AttrValue),
        Rule = RuleFn(RuleAttrName, RuleAttrValue),
        emqx_authz_rule:match(ClientInfo, Action, Topic, Rule)
    end,

    ?assertEqual({matched, allow}, MatchFn("a1", "v1", "a1", "v1")),
    ?assertEqual(nomatch, MatchFn("a1", "v1", "a1", "v2")),
    ?assertEqual(nomatch, MatchFn("a1", "v1", "a2", "v1")),
    ?assertEqual({matched, allow}, MatchFn("a1", {re, "v1"}, "a1", "v1")),
    ?assertEqual({matched, allow}, MatchFn("a1", {re, "^abc.+"}, "a1", "abcd")),
    ?assertEqual(nomatch, MatchFn("a1", {re, "^abc.+"}, "a1", "abc")),
    ?assertEqual(nomatch, MatchFn("a1", {re, "^abc.+"}, "a2", "abcd")),
    ok.

t_matches(_) ->
    ?assertEqual(
        {matched, allow},
        emqx_authz_rule:matches(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 2, retain => true},
            <<"topic/test">>,
            [
                emqx_authz_rule:compile(
                    {allow, {username, "test"}, {subscribe, [{qos, 1}]}, ["topic/test"]}
                ),
                emqx_authz_rule:compile(
                    {allow, {username, "test"}, {all, [{qos, 2}, {retain, true}]}, ["topic/test"]}
                )
            ]
        )
    ),

    Rule = emqx_authz_rule:compile(
        {allow, {username, "test"}, {all, [{qos, 2}, {retain, true}]}, ["topic/test"]}
    ),

    ?assertEqual(
        nomatch,
        emqx_authz_rule:matches(
            client_info(#{clientid => <<"fake">>}),
            #{action_type => publish, qos => 1, retain => true},
            <<"topic/test">>,
            [Rule, Rule, Rule]
        )
    ).

%% Rules split only the filters with a wildcard, never the topic, and match rendered filters
%% as binaries. The result must be the same as the word matching that split every filter and
%% the topic.
t_topic_match_same_as_words(_) ->
    %% A rendered filter that ends with # and a topic with a # level that is not the last
    %% level: the words do not match, so the binaries do not match either.
    ClientInfoA = client_info(#{username => <<"a">>}),
    Sub = #{action_type => subscribe, qos => 0},
    HashRule = emqx_authz_rule:compile({allow, all, all, ["${username}/#"]}),
    ?assertEqual(nomatch, emqx_authz_rule:match(ClientInfoA, Sub, <<"a/#/b">>, HashRule)),
    ?assertEqual({matched, allow}, emqx_authz_rule:match(ClientInfoA, Sub, <<"a/#">>, HashRule)),
    ?assertEqual({matched, allow}, emqx_authz_rule:match(ClientInfoA, Sub, <<"a/b/c">>, HashRule)),
    ?assert(
        proper:quickcheck(
            ?FORALL(
                {Topic, Filter, Username},
                {topic_gen(), topic_filter_gen(), oneof([<<"a">>, <<"$SYS">>, <<"u">>])},
                begin
                    ClientInfo = client_info(#{username => Username}),
                    Rule = emqx_authz_rule:compile({allow, all, all, [Filter]}),
                    New = emqx_authz_rule:match(
                        ClientInfo, #{action_type => subscribe, qos => 0}, Topic, Rule
                    ),
                    Old = word_match(Topic, Filter, Username),
                    ?WHENFAIL(
                        ct:pal("topic: ~0p~nfilter: ~0p~nnew: ~0p~nold: ~0p", [
                            Topic, Filter, New, Old
                        ]),
                        New =:= Old
                    )
                end
            ),
            [{numtests, 20000}, {to_file, user}]
        )
    ).

%% Matching before topic filters were kept as binaries.
word_match(Topic, Filter, Username) ->
    TopicWords = emqx_topic:words(Topic),
    Matched =
        case Filter of
            {eq, F} -> TopicWords =:= emqx_topic:words(bin(F));
            "eq " ++ F -> TopicWords =:= emqx_topic:words(bin(F));
            <<"eq ", F/binary>> -> TopicWords =:= emqx_topic:words(F);
            F -> emqx_topic:match(TopicWords, emqx_topic:words(render_username(bin(F), Username)))
        end,
    case Matched of
        true -> {matched, allow};
        false -> nomatch
    end.

render_username(Filter, Username) ->
    binary:replace(Filter, <<"${username}">>, Username, [global]).

level_gen() ->
    oneof([
        <<"a">>,
        <<"b">>,
        <<"$SYS">>,
        <<"$share">>,
        <<"+">>,
        <<"#">>,
        <<>>,
        <<"abcde">>,
        <<"abcdefgh">>,
        <<"a+">>,
        <<"+a">>,
        <<"#a">>
    ]).

levels_gen() ->
    ?LET(Levels, non_empty(list(level_gen())), lists:join(<<"/">>, Levels)).

%% Valid and invalid topics, and shared subscriptions.
topic_gen() ->
    oneof([
        ?LET(Levels, levels_gen(), iolist_to_binary(Levels)),
        ?LET(Levels, levels_gen(), iolist_to_binary([<<"$share/g/">> | Levels])),
        ?LET(Levels, levels_gen(), #share{group = <<"g">>, topic = iolist_to_binary(Levels)})
    ]).

topic_filter_gen() ->
    ?LET(
        {Levels, Template},
        {levels_gen(), oneof([none, prefix, suffix])},
        begin
            %% A prefix template can render a filter that ends with #.
            F =
                case Template of
                    prefix -> iolist_to_binary([<<"${username}/">>, Levels]);
                    suffix -> iolist_to_binary([Levels, <<"/${username}">>]);
                    none -> iolist_to_binary(Levels)
                end,
            oneof([F, binary_to_list(F), {eq, F}, "eq " ++ binary_to_list(F), <<"eq ", F/binary>>])
        end
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

client_info() ->
    ?CLIENT_INFO_BASE.

client_info(Overrides) ->
    maps:merge(?CLIENT_INFO_BASE, Overrides).

with_security_profile(Profile, Fun) ->
    os:putenv(?PROFILE_ENV_VAR, Profile),
    emqx_security_profile:clear_profile(),
    try
        Fun()
    after
        os:unsetenv(?PROFILE_ENV_VAR),
        emqx_security_profile:clear_profile()
    end.

assert_topic_template_injection_result(Expected) ->
    Action = #{action_type => publish, qos => 0, retain => false},
    Cases = [
        {#{username => <<"+">>}, <<"tenant/other/data">>, "tenant/${username}/data"},
        {#{username => <<"#">>}, <<"tenant/other/data">>, "tenant/${username}"},
        {
            #{username => <<"other/level">>},
            <<"tenant/other/level/data">>,
            "tenant/${username}/data"
        },
        {#{username => "+"}, <<"tenant/other/data">>, "tenant/${username}/data"},
        {#{username => "#"}, <<"tenant/other/data">>, "tenant/${username}"},
        {#{username => "other/level"}, <<"tenant/other/level/data">>, "tenant/${username}/data"}
    ],
    lists:foreach(
        fun({ClientInfoOverride, Topic, TopicTemplate}) ->
            ?assertEqual(
                Expected,
                emqx_authz_rule:match(
                    client_info(ClientInfoOverride),
                    Action,
                    Topic,
                    emqx_authz_rule:compile({allow, all, publish, [TopicTemplate]})
                )
            )
        end,
        Cases
    ).

bin(X) -> iolist_to_binary(X).
