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

-module(emqx_authz_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authz.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, "authorization.no_match = deny, authorization.cache.enable = false"},
            emqx,
            emqx_auth,
            emqx_auth_mnesia
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    emqx_cth_suite:stop(?config(suite_apps, _Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    ok = setup_config(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    _ = emqx_authz:set_feature_available(rich_actions, true),
    ok = emqx_authz_mnesia:purge_rules().

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authz(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    test_authz(
        allow,
        allow,
        {all, #{
            <<"permission">> => <<"allow">>, <<"action">> => <<"subscribe">>, <<"topic">> => <<"t">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    test_authz(
        allow,
        allow,
        {{username, <<"username">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"t/${username}">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t/username">>}
    ),
    test_authz(
        allow,
        allow,
        {{username, <<"username">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"eq t/${username}">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t/${username}">>}
    ),
    test_authz(
        deny,
        deny,
        {{username, <<"username">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"eq t/${username}">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t/username">>}
    ),
    test_authz(
        allow,
        allow,
        {{clientid, <<"clientid">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"eq t/${username}">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t/${username}">>}
    ),
    test_authz(
        allow,
        allow,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"qos">> => <<"1,2">>,
                <<"retain">> => <<"true">>
            }
        },
        {ClientInfo, ?AUTHZ_PUBLISH(1, true), <<"t">>}
    ),
    test_authz(
        deny,
        allow,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"qos">> => <<"1,2">>,
                <<"retain">> => <<"true">>
            }
        },
        {ClientInfo, ?AUTHZ_PUBLISH(0, true), <<"t">>}
    ),
    test_authz(
        deny,
        allow,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"qos">> => <<"1,2">>,
                <<"retain">> => <<"true">>
            }
        },
        {ClientInfo, ?AUTHZ_PUBLISH(1, false), <<"t">>}
    ).

test_authz(Expected, ExpectedNoRichActions, {Who, Rule}, {ClientInfo, Action, Topic}) ->
    test_authz_with_rich_actions(true, Expected, {Who, Rule}, {ClientInfo, Action, Topic}),
    test_authz_with_rich_actions(
        false, ExpectedNoRichActions, {Who, Rule}, {ClientInfo, Action, Topic}
    ).

test_authz_with_rich_actions(
    RichActionsEnabled, Expected, {Who, Rule}, {ClientInfo, Action, Topic}
) ->
    ct:pal("Test authz rich_actions:~p~nwho:~p~nrule:~p~nattempt:~p~nexpected ~p", [
        RichActionsEnabled, Who, Rule, {ClientInfo, Action, Topic}, Expected
    ]),
    try
        _ = emqx_authz:set_feature_available(rich_actions, RichActionsEnabled),
        ok = emqx_authz_mnesia:store_rules(Who, [Rule]),
        ?assertEqual(Expected, emqx_access_control:authorize(ClientInfo, Action, Topic))
    after
        ok = emqx_authz_mnesia:purge_rules()
    end.

t_normalize_rules(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = emqx_authz_mnesia:store_rules(
        {username, <<"username">>},
        [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ?assertException(
        error,
        #{reason := invalid_rule},
        emqx_authz_mnesia:store_rules(
            {username, <<"username">>},
            [[<<"allow">>, <<"publish">>, <<"t">>]]
        )
    ),

    ?assertException(
        error,
        #{reason := invalid_action},
        emqx_authz_mnesia:store_rules(
            {username, <<"username">>},
            [
                #{
                    <<"permission">> => <<"allow">>,
                    <<"action">> => <<"badaction">>,
                    <<"topic">> => <<"t">>
                }
            ]
        )
    ),

    ?assertException(
        error,
        #{reason := invalid_permission},
        emqx_authz_mnesia:store_rules(
            {username, <<"username">>},
            [
                #{
                    <<"permission">> => <<"accept">>,
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"t">>
                }
            ]
        )
    ).

t_destroy(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = emqx_authz_mnesia:store_rules(
        {username, <<"username">>},
        [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ok = emqx_authz_test_lib:reset_authorizers(),

    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ok = setup_config(),

    %% After destroy, the rules should be empty

    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_conf_cli_load(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = emqx_authz_mnesia:store_rules(
        {username, <<"username">>},
        [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),
    PrevRules = ets:tab2list(emqx_acl),
    Hocon = emqx_conf_cli:get_config("authorization"),
    Bin = iolist_to_binary(hocon_pp:do(Hocon, #{})),
    ok = emqx_conf_cli:load_config(Bin, #{mode => merge}),
    %% ensure emqx_acl table not clear
    ?assertEqual(PrevRules, ets:tab2list(emqx_acl)),
    %% still working
    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_mnesia_authz_config() ->
    #{
        <<"enable">> => <<"true">>,
        <<"type">> => <<"built_in_database">>
    }.

setup_config() ->
    emqx_authz_test_lib:setup_config(raw_mnesia_authz_config(), #{}).
