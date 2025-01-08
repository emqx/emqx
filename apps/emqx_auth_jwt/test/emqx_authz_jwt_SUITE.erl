%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_jwt_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SECRET, <<"some_secret">>).
-define(AUTHN_PATH, [authentication]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "authorization.no_match = deny, authorization.cache.enable = false"},
            emqx_auth,
            emqx_auth_jwt
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        ?AUTHN_PATH,
        ?GLOBAL
    ),
    AuthConfig = authn_config(),
    {ok, _} = emqx:update_config(
        ?AUTHN_PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    ok = emqx_authz_test_lib:reset_authorizers(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_authn_test_lib:delete_authenticators(
        ?AUTHN_PATH,
        ?GLOBAL
    ),
    ok = emqx_authz_test_lib:restore_authorizers().

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_topic_rules(_Config) ->
    JWT = #{
        <<"exp">> => erlang:system_time(second) + 60,
        <<"acl">> => #{
            <<"pub">> => [
                <<"eq testpub1/${username}">>,
                <<"testpub2/${clientid}">>,
                <<"testpub3/#">>
            ],
            <<"sub">> => [
                <<"eq testsub1/${username}">>,
                <<"testsub2/${clientid}">>,
                <<"testsub3/#">>
            ],
            <<"all">> => [
                <<"eq testall1/${username}">>,
                <<"testall2/${clientid}">>,
                <<"testall3/#">>
            ]
        },
        <<"username">> => <<"username">>
    },
    test_topic_rules(JWT).

t_topic_rules_v2(_Config) ->
    JWT = #{
        <<"exp">> => erlang:system_time(second) + 60,
        <<"acl">> => [
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"pub">>,
                <<"topics">> => [
                    <<"eq testpub1/${username}">>,
                    <<"testpub2/${clientid}">>,
                    <<"testpub3/#">>
                ]
            },
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"sub">>,
                <<"topics">> =>
                    [
                        <<"eq testsub1/${username}">>,
                        <<"testsub2/${clientid}">>,
                        <<"testsub3/#">>
                    ]
            },
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"all">>,
                <<"topics">> => [
                    <<"eq testall1/${username}">>,
                    <<"testall2/${clientid}">>,
                    <<"testall3/#">>
                ]
            }
        ],
        <<"username">> => <<"username">>
    },
    test_topic_rules(JWT).

test_topic_rules(JWTInput) ->
    JWT = generate_jws(JWTInput),

    {ok, C} = emqtt:start_link(
        [
            {clean_start, true},
            {proto_ver, v5},
            {clientid, <<"clientid">>},
            {username, <<"username">>},
            {password, JWT}
        ]
    ),
    {ok, _} = emqtt:connect(C),

    Cases = [
        {deny, <<"testpub1/username">>},
        {deny, <<"testpub2/clientid">>},
        {deny, <<"testpub3/foobar">>},

        {deny, <<"testsub1/username">>},
        {allow, <<"testsub1/${username}">>},
        {allow, <<"testsub2/clientid">>},
        {allow, <<"testsub3/foobar">>},
        {allow, <<"testsub3/+/foobar">>},
        {allow, <<"testsub3/#">>},

        {deny, <<"testsub2/username">>},
        {deny, <<"testsub1/clientid">>},
        {deny, <<"testsub4/foobar">>},

        {deny, <<"testall1/username">>},
        {allow, <<"testall1/${username}">>},
        {allow, <<"testall2/clientid">>},
        {allow, <<"testall3/foobar">>},
        {allow, <<"testall3/+/foobar">>},
        {allow, <<"testall3/#">>},

        {deny, <<"testall2/username">>},
        {deny, <<"testall1/clientid">>},
        {deny, <<"testall4/foobar">>}
    ],

    lists:foreach(
        fun
            ({allow, Topic}) ->
                ?assertMatch(
                    {ok, #{}, [0]},
                    emqtt:subscribe(C, Topic, 0)
                );
            ({deny, Topic}) ->
                ?assertMatch(
                    {ok, #{}, [?RC_NOT_AUTHORIZED]},
                    emqtt:subscribe(C, Topic, 0)
                )
        end,
        Cases
    ),

    ok = emqtt:disconnect(C).

t_check_pub(_Config) ->
    Payload = #{
        <<"username">> => <<"username">>,
        <<"acl">> => #{<<"sub">> => [<<"a/b">>]},
        <<"exp">> => erlang:system_time(second) + 10
    },

    JWT = generate_jws(Payload),

    {ok, C} = emqtt:start_link(
        [
            {clean_start, true},
            {proto_ver, v5},
            {clientid, <<"clientid">>},
            {username, <<"username">>},
            {password, JWT}
        ]
    ),
    {ok, _} = emqtt:connect(C),

    ok = emqtt:publish(C, <<"a/b">>, <<"hi">>, 0),

    receive
        {publish, #{topic := <<"a/b">>}} ->
            ?assert(false, "Publish to `a/b` should not be allowed")
    after 100 -> ok
    end,

    ok = emqtt:disconnect(C).

t_check_no_recs(_Config) ->
    Payload = #{
        <<"username">> => <<"username">>,
        <<"acl">> => #{},
        <<"exp">> => erlang:system_time(second) + 10
    },

    JWT = generate_jws(Payload),

    {ok, C} = emqtt:start_link(
        [
            {clean_start, true},
            {proto_ver, v5},
            {clientid, <<"clientid">>},
            {username, <<"username">>},
            {password, JWT}
        ]
    ),
    {ok, _} = emqtt:connect(C),

    ?assertMatch(
        {ok, #{}, [?RC_NOT_AUTHORIZED]},
        emqtt:subscribe(C, <<"a/b">>, 0)
    ),

    ok = emqtt:disconnect(C).

t_check_no_acl_claim(_Config) ->
    Payload = #{
        <<"username">> => <<"username">>,
        <<"exp">> => erlang:system_time(second) + 10
    },

    JWT = generate_jws(Payload),

    {ok, C} = emqtt:start_link(
        [
            {clean_start, true},
            {proto_ver, v5},
            {clientid, <<"clientid">>},
            {username, <<"username">>},
            {password, JWT}
        ]
    ),
    {ok, _} = emqtt:connect(C),

    ?assertMatch(
        {ok, #{}, [?RC_NOT_AUTHORIZED]},
        emqtt:subscribe(C, <<"a/b">>, 0)
    ),

    ok = emqtt:disconnect(C).

t_check_str_exp(_Config) ->
    Payload = #{
        <<"username">> => <<"username">>,
        <<"exp">> => integer_to_binary(erlang:system_time(second) + 10),
        <<"acl">> => #{<<"sub">> => [<<"a/b">>]}
    },

    JWT = generate_jws(Payload),

    {ok, C} = emqtt:start_link(
        [
            {clean_start, true},
            {proto_ver, v5},
            {clientid, <<"clientid">>},
            {username, <<"username">>},
            {password, JWT}
        ]
    ),
    {ok, _} = emqtt:connect(C),

    ?assertMatch(
        {ok, #{}, [0]},
        emqtt:subscribe(C, <<"a/b">>, 0)
    ),

    ok = emqtt:disconnect(C).

t_check_expire(_Config) ->
    Payload = #{
        <<"username">> => <<"username">>,
        <<"acl">> => #{<<"sub">> => [<<"a/b">>]},
        <<"exp">> => erlang:system_time(second) + 5
    },

    JWT = generate_jws(Payload),

    {ok, C} = emqtt:start_link(
        [
            {clean_start, true},
            {proto_ver, v5},
            {clientid, <<"clientid">>},
            {username, <<"username">>},
            {password, JWT}
        ]
    ),
    {ok, _} = emqtt:connect(C),
    ?assertMatch(
        {ok, #{}, [0]},
        emqtt:subscribe(C, <<"a/b">>, 0)
    ),

    ?assertMatch(
        {ok, #{}, [0]},
        emqtt:unsubscribe(C, <<"a/b">>)
    ),

    timer:sleep(6000),

    ?assertMatch(
        {ok, #{}, [?RC_NOT_AUTHORIZED]},
        emqtt:subscribe(C, <<"a/b">>, 0)
    ),

    ok = emqtt:disconnect(C).

t_check_no_expire(_Config) ->
    Payload = #{
        <<"username">> => <<"username">>,
        <<"acl">> => #{<<"sub">> => [<<"a/b">>]}
    },

    JWT = generate_jws(Payload),

    {ok, C} = emqtt:start_link(
        [
            {clean_start, true},
            {proto_ver, v5},
            {clientid, <<"clientid">>},
            {username, <<"username">>},
            {password, JWT}
        ]
    ),
    {ok, _} = emqtt:connect(C),
    ?assertMatch(
        {ok, #{}, [0]},
        emqtt:subscribe(C, <<"a/b">>, 0)
    ),

    ?assertMatch(
        {ok, #{}, [0]},
        emqtt:unsubscribe(C, <<"a/b">>)
    ),

    ok = emqtt:disconnect(C).

t_check_undefined_expire(_Config) ->
    Acl = #{expire => undefined, rules => #{<<"sub">> => [<<"a/b">>]}},
    Client = #{acl => Acl},

    ?assertMatch(
        {matched, allow},
        emqx_authz_client_info:authorize(Client, ?AUTHZ_SUBSCRIBE, <<"a/b">>, undefined)
    ),

    ?assertMatch(
        {matched, deny},
        emqx_authz_client_info:authorize(Client, ?AUTHZ_SUBSCRIBE, <<"a/bar">>, undefined)
    ).

t_invalid_rule(_Config) ->
    emqx_logger:set_log_level(debug),
    MakeJWT = fun(Acl) ->
        generate_jws(#{
            <<"exp">> => erlang:system_time(second) + 60,
            <<"username">> => <<"username">>,
            <<"acl">> => Acl
        })
    end,
    InvalidAclList =
        [
            %% missing action
            [#{<<"permission">> => <<"invalid">>}],
            %% missing topic or topics
            [#{<<"permission">> => <<"allow">>, <<"action">> => <<"pub">>}],
            %% invlaid permission, must be allow | deny
            [
                #{
                    <<"permission">> => <<"invalid">>,
                    <<"action">> => <<"pub">>,
                    <<"topic">> => <<"t">>
                }
            ],
            %% invalid action, must be pub | sub | all
            [
                #{
                    <<"permission">> => <<"allow">>,
                    <<"action">> => <<"invalid">>,
                    <<"topic">> => <<"t">>
                }
            ],
            %% invalid qos
            [
                #{
                    <<"permission">> => <<"allow">>,
                    <<"action">> => <<"pub">>,
                    <<"topics">> => [<<"t">>],
                    <<"qos">> => 3
                }
            ]
        ],
    lists:foreach(
        fun(InvalidAcl) ->
            {ok, C} = emqtt:start_link(
                [
                    {clean_start, true},
                    {proto_ver, v5},
                    {clientid, <<"clientid">>},
                    {username, <<"username">>},
                    {password, MakeJWT(InvalidAcl)}
                ]
            ),
            unlink(C),
            ?assertMatch({error, {bad_username_or_password, _}}, emqtt:connect(C))
        end,
        InvalidAclList
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

authn_config() ->
    #{
        <<"mechanism">> => <<"jwt">>,
        <<"use_jwks">> => false,
        <<"algorithm">> => <<"hmac-based">>,
        <<"secret">> => ?SECRET,
        <<"secret_base64_encoded">> => false,
        <<"acl_claim_name">> => <<"acl">>,
        <<"disconnect_after_expire">> => false,
        <<"verify_claims">> => #{
            <<"username">> => ?PH_USERNAME
        }
    }.

generate_jws(Payload) ->
    JWK = jose_jwk:from_oct(?SECRET),
    Header = #{
        <<"alg">> => <<"HS256">>,
        <<"typ">> => <<"JWT">>
    },
    Signed = jose_jwt:sign(JWK, Header, Payload),
    {_, JWS} = jose_jws:compact(Signed),
    JWS.
