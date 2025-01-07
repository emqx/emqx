%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_cinfo_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(AUTHN_ID, <<"mechanism:cinfo">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_cinfo], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    %% ensure module loaded
    _ = emqx_variform_bif:module_info(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_ignore_enhanced_auth(_) ->
    ?assertEqual(ignore, emqx_authn_cinfo:authenticate(#{auth_method => <<"enhanced">>}, state)).

t_username_equal_clientid(_) ->
    Checks =
        [
            #{
                is_match => <<"is_empty_val(username)">>,
                result => deny
            },
            #{
                is_match => <<"str_eq(username, clientid)">>,
                result => allow
            }
        ],
    with_checks(
        Checks,
        fun(State) ->
            ?assertMatch(
                {error, bad_username_or_password},
                emqx_authn_cinfo:authenticate(#{username => <<>>}, State)
            ),
            ?assertMatch(
                {ok, #{}},
                emqx_authn_cinfo:authenticate(#{username => <<"a">>, clientid => <<"a">>}, State)
            ),
            ?assertMatch(
                ignore,
                emqx_authn_cinfo:authenticate(#{username => <<"a">>, clientid => <<"b">>}, State)
            )
        end
    ).

t_ignore_if_is_match_yield_false(_) ->
    Checks =
        [
            #{
                is_match => <<"str_eq(username, 'a')">>,
                result => deny
            }
        ],
    with_checks(
        Checks,
        fun(State) ->
            ?assertEqual(ignore, emqx_authn_cinfo:authenticate(#{username => <<"b">>}, State))
        end
    ).

t_ignore_if_is_match_yield_non_boolean(_) ->
    Checks = [
        #{
            %% return 'no-identity' if both username and clientid are missing
            %% this should lead to a 'false' result for 'is_match'
            is_match => <<"coalesce(username,clientid,'no-identity')">>,
            result => deny
        }
    ],
    with_checks(
        Checks,
        fun(State) ->
            ?assertEqual(ignore, emqx_authn_cinfo:authenticate(#{username => <<"b">>}, State))
        end
    ).

t_multiple_is_match_expressions(_) ->
    Checks = [
        #{
            %% use AND to connect multiple is_match expressions
            %% this one means username is not empty, and clientid is 'super'
            is_match => [
                <<"not(is_empty_val(username))">>, <<"str_eq(clientid, 'super')">>
            ],
            result => allow
        }
    ],
    with_checks(
        Checks,
        fun(State) ->
            ?assertEqual(
                ignore,
                emqx_authn_cinfo:authenticate(#{username => <<"">>, clientid => <<"super">>}, State)
            ),
            ?assertMatch(
                {ok, #{}},
                emqx_authn_cinfo:authenticate(
                    #{username => <<"a">>, clientid => <<"super">>}, State
                )
            )
        end
    ).

t_cert_fields_as_alias(_) ->
    Checks = [
        #{
            is_match => [
                <<"str_eq(clientid, coalesce(cert_common_name,''))">>
            ],
            result => allow
        },
        #{
            is_match => <<"true">>,
            result => deny
        }
    ],
    with_checks(
        Checks,
        fun(State) ->
            ?assertEqual(
                {error, bad_username_or_password},
                emqx_authn_cinfo:authenticate(#{username => <<"u">>, clientid => <<"c">>}, State)
            ),
            ?assertMatch(
                {ok, #{}},
                emqx_authn_cinfo:authenticate(#{cn => <<"CN1">>, clientid => <<"CN1">>}, State)
            )
        end
    ).

t_peerhost_matches_username(_) ->
    Checks = [
        #{
            is_match => [
                <<"str_eq(peerhost, username)">>
            ],
            result => allow
        },
        #{
            is_match => <<"true">>,
            result => deny
        }
    ],
    IPStr1 = "127.0.0.1",
    IPStr2 = "::1",
    {ok, IPTuple1} = inet:parse_address(IPStr1, inet),
    {ok, IPTuple2} = inet:parse_address(IPStr2, inet6),
    with_checks(
        Checks,
        fun(State) ->
            ?assertMatch(
                {ok, #{}},
                emqx_authn_cinfo:authenticate(
                    #{username => list_to_binary(IPStr1), peerhost => IPTuple1}, State
                )
            ),
            ?assertMatch(
                {ok, #{}},
                emqx_authn_cinfo:authenticate(
                    #{username => list_to_binary(IPStr2), peerhost => IPTuple2}, State
                )
            )
        end
    ).

config(Checks) ->
    #{
        mechanism => cinfo,
        checks => Checks
    }.

with_checks(Checks, F) ->
    Config = config(Checks),
    {ok, State} = emqx_authn_cinfo:create(?AUTHN_ID, Config),
    try
        F(State)
    after
        ?assertEqual(ok, emqx_authn_cinfo:destroy(State))
    end,
    ok.
