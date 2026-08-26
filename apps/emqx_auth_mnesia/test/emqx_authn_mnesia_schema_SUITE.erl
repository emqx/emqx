%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_mnesia_schema_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, emqx_authn_test_lib:emqx_appspec()},
            emqx_auth,
            emqx_auth_mnesia
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor().

%% Checks parsed hash defaults for legacy password modes.
t_legacy(_Config) ->
    ok = emqx_common_test_helpers:set_security_profile(legacy),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_common_test_helpers:clear_security_profile()
    end),
    {ok, _} = emqx:update_config([authentication], [
        #{
            <<"mechanism">> => <<"password_based">>,
            <<"backend">> => <<"built_in_database">>
        }
    ]),
    ?assertMatch(
        [
            #{
                autogenerate_password := false,
                password_hash_algorithm := #{name := sha256, salt_position := prefix}
            }
        ],
        emqx:get_config([authentication])
    ),

    {ok, _} = emqx:update_config([authentication], [
        #{
            <<"mechanism">> => <<"password_based">>,
            <<"backend">> => <<"built_in_database">>,
            <<"autogenerate_password">> => true
        }
    ]),
    ?assertMatch(
        [
            #{
                autogenerate_password := true,
                password_hash_algorithm := #{name := sha256, salt_position := disable}
            }
        ],
        emqx:get_config([authentication])
    ),

    {ok, _} = emqx:update_config([authentication], [
        #{
            <<"mechanism">> => <<"password_based">>,
            <<"backend">> => <<"built_in_database">>,
            <<"autogenerate_password">> => false
        }
    ]),
    ?assertMatch(
        [
            #{
                autogenerate_password := false,
                password_hash_algorithm := #{name := sha256, salt_position := prefix}
            }
        ],
        emqx:get_config([authentication])
    ).

%% Checks parsed hash defaults for hardened password modes.
t_hardened(_Config) ->
    ok = emqx_common_test_helpers:set_security_profile(hardened),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_common_test_helpers:clear_security_profile()
    end),
    {ok, _} = emqx:update_config([authentication], [
        #{
            <<"mechanism">> => <<"password_based">>,
            <<"backend">> => <<"built_in_database">>
        }
    ]),
    ?assertMatch(
        [
            #{
                autogenerate_password := true,
                password_hash_algorithm := #{name := sha256, salt_position := disable}
            }
        ],
        emqx:get_config([authentication])
    ),
    {ok, _} = emqx:update_config([authentication], [
        #{
            <<"mechanism">> => <<"password_based">>,
            <<"backend">> => <<"built_in_database">>,
            <<"autogenerate_password">> => true
        }
    ]),
    ?assertMatch(
        [
            #{
                autogenerate_password := true,
                password_hash_algorithm := #{name := sha256, salt_position := disable}
            }
        ],
        emqx:get_config([authentication])
    ),

    {ok, _} = emqx:update_config([authentication], [
        #{
            <<"mechanism">> => <<"password_based">>,
            <<"backend">> => <<"built_in_database">>,
            <<"autogenerate_password">> => false
        }
    ]),
    ?assertMatch(
        [
            #{
                autogenerate_password := false,
                password_hash_algorithm := #{
                    name := pbkdf2,
                    mac_fun := sha256,
                    iterations := 600000
                }
            }
        ],
        emqx:get_config([authentication])
    ),

    %% A pre-7.0 config carries a hash algorithm and no autogenerate_password
    %% field. It must remain valid (as a manual config) under hardened, so
    %% that config import and upgrade do not fail validation.
    {ok, _} = emqx:update_config([authentication], [
        #{
            <<"mechanism">> => <<"password_based">>,
            <<"backend">> => <<"built_in_database">>,
            <<"password_hash_algorithm">> => #{
                <<"name">> => <<"sha256">>,
                <<"salt_position">> => <<"suffix">>
            }
        }
    ]),
    ?assertMatch(
        [
            #{
                autogenerate_password := false,
                password_hash_algorithm := #{name := sha256, salt_position := suffix}
            }
        ],
        emqx:get_config([authentication])
    ).
