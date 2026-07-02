%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%% schema error
-define(ERR(Reason), {error, Reason}).

union_member_selector_mongo_test_() ->
    ok = ensure_schema_load(),
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := mongo_type, expected := _}),
                check("{mechanism = password_based, backend = mongodb, mongo_type = foobar}")
            )
        end},
        {"single", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:mongo_single"}),
                check("{mechanism = password_based, backend = mongodb, mongo_type = single}")
            )
        end},
        {"replica-set", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:mongo_rs"}),
                check("{mechanism = password_based, backend = mongodb, mongo_type = rs}")
            )
        end},
        {"sharded", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:mongo_sharded"}),
                check("{mechanism = password_based, backend = mongodb, mongo_type = sharded}")
            )
        end}
    ].

union_member_selector_jwt_test_() ->
    ok = ensure_schema_load(),
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := use_jwks, expected := "true | false"}),
                check("{mechanism = jwt, use_jwks = 1}")
            )
        end},
        {"jwks", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:jwt_jwks"}),
                check("{mechanism = jwt, use_jwks = true}")
            )
        end},
        {"publick-key", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:jwt_public_key"}),
                check("{mechanism = jwt, use_jwks = false, public_key = 1}")
            )
        end},
        {"hmac-based", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:jwt_hmac"}),
                check("{mechanism = jwt, use_jwks = false}")
            )
        end}
    ].

jwt_jwks_ssl_verify_default_test_() ->
    ok = ensure_schema_load(),
    [
        {"legacy", fun() ->
            emqx_common_test_helpers:with_security_profile("legacy", fun() ->
                ?assertMatch(
                    {ok, #{authentication := [#{ssl := #{verify := verify_none}}]}},
                    check(jwt_jwks_config())
                )
            end)
        end},
        {"hardened", fun() ->
            emqx_common_test_helpers:with_security_profile("hardened", fun() ->
                ?assertMatch(
                    {ok, #{authentication := [#{ssl := #{verify := verify_peer}}]}},
                    check(jwt_jwks_config())
                )
            end)
        end}
    ].

jwt_jwks_ssl_verify_omitted_test_() ->
    ok = ensure_schema_load(),
    [
        {"legacy", fun() ->
            emqx_common_test_helpers:with_security_profile("legacy", fun() ->
                ?assertMatch(
                    {ok, #{authentication := [#{ssl := #{verify := verify_none}}]}},
                    check(jwt_jwks_config_with_ssl())
                )
            end)
        end},
        {"hardened", fun() ->
            emqx_common_test_helpers:with_security_profile("hardened", fun() ->
                ?assertMatch(
                    {ok, #{authentication := [#{ssl := #{verify := verify_peer}}]}},
                    check(jwt_jwks_config_with_ssl())
                )
            end)
        end}
    ].

jwt_on_missing_jwt_default_test_() ->
    ok = ensure_schema_load(),
    [
        {"legacy", fun() ->
            emqx_common_test_helpers:with_security_profile("legacy", fun() ->
                ?assertMatch(
                    {ok, #{authentication := [#{on_missing_jwt := ignore}]}},
                    check(jwt_jwks_config())
                )
            end)
        end},
        {"hardened", fun() ->
            emqx_common_test_helpers:with_security_profile("hardened", fun() ->
                ?assertMatch(
                    {ok, #{authentication := [#{on_missing_jwt := deny}]}},
                    check(jwt_jwks_config())
                )
            end)
        end}
    ].

jwt_on_missing_jwt_explicit_test() ->
    ok = ensure_schema_load(),
    ?assertMatch(
        {ok, #{authentication := [#{on_missing_jwt := ignore}]}},
        check(jwt_jwks_config_on_missing_jwt(ignore))
    ),
    ?assertMatch(
        {ok, #{authentication := [#{on_missing_jwt := deny}]}},
        check(jwt_jwks_config_on_missing_jwt(deny))
    ).

jwt_jwks_max_fail_count_test() ->
    ok = ensure_schema_load(),
    ?assertMatch(
        {ok, #{authentication := [#{max_fail_count := 5}]}},
        check(jwt_jwks_config())
    ),
    ?assertMatch(
        {ok, #{authentication := [#{max_fail_count := 2}]}},
        check(jwt_jwks_config_max_fail_count(2))
    ),
    ?assertMatch(
        ?ERR(_),
        check(jwt_jwks_config_max_fail_count(0))
    ).

union_member_selector_redis_test_() ->
    ok = ensure_schema_load(),
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := redis_type, expected := _}),
                check("{mechanism = password_based, backend = redis, redis_type = 1}")
            )
        end},
        {"single", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:redis_single"}),
                check("{mechanism = password_based, backend = redis, redis_type = single}")
            )
        end},
        {"cluster", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:redis_cluster"}),
                check("{mechanism = password_based, backend = redis, redis_type = cluster}")
            )
        end},
        {"sentinel", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:redis_sentinel"}),
                check("{mechanism = password_based, backend = redis, redis_type = sentinel}")
            )
        end}
    ].

union_member_selector_http_test_() ->
    ok = ensure_schema_load(),
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := method, expected := _}),
                check("{mechanism = password_based, backend = http, method = 1}")
            )
        end},
        {"get", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:http_get"}),
                check("{mechanism = password_based, backend = http, method = get}")
            )
        end},
        {"post", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:http_post"}),
                check("{mechanism = password_based, backend = http, method = post}")
            )
        end}
    ].

check(HoconConf) ->
    emqx_hocon:check(
        #{roots => emqx_authn_schema:global_auth_fields()},
        ["authentication= ", HoconConf]
    ).

jwt_jwks_config() ->
    """
    [
        {
            mechanism = jwt,
            use_jwks = true,
            endpoint = "https://127.0.0.1/jwks.json"
        }
    ]
    """.

jwt_jwks_config_on_missing_jwt(OnMissingJWT) ->
    C = """
    [
        {
            mechanism = jwt,
            use_jwks = true,
            endpoint = "https://127.0.0.1/jwks.json"
            on_missing_jwt = ~s
        }
    ]
    """,
    io_lib:format(C, [atom_to_list(OnMissingJWT)]).

jwt_jwks_config_max_fail_count(MaxFailCount) ->
    C = """
    [
        {
            mechanism = jwt,
            use_jwks = true,
            endpoint = "https://127.0.0.1/jwks.json"
            max_fail_count = ~B
        }
    ]
    """,
    io_lib:format(C, [MaxFailCount]).

jwt_jwks_config_with_ssl() ->
    """
    [
        {
            mechanism = jwt,
            use_jwks = true,
            endpoint = "https://127.0.0.1/jwks.json",
            ssl = {
                enable = true
            }
        }
    ]
    """.

ensure_schema_load() ->
    _ = emqx_conf_schema:roots(),
    ok.
