%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_sso_oidc_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

oidc_config(Overrides) ->
    Default = #{
        <<"backend">> => <<"oidc">>,
        <<"client_jwks">> => <<"none">>,
        <<"clientid">> => <<"string">>,
        <<"dashboard_addr">> => <<"http://127.0.0.1:18083">>,
        <<"enable">> => false,
        <<"fallback_methods">> => [<<"RS256">>],
        <<"issuer">> => <<"https://string">>,
        <<"name_var">> => <<"${sub}">>,
        <<"preferred_auth_methods">> =>
            [<<"client_secret_post">>, <<"client_secret_basic">>, <<"none">>],
        <<"provider">> => <<"generic">>,
        <<"require_pkce">> => false,
        <<"scopes">> => [<<"openid">>],
        <<"secret">> => <<"R4ND0M/S∃CЯ∃T"/utf8>>,
        <<"session_expiry">> => <<"1h">>
    },
    maps:merge(Default, Overrides).

parse_and_check(InnerConfigs) ->
    RawConf = #{<<"dashboard">> => #{<<"sso">> => #{<<"oidc">> => InnerConfigs}}},
    #{<<"dashboard">> := #{<<"sso">> := #{<<"oidc">> := Checked}}} = hocon_tconf:check_plain(
        emqx_dashboard_schema,
        RawConf,
        #{
            required => false,
            atom_key => false,
            make_serializable => false
        }
    ),
    Checked.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

issuer_validation_test_() ->
    [
        {"ok issuer",
            ?_assertMatch(
                #{},
                parse_and_check(oidc_config(#{<<"issuer">> => <<"https://string.com:999">>}))
            )},
        {"ok issuer url with port",
            ?_assertMatch(
                #{},
                parse_and_check(
                    oidc_config(#{
                        <<"issuer">> =>
                            <<"https://xxx:8443/webman/sso/.well-known/openid-configuration">>
                    })
                )
            )},
        {"no scheme",
            ?_assertThrow(
                {_, [
                    #{
                        reason := invalid_issuer_url,
                        value := <<"string">>,
                        path := "dashboard.sso.oidc.issuer",
                        kind := validation_error
                    }
                ]},
                parse_and_check(oidc_config(#{<<"issuer">> => <<"string">>}))
            )},
        {"bad scheme",
            ?_assertThrow(
                {_, [
                    #{
                        reason := invalid_issuer_url,
                        value := _,
                        path := "dashboard.sso.oidc.issuer",
                        kind := validation_error
                    }
                ]},
                parse_and_check(oidc_config(#{<<"issuer">> => <<"pulsar+ssl://string">>}))
            )}
    ].

check_ssl_opts_test_() ->
    [
        {"https issuer with ssl disabled is rejected",
            ?_assertMatch(
                {error, {invalid_ssl_opts, _}},
                emqx_dashboard_sso_oidc:check_ssl_opts(#{
                    issuer => <<"https://example.com">>,
                    ssl => #{enable => false}
                })
            )},
        {"https issuer with ssl enabled is allowed",
            ?_assertEqual(
                ok,
                emqx_dashboard_sso_oidc:check_ssl_opts(#{
                    issuer => <<"https://example.com">>,
                    ssl => #{enable => true}
                })
            )},
        {"http issuer with ssl disabled is allowed",
            ?_assertEqual(
                ok,
                emqx_dashboard_sso_oidc:check_ssl_opts(#{
                    issuer => <<"http://example.com">>,
                    ssl => #{enable => false}
                })
            )},
        {"https issuer without an ssl config is allowed",
            ?_assertEqual(
                ok,
                emqx_dashboard_sso_oidc:check_ssl_opts(#{issuer => <<"https://example.com">>})
            )}
    ].

create_rejects_https_issuer_with_ssl_disabled_test() ->
    %% check_ssl_opts/1 runs before any network I/O, so this returns without
    %% starting an oidcc session.
    Config = #{
        name_var => <<"${sub}">>,
        issuer => <<"https://example.com">>,
        ssl => #{enable => false}
    },
    ?assertMatch(
        {error, {invalid_ssl_opts, _}},
        emqx_dashboard_sso_oidc:create(Config)
    ).
