%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        {"no scheme",
            ?_assertThrow(
                {_, [
                    #{
                        reason := "missing_scheme",
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
                        reason := "unsupported_scheme",
                        value := _,
                        path := "dashboard.sso.oidc.issuer",
                        kind := validation_error
                    }
                ]},
                parse_and_check(oidc_config(#{<<"issuer">> => <<"pulsar+ssl://string">>}))
            )}
    ].
