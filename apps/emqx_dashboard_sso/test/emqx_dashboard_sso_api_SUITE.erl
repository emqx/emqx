%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

-define(CONF_DEFAULT, #{
    node =>
        #{
            name => "emqx1@127.0.0.1",
            cookie => "emqxsecretcookie",
            data_dir => "data"
        },
    log => #{audit => #{enable => true}}
}).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_ctl,
            emqx,
            {emqx_conf, #{config => ?CONF_DEFAULT, schema_mod => emqx_conf_schema}},
            emqx_audit,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            emqx_dashboard_sso
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

%% Reproduces emqx/emqx#18711: `POST /api/v5/sso/login/:backend' sets
%% `log_source' to the bare backend atom before any user identity is known
%% (`emqx_dashboard_sso_api:login/2'). Auditing that request must not crash
%% the request process, and the audit hook must record the attempted
%% backend as `source'.
-doc """
Posts a login request for an unconfigured backend (so it 404s without ever
reaching a real SSO provider) and asserts the response completes normally
and the audit trail records `source => <<"oidc">>`.
""".
t_login_unconfigured_backend_audited(_Config) ->
    StartAt = erlang:system_time(microsecond),
    LoginPath = emqx_mgmt_api_test_util:api_path(["sso", "login", "oidc"]),
    ?assertMatch(
        {ok, 404, _},
        emqx_mgmt_api_test_util:request_api_with_body(
            post, LoginPath, #{<<"backend">> => <<"oidc">>}
        )
    ),
    AuditPath = emqx_mgmt_api_test_util:api_path(["audit"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Query = lists:flatten(
        io_lib:format("source=oidc&from=dashboard&gte_created_at=~B", [StartAt])
    ),
    Res = wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, 2000),
    ?assertMatch(
        #{
            <<"data">> := [
                #{
                    <<"from">> := <<"dashboard">>,
                    <<"source">> := <<"oidc">>,
                    <<"http_status_code">> := 404
                }
            ]
        },
        emqx_utils_json:decode(Res)
    ),
    ok.

wait_for_matching_audit_entry(_AuditPath, _Query, _AuthHeader, RemainMs) when RemainMs =< 0 ->
    ct:fail(audit_entry_not_found_in_time);
wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, RemainMs) ->
    case emqx_mgmt_api_test_util:request_api(get, AuditPath, Query, AuthHeader) of
        {ok, Res} ->
            case emqx_utils_json:decode(Res) of
                #{<<"data">> := [_ | _]} ->
                    Res;
                _ ->
                    SleepMs = 100,
                    ct:sleep(SleepMs),
                    wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, RemainMs - SleepMs)
            end;
        _ ->
            SleepMs = 100,
            ct:sleep(SleepMs),
            wait_for_matching_audit_entry(AuditPath, Query, AuthHeader, RemainMs - SleepMs)
    end.
