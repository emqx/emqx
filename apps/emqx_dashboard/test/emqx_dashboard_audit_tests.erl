%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_audit_tests).

-include_lib("eunit/include/eunit.hrl").

%% `log_source' is set to the bare backend atom (e.g. `oidc') during SSO
%% pre-authentication, before any user identity is known. `log_meta/3' must
%% not crash on that shape (regression test for
%% https://github.com/emqx/emqx/issues/18711).
log_meta_bare_backend_atom_log_source_test() ->
    Meta = #{
        method => post,
        code => 302,
        req_start => erlang:monotonic_time(),
        req_end => erlang:monotonic_time(),
        log_from => dashboard,
        log_source => oidc
    },
    Req = #{headers => #{}, peer => {{127, 0, 0, 1}, 0}},
    LogMeta = emqx_dashboard_audit:log_meta(100, Meta, Req),
    ?assertMatch(#{source := <<"oidc">>}, LogMeta).
