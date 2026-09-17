%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_tests).

-include_lib("eunit/include/eunit.hrl").

%% `emqx_dashboard:listeners/1' drops a listener bound to port 0, so it never
%% starts. Leaving its config untouched is what keeps the node from generating a
%% certificate for a server that does not run: everything that generates one sits
%% behind this call.
ensure_ssl_cert_skips_disabled_https_test() ->
    Listeners = #{https => #{bind => 0, ssl_options => #{}}},
    ?assertEqual({ok, Listeners}, emqx_dashboard:ensure_ssl_cert(Listeners)).

ensure_ssl_cert_leaves_other_listeners_alone_test() ->
    Listeners = #{http => #{bind => 18083}},
    ?assertEqual({ok, Listeners}, emqx_dashboard:ensure_ssl_cert(Listeners)).

%% An HTTPS listener that cannot be given a certificate must not stop the others
%% from starting, so the failure is reported rather than raised.
ensure_ssl_cert_reports_a_missing_certificate_test() ->
    meck:new(emqx_default_cert, [passthrough, no_link, no_history]),
    meck:expect(emqx_default_cert, ensure_localhost_bundle, fun() -> {error, no_bundle} end),
    try
        ?assertMatch(
            {error, #{error := <<"no_default_tls_certificate">>}},
            emqx_dashboard:ensure_ssl_cert(#{https => #{bind => 18084, ssl_options => #{}}})
        )
    after
        meck:unload(emqx_default_cert)
    end.
