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
    ?assertEqual(Listeners, emqx_dashboard:ensure_ssl_cert(Listeners)).

ensure_ssl_cert_leaves_other_listeners_alone_test() ->
    Listeners = #{http => #{bind => 18083}},
    ?assertEqual(Listeners, emqx_dashboard:ensure_ssl_cert(Listeners)).
