%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_browser_binding_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, emqx_dashboard_sso_browser_binding).

cookie(Backend, Value, Opts) ->
    #{<<"set-cookie">> := Cookie} = ?MOD:set_cookie_header(Backend, Value, Opts),
    Cookie.

req(Cookie) ->
    #{headers => #{<<"cookie">> => Cookie}}.

contains(Needle, Haystack) ->
    binary:match(Haystack, Needle) =/= nomatch.

oidc_cookie_over_http_test() ->
    Cookie = cookie(oidc, <<"the-state">>, #{max_age => 30, url => <<"http://emqx:18083">>}),
    ?assert(contains(<<"emqx_sso_oidc=the-state">>, Cookie)),
    ?assert(contains(<<"Path=/api/v5/sso">>, Cookie)),
    ?assert(contains(<<"HttpOnly">>, Cookie)),
    ?assert(contains(<<"Max-Age=30">>, Cookie)),
    %% The OIDC callback is a top level GET, so `Lax' is enough.
    ?assert(contains(<<"SameSite=Lax">>, Cookie)),
    ?assertNot(contains(<<"Secure">>, Cookie)).

oidc_cookie_over_https_test() ->
    Cookie = cookie(oidc, <<"the-state">>, #{max_age => 30, url => <<"https://emqx:18083">>}),
    ?assert(contains(<<"SameSite=Lax">>, Cookie)),
    ?assert(contains(<<"Secure">>, Cookie)).

saml_cookie_over_https_test() ->
    Cookie = cookie(saml, <<"relay">>, #{max_age => 300, url => <<"https://emqx:18083">>}),
    ?assert(contains(<<"emqx_sso_saml=relay">>, Cookie)),
    %% The assertion consumer service is a cross site POST, which carries only a
    %% `None' cookie, and `None' requires `Secure'.
    ?assert(contains(<<"SameSite=None">>, Cookie)),
    ?assert(contains(<<"Secure">>, Cookie)).

saml_cookie_over_http_test() ->
    %% No cookie is delivered on a cross site POST over plain HTTP. `Lax' keeps
    %% the response valid; the callback then rejects the login.
    Cookie = cookie(saml, <<"relay">>, #{max_age => 300, url => <<"http://emqx:18083">>}),
    ?assert(contains(<<"SameSite=Lax">>, Cookie)),
    ?assertNot(contains(<<"Secure">>, Cookie)).

max_age_floor_test() ->
    Cookie = cookie(oidc, <<"s">>, #{max_age => 0, url => <<"http://emqx:18083">>}),
    ?assert(contains(<<"Max-Age=1">>, Cookie)).

clear_cookie_test() ->
    #{<<"set-cookie">> := Cookie} = ?MOD:clear_cookie_header(oidc),
    ?assert(contains(<<"emqx_sso_oidc=;">>, Cookie)),
    ?assert(contains(<<"Path=/api/v5/sso">>, Cookie)),
    ?assert(contains(<<"Max-Age=0">>, Cookie)).

bound_value_test() ->
    ?assertEqual(
        <<"v1">>,
        ?MOD:bound_value(oidc, req(<<"other=x; emqx_sso_oidc=v1; emqx_sso_saml=v2">>))
    ),
    ?assertEqual(
        <<"v2">>,
        ?MOD:bound_value(saml, req(<<"other=x; emqx_sso_oidc=v1; emqx_sso_saml=v2">>))
    ),
    ?assertEqual(undefined, ?MOD:bound_value(oidc, req(<<"other=x">>))),
    ?assertEqual(undefined, ?MOD:bound_value(oidc, #{headers => #{}})),
    ?assertEqual(undefined, ?MOD:bound_value(oidc, #{})).

verify_test() ->
    ?assertEqual(ok, ?MOD:verify(oidc, req(<<"emqx_sso_oidc=v1">>), <<"v1">>)),
    ?assertEqual(
        {error, browser_binding_mismatch},
        ?MOD:verify(oidc, req(<<"emqx_sso_oidc=v1">>), <<"v2">>)
    ),
    %% A prefix must not pass.
    ?assertEqual(
        {error, browser_binding_mismatch},
        ?MOD:verify(oidc, req(<<"emqx_sso_oidc=v1">>), <<"v12">>)
    ),
    %% The cookie of the other backend must not pass.
    ?assertEqual(
        {error, browser_binding_mismatch},
        ?MOD:verify(oidc, req(<<"emqx_sso_saml=v1">>), <<"v1">>)
    ),
    %% No cookie at all.
    ?assertEqual({error, browser_binding_mismatch}, ?MOD:verify(oidc, #{}, <<"v1">>)),
    %% Nothing echoed back by the identity provider.
    ?assertEqual(
        {error, browser_binding_mismatch},
        ?MOD:verify(saml, req(<<"emqx_sso_saml=v1">>), undefined)
    ),
    ?assertEqual(
        {error, browser_binding_mismatch},
        ?MOD:verify(saml, req(<<"emqx_sso_saml=v1">>), <<>>)
    ).

malformed_cookie_header_test() ->
    ?assertEqual({error, browser_binding_mismatch}, ?MOD:verify(oidc, req(<<"=v1">>), <<"v1">>)),
    ?assertEqual({error, browser_binding_mismatch}, ?MOD:verify(oidc, req(<<>>), <<"v1">>)).

new_value_test() ->
    V1 = ?MOD:new_value(),
    V2 = ?MOD:new_value(),
    ?assertEqual(32, byte_size(V1)),
    ?assertNotEqual(V1, V2).
