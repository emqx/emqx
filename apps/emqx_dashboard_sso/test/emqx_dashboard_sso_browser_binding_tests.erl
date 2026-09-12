%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_browser_binding_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MOD, emqx_dashboard_sso_browser_binding).
-define(MISMATCH, {error, browser_binding_mismatch}).

set_cookie(Backend, Value, Opts) ->
    #{<<"set-cookie">> := Cookie} = ?MOD:set_cookie_headers(Backend, Value, Opts),
    Cookie.

%% The `name=value' pair a browser sends back for the cookie bound to `Value'.
pair(Backend, Value) ->
    <<(?MOD:cookie_name(Backend, Value))/binary, "=", Value/binary>>.

req(CookieHeader) ->
    #{headers => #{<<"cookie">> => CookieHeader}}.

contains(Needle, Haystack) ->
    binary:match(Haystack, Needle) =/= nomatch.

cookie_name_test() ->
    Name = ?MOD:cookie_name(oidc, <<"v1">>),
    ?assertMatch({match, _}, re:run(Name, "^emqx_sso_oidc_[0-9a-f]{16}$")),
    ?assertEqual(Name, ?MOD:cookie_name(oidc, <<"v1">>)),
    %% Each login value gets its own cookie.
    ?assertNotEqual(Name, ?MOD:cookie_name(oidc, <<"v2">>)),
    %% Backends never share a cookie.
    ?assertMatch(
        {match, _}, re:run(?MOD:cookie_name(saml, <<"v1">>), "^emqx_sso_saml_[0-9a-f]{16}$")
    ).

oidc_cookie_over_http_test() ->
    Cookie = set_cookie(oidc, <<"the-state">>, #{max_age => 30, url => <<"http://emqx:18083">>}),
    ?assert(contains(<<(pair(oidc, <<"the-state">>))/binary, ";">>, Cookie)),
    ?assert(contains(<<"Path=/api/v5/sso">>, Cookie)),
    ?assert(contains(<<"HttpOnly">>, Cookie)),
    ?assert(contains(<<"Max-Age=30">>, Cookie)),
    %% The OIDC callback is a top level GET, so `Lax' is enough.
    ?assert(contains(<<"SameSite=Lax">>, Cookie)),
    ?assertNot(contains(<<"Secure">>, Cookie)).

oidc_cookie_over_https_test() ->
    Cookie = set_cookie(oidc, <<"the-state">>, #{max_age => 30, url => <<"https://emqx:18083">>}),
    ?assert(contains(<<"SameSite=Lax">>, Cookie)),
    ?assert(contains(<<"Secure">>, Cookie)).

saml_cookie_over_https_test() ->
    Cookie = set_cookie(saml, <<"relay">>, #{max_age => 300, url => <<"https://emqx:18083">>}),
    ?assert(contains(<<(pair(saml, <<"relay">>))/binary, ";">>, Cookie)),
    %% The assertion consumer service is a cross site POST, which carries only a
    %% `None' cookie, and `None' requires `Secure'.
    ?assert(contains(<<"SameSite=None">>, Cookie)),
    ?assert(contains(<<"Secure">>, Cookie)).

saml_cookie_over_http_test() ->
    %% No cookie is delivered on a cross site POST over plain HTTP. `Lax' keeps
    %% the response valid; the callback then rejects the login.
    Cookie = set_cookie(saml, <<"relay">>, #{max_age => 300, url => <<"http://emqx:18083">>}),
    ?assert(contains(<<"SameSite=Lax">>, Cookie)),
    ?assertNot(contains(<<"Secure">>, Cookie)).

%% URI schemes are case-insensitive. An upper or mixed case `https' address still
%% gets a `Secure' cookie, and `SameSite=None' for SAML.
scheme_case_insensitive_test() ->
    lists:foreach(
        fun(Url) ->
            Saml = set_cookie(saml, <<"relay">>, #{max_age => 300, url => Url}),
            ?assert(contains(<<"SameSite=None">>, Saml)),
            ?assert(contains(<<"Secure">>, Saml)),
            Oidc = set_cookie(oidc, <<"s">>, #{max_age => 30, url => Url}),
            ?assert(contains(<<"Secure">>, Oidc))
        end,
        [<<"HTTPS://emqx:18083">>, <<"Https://emqx:18083">>, <<"hTtPs://emqx:18083">>]
    ),
    %% An upper case plain HTTP address stays non-secure.
    Plain = set_cookie(saml, <<"relay">>, #{max_age => 300, url => <<"HTTP://emqx:18083">>}),
    ?assert(contains(<<"SameSite=Lax">>, Plain)),
    ?assertNot(contains(<<"Secure">>, Plain)).

max_age_floor_test() ->
    Cookie = set_cookie(oidc, <<"s">>, #{max_age => 0, url => <<"http://emqx:18083">>}),
    ?assert(contains(<<"Max-Age=1">>, Cookie)).

clear_cookie_test() ->
    #{<<"set-cookie">> := Cookie} = ?MOD:clear_cookie_headers(oidc, <<"v1">>),
    Name = ?MOD:cookie_name(oidc, <<"v1">>),
    ?assertMatch({0, _}, binary:match(Cookie, <<Name/binary, "=;">>)),
    ?assert(contains(<<"Path=/api/v5/sso">>, Cookie)),
    ?assert(contains(<<"Max-Age=0">>, Cookie)).

verify_test() ->
    ?assertEqual(ok, ?MOD:verify(oidc, req(pair(oidc, <<"v1">>)), <<"v1">>)),
    %% The cookie of another login does not match this login.
    ?assertEqual(?MISMATCH, ?MOD:verify(oidc, req(pair(oidc, <<"v2">>)), <<"v1">>)),
    %% The right name with the wrong value does not match either.
    WrongValue = <<(?MOD:cookie_name(oidc, <<"v1">>))/binary, "=v2">>,
    ?assertEqual(?MISMATCH, ?MOD:verify(oidc, req(WrongValue), <<"v1">>)),
    %% The cookie of the other backend does not match.
    ?assertEqual(?MISMATCH, ?MOD:verify(oidc, req(pair(saml, <<"v1">>)), <<"v1">>)),
    %% No cookie at all.
    ?assertEqual(?MISMATCH, ?MOD:verify(oidc, #{}, <<"v1">>)),
    ?assertEqual(?MISMATCH, ?MOD:verify(oidc, #{headers => #{}}, <<"v1">>)),
    %% Nothing echoed back by the identity provider.
    ?assertEqual(?MISMATCH, ?MOD:verify(saml, req(pair(saml, <<"v1">>)), undefined)),
    ?assertEqual(?MISMATCH, ?MOD:verify(saml, req(pair(saml, <<"v1">>)), <<>>)).

concurrent_logins_test() ->
    %% Two tabs of one browser each start a login. The browser sends both
    %% cookies on every callback, and each callback finds its own.
    Jar = iolist_to_binary([pair(oidc, <<"tab-a">>), <<"; ">>, pair(oidc, <<"tab-b">>)]),
    ?assertEqual(ok, ?MOD:verify(oidc, req(Jar), <<"tab-a">>)),
    ?assertEqual(ok, ?MOD:verify(oidc, req(Jar), <<"tab-b">>)),
    ?assertEqual(?MISMATCH, ?MOD:verify(oidc, req(Jar), <<"tab-c">>)).

malformed_cookie_header_test() ->
    ?assertEqual(?MISMATCH, ?MOD:verify(oidc, req(<<"=v1">>), <<"v1">>)),
    ?assertEqual(?MISMATCH, ?MOD:verify(oidc, req(<<>>), <<"v1">>)).

new_value_test() ->
    V1 = ?MOD:new_value(),
    V2 = ?MOD:new_value(),
    ?assertEqual(32, byte_size(V1)),
    ?assertNotEqual(V1, V2).
