%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_browser_binding).

-moduledoc """
Binds a redirect-based SSO login round-trip to the browser that started it.

At login start the backend sends a random value to the browser in a cookie and
to the identity provider in a protocol field the provider echoes back (`state`
for OIDC, `RelayState` for SAML). At the callback both values must match, so a
callback replayed in another browser is rejected before it can mint a login
code.
""".

-export([
    new_value/0,
    set_cookie_header/3,
    clear_cookie_header/1,
    cookie_name/1,
    bound_value/2,
    verify/3
]).

-export_type([backend/0]).

-type backend() :: oidc | saml.

%% Both login start endpoints (`/api/v5/sso/login/<backend>') and both callback
%% endpoints (`/api/v5/sso/oidc/callback', `/api/v5/sso/saml/acs') live under
%% this prefix, so the cookie never reaches any other endpoint.
-define(COOKIE_PATH, <<"/api/v5/sso">>).
-define(VALUE_LEN, 32).

-doc "Random value to bind one login round-trip to one browser.".
-spec new_value() -> binary().
new_value() ->
    emqx_utils_conv:bin(emqx_utils:gen_id(?VALUE_LEN)).

-spec cookie_name(backend()) -> binary().
cookie_name(oidc) -> <<"emqx_sso_oidc">>;
cookie_name(saml) -> <<"emqx_sso_saml">>.

-doc """
Build the `set-cookie' header that binds `Value' to this browser.

`url' is the address the callback arrives at, that is the configured
`dashboard_addr'. An `https' address gets a `Secure' cookie.
""".
-spec set_cookie_header(backend(), binary(), #{max_age := pos_integer(), url := binary()}) ->
    #{binary() => binary()}.
set_cookie_header(Backend, Value, #{max_age := MaxAge, url := Url}) ->
    Secure = is_https(Url),
    Opts = #{
        path => ?COOKIE_PATH,
        http_only => true,
        secure => Secure,
        same_site => same_site(Backend, Secure),
        max_age => max(1, MaxAge)
    },
    set_cookie(Backend, Value, Opts).

-doc "Build the `set-cookie' header that deletes the cookie.".
-spec clear_cookie_header(backend()) -> #{binary() => binary()}.
clear_cookie_header(Backend) ->
    set_cookie(Backend, <<>>, #{path => ?COOKIE_PATH, http_only => true, max_age => 0}).

-doc "Read the bound value from a minirest request. Returns `undefined' when absent.".
-spec bound_value(backend(), map()) -> binary() | undefined.
bound_value(Backend, Req) ->
    Headers = maps:get(headers, Req, #{}),
    case maps:get(<<"cookie">>, Headers, undefined) of
        undefined ->
            undefined;
        Raw ->
            find_cookie(cookie_name(Backend), Raw)
    end.

-doc """
Check that the callback request carries the cookie set at login start.

`Expected' is the value echoed back by the identity provider.
""".
-spec verify(backend(), map(), binary() | undefined) -> ok | {error, browser_binding_mismatch}.
verify(Backend, Req, Expected) when is_binary(Expected), Expected =/= <<>> ->
    case bound_value(Backend, Req) of
        undefined ->
            {error, browser_binding_mismatch};
        Value ->
            case equals(Value, Expected) of
                true -> ok;
                false -> {error, browser_binding_mismatch}
            end
    end;
verify(_Backend, _Req, _Expected) ->
    {error, browser_binding_mismatch}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

set_cookie(Backend, Value, Opts) ->
    Cookie = cow_cookie:setcookie(cookie_name(Backend), Value, Opts),
    #{<<"set-cookie">> => iolist_to_binary(Cookie)}.

%% The OIDC callback is a top-level GET, which carries a `Lax' cookie. The SAML
%% assertion consumer service is a cross-site top-level POST, which carries only
%% a `None' cookie, and browsers accept `SameSite=None' only together with
%% `Secure'. Over plain HTTP no attribute makes the browser send the cookie on
%% that POST, so SAML login needs an `https' dashboard address.
same_site(oidc, _Secure) -> lax;
same_site(saml, true) -> none;
same_site(saml, false) -> lax.

find_cookie(Name, Raw) ->
    try cow_cookie:parse_cookie(Raw) of
        Cookies ->
            case lists:keyfind(Name, 1, Cookies) of
                {_, Value} -> Value;
                false -> undefined
            end
    catch
        _:_ ->
            undefined
    end.

%% Hashing first keeps the comparison constant time for values of different
%% lengths, which `crypto:hash_equals/2' alone does not accept.
equals(A, B) ->
    crypto:hash_equals(crypto:hash(sha256, A), crypto:hash(sha256, B)).

is_https(Url) ->
    case uri_string:parse(Url) of
        #{scheme := <<"https">>} -> true;
        #{scheme := "https"} -> true;
        _ -> false
    end.
