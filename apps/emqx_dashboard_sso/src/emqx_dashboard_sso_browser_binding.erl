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

Each login gets its own cookie, named after a hash of its value. Logins started
in several tabs of one browser therefore keep separate cookies, and each of them
can complete.

A backend turns the check off with `skip_login_cookie_check`.
""".

-include_lib("emqx/include/logger.hrl").

-export([
    new_value/0,
    cookie_name/2,
    set_cookie_headers/3,
    clear_cookie_headers/2,
    verify/3,
    check/4,
    maybe_warn_check_skipped/2
]).

-export_type([backend/0]).

-type backend() :: oidc | saml.

%% Both login start endpoints (`/api/v5/sso/login/<backend>') and both callback
%% endpoints (`/api/v5/sso/oidc/callback', `/api/v5/sso/saml/acs') live under
%% this prefix, so the cookie never reaches any other endpoint.
-define(COOKIE_PATH, <<"/api/v5/sso">>).
-define(VALUE_LEN, 32).
%% Hex digits of the value hash in the cookie name. 64 bits keep the concurrent
%% logins of one browser apart.
-define(NAME_HASH_LEN, 16).

-doc "Random value to bind one login round-trip to one browser.".
-spec new_value() -> binary().
new_value() ->
    emqx_utils_conv:bin(emqx_utils:gen_id(?VALUE_LEN)).

-doc "Name of the cookie that binds `Value'. Each login value gets its own name.".
-spec cookie_name(backend(), binary()) -> binary().
cookie_name(Backend, Value) ->
    Hex = binary:encode_hex(crypto:hash(sha256, Value), lowercase),
    <<Hash:?NAME_HASH_LEN/binary, _/binary>> = Hex,
    <<(name_prefix(Backend))/binary, Hash/binary>>.

-doc """
Build the response headers that bind `Value' to this browser.

`url' is the address the callback arrives at, that is the configured
`dashboard_addr'. An `https' address gets a `Secure' cookie.
""".
-spec set_cookie_headers(backend(), binary(), #{max_age := pos_integer(), url := binary()}) ->
    #{binary() => binary()}.
set_cookie_headers(Backend, Value, #{max_age := MaxAge, url := Url}) ->
    Secure = is_https(Url),
    Opts = #{
        path => ?COOKIE_PATH,
        http_only => true,
        secure => Secure,
        same_site => same_site(Backend, Secure),
        max_age => max(1, MaxAge)
    },
    set_cookie(cookie_name(Backend, Value), Value, Opts).

-doc "Build the response headers that delete the cookie bound to `Value'.".
-spec clear_cookie_headers(backend(), binary()) -> #{binary() => binary()}.
clear_cookie_headers(Backend, Value) ->
    Opts = #{path => ?COOKIE_PATH, http_only => true, max_age => 0},
    set_cookie(cookie_name(Backend, Value), <<>>, Opts).

-doc """
Check that the callback request carries the cookie set when this login started.

`Expected' is the value echoed back by the identity provider. It also names the
cookie to look for, so the other logins of the same browser do not interfere.
""".
-spec verify(backend(), map(), binary() | undefined) -> ok | {error, browser_binding_mismatch}.
verify(Backend, Req, Expected) when is_binary(Expected), Expected =/= <<>> ->
    case find_cookie(cookie_name(Backend, Expected), Req) of
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

-doc """
Check the login cookie as `verify/3` does, unless the backend `Config` sets
`skip_login_cookie_check`.
""".
-spec check(backend(), map(), map(), binary() | undefined) ->
    ok | {error, browser_binding_mismatch}.
check(Backend, Config, Req, Expected) ->
    case skips_check(Config) of
        true -> ok;
        false -> verify(Backend, Req, Expected)
    end.

-doc "Log a warning when an enabled backend skips the login cookie check.".
-spec maybe_warn_check_skipped(backend(), map()) -> ok.
maybe_warn_check_skipped(Backend, #{enable := true} = Config) ->
    case skips_check(Config) of
        true ->
            ?SLOG(warning, #{
                msg => "sso_login_cookie_check_skipped",
                backend => Backend,
                reason => "SSO logins are not bound to the browser that started them"
            }),
            ok;
        false ->
            ok
    end;
maybe_warn_check_skipped(_Backend, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

skips_check(Config) ->
    maps:get(skip_login_cookie_check, Config, false) =:= true.

name_prefix(oidc) -> <<"emqx_sso_oidc_">>;
name_prefix(saml) -> <<"emqx_sso_saml_">>.

set_cookie(Name, Value, Opts) ->
    #{<<"set-cookie">> => iolist_to_binary(cow_cookie:setcookie(Name, Value, Opts))}.

%% The OIDC callback is a top-level GET, which carries a `Lax' cookie. The SAML
%% assertion consumer service is a cross-site top-level POST, which carries only
%% a `None' cookie, and browsers accept `SameSite=None' only together with
%% `Secure'. Over plain HTTP no attribute makes the browser send the cookie on
%% that POST, so SAML login needs an `https' dashboard address.
same_site(oidc, _Secure) -> lax;
same_site(saml, true) -> none;
same_site(saml, false) -> lax.

find_cookie(Name, Req) ->
    Headers = maps:get(headers, Req, #{}),
    case maps:get(<<"cookie">>, Headers, undefined) of
        undefined -> undefined;
        Raw -> find_cookie_in_header(Name, Raw)
    end.

find_cookie_in_header(Name, Raw) ->
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

%% URI schemes are case-insensitive (RFC 3986, section 3.1), and
%% `dashboard_addr' is not normalised.
is_https(Url) ->
    case uri_string:parse(Url) of
        #{scheme := Scheme} -> string:equal(Scheme, <<"https">>, true);
        _ -> false
    end.
