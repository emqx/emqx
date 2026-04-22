%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc One-time SSO code exchange.
%%
%% After SAML/OIDC authentication, instead of putting JWT tokens or MFA
%% temp tokens directly in the redirect URL query string (which leaks to
%% proxy logs and browser history), we generate a short-lived one-time
%% code.  The frontend exchanges this code for the real payload via a
%% POST request.
%%
%% Storage: `sso_code' key in the `extra' map of `emqx_admin' records
%% (same pattern as MFA pending tokens).
%% TTL: 60 seconds.  Codes are consumed (cleared) atomically on exchange.
-module(emqx_dashboard_sso_code).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

%% API
-export([
    create_code/2,
    exchange_code/3
]).

-ifdef(TEST).
-define(TTL_SEC, 5).
-else.
-define(TTL_SEC, 60).
-endif.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Create a one-time code for the given username and payload.
%% The code is stored in the `sso_code' key of the user's `extra' map.
%% Returns the code binary (prefixed with "sso_").
-spec create_code(dashboard_username(), map()) -> binary().
create_code(Username, Payload) when is_map(Payload) ->
    Code = generate_code(),
    Now = erlang:system_time(second),
    SsoCode = #{
        code => Code,
        payload => Payload,
        exptime => Now + ?TTL_SEC
    },
    {ok, ok} = emqx_dashboard_admin:set_sso_code(Username, SsoCode),
    Code.

%% @doc Exchange a one-time code for its payload.
%% Looks up by username (O(1) ets:lookup) and compares stored code.
-spec exchange_code(atom(), binary(), binary()) -> {ok, map()} | {error, expired | not_found}.
exchange_code(Backend, Username, Code) when is_binary(Code) ->
    do_exchange(?SSO_USERNAME(Backend, Username), Code).

do_exchange(SsoUsername, Code) ->
    case emqx_dashboard_admin:get_sso_code(SsoUsername) of
        {ok, #{code := StoredCode, payload := Payload, exptime := Exp}} when StoredCode =:= Code ->
            _ = emqx_dashboard_admin:clear_sso_code(SsoUsername),
            Now = erlang:system_time(second),
            case Now =< Exp of
                true -> {ok, Payload};
                false -> {error, expired}
            end;
        _ ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% @doc Generate a random one-time code with "sso_" prefix.
generate_code() ->
    emqx_dashboard_mfa:generate_token(<<"sso_">>).
