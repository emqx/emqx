%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc multi-factor authenticaion interface.
-module(emqx_dashboard_mfa).

-export([
    init/1,
    verify/2,
    make_token_missing_error/1,
    is_mfa_error/1,
    is_need_setup_error/1
]).

-export_type([mfa_state/0]).

-type mechanism() :: totp.
-type totp_state() :: #{mechanism := totp, secret := binary(), first_verify_ts => integer()}.
-type mfa_state() :: totp_state().

-define(TOTP_KEY_BYTES, 20).
-define(NO_FIRST_VERIFY_TS, 0).

%% @doc Initialize MFA state.
-spec init(mechanism()) -> {ok, mfa_state()}.
init(totp) ->
    Secret = pot_base32:encode(crypto:strong_rand_bytes(?TOTP_KEY_BYTES), [nopad]),
    {ok, #{mechanism => totp, secret => Secret}}.

%% @doc Verify MFA token.
%% Returns `ok' for most of the happy paths.
%% Returns `{ok, NewState}' for a token to be verified OK for the first time.
%% Returns `{error, Reason}' for invalid tokens.
-spec verify(mfa_state(), binary()) -> ok | {ok, mfa_state()} | {error, map()}.
verify(State, Token) ->
    case do_verify(State, Token) of
        ok ->
            case first_verify_ts(State) =:= ?NO_FIRST_VERIFY_TS of
                true ->
                    {ok, State#{first_verify_ts => erlang:system_time(second)}};
                false ->
                    ok
            end;
        {error, Reason} ->
            {error, Reason}
    end.

do_verify(#{mechanism := totp, secret := Secret}, Token) ->
    verify_totp(Token, Secret).

first_verify_ts(#{first_verify_ts := Ts}) when is_integer(Ts) andalso Ts > 0 ->
    Ts;
first_verify_ts(_) ->
    ?NO_FIRST_VERIFY_TS.

%% @doc Make a token missing error message for login failure
%% response.
-spec make_token_missing_error(mfa_state()) -> map().
make_token_missing_error(#{mechanism := totp, secret := Secret} = State) ->
    Reason = totp_error(missing_mfa_token),
    case is_need_setup_state(State) of
        true ->
            %% the token has never been verified before
            %% we assume the user needs to setup the authenticator app for the first time
            %% so we return the secret for dashboard to render the QR code
            Reason#{secret => Secret};
        false ->
            %% the token was once verified before
            %% we assume the user already has authenticator app setup
            %% so there is no need to return the secret, only hint the cluster name
            Reason
    end.

%% @doc Call this function to check if a error information map is the token-missing
%% error created by this module.
-spec is_mfa_error(map()) -> boolean().
is_mfa_error(#{error := missing_mfa_token}) -> true;
is_mfa_error(#{error := bad_mfa_token}) -> true;
is_mfa_error(_) -> false.

is_need_setup_state(#{first_verify_ts := Ts}) ->
    Ts =:= 0;
is_need_setup_state(_) ->
    true.

%% @doc Call this function to check if the error indicates a MFA setup is required.
-spec is_need_setup_error(map()) -> boolean().
is_need_setup_error(#{mechanism := totp, error := missing_mfa_token, secret := Secret}) when
    is_binary(Secret)
->
    true;
is_need_setup_error(_) ->
    false.

verify_totp(Token, Secret) ->
    try
        case pot:valid_totp(Token, Secret) of
            true ->
                ok;
            false ->
                {error, totp_error(bad_mfa_token)}
        end
    catch
        _:_ ->
            {error, totp_error(bad_mfa_token)}
    end.

totp_error(Reason) ->
    #{
        mechanism => totp,
        error => Reason,
        cluster_name => emqx_sys:cluster_name()
    }.
