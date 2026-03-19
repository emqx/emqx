%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Multifactor authentication interface.
-module(emqx_dashboard_mfa).

-include("emqx_dashboard.hrl").

-export([
    init/1,
    verify/2,
    mechanism/1,
    make_token_missing_error/1,
    is_mfa_error/1,
    is_need_setup_error/1,
    supported_mechanisms/0
]).

%% SSO MFA temporary tokens — stored in extra map of emqx_admin record
-export([
    create_setup_token/2,
    create_verify_token/1,
    verify_temp_token/2,
    peek_temp_token/2,
    generate_token/1
]).

-export_type([mfa_state/0, mechanism/0, temp_token_purpose/0]).

-type mechanism() :: totp.
-type totp_state() :: #{
    mechanism := totp,
    secret := binary(),
    first_verify_ts => integer()
}.
-type mfa_state() :: disabled | totp_state().
-type temp_token_purpose() :: {setup, binary()} | verify.

-define(TOTP_KEY_BYTES, 20).
-define(NO_FIRST_VERIFY_TS, 0).
%% Temporary token validity: 5 minutes
-define(TEMP_TOKEN_TTL_SEC, 300).
-define(TOKEN_BYTES, 32).

%% @doc Translate binary format mechanism name to atom.
-spec mechanism(binary()) -> mechanism().
mechanism(<<"totp">>) -> totp;
mechanism(_X) -> throw(unsupported_mfa_mechanism).

%% @doc Return a list of supported mechanisms
-spec supported_mechanisms() -> [totp].
supported_mechanisms() -> [totp].

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

%%--------------------------------------------------------------------
%% SSO MFA temporary tokens — stored in extra map of emqx_admin record
%%
%% Tokens are stored as mfa_pending key in the emqx_admin record's
%% extra map. This ensures SSO MFA works correctly in clustered
%% deployments without a separate Mnesia table.
%%--------------------------------------------------------------------

%% @doc Create a short-lived temporary token for SSO MFA setup flow.
%% The TOTP secret is stored in the pending record (not in user's enabled MFA state).
-spec create_setup_token(binary(), binary()) -> binary().
create_setup_token(Username, TotpSecret) ->
    Token = generate_token(<<"mfa_">>),
    Pending = #{
        type => setup,
        token => Token,
        secret => TotpSecret,
        timestamp => erlang:system_time(second)
    },
    {ok, ok} = emqx_dashboard_admin:set_mfa_pending(Username, Pending),
    Token.

%% @doc Create a short-lived temporary token for SSO MFA verify flow.
-spec create_verify_token(binary()) -> binary().
create_verify_token(Username) ->
    Token = generate_token(<<"mfa_">>),
    Pending = #{
        type => challenge,
        token => Token,
        timestamp => erlang:system_time(second)
    },
    {ok, ok} = emqx_dashboard_admin:set_mfa_pending(Username, Pending),
    Token.

%% @doc Verify and consume a temporary token.
%% Looks up the pending token by username (O(1) ets:lookup) and compares.
-spec verify_temp_token(dashboard_username(), binary()) ->
    {ok, term(), temp_token_purpose()} | {error, term()}.
verify_temp_token(SsoUsername, Token) ->
    case emqx_dashboard_admin:get_mfa_pending(SsoUsername) of
        {ok, #{token := StoredToken, type := Type, timestamp := Timestamp} = Pending} when
            StoredToken =:= Token
        ->
            case is_token_valid(Timestamp) of
                true ->
                    {ok, ok} = emqx_dashboard_admin:clear_mfa_pending(SsoUsername),
                    {ok, SsoUsername, to_purpose(Type, Pending)};
                false ->
                    {error, token_expired}
            end;
        _ ->
            {error, invalid_token}
    end.

%% @doc Peek at a temporary token without consuming it.
-spec peek_temp_token(dashboard_username(), binary()) ->
    {ok, term(), temp_token_purpose()} | {error, term()}.
peek_temp_token(SsoUsername, Token) ->
    case emqx_dashboard_admin:get_mfa_pending(SsoUsername) of
        {ok, #{token := StoredToken, type := Type, timestamp := Timestamp} = Pending} when
            StoredToken =:= Token
        ->
            case is_token_valid(Timestamp) of
                true ->
                    {ok, SsoUsername, to_purpose(Type, Pending)};
                false ->
                    {error, token_expired}
            end;
        _ ->
            {error, invalid_token}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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
        error => Reason
    }.

%% @doc Generate a random token with a prefix.
%% Format: <<Prefix, RandomHex:64>>
-spec generate_token(binary()) -> binary().
generate_token(Prefix) ->
    Hex = binary:encode_hex(crypto:strong_rand_bytes(?TOKEN_BYTES), lowercase),
    <<Prefix/binary, Hex/binary>>.

is_token_valid(Timestamp) when is_integer(Timestamp) ->
    Now = erlang:system_time(second),
    (Now - Timestamp) < ?TEMP_TOKEN_TTL_SEC;
is_token_valid(_) ->
    false.

to_purpose(setup, #{secret := Secret}) -> {setup, Secret};
to_purpose(challenge, _Pending) -> verify.
