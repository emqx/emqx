%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc SSO MFA setup and verify API endpoints.
%% These endpoints are public (no Bearer Token required),
%% authenticated via short-lived temporary tokens from SSO login flow.
-module(emqx_dashboard_sso_mfa_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-import(hoconsc, [mk/2, ref/1, ref/2]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0,
    fields/1,
    scopes/0
]).

-export([
    mfa_setup/2,
    mfa_verify/2,
    mfa_setup_info/2
]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(UNAUTHORIZED, 'UNAUTHORIZED').
-define(TAGS, <<"Dashboard Single Sign-On">>).

namespace() -> "dashboard_sso_mfa".

scopes() -> ?SCOPE_DENIED.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/sso/mfa/setup_info",
        "/sso/mfa/setup",
        "/sso/mfa/verify"
    ].

schema("/sso/mfa/setup_info") ->
    #{
        'operationId' => mfa_setup_info,
        post => #{
            tags => [?TAGS],
            desc => ?DESC(mfa_setup_info),
            'requestBody' => ref(mfa_setup_info_request),
            responses => #{
                200 => ref(mfa_setup_info_response),
                401 => response_schema(401)
            },
            %% No standard auth — authenticated by the short-lived
            %% setup_token in the request body (issued after SSO login).
            security => []
        }
    };
schema("/sso/mfa/setup") ->
    #{
        'operationId' => mfa_setup,
        post => #{
            tags => [?TAGS],
            desc => ?DESC(mfa_setup),
            'requestBody' => ref(mfa_setup_request),
            responses => #{
                200 => ref(emqx_dashboard_sso_api, login_success_response),
                400 => response_schema(400),
                401 => response_schema(401)
            },
            %% No standard auth — authenticated by the short-lived
            %% setup_token in the request body (issued after SSO login).
            security => []
        }
    };
schema("/sso/mfa/verify") ->
    #{
        'operationId' => mfa_verify,
        post => #{
            tags => [?TAGS],
            desc => ?DESC(mfa_verify),
            'requestBody' => ref(mfa_verify_request),
            responses => #{
                200 => ref(emqx_dashboard_sso_api, login_success_response),
                400 => response_schema(400),
                401 => response_schema(401)
            },
            %% No standard auth — authenticated by the short-lived
            %% verify_token in the request body (issued after SSO login).
            security => []
        }
    }.

fields(mfa_setup_info_request) ->
    [
        {setup_token, mk(binary(), #{desc => ?DESC(setup_token), required => true})},
        {username, mk(binary(), #{desc => ?DESC(sso_username), required => true})},
        {backend, mk(binary(), #{desc => ?DESC(sso_backend), required => true})}
    ];
fields(mfa_setup_info_response) ->
    [
        {secret, mk(binary(), #{desc => ?DESC(totp_secret), required => true})},
        {mechanism, mk(binary(), #{desc => ?DESC(mfa_mechanism), required => true})}
    ];
fields(mfa_setup_request) ->
    [
        {setup_token, mk(binary(), #{desc => ?DESC(setup_token), required => true})}
        | fields(mfa_common_fields)
    ];
fields(mfa_verify_request) ->
    [
        {verify_token, mk(binary(), #{desc => ?DESC(verify_token), required => true})}
        | fields(mfa_common_fields)
    ];
fields(mfa_common_fields) ->
    [
        {totp_code, mk(binary(), #{desc => ?DESC(totp_code), required => true})},
        {username, mk(binary(), #{desc => ?DESC(sso_username), required => true})},
        {backend, mk(binary(), #{desc => ?DESC(sso_backend), required => true})}
    ].

%%--------------------------------------------------------------------
%% API handlers
%%--------------------------------------------------------------------

%% @doc Handle MFA setup info: peek at the setup token to retrieve TOTP secret
%% for QR code display. Does NOT consume the token.
mfa_setup_info(post, #{
    body := #{
        <<"setup_token">> := SetupToken,
        <<"username">> := Username,
        <<"backend">> := BackendBin
    }
}) ->
    case parse_backend(BackendBin) of
        {ok, BackendAtom} ->
            SsoUsername = ?SSO_USERNAME(BackendAtom, Username),
            case emqx_dashboard_mfa:peek_temp_token(SsoUsername, SetupToken) of
                {ok, _SsoUsername, {setup, TotpSecret}} ->
                    {200, #{secret => TotpSecret, mechanism => <<"totp">>}};
                {ok, _SsoUsername, _WrongPurpose} ->
                    {401, #{code => ?UNAUTHORIZED, message => <<"Invalid token type">>}};
                {error, Reason} ->
                    {401, #{code => ?UNAUTHORIZED, message => format_error(Reason)}}
            end;
        {error, _} ->
            {401, #{code => ?UNAUTHORIZED, message => <<"Unknown SSO backend">>}}
    end;
mfa_setup_info(post, _) ->
    {400, #{code => ?BAD_REQUEST, message => <<"Missing required fields">>}}.

%% @doc Handle MFA setup: validate the setup token, verify TOTP code,
%% consume the token, finalize MFA binding, and issue JWT.
mfa_setup(post, #{
    body := #{
        <<"setup_token">> := SetupToken,
        <<"totp_code">> := TotpCode,
        <<"username">> := Username,
        <<"backend">> := BackendBin
    }
}) ->
    case parse_backend(BackendBin) of
        {ok, BackendAtom} ->
            SsoUsername = ?SSO_USERNAME(BackendAtom, Username),
            case emqx_dashboard_mfa:peek_temp_token(SsoUsername, SetupToken) of
                {ok, SsoUsername, {setup, TotpSecret}} ->
                    do_mfa_setup(SsoUsername, SetupToken, TotpCode, TotpSecret);
                {ok, _SsoUsername, _WrongPurpose} ->
                    {401, #{code => ?UNAUTHORIZED, message => <<"Invalid token type">>}};
                {error, Reason} ->
                    {401, #{code => ?UNAUTHORIZED, message => format_error(Reason)}}
            end;
        {error, _} ->
            {401, #{code => ?UNAUTHORIZED, message => <<"Unknown SSO backend">>}}
    end;
mfa_setup(post, _) ->
    {400, #{code => ?BAD_REQUEST, message => <<"Missing required fields">>}}.

%% @doc Handle MFA verify: validate the verify token, check TOTP code,
%% consume the token, and issue JWT.
mfa_verify(post, #{
    body := #{
        <<"verify_token">> := VerifyToken,
        <<"totp_code">> := TotpCode,
        <<"username">> := Username,
        <<"backend">> := BackendBin
    }
}) ->
    case parse_backend(BackendBin) of
        {ok, BackendAtom} ->
            SsoUsername = ?SSO_USERNAME(BackendAtom, Username),
            case emqx_dashboard_mfa:peek_temp_token(SsoUsername, VerifyToken) of
                {ok, SsoUsername, verify} ->
                    do_mfa_verify(SsoUsername, VerifyToken, TotpCode);
                {ok, _SsoUsername, _WrongPurpose} ->
                    {401, #{code => ?UNAUTHORIZED, message => <<"Invalid token type">>}};
                {error, Reason} ->
                    {401, #{code => ?UNAUTHORIZED, message => format_error(Reason)}}
            end;
        {error, _} ->
            {401, #{code => ?UNAUTHORIZED, message => <<"Unknown SSO backend">>}}
    end;
mfa_verify(post, _) ->
    {400, #{code => ?BAD_REQUEST, message => <<"Missing required fields">>}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_mfa_setup(SsoUsername, SetupToken, TotpCode, TotpSecret) ->
    %% Build MFA state from the secret stored in the setup token.
    MfaState = #{mechanism => totp, secret => TotpSecret},
    case emqx_dashboard_mfa:verify(MfaState, TotpCode) of
        ok ->
            consume_token_and_respond(
                SsoUsername,
                SetupToken,
                {setup, TotpSecret},
                fun() ->
                    finalize_mfa_setup(
                        SsoUsername,
                        MfaState#{first_verify_ts => erlang:system_time(second)}
                    )
                end
            );
        {ok, NewState} ->
            consume_token_and_respond(
                SsoUsername,
                SetupToken,
                {setup, TotpSecret},
                fun() -> finalize_mfa_setup(SsoUsername, NewState) end
            );
        {error, _} ->
            bad_totp_response(SsoUsername, SetupToken)
    end.

finalize_mfa_setup(SsoUsername, MfaState) ->
    case resolve_sso_user(SsoUsername) of
        {ok, Backend, Name, User} ->
            case emqx_dashboard_admin:set_mfa_state(SsoUsername, MfaState) of
                {ok, ok} ->
                    sign_and_respond(User, Name, Backend);
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "failed_to_persist_mfa_state",
                        username => SsoUsername,
                        reason => Reason
                    }),
                    {500, #{
                        code => <<"INTERNAL_ERROR">>,
                        message => <<"Failed to persist MFA state">>
                    }}
            end;
        {error, Reason} ->
            {401, #{code => ?UNAUTHORIZED, message => format_error(Reason)}}
    end.

do_mfa_verify(SsoUsername, VerifyToken, TotpCode) ->
    case resolve_sso_user(SsoUsername) of
        {ok, Backend, Name, User} ->
            case emqx_dashboard_admin:get_mfa_state(SsoUsername) of
                {ok, #{mechanism := totp} = MfaState} ->
                    verify_and_respond(
                        MfaState, TotpCode, SsoUsername, VerifyToken, User, Name, Backend
                    );
                _ ->
                    {401, #{code => ?UNAUTHORIZED, message => <<"MFA state not found">>}}
            end;
        {error, Reason} ->
            {401, #{code => ?UNAUTHORIZED, message => format_error(Reason)}}
    end.

verify_and_respond(MfaState, TotpCode, SsoUsername, VerifyToken, User, Name, Backend) ->
    case emqx_dashboard_mfa:verify(MfaState, TotpCode) of
        ok ->
            consume_token_and_respond(
                SsoUsername,
                VerifyToken,
                verify,
                fun() -> sign_and_respond(User, Name, Backend) end
            );
        {ok, NewState} ->
            consume_token_and_respond(
                SsoUsername,
                VerifyToken,
                verify,
                fun() -> persist_state_and_respond(SsoUsername, NewState, User, Name, Backend) end
            );
        {error, _} ->
            bad_totp_response(SsoUsername, VerifyToken)
    end.

consume_token_and_respond(SsoUsername, Token, ExpectedPurpose, RespondFun) ->
    case emqx_dashboard_mfa:verify_temp_token(SsoUsername, Token) of
        {ok, SsoUsername, ExpectedPurpose} ->
            RespondFun();
        {ok, _SsoUsername, _WrongPurpose} ->
            {401, #{code => ?UNAUTHORIZED, message => <<"Invalid token type">>}};
        {error, Reason} ->
            {401, #{code => ?UNAUTHORIZED, message => format_error(Reason)}}
    end.

bad_totp_response(SsoUsername, Token) ->
    case emqx_dashboard_mfa:record_temp_token_failure(SsoUsername, Token) of
        ok ->
            {401, #{code => ?UNAUTHORIZED, message => <<"Invalid TOTP code">>}};
        {error, Reason} ->
            {401, #{code => ?UNAUTHORIZED, message => format_error(Reason)}}
    end.

persist_state_and_respond(SsoUsername, NewState, User, Name, Backend) ->
    case emqx_dashboard_admin:set_mfa_state(SsoUsername, NewState) of
        {ok, ok} ->
            sign_and_respond(User, Name, Backend);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_persist_mfa_state_after_verify",
                username => Name,
                backend => Backend,
                reason => Reason
            }),
            {500, #{
                code => <<"INTERNAL_ERROR">>,
                message => <<"Failed to persist MFA state">>
            }}
    end.

sign_and_respond(User, Username, Backend) ->
    {ok, Role, Token, _Namespace} = emqx_dashboard_token:sign(User),
    {200, emqx_dashboard_sso_api:login_meta(Username, Role, Token, Backend)}.

%% @doc Resolve SSO username tuple to backend, name, and user record.
resolve_sso_user(?SSO_USERNAME(Backend, Name)) ->
    case emqx_dashboard_admin:lookup_user(Backend, Name) of
        [User] ->
            {ok, Backend, Name, User};
        [] ->
            {error, <<"User not found">>}
    end.

response_schema(400) ->
    emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>);
response_schema(401) ->
    emqx_dashboard_swagger:error_codes([?UNAUTHORIZED], <<"Unauthorized">>).

format_error(token_expired) -> <<"Token expired">>;
format_error(invalid_token) -> <<"Invalid token">>;
format_error(Reason) when is_binary(Reason) -> Reason.

parse_backend(BackendBin) ->
    emqx_dashboard_sso:parse_backend(BackendBin).
