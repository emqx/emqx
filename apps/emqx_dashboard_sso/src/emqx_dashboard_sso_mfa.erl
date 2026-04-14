%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Common SSO MFA enforcement logic.
%% Called by SSO backend callbacks (OIDC/SAML/LDAP) after user authentication
%% to enforce force_mfa policy before issuing JWT tokens.
-module(emqx_dashboard_sso_mfa).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([
    check_sso_mfa/2,
    get_force_mfa/1
]).

-define(MOD_KEY_PATH(Sub), [dashboard, sso, Sub]).

%% @doc Check SSO MFA enforcement for a user.
%% This function should be called after ensure_user_exists succeeds,
%% with the User record and the SSO backend atom.
%%
%% Returns:
%%   {ok, login}                    - No MFA needed, JWT should be signed at exchange time
%%   {mfa_setup, SetupToken, QRInfo} - User needs to bind TOTP first
%%   {mfa_verify, VerifyToken}      - User needs to verify TOTP
-spec check_sso_mfa(dashboard_user(), atom()) ->
    {ok, login}
    | {mfa_setup, binary(), map()}
    | {mfa_verify, binary()}.
check_sso_mfa(User, Backend) ->
    case get_force_mfa(Backend) of
        false ->
            {ok, login};
        true ->
            #?ADMIN{username = Username} = User,
            check_user_mfa_state(Username, User)
    end.

%% @doc Get force_mfa config for a given SSO backend.
-spec get_force_mfa(atom()) -> boolean().
get_force_mfa(Backend) ->
    case emqx:get_config(?MOD_KEY_PATH(Backend), undefined) of
        #{force_mfa := ForceMfa} -> ForceMfa;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec check_user_mfa_state(term(), dashboard_user()) ->
    {ok, login}
    | {mfa_setup, binary(), map()}
    | {mfa_verify, binary()}.
check_user_mfa_state(Username, _User) ->
    MfaState = emqx_dashboard_admin:get_mfa_state(Username),
    case classify_mfa_state(MfaState) of
        admin_disabled ->
            {ok, login};
        not_configured ->
            init_and_create_setup(Username);
        setup_required ->
            %% Secret exists (from enable_mfa/reinit) but user hasn't scanned QR yet
            {ok, #{secret := Secret}} = MfaState,
            create_setup_from_existing(Username, Secret);
        enabled ->
            VerifyToken = emqx_dashboard_mfa:create_verify_token(Username),
            {mfa_verify, VerifyToken}
    end.

-spec classify_mfa_state(term()) -> not_configured | setup_required | enabled | admin_disabled.
classify_mfa_state({ok, disabled}) ->
    admin_disabled;
classify_mfa_state({ok, #{mechanism := totp, first_verify_ts := _}}) ->
    %% User has completed TOTP setup (scanned QR code and verified once)
    enabled;
classify_mfa_state({ok, #{mechanism := totp}}) ->
    %% Has secret but never verified — needs to scan QR code first
    setup_required;
classify_mfa_state(_) ->
    not_configured.

-spec init_and_create_setup(term()) ->
    {mfa_setup, binary(), map()}.
init_and_create_setup(Username) ->
    {ok, #{secret := Secret}} = emqx_dashboard_mfa:init(totp),
    %% Store TOTP secret in Mnesia pending record (not in user's enabled MFA state).
    %% It will be promoted to enabled=true only after the user
    %% successfully verifies their first TOTP code (in do_mfa_setup).
    %% Username is the SSO username tuple, e.g. {ldap, <<"user">>}.
    SetupToken = emqx_dashboard_mfa:create_setup_token(Username, Secret),
    QRInfo = #{
        secret => Secret,
        mechanism => totp
    },
    {mfa_setup, SetupToken, QRInfo}.

%% @doc Create a setup flow from an existing secret (e.g. after admin re-enable).
%% The secret is already in mfa_state but user hasn't verified it yet.
-spec create_setup_from_existing(term(), binary()) ->
    {mfa_setup, binary(), map()}.
create_setup_from_existing(Username, Secret) ->
    SetupToken = emqx_dashboard_mfa:create_setup_token(Username, Secret),
    QRInfo = #{
        secret => Secret,
        mechanism => totp
    },
    {mfa_setup, SetupToken, QRInfo}.
