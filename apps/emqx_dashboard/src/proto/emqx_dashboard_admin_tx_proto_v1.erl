%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_dashboard_admin. A replicant passes them to
%% mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_dashboard_admin_tx_proto_v1).

-behaviour(emqx_bpapi).

-include("emqx_dashboard.hrl").
-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    add_user/5,
    clear_mfa_state/1,
    disable_mfa/1,
    set_mfa_state/2,
    reset_mfa_state/2,
    set_mfa_pending/2,
    clear_mfa_pending/1,
    set_sso_code/2,
    clear_sso_code/1,
    set_login_lock/2,
    clear_login_lock/1,
    set_admin_override/2,
    clear_admin_override/1,
    set_user_scopes/2,
    clear_user_scopes/1,
    force_add_user/5,
    remove_user/1,
    update_user/4,
    update_pwd/2,
    upsert_sso_user/5
]).

introduced_in() ->
    "6.3.3".

-spec add_user(dashboard_username(), binary(), dashboard_user_role(), binary(), map()) -> map().
add_user(Username, Password, Role, Desc, Extra) ->
    emqx_dashboard_admin:add_user_tx(Username, Password, Role, Desc, Extra).

-spec clear_mfa_state(dashboard_username()) -> ok.
clear_mfa_state(Username) ->
    emqx_dashboard_admin:clear_mfa_state_tx(Username).

-spec disable_mfa(dashboard_username()) -> ok.
disable_mfa(Username) ->
    emqx_dashboard_admin:disable_mfa_tx(Username).

-spec set_mfa_state(dashboard_username(), emqx_dashboard_mfa:mfa_state()) -> ok.
set_mfa_state(Username, MfaState) ->
    emqx_dashboard_admin:set_mfa_state_tx(Username, MfaState).

-spec reset_mfa_state(dashboard_username(), emqx_dashboard_mfa:mfa_state()) -> ok.
reset_mfa_state(Username, MfaState) ->
    emqx_dashboard_admin:reset_mfa_state_tx(Username, MfaState).

-spec set_mfa_pending(dashboard_username(), map()) -> ok.
set_mfa_pending(Username, Pending) ->
    emqx_dashboard_admin:set_mfa_pending_tx(Username, Pending).

-spec clear_mfa_pending(dashboard_username()) -> ok.
clear_mfa_pending(Username) ->
    emqx_dashboard_admin:clear_mfa_pending_tx(Username).

-spec set_sso_code(dashboard_username(), map()) -> ok.
set_sso_code(Username, SsoCode) ->
    emqx_dashboard_admin:set_sso_code_tx(Username, SsoCode).

-spec clear_sso_code(dashboard_username()) -> ok.
clear_sso_code(Username) ->
    emqx_dashboard_admin:clear_sso_code_tx(Username).

-spec set_login_lock(dashboard_username(), integer()) -> ok.
set_login_lock(Username, LockedUntil) ->
    emqx_dashboard_admin:set_login_lock_tx(Username, LockedUntil).

-spec clear_login_lock(dashboard_username()) -> ok.
clear_login_lock(Username) ->
    emqx_dashboard_admin:clear_login_lock_tx(Username).

-spec set_admin_override(dashboard_username(), ?ADMIN_MFA_REQUIRED | ?ADMIN_MFA_EXEMPTED) -> ok.
set_admin_override(Username, Override) ->
    emqx_dashboard_admin:set_admin_override_tx(Username, Override).

-spec clear_admin_override(dashboard_username()) -> ok.
clear_admin_override(Username) ->
    emqx_dashboard_admin:clear_admin_override_tx(Username).

-spec set_user_scopes(dashboard_username(), [binary()]) -> ok.
set_user_scopes(Username, Scopes) ->
    emqx_dashboard_admin:set_user_scopes_tx(Username, Scopes).

-spec clear_user_scopes(dashboard_username()) -> ok.
clear_user_scopes(Username) ->
    emqx_dashboard_admin:clear_user_scopes_tx(Username).

-spec force_add_user(dashboard_username(), binary(), dashboard_user_role(), binary(), map()) -> ok.
force_add_user(Username, Password, Role, Desc, Extra) ->
    emqx_dashboard_admin:force_add_user_tx(Username, Password, Role, Desc, Extra).

-spec remove_user(dashboard_username()) -> ok.
remove_user(Username) ->
    emqx_dashboard_admin:remove_user_tx(Username).

-spec update_user(dashboard_username(), dashboard_user_role(), binary(), map()) ->
    {boolean(), map()}.
update_user(Username, Role, Desc, Extra) ->
    emqx_dashboard_admin:update_user_tx(Username, Role, Desc, Extra).

-spec update_pwd(dashboard_username(), binary()) -> ok.
update_pwd(Username, PasswordHash) ->
    emqx_dashboard_admin:update_pwd_tx(Username, PasswordHash).

-spec upsert_sso_user(
    dashboard_sso_backend(),
    binary(),
    dashboard_user_role() | undefined,
    binary() | undefined,
    binary()
) -> term().
upsert_sso_user(Backend, Username, MaybeRole, MaybeNamespace, Desc) ->
    emqx_dashboard_admin:ensure_sso_user_tx(Backend, Username, MaybeRole, MaybeNamespace, Desc).
