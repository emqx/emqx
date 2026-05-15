%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% API Key scope name constants.
%%
%% Every minirest_api module MUST export a `scopes/0` callback that
%% returns one of these macros (or a map of path => macro for modules
%% whose endpoints span multiple scopes).
%%
%% Using macros ensures compile-time safety: a typo in a scope name
%% will cause a compilation error rather than a silent runtime bug.

%% ── User-visible scopes ────────────────────────────────────────────

-define(SCOPE_CONNECTIONS, <<"connections">>).
-define(SCOPE_PUBLISH, <<"publish">>).
-define(SCOPE_DATA_INTEGRATION, <<"data_integration">>).
-define(SCOPE_ACCESS_CONTROL, <<"access_control">>).
-define(SCOPE_GATEWAYS, <<"gateways">>).
-define(SCOPE_MONITORING, <<"monitoring">>).
-define(SCOPE_CLUSTER_OPERATIONS, <<"cluster_operations">>).
-define(SCOPE_SYSTEM, <<"system">>).
-define(SCOPE_AUDIT, <<"audit">>).
-define(SCOPE_LICENSE, <<"license">>).

%% ── Internal scopes ────────────────────────────────────────────────

%% Endpoints that API Keys must never access (dashboard login/SSO/
%% API Key self-management).  Not exposed to users.
-define(SCOPE_DENIED, <<"$denied">>).

%% Sentinel for paths that are intentionally unscoped: pre-login entry
%% points (/login, /sso/login/:backend, ...) and meta endpoints that
%% only return static catalog data (/user_scopes, /api_key_scopes).
%% Modules using the map form of scopes/0 must declare such paths with
%% this value rather than omitting them, so that genuinely forgotten
%% paths still produce a startup warning. The collector treats this
%% value as: do not insert into the runtime cache (preserves fail-open
%% semantics) and do not emit path_missing_from_scopes_map.
-define(SCOPE_PUBLIC, public).

%% ── Login-user-only scopes (since 5.10.4) ─────────────────────────────
%%
%% These scopes apply to dashboard login users only. API keys MUST NOT
%% hold any of them — schema validation rejects creation/update of API
%% keys whose scope list includes any login-only scope; bootstrap-file
%% loader filters them with a warning.
%%
%% Among the four:
%%   * `user_management`, `sso_management`, `api_key_management` are
%%     ADMIN-ONLY — only login users with role=administrator may hold
%%     them. Schema validation enforces this in POST /users / PUT
%%     /users/:name handlers.
%%   * `mfa_management` is available to ANY login user role (including
%%     viewer / SSO viewer). For non-admin holders it acts as a
%%     "self-exemption key" — the user may bypass force_mfa /
%%     admin_required locks on THEIR OWN MFA only. Non-admins with
%%     mfa_management still cannot manage other users' MFA.

-define(SCOPE_USER_MGMT, <<"user_management">>).
-define(SCOPE_MFA_MGMT, <<"mfa_management">>).
-define(SCOPE_SSO_MGMT, <<"sso_management">>).
-define(SCOPE_API_KEY_MGMT, <<"api_key_management">>).

%% All four login-only scopes. Used by:
%%   * API key schema validation (reject any in this list)
%%   * bootstrap-file loader (drop any in this list with warning)
%%   * login user scope catalog (/user_scopes endpoint)
-define(LOGIN_ONLY_SCOPES, [
    ?SCOPE_USER_MGMT,
    ?SCOPE_MFA_MGMT,
    ?SCOPE_SSO_MGMT,
    ?SCOPE_API_KEY_MGMT
]).

%% Subset of login-only scopes that require role=administrator.
%% mfa_management is intentionally NOT in this list — see comment above.
-define(ADMIN_ONLY_SCOPES, [
    ?SCOPE_USER_MGMT,
    ?SCOPE_SSO_MGMT,
    ?SCOPE_API_KEY_MGMT
]).

%% Generic (API-key-compatible) scopes — these scopes are usable
%% both by API keys and by dashboard login users. Used to compute the
%% role-default fallback for users whose extra.scopes is absent:
%%   * administrator default = GENERIC_SCOPES ++ LOGIN_ONLY_SCOPES
%%   * viewer default        = GENERIC_SCOPES
-define(GENERIC_SCOPES, [
    ?SCOPE_CONNECTIONS,
    ?SCOPE_PUBLISH,
    ?SCOPE_DATA_INTEGRATION,
    ?SCOPE_ACCESS_CONTROL,
    ?SCOPE_GATEWAYS,
    ?SCOPE_MONITORING,
    ?SCOPE_CLUSTER_OPERATIONS,
    ?SCOPE_SYSTEM,
    ?SCOPE_AUDIT,
    ?SCOPE_LICENSE
]).
