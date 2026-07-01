%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Static catalog of EMQX permission scopes.
%%
%% Pure data; this module has no runtime dependencies. The scope name
%% macros it references are defined in
%% `emqx_utils/include/emqx_api_key_scopes.hrl'. The data is consumed
%% by both `emqx_management' (API-key authorization, /api_key_scopes
%% endpoint) and `emqx_dashboard' (login user scope schema validation,
%% /user_scopes endpoint).
%%
%% Runtime reflection logic (path -> scope resolution, OpenAPI module
%% scanning, cache management, denied-scope handling) lives in
%% `emqx_mgmt_api_key_scopes' because it depends on
%% `application:loaded_applications/0' and on having the management
%% supervision tree available.
-module(emqx_scope_catalog).

-include("emqx_api_key_scopes.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    common_scope_catalog/0,
    admin_only_scope_catalog/0,
    scope_catalog/0,
    login_user_scope_catalog/0,
    partition_privilege_scopes/1,
    check_privilege_scope_mutex/1
]).

%%--------------------------------------------------------------------
%% Common scopes: holdable by both API keys and dashboard login users
%%--------------------------------------------------------------------

-doc """
Catalog of generic scopes shared by API keys and dashboard login
users. Each entry has a `name` (the stable identifier stored in
records) and a `desc` (i18n description handle for the UI).

The `$denied` scope is excluded — it is internal-only.
""".
-spec common_scope_catalog() ->
    [#{name := binary(), desc := term()}].
common_scope_catalog() ->
    [
        #{name => ?SCOPE_CONNECTIONS, desc => ?DESC(scope_connections)},
        #{name => ?SCOPE_PUBLISH, desc => ?DESC(scope_publish)},
        #{name => ?SCOPE_DATA_INTEGRATION, desc => ?DESC(scope_data_integration)},
        #{name => ?SCOPE_ACCESS_CONTROL, desc => ?DESC(scope_access_control)},
        #{name => ?SCOPE_GATEWAYS, desc => ?DESC(scope_gateways)},
        #{name => ?SCOPE_MONITORING, desc => ?DESC(scope_monitoring)},
        #{name => ?SCOPE_CLUSTER_OPERATIONS, desc => ?DESC(scope_cluster_operations)},
        #{name => ?SCOPE_SYSTEM, desc => ?DESC(scope_system)},
        #{name => ?SCOPE_AUDIT, desc => ?DESC(scope_audit)},
        #{name => ?SCOPE_LICENSE, desc => ?DESC(scope_license)}
    ].

%%--------------------------------------------------------------------
%% Login-only scopes: holdable by dashboard users only
%%--------------------------------------------------------------------

-doc """
Catalog of login-only scopes. API keys MUST NOT hold any of these —
schema validation in `emqx_mgmt_api_api_keys' rejects API key
creation or update if the scope list contains any login-only scope,
and the bootstrap-file loader filters such names with a warning.

The `admin_only` flag indicates whether the scope is restricted to
administrator role. `mfa_management` is the only login-only scope
non-administrators may hold; for non-admins it acts as a self-
exemption key allowing the holder to bypass force_mfa and
admin_override locks on their OWN MFA — they still cannot manage
other users' MFA (enforced by RBAC and by
`emqx_dashboard_api:authorize_mfa_change/3').
""".
-spec admin_only_scope_catalog() ->
    [#{name := binary(), desc := term(), admin_only := boolean()}].
admin_only_scope_catalog() ->
    [
        #{
            name => ?SCOPE_USER_MGMT,
            admin_only => true,
            desc => ?DESC(scope_user_management)
        },
        #{
            name => ?SCOPE_MFA_MGMT,
            admin_only => false,
            desc => ?DESC(scope_mfa_management)
        },
        #{
            name => ?SCOPE_SSO_MGMT,
            admin_only => true,
            desc => ?DESC(scope_sso_management)
        },
        #{
            name => ?SCOPE_API_KEY_MGMT,
            admin_only => true,
            desc => ?DESC(scope_api_key_management)
        }
    ].

%%--------------------------------------------------------------------
%% Aggregated catalogs
%%--------------------------------------------------------------------

-doc """
Catalog of all user-visible API-key scopes. Currently identical to
`common_scope_catalog/0' — API keys may hold any generic scope but
never a login-only scope.
""".
-spec scope_catalog() -> [#{name := binary(), desc := term()}].
scope_catalog() ->
    common_scope_catalog().

-doc """
Catalog of all scopes a dashboard login user may hold: every generic
scope (tagged `admin_only => false') plus the login-only scopes.
""".
-spec login_user_scope_catalog() ->
    [#{name := binary(), desc := term(), admin_only := boolean()}].
login_user_scope_catalog() ->
    [Entry#{admin_only => false} || Entry <- common_scope_catalog()] ++
        admin_only_scope_catalog().

%%--------------------------------------------------------------------
%% Privilege scope mutual-exclusion
%%--------------------------------------------------------------------

-doc """
Split a scope list into `{Privilege, Other}` where `Privilege` holds
the members of `?PRIVILEGE_SCOPES` (administrator-equivalent scopes)
and `Other` holds the rest. Order within each sublist follows the
input.
""".
-spec partition_privilege_scopes([binary()]) -> {[binary()], [binary()]}.
partition_privilege_scopes(Scopes) when is_list(Scopes) ->
    lists:partition(fun(S) -> lists:member(S, ?PRIVILEGE_SCOPES) end, Scopes).

-doc """
Reject a scope list that combines any privilege scope with any
non-privilege scope. Each privilege scope (`system`,
`user_management`, `api_key_management`, `sso_management`) is
administrator-equivalent in effect, so pairing one with a restricted
scope list cannot meaningfully restrict the account.

`undefined` and `[]` pass through unchanged (the legacy
unrestricted / deny-all cases, not an explicit mixed list); so do
lists containing only privilege scopes or only non-privilege scopes.
""".
-spec check_privilege_scope_mutex([binary()] | undefined) -> ok | {error, binary()}.
check_privilege_scope_mutex(undefined) ->
    ok;
check_privilege_scope_mutex([]) ->
    ok;
check_privilege_scope_mutex(Scopes) when is_list(Scopes) ->
    case partition_privilege_scopes(Scopes) of
        {[], _} ->
            ok;
        {_, []} ->
            ok;
        {Priv, Other} ->
            Msg = iolist_to_binary([
                <<"Privilege scopes cannot be combined with other scopes. ">>,
                <<"Privilege scopes present: ">>,
                lists:join(<<", ">>, Priv),
                <<". Other scopes present: ">>,
                lists:join(<<", ">>, Other),
                <<". Assign either privilege scopes alone (administrator-equivalent), ">>,
                <<"or non-privilege scopes alone.">>
            ]),
            {error, Msg}
    end.
