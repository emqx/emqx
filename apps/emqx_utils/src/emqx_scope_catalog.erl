%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Static catalog of EMQX permission scopes.
%%
%% Pure data; this module has no runtime dependencies. The scope name
%% macros it references are defined in
%% `emqx_utils/include/emqx_api_key_scopes.hrl'. The data is consumed by
%% both `emqx_management' (API-key authorization, /api_key_scopes
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

-export([
    common_scope_catalog/0,
    admin_only_scope_catalog/0,
    scope_catalog/0,
    login_user_scope_catalog/0
]).

%%--------------------------------------------------------------------
%% Common scopes: holdable by both API keys and dashboard login users
%%--------------------------------------------------------------------

-doc """
Catalog of generic scopes shared by API keys and dashboard login
users. Each entry has a `name` (the stable identifier stored in
records) and a `desc` (human-readable description for the UI).

The `$denied` scope is excluded — it is internal-only.
""".
-spec common_scope_catalog() ->
    [#{name := binary(), desc := binary()}].
common_scope_catalog() ->
    [
        #{
            name => ?SCOPE_CONNECTIONS,
            desc => <<
                "Client connections, subscriptions, topics, banning, "
                "retained messages, file transfer, and delayed messages"
            >>
        },
        #{
            name => ?SCOPE_PUBLISH,
            desc => <<"MQTT message publishing">>
        },
        #{
            name => ?SCOPE_DATA_INTEGRATION,
            desc => <<
                "Rules, bridges, connectors, schema registry, "
                "schema validation, message transformation, ExHook, and AI completion"
            >>
        },
        #{
            name => ?SCOPE_ACCESS_CONTROL,
            desc => <<"Client authentication and authorization configuration">>
        },
        #{
            name => ?SCOPE_GATEWAYS,
            desc => <<
                "Protocol gateways (CoAP, LwM2M, etc.) "
                "and their authentication, clients, and listeners"
            >>
        },
        #{
            name => ?SCOPE_MONITORING,
            desc => <<
                "Metrics, monitoring, alarms, trace, slow subscriptions, "
                "telemetry, and Prometheus data endpoints"
            >>
        },
        #{
            name => ?SCOPE_CLUSTER_OPERATIONS,
            desc => <<
                "Cluster management, node operations, "
                "load rebalancing, node eviction, and multi-tenancy"
            >>
        },
        #{
            name => ?SCOPE_SYSTEM,
            desc => <<
                "Core configuration, listeners, plugins, storage, backup, "
                "status, hot upgrade, Prometheus settings, and OpenTelemetry"
            >>
        },
        #{
            name => ?SCOPE_AUDIT,
            desc => <<"Audit log query">>
        },
        #{
            name => ?SCOPE_LICENSE,
            desc => <<"License management">>
        }
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
    [#{name := binary(), desc := binary(), admin_only := boolean()}].
admin_only_scope_catalog() ->
    [
        #{
            name => ?SCOPE_USER_MGMT,
            admin_only => true,
            desc => <<
                "Manage dashboard users (create, update, delete, "
                "change other users' password)"
            >>
        },
        #{
            name => ?SCOPE_MFA_MGMT,
            admin_only => false,
            desc => <<
                "Manage MFA. For administrators: reset and re-key any "
                "user's MFA, override force_mfa and admin_override "
                "locks. For non-administrators: self-exemption only — "
                "bypass force_mfa / admin_override locks on the "
                "holder's own MFA"
            >>
        },
        #{
            name => ?SCOPE_SSO_MGMT,
            admin_only => true,
            desc => <<"Configure SSO backends (LDAP, OIDC, SAML)">>
        },
        #{
            name => ?SCOPE_API_KEY_MGMT,
            admin_only => true,
            desc => <<"Manage API keys (create, update, delete)">>
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
-spec scope_catalog() -> [#{name := binary(), desc := binary()}].
scope_catalog() ->
    common_scope_catalog().

-doc """
Catalog of all scopes a dashboard login user may hold: every generic
scope (tagged `admin_only => false') plus the login-only scopes.
""".
-spec login_user_scope_catalog() ->
    [#{name := binary(), desc := binary(), admin_only := boolean()}].
login_user_scope_catalog() ->
    [Entry#{admin_only => false} || Entry <- common_scope_catalog()] ++
        admin_only_scope_catalog().
