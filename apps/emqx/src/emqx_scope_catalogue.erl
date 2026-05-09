%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Static catalogue of EMQX permission scopes.
%%
%% This module hosts the human-readable descriptions for the global
%% scope vocabulary. It lives in `emqx' (the base app) because the
%% scope name macros it references are already in
%% `emqx/include/emqx_api_key_scopes.hrl' and the data is consumed
%% by both `emqx_management' (API-key authorisation, /api_key_scopes
%% endpoint) and `emqx_dashboard' (login user scope schema validation,
%% /user_scopes endpoint). Putting it here keeps the data above the
%% sibling apps in the dependency graph and avoids the implicit
%% cross-app call from emqx_dashboard into emqx_management.
%%
%% Runtime reflection logic (path -> scope resolution, OpenAPI module
%% scanning, cache management, denied-scope handling) remains in
%% `emqx_mgmt_api_key_scopes' because it depends on
%% `application:loaded_applications/0' and on having the management
%% supervision tree available.
-module(emqx_scope_catalogue).

-include("emqx_api_key_scopes.hrl").

-export([
    scope_catalogue/0,
    login_user_scope_catalogue/0
]).

%%--------------------------------------------------------------------
%% Scope catalogue — user-visible scope list
%%--------------------------------------------------------------------

-doc """
Return the catalogue of all user-visible scopes.
Each entry has a `name` (the stable identifier stored in API key
records) and a `desc` (human-readable description for the UI).

The `$denied` scope is excluded — it is internal-only.
""".
-spec scope_catalogue() ->
    [#{name := binary(), desc := binary()}].
scope_catalogue() ->
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

-doc """
Catalogue of all scopes that a dashboard LOGIN USER may hold.

Comprises the 10 API-key catalogue scopes plus the four login-only
scopes introduced by feat/dashboard-user-scopes:

  * `user_management`  (administrator-only)
  * `mfa_management`   (any role; non-admin holders gain only
                        self-exemption from force_mfa /
                        admin_required locks on their own MFA —
                        they still cannot manage other users' MFA,
                        which is enforced both by RBAC and by
                        emqx_dashboard_api:authorize_mfa_change/3)
  * `sso_management`   (administrator-only)
  * `api_key_management` (administrator-only)

API keys MUST NOT hold any of the four login-only scopes — schema
validation in emqx_mgmt_api_api_keys rejects API key creation /
update if the scope list contains any login-only scope; the
bootstrap-file loader filters such names with a warning.
""".
-spec login_user_scope_catalogue() ->
    [#{name := binary(), desc := binary(), admin_only := boolean()}].
login_user_scope_catalogue() ->
    %% Tag each API-key catalogue entry as not admin-only and append
    %% the four new entries.
    [Entry#{admin_only => false} || Entry <- scope_catalogue()] ++
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
                    "Manage MFA. For administrators: reset and re-key "
                    "any user's MFA, override force_mfa and "
                    "admin_required locks. For non-administrators: "
                    "self-exemption only — bypass force_mfa / "
                    "admin_required locks on the holder's own MFA"
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
