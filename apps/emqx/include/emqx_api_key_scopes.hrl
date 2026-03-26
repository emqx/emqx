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
