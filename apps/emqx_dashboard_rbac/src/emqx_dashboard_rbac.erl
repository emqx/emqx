%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-export([
    check_rbac/3,
    check_login_user_scopes/2,
    parse_dashboard_role/1,
    parse_api_role/1,
    serialize_role/1,
    role_list/1
]).

-export_type([actor_context/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type actor_context() :: #{
    ?actor := username() | api_key(),
    ?role := role(),
    ?namespace := ?global_ns | namespace(),
    ?backend => atom()
}.

-type username() :: binary().
-type api_key() :: binary().
-type role() :: binary().
-type namespace() :: binary().

-define(API(MOD, METHOD, FN), #{method := METHOD, module := MOD, function := FN}).
-define(DASHBOARD_API(METHOD, FN), ?API(emqx_dashboard_api, METHOD, FN)).
-define(CONNECTOR_API(METHOD, FN), ?API(emqx_connector_api, METHOD, FN)).
-define(BRIDGE_V2_API(METHOD, FN), ?API(emqx_bridge_v2_api, METHOD, FN)).
-define(RULE_API(METHOD, FN), ?API(emqx_rule_engine_api, METHOD, FN)).
-define(TRACE_API(METHOD, FN), ?API(emqx_mgmt_api_trace, METHOD, FN)).
-define(PUBLISH_API(METHOD, FN), ?API(emqx_mgmt_api_publish, METHOD, FN)).
-define(DATA_BACKUP_API(METHOD, FN), ?API(emqx_mgmt_api_data_backup, METHOD, FN)).
-define(AUTHZ_MNESIA_API(METHOD, FN), ?API(emqx_authz_api_mnesia, METHOD, FN)).
-define(AUTHN_API(METHOD, FN), ?API(emqx_authn_api, METHOD, FN)).
-define(CERTS_API(METHOD, FN), ?API(emqx_mgmt_api_certs, METHOD, FN)).
-define(A2A_REGISTRY_API(METHOD, FN), ?API(emqx_a2a_registry_api, METHOD, FN)).
-define(CLIENTS_API(METHOD, FN), ?API(emqx_mgmt_api_clients, METHOD, FN)).
-define(RETAINER_API(METHOD, FN), ?API(emqx_retainer_api, METHOD, FN)).
-define(DELAYED_API(METHOD, FN), ?API(emqx_delayed_api, METHOD, FN)).
-define(TOPIC_METRICS2_API(METHOD, FN), ?API(emqx_topic_metrics2_api, METHOD, FN)).
-define(API_KEY_API(METHOD, FN), ?API(emqx_mgmt_api_api_keys, METHOD, FN)).
-define(FT_API(METHOD, FN), ?API(emqx_ft_api, METHOD, FN)).
-define(FT_FS_API(METHOD, FN), ?API(emqx_ft_storage_exporter_fs_api, METHOD, FN)).

%%=====================================================================
%% API
-spec check_rbac(emqx_dashboard:request(), emqx_dashboard:handler_info(), actor_context()) ->
    {ok, actor_context()} | {error, binary()}.
check_rbac(Req, HandlerInfo, ActorContext) ->
    maybe
        true ?= do_check_rbac(ActorContext, Req, HandlerInfo),
        {ok, ActorContext}
    end.

parse_dashboard_role(Role) ->
    parse_role(dashboard, Role).

%% Look up the login user's `scopes' from the admin record's extra map
%% and cross-reference against the path-to-scope mapping built from all
%% minirest_api modules' scopes/0 callbacks. Semantics:
%%
%%   * scopes absent  (undefined)        -> fall back to RBAC default
%%                                          (already passed at this
%%                                          point), so allow.
%%   * scopes = [...]  (list)            -> path must map to one of
%%                                          the listed scopes; unmapped
%%                                          paths fail-open (allow).
%%
%% The unmapped-path fail-open is consistent with API key scope
%% semantics (emqx_mgmt_auth:check_path_in_scopes/2). CT
%% t_all_endpoints_covered_by_scopes guards against accidentally
%% leaving a non-public path unmapped.
%%
%% IMPORTANT: this predicate is for dashboard LOGIN users only. It must
%% NOT be invoked from API-key authorisation paths because:
%%   1. API keys have their own scope mechanism via
%%      emqx_mgmt_auth:check_path_in_scopes/2 — invoking this on top
%%      is redundant.
%%   2. If an API-key string value collided with a dashboard username,
%%      this lookup would resolve against that user's extra.scopes and
%%      produce a wrong authorisation decision for the API key.
%% Callers MUST ensure `Username' is the dashboard admin record's
%% primary key (binary for local users, ?SSO_USERNAME tuple for SSO
%% users). The dashboard token verifier reconstructs the SSO tuple via
%% emqx_dashboard_token:resolve_admin_key/1 before invoking us.
check_login_user_scopes(Username, Req) when is_map(Req) ->
    AbsPath = cowboy_req:path(Req),
    case emqx_dashboard_swagger:get_relative_uri(AbsPath) of
        {ok, Path} ->
            check_login_user_scopes_for_path(Username, Path);
        _ ->
            %% Requests outside the `/api/v5' management API — e.g. the
            %% OpenAPI spec endpoints (`/api-docs/swagger.json',
            %% `/api-spec.html', `/api-spec.md', ...) served by
            %% emqx_dashboard_api_spec_handler — are not scope-mapped
            %% management operations. They are already gated by
            %% authentication and role-based RBAC; the login-user scope
            %% layer must not deny them. Treat them as unmapped (allow),
            %% consistent with check_login_user_scopes_strict/2 which
            %% allows any path that has no scope mapping.
            true
    end;
check_login_user_scopes(Username, Path) when is_binary(Path) ->
    check_login_user_scopes_for_path(Username, Path).

%% Self-service no longer needs a path-parsing exception here: it lives
%% on `/current_user/*', which `emqx_dashboard_api:scopes/0' declares
%% ?SCOPE_PUBLIC, so `check_login_user_scopes_strict/2' allows it
%% through the `public' branch. Everything under `/users/' is now
%% management of ANOTHER user and is scope-checked without exception.
check_login_user_scopes_for_path(Username, Path) ->
    check_login_user_scopes_strict(Username, Path).

parse_api_role(Role) ->
    parse_role(api, Role).

-doc "Render a parsed role map back to its wire string (inverse of `parse_api_role/1`).".
-spec serialize_role(#{?role := role(), ?namespace := ?global_ns | namespace()}) -> role().
serialize_role(#{?role := Role, ?namespace := ?global_ns}) ->
    Role;
serialize_role(#{?role := Role, ?namespace := Namespace}) when is_binary(Namespace) ->
    <<"ns:", Namespace/binary, "::", Role/binary>>.

check_login_user_scopes_strict(Username, Path) ->
    case emqx_mgmt_api_key_scopes:classify_path(Path) of
        %% Explicitly public endpoint — allow regardless of scopes.
        public ->
            true;
        %% Path maps to no known scope. Fail closed only for users that
        %% carry an explicit scope list (deliberately restricted), so a
        %% catalog gap cannot silently grant them an unmapped endpoint.
        %% Users with no explicit scopes are not scope-restricted and stay
        %% governed by role-based RBAC alone, so they are not locked out.
        not_found ->
            emqx_dashboard_admin:scopes_of(Username) =:= undefined;
        {scope, PathScope} ->
            %% Work on the effective scope list (role-default expanded) so
            %% administrators with no explicit scopes implicitly hold the
            %% full catalog and viewers implicitly hold the common scopes.
            %% Explicit [] is honoured as "no permissions".
            Scopes = emqx_dashboard_admin:effective_scopes_of(Username),
            lists:member(PathScope, Scopes)
    end.

parse_role(Type, Role0) ->
    maybe
        {ok, #{?role := Role} = ParsedRole} ?= do_parse_role(Role0),
        true ?= lists:member(Role, role_list(Type)),
        {ok, ParsedRole}
    else
        false ->
            {error, <<"Role does not exist">>};
        Error ->
            Error
    end.

do_parse_role(Role0) when is_binary(Role0) ->
    maybe
        [NsTag, Role] ?= binary:split(Role0, <<"::">>),
        {ok, Ns} ?= parse_namespace_tag(NsTag),
        {ok, #{?role => Role, ?namespace => Ns}}
    else
        [Role1] ->
            {ok, #{?role => Role1, ?namespace => ?global_ns}};
        {error, _} = Error ->
            Error;
        _ ->
            {error, <<"Role does not exist">>}
    end;
do_parse_role(_) ->
    {error, <<"Invalid role">>}.

parse_namespace_tag(NsTag) ->
    case binary:split(NsTag, <<":">>) of
        [<<"ns">>, Ns] ->
            case emqx:is_denied_namespace(Ns) of
                true ->
                    {error, <<"Denied namespace">>};
                false ->
                    {ok, Ns}
            end;
        _ ->
            {error, <<"Invalid namespace tag">>}
    end.

%% ===================================================================
-spec do_check_rbac(actor_context(), emqx_dashboard:request(), emqx_dashboard:handler_info()) ->
    true | {error, binary()}.
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := ?global_ns}, _, _) ->
    %% Global administrator
    true;
do_check_rbac(#{?namespace := Namespace}, _, ?CLIENTS_API(get, Fn)) when
    is_binary(Namespace) andalso
        (Fn == mqueue_msgs orelse Fn == inflight_msgs)
->
    %% Whole-endpoint visibility policy belongs in RBAC. These endpoints
    %% expose MQTT payloads for arbitrary clients and cannot be safely scoped
    %% by a generic route filter.
    {error, <<"Per-client message endpoints are not available to namespaced users">>};
do_check_rbac(#{?namespace := Namespace}, _, ?RETAINER_API(_, Fn)) when
    is_binary(Namespace) andalso
        (Fn == '/messages' orelse Fn == with_topic_warp)
->
    %% The retained message store is global. Listing, fetching, or deleting by
    %% topic would expose or mutate messages outside the caller's namespace.
    {error, <<"Retained message endpoints are not available to namespaced users">>};
do_check_rbac(#{?namespace := Namespace}, _, ?DELAYED_API(_, Fn)) when
    is_binary(Namespace) andalso
        (Fn == delayed_messages orelse
            Fn == delayed_message orelse
            Fn == delayed_message_topic)
->
    %% Delayed message records are global and include MQTT payloads. Keep the
    %% coarse global-only endpoint decision here; filters should resolve or
    %% validate a namespace, not define the static RBAC surface.
    {error, <<"Delayed message endpoints are not available to namespaced users">>};
do_check_rbac(#{?namespace := Namespace}, _, ?FT_API(get, Fn)) when
    is_binary(Namespace) andalso
        (Fn == '/file_transfer/files' orelse
            Fn == '/file_transfer/files/:clientid/:fileid')
->
    %% The File Transfer store is global and holds client-uploaded file content.
    %% Listing or downloading would expose files uploaded outside the caller's
    %% namespace.
    {error, <<"File Transfer endpoints are not available to namespaced users">>};
do_check_rbac(#{?namespace := Namespace}, _, ?FT_FS_API(get, '/file_transfer/file')) when
    is_binary(Namespace)
->
    {error, <<"File Transfer endpoints are not available to namespaced users">>};
do_check_rbac(#{?role := ?ROLE_SUPERUSER}, _, #{method := get}) ->
    %% Namespaced administrator; It's fine for such admins to `GET` anything, even outside
    %% their namespace.  Namespaces are mostly to avoid accidentally mutating the wrong
    %% resources rather than hiding information.
    true;
do_check_rbac(#{?role := ?ROLE_VIEWER}, _, #{method := get}) ->
    true;
do_check_rbac(
    #{?role := ?ROLE_API_PUBLISHER, ?namespace := ?global_ns},
    _,
    ?PUBLISH_API(post, Fn)
) when Fn == publish; Fn == publish_batch ->
    %% emqx_mgmt_api_publish:publish
    %% emqx_mgmt_api_publish:publish_batch
    %% Currently, only non-namespaced publisher roles may publish with these APIs.
    true;
do_check_rbac(
    #{?role := ?ROLE_API_PUBLISHER, ?namespace := _},
    _,
    ?PUBLISH_API(post, Fn)
) when Fn == publish; Fn == publish_batch ->
    %% emqx_mgmt_api_publish:publish
    %% emqx_mgmt_api_publish:publish_batch
    %% Currently, only namespaced publisher roles may not use these APIs.
    {error, <<"Publishing is not allowed for namespaced API keys">>};
%% everyone should allow to logout
do_check_rbac(#{}, _, ?DASHBOARD_API(post, logout)) ->
    %% emqx_dashboard_api:logout
    true;
%% Self-service: the caller IS the subject, so the authenticated
%% identity alone authorizes the operation. There is no `:username' in
%% these paths to compare the actor against, hence no `IsSelf' rule and
%% nothing to spoof. Any authenticated dashboard user is allowed --
%% including a viewer and a namespaced administrator, who each manage
%% their own account here and nothing else. API keys never reach these
%% functions: `emqx_mgmt_auth:authorize/4' refuses them by handler name
%% before RBAC runs.
do_check_rbac(#{}, _, ?DASHBOARD_API(_, Fn)) when
    Fn == current_user;
    Fn == current_user_change_pwd;
    Fn == current_user_mfa
->
    %% emqx_dashboard_api:current_user
    %% emqx_dashboard_api:current_user_change_pwd
    %% emqx_dashboard_api:current_user_mfa
    true;
%% Managing another user's MFA is a global-administrator operation. A
%% namespaced administrator must not reach it even inside its own
%% namespace: resetting a tenant user's MFA is a known social-
%% engineering vector. Their own MFA is at `/current_user/mfa'.
%%
%% Viewers and other non-administrator roles fall through to the
%% catch-all deny below; only GET is granted to them earlier, and these
%% routes have no GET.
do_check_rbac(
    #{?role := ?ROLE_SUPERUSER, ?namespace := Namespace},
    _Req,
    ?DASHBOARD_API(_, change_mfa)
) when is_binary(Namespace) ->
    {error, <<
        "Namespaced administrators may not manage another user's MFA. "
        "Use /current_user/mfa for your own account."
    >>};
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?CONNECTOR_API(_, _)) when
    is_binary(Namespace)
->
    %% Namespaced connector API; may only alter resources in its own namespace.
    %% This is enforced by the handlers themselves, by only fetching/acting on the
    %% appropriate namespace.
    true;
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?BRIDGE_V2_API(_, _)) when
    is_binary(Namespace)
->
    %% Namespaced action/source APIs; may only alter resources in its own namespace.  This
    %% is enforced by the handlers themselves, by only fetching/acting on the appropriate
    %% namespace.
    true;
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?RULE_API(_, _)) when
    is_binary(Namespace)
->
    %% Namespaced rule APIs; may only alter resources in its own namespace.  This
    %% is enforced by the handlers themselves, by only fetching/acting on the appropriate
    %% namespace.
    true;
do_check_rbac(#{?namespace := Namespace}, _Req, ?TRACE_API(put, config)) when
    is_binary(Namespace)
->
    {error, <<"Namespaced users may not update global tracing configuration">>};
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?TRACE_API(_, _)) when
    is_binary(Namespace)
->
    %% Used by rule simulation API.
    true;
do_check_rbac(
    #{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?DATA_BACKUP_API(_, _)
) when
    is_binary(Namespace)
->
    %% Namespaced configuration backup export/import and per-namespace backup
    %% file management.  The handlers isolate each namespace to its own backup
    %% directory, so a namespaced administrator only ever sees or acts on its
    %% own archives, never global (or legacy) ones.
    true;
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?API_KEY_API(_, _)) when
    is_binary(Namespace)
->
    %% Namespaced administrators may manage API keys within their own namespace.
    %% The handler enforces that the request's effective namespace (create) or the
    %% target key's namespace (read/update/delete) matches the caller's; global and
    %% cross-namespace keys are rejected or hidden there.
    true;
do_check_rbac(
    #{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?AUTHZ_MNESIA_API(_, _)
) when
    is_binary(Namespace)
->
    %% Built-in / mnesia authz.
    true;
do_check_rbac(
    #{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?AUTHN_API(_, Fn)
) when
    is_binary(Namespace) andalso
        (Fn == authenticator_users orelse Fn == authenticator_user orelse
            Fn == authenticator_user_password_rotate)
->
    %% Authentication management.
    %%
    %% We only allow user management for namespaced users.  Actual check for matching
    %% namespace is done in the handlers/filters of the module.
    true;
do_check_rbac(
    #{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, Req, ?CERTS_API(_, _)
) when
    is_binary(Namespace)
->
    %% Centralized certificate management.
    case Req of
        #{bindings := #{namespace := Namespace}} ->
            true;
        _ ->
            {error,
                <<"Namespaced administrators may only manage certificates in their own namespace">>}
    end;
do_check_rbac(
    #{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?A2A_REGISTRY_API(_, _)
) when
    is_binary(Namespace)
->
    %% Agent-to-agent (A2A) registry; may only alter resources in its own namespace.
    %% This is enforced by the handlers themselves, by only fetching/acting on the
    %% appropriate namespace.
    true;
do_check_rbac(
    #{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?TOPIC_METRICS2_API(_, _)
) when
    is_binary(Namespace)
->
    %% v2 topic-metrics: namespaced admins may CRUD collections owned by
    %% their namespace. The handler resolves the URL `:name' against the
    %% actor's namespace, so cross-namespace access by short name is
    %% impossible and there is nothing for RBAC to additionally guard.
    true;
do_check_rbac(_, _, _) ->
    {error, <<"You don't have permission to access this resource">>}.

role_list(dashboard) ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER];
role_list(api) ->
    [?ROLE_API_VIEWER, ?ROLE_API_PUBLISHER, ?ROLE_API_SUPERUSER].
