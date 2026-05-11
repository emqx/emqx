%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-export([
    check_rbac/3,
    check_login_user_scopes/2,
    parse_dashboard_role/1,
    parse_api_role/1
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

%%=====================================================================
%% API
-spec check_rbac(emqx_dashboard:request(), emqx_dashboard:handler_info(), actor_context()) ->
    {ok, actor_context()} | false.
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
        {ok, Path} -> check_login_user_scopes_for_path(Username, Path);
        _ -> false
    end;
check_login_user_scopes(Username, Path) when is_binary(Path) ->
    check_login_user_scopes_for_path(Username, Path).

check_login_user_scopes_for_path(Username, Path) ->
    case emqx_dashboard_admin:scopes_of(Username) of
        undefined ->
            true;
        Scopes when is_list(Scopes) ->
            case emqx_mgmt_api_key_scopes:path_to_scope(Path) of
                undefined -> true;
                PathScope -> lists:member(PathScope, Scopes)
            end
    end.

parse_api_role(Role) ->
    parse_role(api, Role).

%% ===================================================================

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
            {ok, Ns};
        _ ->
            {error, <<"Invalid namespace tag">>}
    end.

%% ===================================================================
-spec do_check_rbac(actor_context(), emqx_dashboard:request(), emqx_dashboard:handler_info()) ->
    boolean().
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := ?global_ns}, _, _) ->
    %% Global administrator
    true;
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
    false;
%% everyone should allow to logout
do_check_rbac(#{}, _, ?DASHBOARD_API(post, logout)) ->
    %% emqx_dashboard_api:logout
    true;
%% viewer should allow to change self password and (re)setup multi-factor auth for self,
%% superuser should allow to change any user
do_check_rbac(
    #{?role := ?ROLE_VIEWER, ?actor := Username},
    Req,
    ?DASHBOARD_API(post, Fn)
) when Fn == change_pwd; Fn == change_mfa ->
    %% emqx_dashboard_api:change_pwd
    %% emqx_dashboard_api:change_mfa
    case Req of
        #{bindings := #{username := Username}} ->
            true;
        _ ->
            false
    end;
do_check_rbac(
    #{?role := ?ROLE_VIEWER, ?actor := Username},
    Req,
    ?DASHBOARD_API(delete, change_mfa)
) ->
    %% RBAC decides only that viewer may DELETE its OWN mfa endpoint.
    %% Policy state — force_mfa snapshot, admin_required lock,
    %% mfa_management self-exemption — is decided in
    %% emqx_dashboard_api:authorize_mfa_change/3. Mixing the live
    %% backend force_mfa flag in here previously bypassed the snapshot
    %% and the scope-based self-exemption (SPEC §6.3).
    case Req of
        #{bindings := #{username := Username}} -> true;
        _ -> false
    end;
do_check_rbac(
    #{?role := ?ROLE_SUPERUSER, ?namespace := Namespace, ?actor := Username},
    Req,
    ?DASHBOARD_API(post, change_mfa)
) when is_binary(Namespace) ->
    %% Namespaced administrators may manage MFA only for themselves.
    case Req of
        #{bindings := #{username := Username}} -> true;
        _ -> false
    end;
do_check_rbac(
    #{?role := ?ROLE_SUPERUSER, ?namespace := Namespace, ?actor := Username},
    Req,
    ?DASHBOARD_API(delete, change_mfa)
) when is_binary(Namespace) ->
    %% Namespaced administrators: same handler-decides policy as viewer.
    case Req of
        #{bindings := #{username := Username}} -> true;
        _ -> false
    end;
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
    %% Configuration backup export/import.
    true;
do_check_rbac(_, _, _) ->
    false.

decode_path_segments(SubPath) ->
    [uri_string:percent_decode(Segment) || Segment <- binary:split(SubPath, <<"/">>, [global])].


role_list(dashboard) ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER];
role_list(api) ->
    [?ROLE_API_VIEWER, ?ROLE_API_PUBLISHER, ?ROLE_API_SUPERUSER].
