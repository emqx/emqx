%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([
    check_rbac/3,
    valid_dashboard_role/1,
    valid_api_role/1
]).

-export_type([actor_context/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(actor, actor).
-define(role, role).

-type actor_context() :: #{
    ?actor := username() | api_key(),
    ?role := role()
}.

-type username() :: binary().
-type api_key() :: binary().
-type role() :: binary().

%%=====================================================================
%% API
-spec check_rbac(emqx_dashboard:request(), emqx_dashboard:handler_info(), actor_context()) ->
    {ok, actor_context()} | false.
check_rbac(Req, HandlerInfo, ActorContext) ->
    maybe
        true ?= do_check_rbac(ActorContext, Req, HandlerInfo),
        {ok, ActorContext}
    end.

valid_dashboard_role(Role) ->
    valid_role(dashboard, Role).

valid_api_role(Role) ->
    valid_role(api, Role).

%% ===================================================================

valid_role(Type, Role) ->
    case lists:member(Role, role_list(Type)) of
        true ->
            ok;
        _ ->
            {error, <<"Role does not exist">>}
    end.

%% ===================================================================
-spec do_check_rbac(actor_context(), emqx_dashboard:request(), emqx_dashboard:handler_info()) ->
    boolean().
do_check_rbac(#{?role := ?ROLE_SUPERUSER}, _, _) ->
    true;
do_check_rbac(#{?role := ?ROLE_VIEWER}, _, #{method := get}) ->
    true;
do_check_rbac(
    #{?role := ?ROLE_API_PUBLISHER},
    _,
    #{method := post, module := emqx_mgmt_api_publish, function := Fn}
) when Fn == publish; Fn == publish_batch ->
    %% emqx_mgmt_api_publish:publish
    %% emqx_mgmt_api_publish:publish_batch
    true;
%% everyone should allow to logout
do_check_rbac(
    #{?role := ?ROLE_VIEWER}, _, #{method := post, module := emqx_dashboard_api, function := logout}
) ->
    %% emqx_dashboard_api:logout
    true;
%% viewer should allow to change self password and (re)setup multi-factor auth for self,
%% superuser should allow to change any user
do_check_rbac(
    #{?role := ?ROLE_VIEWER, ?actor := Username},
    Req,
    #{method := post, module := emqx_dashboard_api, function := Fn}
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
    #{method := delete, module := emqx_dashboard_api, function := change_mfa}
) ->
    %% emqx_dashboard_api:change_mfa
    case Req of
        #{bindings := #{username := Username}} ->
            true;
        _ ->
            false
    end;
do_check_rbac(_, _, _) ->
    false.

role_list(dashboard) ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER];
role_list(api) ->
    [?ROLE_API_VIEWER, ?ROLE_API_PUBLISHER, ?ROLE_API_SUPERUSER].
