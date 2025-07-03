%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([
    check_rbac/4,
    role/1,
    valid_dashboard_role/1,
    valid_api_role/1
]).

%%=====================================================================
%% API
check_rbac(Req, HandlerInfo, Username, Extra) ->
    Role = role(Extra),
    do_check_rbac(Role, Req, HandlerInfo, Username).

%% For compatibility
role(#?ADMIN{role = undefined}) ->
    ?ROLE_SUPERUSER;
role(#?ADMIN{role = Role}) ->
    Role;
%% For compatibility
role([]) ->
    ?ROLE_SUPERUSER;
role(#{role := Role}) ->
    Role;
role(Role) when is_binary(Role) ->
    Role.

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
do_check_rbac(?ROLE_SUPERUSER, _, _, _) ->
    true;
do_check_rbac(?ROLE_VIEWER, _, #{method := get}, _) ->
    true;
do_check_rbac(
    ?ROLE_API_PUBLISHER, _, #{method := post, module := emqx_mgmt_api_publish, function := Fn}, _
) when Fn == publish; Fn == publish_batch ->
    %% emqx_mgmt_api_publish:publish
    %% emqx_mgmt_api_publish:publish_batch
    true;
%% everyone should allow to logout
do_check_rbac(
    ?ROLE_VIEWER, _, #{method := post, module := emqx_dashboard_api, function := logout}, _
) ->
    %% emqx_dashboard_api:logout
    true;
%% viewer should allow to change self password and (re)setup multi-factor auth for self,
%% superuser should allow to change any user
do_check_rbac(
    ?ROLE_VIEWER, Req, #{method := post, module := emqx_dashboard_api, function := Fn}, Username
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
    ?ROLE_VIEWER,
    Req,
    #{method := delete, module := emqx_dashboard_api, function := change_mfa},
    Username
) ->
    %% emqx_dashboard_api:change_mfa
    case Req of
        #{bindings := #{username := Username}} ->
            true;
        _ ->
            false
    end;
do_check_rbac(_, _, _, _) ->
    false.

role_list(dashboard) ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER];
role_list(api) ->
    [?ROLE_API_VIEWER, ?ROLE_API_PUBLISHER, ?ROLE_API_SUPERUSER].
