%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([
    check_rbac/3,
    check_login_user_scopes/2,
    role/1,
    valid_dashboard_role/1,
    valid_api_role/1
]).

-dialyzer({nowarn_function, role/1}).
%%=====================================================================
%% API
check_rbac(Req, Username, Extra) ->
    Role = role(Extra),
    Backend = backend(Extra),
    Method = cowboy_req:method(Req),
    AbsPath = cowboy_req:path(Req),
    case emqx_dashboard_swagger:get_relative_uri(AbsPath) of
        {ok, Path} ->
            case check_rbac(Role, Method, Path, Username, Backend) of
                true ->
                    %% RBAC pass — now apply login-user scope check.
                    %% Returns true (allow) or false (deny). RBAC and
                    %% scope check stack: only paths that pass BOTH
                    %% reach the handler. See SPEC-dashboard-user-scopes.md
                    %% §7.8.1 (hook point) and §7.11 (RBAC × scope).
                    check_login_user_scopes(Username, Path);
                false ->
                    false
            end;
        _ ->
            false
    end.

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
check_login_user_scopes(Username, Path) ->
    case emqx_dashboard_admin:scopes_of(Username) of
        undefined ->
            true;
        Scopes when is_list(Scopes) ->
            case emqx_mgmt_api_key_scopes:path_to_scope(Path) of
                undefined -> true;
                PathScope -> lists:member(PathScope, Scopes)
            end
    end.

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

backend(#{backend := Backend}) ->
    Backend;
backend(_) ->
    ?BACKEND_LOCAL.

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
check_rbac(?ROLE_SUPERUSER, _, _, _, _) ->
    true;
check_rbac(?ROLE_VIEWER, <<"GET">>, _, _, _) ->
    true;
check_rbac(?ROLE_API_PUBLISHER, <<"POST">>, <<"/publish">>, _, _) ->
    true;
check_rbac(?ROLE_API_PUBLISHER, <<"POST">>, <<"/publish/bulk">>, _, _) ->
    true;
%% everyone should allow to logout
check_rbac(?ROLE_VIEWER, <<"POST">>, <<"/logout">>, _, _) ->
    true;
%% viewer should allow to change self password and (re)setup multi-factor auth for self,
%% superuser should allow to change any user
check_rbac(?ROLE_VIEWER, <<"POST">>, <<"/users/", SubPath/binary>>, Username, _) ->
    case decode_path_segments(SubPath) of
        [Username, <<"change_pwd">>] -> true;
        [Username, <<"mfa">>] -> true;
        _ -> false
    end;
check_rbac(?ROLE_VIEWER, <<"DELETE">>, <<"/users/", SubPath/binary>>, Username, Backend) ->
    case decode_path_segments(SubPath) of
        [Username, <<"mfa">>] -> not is_forced_sso_mfa(Backend);
        _ -> false
    end;
check_rbac(_, _, _, _, _) ->
    false.

%% force_mfa is an SSO-backend policy only; regular dashboard accounts
%% authenticated via the local backend do not participate in SSO MFA enforcement.
is_forced_sso_mfa(?BACKEND_LOCAL) ->
    false;
is_forced_sso_mfa(Backend) ->
    case emqx:get_config([dashboard, sso, Backend], undefined) of
        #{force_mfa := true} -> true;
        _ -> false
    end.

decode_path_segments(SubPath) ->
    [uri_string:percent_decode(Segment) || Segment <- binary:split(SubPath, <<"/">>, [global])].

role_list(dashboard) ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER];
role_list(api) ->
    [?ROLE_API_VIEWER, ?ROLE_API_PUBLISHER, ?ROLE_API_SUPERUSER].
