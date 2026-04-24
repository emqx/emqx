%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([
    check_rbac/3,
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
            check_rbac(Role, Method, Path, Username, Backend);
        _ ->
            false
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
