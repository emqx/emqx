%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([check_rbac/2, role/1, valid_role/1]).

-dialyzer({nowarn_function, role/1}).
%%=====================================================================
%% API
check_rbac(Req, Extra) ->
    Role = role(Extra),
    Method = cowboy_req:method(Req),
    AbsPath = cowboy_req:path(Req),
    case emqx_dashboard_swagger:get_relative_uri(AbsPath) of
        {ok, Path} ->
            check_rbac(Role, Method, Path);
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
    Role.

valid_role(Role) ->
    case lists:member(Role, role_list()) of
        true ->
            ok;
        _ ->
            {error, <<"Role does not exist">>}
    end.
%% ===================================================================
check_rbac(?ROLE_SUPERUSER, _, _) ->
    true;
check_rbac(?ROLE_VIEWER, <<"GET">>, _) ->
    true;
%% this API is a special case
check_rbac(?ROLE_VIEWER, <<"POST">>, <<"/logout">>) ->
    true;
check_rbac(_, _, _) ->
    false.

role_list() ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER].
