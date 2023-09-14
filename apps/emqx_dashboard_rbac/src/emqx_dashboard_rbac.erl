%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([check_rbac/2, role/1, legal_role/1]).

-dialyzer({nowarn_function, role/1}).
%%=====================================================================
%% API
check_rbac(Req, Extra) ->
    Method = cowboy_req:method(Req),
    Role = role(Extra),
    check_rbac_with_method(Role, Method).

role(#?ADMIN{role = undefined}) ->
    ?ROLE_SUPERUSER;
role(#?ADMIN{role = Role}) ->
    Role;
role([]) ->
    ?ROLE_SUPERUSER;
role(#{role := Role}) ->
    Role.

legal_role(Role) ->
    case lists:member(Role, role_list()) of
        true ->
            ok;
        _ ->
            {error, <<"Role does not exist">>}
    end.
%% ===================================================================
check_rbac_with_method(?ROLE_SUPERUSER, _) ->
    true;
check_rbac_with_method(?ROLE_VIEWER, <<"get">>) ->
    true;
check_rbac_with_method(_, _) ->
    false.

role_list() ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER].
