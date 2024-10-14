%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 25. 8月 2022 上午12:04
%%%-------------------------------------------------------------------
-module(alinkdata_role_service).
-author("yqfclid").

%% API
-export([
    select_role_list/4,
    query_role/4,
    add_role/4,
    edit_role/4,
    remove_role/4,
    change_status/4,
    data_scope/4,
    option_select/4,
    export/4
]).

-export([check_role_data_scope/2]).

%%%===================================================================
%%% API
%%%===================================================================
select_role_list(OperationID, Args, #{token := Token} = Context, Req) ->
    NArgs = alinkdata_common_service:handle_data_scope(Args, Token, <<"d">>, undefined),
    alinkdata_common_service:common_handle(OperationID, NArgs, Context, Req).


query_role(_OperationID, #{<<"roleId">> := RoleId} = Args, #{token := Token} = _Context, Req) ->
    case check_role_data_scope(RoleId, Token) of
        ok ->
            {ok, [Role]} = alinkdata_dao:query_no_count(select_role_by_id, Args),
            Res = alinkdata_ajax_result:success_result(Role),
            alinkdata_common_service:response(Res, Req);
        {error, Reason} ->
            Res = alinkdata_ajax_result:error_result(Reason),
            alinkdata_common_service:response(Res, Req)
    end.


add_role(_OperationID, Args, #{token := Token} = _Context, Req) ->
    Check =
        case check_role_name_unique(Args) of
            ok ->
                check_role_key_unique(Args);
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                NArgs = Args#{<<"createBy">> => SelfUserName},
                ok = alinkdata_dao:query_no_count(insert_role, NArgs),
                {ok, [#{<<"roleId">> := RoleId}]} = alinkdata_dao:query_no_count(select_role_list, Args),
                Mnnus =
                    lists:map(
                        fun(MenuId) ->
                            #{<<"roleId">> => RoleId, <<"menuId">> => MenuId}
                    end, maps:get(<<"menuIds">>, Args, [])),
                case length(Mnnus) of
                    0 ->
                        ignore;
                    _ ->
                        ok = alinkdata_dao:query_no_count(batch_role_menu, #{<<"iterms">> => Mnnus})
                end,
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


edit_role(_OperationID, #{<<"roleId">> := RoleId,
                          <<"menuIds">> := MenuIds} = Args, #{token := Token} = _Context, Req) ->
    Check =
        case alinkdata_common:is_admin(RoleId) of
            true ->
                {error, <<"不允许操作超级管理员角色"/utf8>>};
            false ->
                case check_role_data_scope(RoleId, Token) of
                    ok ->
                        case check_role_name_unique(Args) of
                            ok ->
                                check_role_key_unique(Args);
                            {error, Reason} ->
                                {error, Reason}
                        end;
                    {error, Reason} ->
                        {error, Reason}
                end
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                ok = alinkdata_dao:query_no_count(update_role, Args#{<<"updateBy">> => SelfUserName}),
                ok = alinkdata_dao:query_no_count(delete_role_menu_by_role_id, Args),
                Iterms = lists:map(fun(MenuId) -> #{<<"roleId">> => RoleId, <<"menuId">> => MenuId} end, MenuIds),
                BatchMenuArgs = #{<<"iterms">> => Iterms},
                ok = alinkdata_dao:query_no_count(batch_role_menu, BatchMenuArgs),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


remove_role(_OperationID, Args, #{token := Token} = _Context, Req) ->
    RoleIds = binary:split(maps:get(<<"roleId">>, Args, []), <<",">>, [global]),
    Check =
        lists:foldl(
            fun(RoleId, ok) ->
                case alinkdata_common:is_admin(RoleId) of
                    true ->
                        {error, <<"不允许操作超级管理员角色"/utf8>>};
                    false ->
                        case check_role_data_scope(RoleId, Token) of
                            ok ->
                                case alinkdata_dao:query_no_count(count_user_role_by_role_id, #{<<"roleId">> => RoleId}) of
                                    {ok, [#{<<"count">> := 0}]} ->
                                        ok;
                                    {ok, _} ->
                                        {error, <<"有角色已分配,不能删除"/utf8>>};
                                    {error, Reason} ->
                                        {error, Reason}
                                end;
                            {error, Reason} ->
                                {error, Reason}
                        end
                 end;
                (_, {error, Reason}) ->
                    {error, Reason}
            end , ok, RoleIds),
    Res =
        case Check of
            ok ->
                ok = alinkdata_dao:query_no_count(delete_role_menu, #{<<"roleIds">> => RoleIds}),
                ok = alinkdata_dao:query_no_count(delete_role_dept, #{<<"roleIds">> => RoleIds}),
                ok = alinkdata_dao:query_no_count(delete_role_by_ids, #{<<"roleIds">> => RoleIds}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


change_status(_OperationID, #{<<"roleId">> := RoleId} = Args, #{token := Token} = _Context, Req) ->
    Check =
        case alinkdata_common:is_admin(RoleId) of
            true ->
                {error, <<"不允许操作超级管理员角色"/utf8>>};
            false ->
                case check_role_data_scope(RoleId, Token) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        {error, Reason}
                end
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                NArgs = Args#{<<"updateBy">> => SelfUserName},
                ok = alinkdata_dao:query_no_count(update_role, NArgs),
                alinkdata_ajax_result:success_result();
            {error, Reason2} ->
                alinkdata_ajax_result:error_result(Reason2)
        end,
    alinkdata_common_service:response(Res, Req).



data_scope(_OperationID, #{<<"roleId">> := RoleId} = Args, #{token := Token} = _Context, Req) ->
    Check =
        case alinkdata_common:is_admin(RoleId) of
            true ->
                {error, <<"不允许操作超级管理员角色"/utf8>>};
            false ->
                case check_role_data_scope(RoleId, Token) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        {error, Reason}
                end
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                NArgs = Args#{<<"updateBy">> => SelfUserName},
                ok = alinkdata_dao:query_no_count(update_role, NArgs),
                ok = alinkdata_dao:query_no_count(delete_role_dept_by_role_id, #{<<"roleId">> => RoleId}),
                DeptIds = maps:get(<<"deptIds">>, Args, []),
                Iterms =
                    lists:map(
                        fun(DeptId) ->
                            #{<<"roleId">> => RoleId, <<"deptId">> => DeptId}
                    end, DeptIds),
                case length(Iterms) > 0 of
                    true ->
                        ok = alinkdata_dao:query_no_count(batch_role_dept, #{<<"iterms">> => Iterms});
                    _ ->
                        ok
                end,
                alinkdata_ajax_result:success_result();
            {error, Reason2} ->
                alinkdata_ajax_result:error_result(Reason2)
        end,
    alinkdata_common_service:response(Res, Req).



option_select(OperationID, Args, #{token := Token} = Context, Req) ->
    NArgs = alinkdata_common_service:handle_data_scope(Args, Token, <<"d">>, undefined),
    alinkdata_common_service:common_handle(OperationID, NArgs, Context, Req).


export(_OperationID, Args, #{token := Token}, _Req) ->
    NArgs = alinkdata_common_service:handle_data_scope(Args, Token, <<"d">>, undefined),
    {ok,Roles} = alinkdata_dao:query_no_count(select_role_list, NArgs),
    Content = alinkdata_common:to_file_content(Roles),
    Headers = alinkdata_common:file_headers(Content, <<"\"role_data.csv\""/utf8>>),
    {200, Headers, Content}.
%%%===================================================================
%%% Internal functions
%%%===================================================================
check_role_data_scope(RoleId, Token) ->
    #{<<"user">> := #{<<"userId">> := SelfUserId}} = ehttpd_auth:get_session(Token),
    case alinkdata_common:is_admin(SelfUserId) of
        true ->
            ok;
        false ->
            Args = alinkdata_common_service:handle_data_scope(#{<<"roleId">> => RoleId}, Token, <<"d">>, undefined),
            case alinkdata_dao:query_no_count(select_role_list, Args) of
                {ok, []} ->
                    {error, <<"没有权限访问角色数据！"/utf8>>};
                {error, Reason} ->
                    {error, Reason};
                _ ->
                    ok
            end
    end.

check_role_name_unique(Args) ->
    RoleId  = maps:get(<<"roleId">>, Args, -1),
    case alinkdata_dao:query_no_count(check_role_name_unique, Args) of
        {ok, []} ->
            ok;
        {ok, [#{<<"roleId">> := RoleId}]} ->
            ok;
        {ok, _} ->
            RoleName = maps:get(<<"roleName">>, Args, <<>>),
            {error, <<"新增角色'"/utf8, RoleName/binary, "'失败，角色名称已存在"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.


check_role_key_unique(Args) ->
    RoleId  = maps:get(<<"roleId">>, Args, -1),
    case alinkdata_dao:query_no_count(check_role_key_unique, Args) of
        {ok, []} ->
            ok;
        {ok, [#{<<"roleId">> := RoleId}]} ->
            ok;
        {ok, _} ->
            RoleKey = maps:get(<<"roleKey">>, Args, <<>>),
            {error, <<"新增角色'"/utf8, RoleKey/binary, "'失败，角色权限已存在"/utf8>>};
        {error, Reason} ->
            {error, Reason}
end.