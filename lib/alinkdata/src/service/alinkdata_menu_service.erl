%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 8月 2022 下午11:13
%%%-------------------------------------------------------------------
-module(alinkdata_menu_service).
-author("yqfclid").

%% API
-export([
    select_menu_list/4,
    query_menu/4,
    add_menu/4,
    edit_menu/4,
    remove_menu/4
]).

%%%===================================================================
%%% API
%%%===================================================================
select_menu_list(_OperationID, #{<<"queryFun">> := <<"roleMenuTreeselect">>} = Args, #{token := Token} = Context, Req) ->
    #{<<"user">> := #{<<"userId">> := UserId}} = ehttpd_auth:get_session(Token),
    DaoId =
        case alinkdata_common:is_admin(UserId) of
            true ->
                select_menu_list;
            _ ->
                select_menu_list_by_user_id
        end,
    Res =
        case alinkdata_dao:query_no_count(DaoId, Args#{<<"userId">> => UserId}) of
            {ok, Menus} ->
                Success = alinkdata_ajax_result:success_result(),
                {ok, [Role]} = alinkdata_dao:query_no_count(select_role_by_id, Args),
                CachedKeyArgs = Args#{<<"menuCheckStrictly">> => maps:get(<<"menuCheckStrictly">>, Role, undefined)},
                {ok, MenuIds} = alinkdata_dao:query_no_count(select_menu_list_by_role_id, CachedKeyArgs),
                CachedKeys =
                    lists:map(
                        fun(#{<<"menuId">> := MenuId}) -> MenuId
                    end, MenuIds),
                Success#{
                    <<"menus">> => alinkdata_common_service:build_tree(Context, Menus),
                    <<"checkedKeys">> => CachedKeys
                };
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req);


select_menu_list(_OperationID, Args, #{token := Token} = Context, Req) ->
    #{<<"user">> := #{<<"userId">> := UserId}} = ehttpd_auth:get_session(Token),
    DaoId =
        case alinkdata_common:is_admin(UserId) of
            true ->
                select_menu_list;
            _ ->
                select_menu_list_by_user_id
        end,
    alinkdata_common_service:common_handle(DaoId, Args#{<<"userId">> => UserId}, Context, Req).

query_menu(_OperationID, Args, _Context, Req) ->
    Res =
        case alinkdata_dao:query_no_count(select_menu_by_id, Args) of
            {ok, [Menu]} ->
                alinkdata_ajax_result:success_result(Menu);
            {ok, []} ->
                alinkdata_ajax_result:error_result(<<"not found">>);
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).

add_menu(_OperationID, Args, #{token := Token} = _Context, Req) ->
    Check =
        case check_menu_name_unique(Args) of
            ok ->
                check_frame(Args);
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                ok = alinkdata_dao:query_no_count(insert_menu, Args#{<<"createBy">> => SelfUserName}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


edit_menu(_OperationID, #{<<"menuId">> := MenuId} = Args, #{token := Token} = _Context, Req) ->
    Check =
        case check_menu_name_unique(Args) of
            ok ->
                case maps:get(<<"parentId">>, Args, 0) =:= MenuId of
                    true ->
                        MenuName = maps:get(<<"menuName">>, Args, <<>>),
                        {error, <<"修改菜单'"/utf8, MenuName/binary, "'失败，上级菜单不能选择自己"/utf8>>};
                    _ ->
                        case check_frame(Args) of
                            ok ->
                                ok;
                            {error, Reason} ->
                                {error, Reason}
                        end
                end;
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                ok = alinkdata_dao:query_no_count(update_menu, Args#{<<"updateBy">> => SelfUserName}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


remove_menu(_OperationID,  #{<<"menuId">> := MenuId}, _Context, Req) ->
    Check =
        case has_child_by_menu_id(MenuId) of
            ok ->
                check_menu_exist_role(MenuId);
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case Check of
            ok ->
                ok = alinkdata_dao:query_no_count(delete_menu_by_id, #{<<"menuId">> => MenuId}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================
check_menu_name_unique(Args) ->
    MenuId  = maps:get(<<"menuId">>, Args, -1),
    case alinkdata_dao:query_no_count(check_menu_name_unique, Args) of
        {ok, []} ->
            ok;
        {ok, [#{<<"menuId">> := MenuId}]} ->
            ok;
        {ok, _} ->
            MenuName = maps:get(<<"menuName">>, Args, <<>>),
            {error, <<"新增菜单'"/utf8, MenuName/binary, "'失败，菜单名称已存在"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.


check_frame(Args) ->
    case maps:get(<<"isFrame">>, Args, <<"1">>) of
        <<"0">> ->
            case maps:get(<<"path">>, Args, <<>>) of
                <<"http", _/binary>> ->
                    ok;
                _ ->
                    MenuName = maps:get(<<"menuName">>, Args, <<>>),
                    {error, <<"新增菜单'"/utf8, MenuName/binary, "'失败，地址必须以http(s)://开头"/utf8>>}
            end;
        _ ->
            ok
    end.


has_child_by_menu_id(MenuId) ->
    case alinkdata_dao:query_no_count(has_child_by_menu_id, #{<<"menuId">> => MenuId}) of
        {ok, [#{<<"count(1)">> := 0}]} ->
            ok;
        {ok, _} ->
            {error, <<"存在子菜单,不允许删除"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.

check_menu_exist_role(MenuId) ->
    case alinkdata_dao:query_no_count(check_menu_exist_role, #{<<"menuId">> => MenuId}) of
        {ok, [#{<<"count(1)">> := 0}]} ->
            ok;
        {ok, _} ->
            {error, <<"菜单已分配,不允许删除"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.

