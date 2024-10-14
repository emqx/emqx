%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 31. 8月 2022 上午12:43
%%%-------------------------------------------------------------------
-module(alinkdata_dept_service).
-author("yqfclid").

%% API
-export([
    list_exclude_dept/4,
    list_dept/4,
    query_dept/4,
    add_dept/4,
    edit_dept/4,
    remove_dept/4,
    select_role_dept_tree/4
]).

%%%===================================================================
%%% API
%%%===================================================================
list_exclude_dept(_OperationID, Args, #{token := Token}, Req) ->
    CheckDeptId = maps:get(<<"deptId">>, Args, undefined),
    DeptArgs = alinkdata_common_service:handle_data_scope(#{}, Token, <<"d">>, undefined),
    {ok, Depts} = alinkdata_dao:query_no_count(select_dept_list, DeptArgs),
    FilterDepts =
        lists:filter(
            fun(#{<<"deptId">> := DeptId}) when DeptId =:= CheckDeptId ->
                false;
               (#{<<"deptId">> := DeptId, <<"ancestors">> := Ancestors}) ->
                   DeptIdB = alinkutil_type:to_binary(DeptId),
                   Parents = binary:split(Ancestors, <<",">>, [global]),
                   not lists:member(DeptIdB, Parents)
        end, Depts),
    Res = alinkdata_ajax_result:success_result(FilterDepts),
    alinkdata_common_service:response(Res, Req).


list_dept(_OperationID, Args, Context, Req) ->
    Show = maps:get(<<"show">>, Args, <<>>),
    Res =
        case alinkdata_dao:query(select_dept_list, Args) of
            {ok, Rows} when Show =:= <<"treeselect">> ->
                Tree = alinkdata_common_service:build_tree(Context, Rows),
                R = alinkdata_ajax_result:success_result(Tree),
                merge_checked_keys(R, Args);
            {ok, Rows} ->
                alinkdata_ajax_result:success_result(Rows);
            {ok, Count, Rows} ->
                CommonSuccessRes = alinkdata_ajax_result:success_result(),
                maps:merge(#{total => Count, rows => Rows}, CommonSuccessRes);
            {error, Reason} ->
                logger:error("list dept failed ~p", [Reason]),
                alinkdata_ajax_result:error_result(<<"Internal Error"/utf8>>)
        end,
    alinkdata_common_service:response(Res, Req).


query_dept(_OperationID, #{<<"deptId">> := DeptId} = Args, #{token := Token} = _Context, Req) ->
    case check_dept_data_scope(DeptId, Token) of
        ok ->
            {ok, [Dept]} = alinkdata_dao:query_no_count(select_dept_by_id, Args),
            Res = alinkdata_ajax_result:success_result(Dept),
            alinkdata_common_service:response(Res, Req);
        {error, Reason} ->
            Res = alinkdata_ajax_result:error_result(Reason),
            alinkdata_common_service:response(Res, Req)
    end.


add_dept(_OperationID, Args, #{token := Token} = _Context, Req) ->
    Res =
        case check_dept_name_unique(Args) of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                NArgs = Args#{<<"createBy">> => SelfUserName},
                ok = alinkdata_dao:query_no_count(insert_dept, NArgs),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


edit_dept(_OperationID, #{<<"deptId">> := DeptId} = Args, #{token := Token} = _Context, Req) ->
    Check =
        case check_dept_data_scope(DeptId, Token) of
            ok ->
                case check_dept_name_unique(Args) of
                    ok ->
                        case maps:get(<<"parentId">>, Args, 0) =:= DeptId of
                            true ->
                                DeptName = maps:get(<<"deptName">>, Args, <<>>),
                                {error, <<"修改部门'"/utf8, DeptName/binary, "'失败，上级部门不能是自己"/utf8>>};
                            _ ->
                                case check_dept_disable(Args) of
                                    ok ->
                                        ok;
                                    {error, Reason} ->
                                        {error, Reason}
                                end
                        end;
                    {error, Reason} ->
                        {error, Reason}
                end;
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                ok = alinkdata_dao:query_no_count(update_dept, Args#{<<"updateBy">> => SelfUserName}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


remove_dept(_OperationID, #{<<"deptId">> := DeptId} = _Args, #{token := Token} = _Context, Req) ->
    Check =
        case check_dept_data_scope(DeptId, Token) of
            ok ->
                case has_child_by_dept_id(DeptId) of
                    ok ->
                        case check_dept_exist_user(DeptId) of
                            ok ->
                                ok;
                            {error, Reason} ->
                                {error, Reason}
                        end;
                    {error, Reason} ->
                        {error, Reason}
                end;
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case Check of
            ok ->
                ok = alinkdata_dao:query_no_count(delete_dept_by_id, #{<<"deptId">> => DeptId}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).



select_role_dept_tree(_OperationID, #{<<"roleId">> := RoleId} = Args, #{token := Token} = _Context, Req) ->
    NArgs = alinkdata_common_service:handle_data_scope(Args, Token, <<"d">>, undefined),
    {ok, Depts} = alinkdata_dao:query_no_count(select_dept_list, NArgs),
    {ok, [Role]} = alinkdata_dao:query_no_count(select_role_by_id, #{<<"roleId">> => RoleId}),
    Arg1 = #{<<"deptCheckStrictly">> => maps:get(<<"deptCheckStrictly">>, Role, undefined), <<"roleId">> => RoleId},
    {ok, DeptIds} = alinkdata_dao:query_no_count(select_dept_list_by_role_id, Arg1),
    Res = alinkdata_ajax_result:success_result(),
    NRes = Res#{
        <<"checkedKeys">> => lists:map(fun(#{<<"deptId">> := DeptId}) -> DeptId end, DeptIds),
        <<"depts">> => alinkdata_common_service:build_tree(#{extend => #{<<"table">> => <<"dept">>}}, Depts)
    },
    alinkdata_common_service:response(NRes, Req).

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_dept_data_scope(DeptId, Token) ->
    #{<<"user">> := #{<<"userId">> := SelfUserId}} = ehttpd_auth:get_session(Token),
    case alinkdata_common:is_admin(SelfUserId) of
        true ->
            ok;
        false ->
            Args = alinkdata_common_service:handle_data_scope(#{<<"deptId">> => DeptId}, Token, <<"d">>, undefined),
            case alinkdata_dao:query_no_count(select_dept_list, Args) of
                {ok, []} ->
                    {error, <<"没有权限访问部门数据！"/utf8>>};
                {error, Reason} ->
                    {error, Reason};
                _ ->
                    ok
            end
    end.

check_dept_name_unique(Args) ->
    DeptId  = maps:get(<<"deptId">>, Args, -1),
    case alinkdata_dao:query_no_count(check_dept_name_unique, Args) of
        {ok, []} ->
            ok;
        {ok, [#{<<"deptId">> := DeptId}]} ->
            ok;
        {ok, _} ->
            DeptName = maps:get(<<"deptName">>, Args, <<>>),
            {error, <<"新增角色'"/utf8, DeptName/utf8, "'失败，角色名称已存在"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.

check_dept_disable(Args) ->
    case maps:get(<<"status">>, Args, <<"0">>) of
        <<"1">> ->
            case alinkdata_dao:query_no_count(select_normal_children_dept_by_id, Args) of
                {ok, [#{<<"count(*)">> := Count}]} when Count =:= 0->
                    ok;
                {ok, _} ->
                    {error, <<"该部门包含未停用的子部门！"/utf8>>};
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            ok
    end.


has_child_by_dept_id(DeptId) ->
    case alinkdata_dao:query_no_count(has_child_by_dept_id, #{<<"deptId">> => DeptId}) of
        {ok, [#{<<"count(1)">> := 0}]} ->
            ok;
        {ok, _} ->
            {error, <<"存在下级部门,不允许删除"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.

check_dept_exist_user(DeptId) ->
    case alinkdata_dao:query_no_count(check_dept_exist_user, #{<<"deptId">> => DeptId}) of
        {ok, [#{<<"count(1)">> := 0}]} ->
            ok;
        {ok, _} ->
            {error, <<"部门存在用户,不允许删除"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.


merge_checked_keys(R, Args) ->
    case maps:get(<<"roleId">>, Args, 0) of
        0 ->
            R;
        RoleId ->
            CheckKeys = select_dept_list_by_role_id(RoleId),
            R#{<<"checkedKeys">> => CheckKeys}
    end.

select_dept_list_by_role_id(RoleId) ->
    case alinkdata_dao:query_no_count(select_role_by_id, #{<<"roleId">> => RoleId}) of
        {ok, [#{<<"deptCheckStrictly">> := DeptCheckStrictly}]} ->
            Args = #{<<"deptCheckStrictly">> => DeptCheckStrictly, <<"roleId">> => RoleId},
            case alinkdata_dao:query_no_count(select_dept_list_by_role_id, Args) of
                {ok, Depts} ->
                    lists:map(
                        fun(#{<<"deptId">> := DeptId}) ->
                            DeptId
                    end, Depts);
                {error, Reason} ->
                    logger:error("query dept list failed:~p", [Reason]),
                    []
            end;
        {error, Reason} ->
            logger:error("select_role_by_id failed:~p", [Reason]),
            []
    end.