%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 10. 8月 2022 上午12:37
%%%-------------------------------------------------------------------
-module(alinkdata_common_service).
-author("yqfclid").

%% API
-export([
    common_handle/4,
    handle_with_dao/5,
    response/2,
    handle_data_scope/4,
    build_tree/2
]).

%%%===================================================================
%%% API
%%%===================================================================
common_handle(OperationID, Args, Context, Req) ->
    DaoId = maps:get(OperationID, dao_map()),
    handle_with_dao(DaoId, OperationID, Args, Context, Req).


handle_with_dao(DaoId, OperationID, Args, Context, Req) ->
    Show = maps:get(<<"show">>, Args, <<>>),
    case alinkdata_dao:query(DaoId, Args) of
        ok ->
            response(alinkdata_ajax_result:success_result(), Req);
        {ok, Rows} when Show =:= <<"treeselect">>->
            Tree = build_tree(Context, Rows),
            response(alinkdata_ajax_result:success_result(Tree), Req);
        {ok, Rows} ->
            response(alinkdata_ajax_result:success_result(Rows), Req);
        {ok, Count, Rows} ->
            CommonSuccessRes = alinkdata_ajax_result:success_result(),
            Res = maps:merge(#{total => Count, rows => Rows}, CommonSuccessRes),
            response(Res, Req);
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            response(alinkdata_ajax_result:error_result(Msg), Req)
    end.





response(Result, Req) ->
    Header = #{},
    case maps:get(<<"code">>, Result, 500) of
        500 ->
            logger:error("get error response ~p with req ~p", [Result, Req]);
        _ ->
            ok
    end,
    {maps:get(<<"code">>, Result, 500), Header, Result, Req}.


handle_data_scope(Args, undefined, _DeptAlias, _UserAlias) ->
    Args;
handle_data_scope(Args, Token, DeptAlias, UserAlias) ->
    #{<<"user">> := User, <<"roles">> := Roles} = ehttpd_auth:get_session(Token),
    case alinkdata_common:is_admin(maps:get(<<"userId">>, User)) of
        true ->
            Args;
        false ->
            F = fun(RoleKey, {Contains, Sql}) ->
                {ok, [Role]} =  alinkdata_dao:query_no_count(select_role_list, #{<<"roleKey">> => RoleKey}),
                DataScope = maps:get(<<"dataScope">>, Role),
                IsInContainers = lists:member(DataScope, Contains),
                IsScopeAll = lists:member(<<"1">>, Contains),
                case DataScope of
                    <<"1">> ->
                        {[<<"1">>| Contains], <<>>};
                    _ when IsScopeAll =:= true ->
                        {Contains, <<>>};
                    _ when DataScope =/= <<"2">> andalso IsInContainers->
                        {Contains, Sql};
                    <<"2">> ->
                        RoleId = alinkdata_common:to_binary(maps:get(<<"roleId">>, User)),
                        SubSql = <<" OR ", DeptAlias/binary, ".dept_id IN ( SELECT dept_id FROM sys_role_dept WHERE role_id = ", RoleId/binary, " ) ">>,
                        {[DataScope|Contains], <<Sql/binary, SubSql/binary>>};
                    <<"3">> ->
                        DeptId = alinkdata_common:to_binary(maps:get(<<"deptId">>, User)),
                        SubSql = <<" OR ", DeptAlias/binary, ".dept_id = ", DeptId/binary, " ">>,
                        {[DataScope|Contains], <<Sql/binary, SubSql/binary>>};
                    <<"4">> ->
                        DeptId = alinkdata_common:to_binary(maps:get(<<"deptId">>, User)),
                        SubSql = <<" OR ", DeptAlias/binary,
                            ".dept_id IN ( SELECT dept_id FROM sys_dept WHERE dept_id = ",
                            DeptId/binary, " or find_in_set( ",
                            DeptId/binary, " , ancestors ) )">>,
                        {[DataScope|Contains], <<Sql/binary, SubSql/binary>>};
                    <<"5">> when UserAlias =/= undefined orelse UserAlias =/= <<>>->
                        UserId = alinkdata_common:to_binary(maps:get(<<"userId">>, User)),
                        SubSql = <<" OR ", UserAlias/binary, ".user_id = ", UserId/binary, " ">>,
                        {[DataScope|Contains], <<Sql/binary, SubSql/binary>>};
                    <<"5">> ->
                        SubSql = <<" OR ", DeptAlias/binary, ".dept_id = 0 `">>,
                        {[DataScope|Contains], <<Sql/binary, SubSql/binary>>}
                end
            end,
            {_, AppendSql} = lists:foldl(F, {[], <<>>}, Roles),
            case AppendSql of
                <<>> ->
                    Args;
                _ ->
                    S = binary:part(AppendSql, {4, byte_size(AppendSql) - 4}),
                    Args#{<<"dataScope">> => <<" AND (", S/binary, ")">>}
            end
    end.

build_tree(#{extend := #{
    <<"table">> := Table
}}, Rows) ->
    Fun = fun(Row) -> alinkdata_formater:format_tree(Table, Row) end,
    alinkdata_utils:create_tree(Rows, Fun).


%%%===================================================================
%%% Internal functions
%%%===================================================================
dao_map() ->
    #{
        get_table_user_list => select_user_list,
        post_row_user => insert_user,
        get_table_role_optionselect => select_role_list,
        get_table_role_list => select_role_list,
        get_table_post_list => select_post_list,
        get_row_post_postid => select_post_by_id,
        get_table_post_optionselect => select_post_all,
        select_menu_list => select_menu_list,
        select_menu_list_by_id => select_menu_list_by_id,
        select_menu_list_by_user_id => select_menu_list_by_user_id,
        get_row_menu_menuid => select_menu_by_id,

        get_table_notice_list => select_notice_list,
        get_row_notice_noticeid => select_notice_by_id,

        get_table_dept_list => select_dept_list,

        get_table_dict_type_list => select_dict_type_list,
        get_row_dict_type_dictid => select_dict_type_by_id,
        get_table_dict_type_optionselect => select_dict_type_all,

        get_table_dict_data_list => select_dict_data_list,
        get_row_dict_data_dictcode => select_dict_data_by_id,



        get_table_config_list => select_config_list,
        get_row_config_configid => select_config,

        get_table_role_authuser_allocatedlist => select_allocated_list,
        get_table_role_authuser_unallocatedlist => select_unallocated_list,


        get_table_oper_log_list => select_oper_log_list,

        get_table_logininfor_list => select_login_infor_list

    }.
