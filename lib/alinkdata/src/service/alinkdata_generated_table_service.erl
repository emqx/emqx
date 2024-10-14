%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 23. 2月 2023 上午12:23
%%%-------------------------------------------------------------------
-module(alinkdata_generated_table_service).
-author("yqfclid").

%% API
-export([
    handle/4
]).

%%%===================================================================
%%% API
%%%===================================================================
handle(OperationID, Args, Context, Req) ->
    case ets:lookup(alinkdata_dao_cache, {route, OperationID}) of
        [{_, {M, F}}] ->
            M:F(OperationID, Args, Context, Req);
        _ ->
            do_handle(OperationID, Args, Context, Req)
    end.

do_handle(OperationID, RArgs, Context, Req) ->
    {DaoId, Table, Action, IdName} = parse_extend(Context),
    Args =
        case maps:get(token, Context, undefined) of
            undefined ->
                RArgs;
            Token when Action =:= <<"QUERY">>
                orelse Action =:= <<"POST">> ->
                #{<<"user">> := #{<<"userId">> := UserId, <<"deptId">> := DeptId}} = ehttpd_auth:get_session(Token),
                case alinkdata_common:is_admin(UserId) of
                    true ->
                        RArgs;
                    _ ->
                        RArgs#{<<"owner">> => UserId, <<"ownerDept">> => DeptId}
                end;
            _ ->
                RArgs
        end,
    {Res, InputArgs} =
        case Action of
            <<"GET">> ->
                {NArgs, IsSingle} =
                    case maps:get(IdName, Args, <<>>) of
                        <<>> ->
                            {Args, false};
                        IdValue ->
                            Ids0 = binary:split(IdValue, <<",">>, [global]),
                            Ids1 = lists:filter(fun(X) -> X =/= <<>> end, lists:usort(Ids0)),
                            {Args#{IdName => Ids1}, 1 >= length(Ids0)}
                    end,
                Res1 =
                    case alinkdata_dao:query_no_count(DaoId, NArgs) of
                        ok ->
                            response(alinkdata_ajax_result:success_result(), Req);
                        {ok, [Row]} when IsSingle =:= true ->
                            response(alinkdata_ajax_result:success_result(Row), Req);
                        {ok, []} when IsSingle =:= true ->
                            response(alinkdata_ajax_result:error_result(<<"未找到该id"/utf8>>), Req);
                        {ok, Rows} ->
                            response(alinkdata_ajax_result:success_result(Rows), Req);
                        {error, Reason} ->
                            Msg = list_to_binary(io_lib:format("~p", [Reason])),
                            response(alinkdata_ajax_result:error_result(Msg), Req)
                    end,
                {Res1, NArgs};
            <<"DELETE">> when Table =:= <<"device">> ->
                NArgs =
                    case maps:get(IdName, Args, <<>>) of
                        <<>> ->
                            Args;
                        IdValue ->
                            RealIds = binary:split(IdValue, <<",">>, [global]),
                            Addrs =
                                lists:foldl(
                                    fun(DeviceId, Acc) ->
                                        case alinkdata_dao:query_no_count('QUERY_device', #{<<"id">> => DeviceId}) of
                                            {ok, [#{<<"addr">> := Addr}|_]} ->
                                                [Addr|Acc];
                                            _ ->
                                                Acc
                                        end
                                end, [], RealIds),
                            Args#{IdName => RealIds, <<"addr">> => Addrs}
                    end,
                Res1 =
                    case alinkdata_dao:query_no_count(DaoId, NArgs) of
                        ok ->
                            response(alinkdata_ajax_result:success_result(), Req);
                        {error, Reason} ->
                            Msg = list_to_binary(io_lib:format("~p", [Reason])),
                            response(alinkdata_ajax_result:error_result(Msg), Req)
                    end,
                {Res1, NArgs};
            <<"DELETE">> when Table =:= <<"product">> ->
                NArgs =
                    case maps:get(IdName, Args, <<>>) of
                        <<>> ->
                            Args;
                        IdValue ->
                            RealIds = binary:split(IdValue, <<",">>, [global]),
                            Pkss =
                                lists:foldl(
                                    fun(ProductId, Acc) ->
                                        case alinkdata_dao:query_no_count('QUERY_product', #{<<"id">> => ProductId}) of
                                            {ok, [#{<<"pk">> := Pk, <<"ps">> := Ps}|_]} when Pk =/= <<>>
                                                                                        andalso Pk =/= null
                                                                                        andalso Ps =/= <<>>
                                                                                        andalso Ps =/= null ->
                                                [#{<<"pk">> => Pk, <<"ps">> => Ps}|Acc];
                                            _ ->
                                                Acc
                                        end
                                    end, [], RealIds),
                            Args#{IdName => RealIds, <<"pkss">> => Pkss}
                    end,
                Res1 =
                    case alinkdata_dao:query_no_count(DaoId, NArgs) of
                        ok ->
                            response(alinkdata_ajax_result:success_result(), Req);
                        {error, Reason} ->
                            Msg = list_to_binary(io_lib:format("~p", [Reason])),
                            response(alinkdata_ajax_result:error_result(Msg), Req)
                    end,
                {Res1, NArgs};
            <<"DELETE">> when Table =:= <<"sub_device">> ->
                NArgs =
                    case maps:get(IdName, Args, <<>>) of
                        <<>> ->
                            Args#{<<"gateways">> => []};
                        IdValue ->
                            RealIds = binary:split(IdValue, <<",">>, [global]),
                            Gateways =
                                lists:foldl(
                                    fun(DeviceId, Acc) ->
                                        case alinkdata_dao:query_no_count('QUERY_sub_device', #{<<"id">> => DeviceId}) of
                                            {ok, [#{<<"gateway">> := Addr}|_]} when Addr =/= <<>>
                                                                                andalso Addr =/= undefined
                                                                                andalso Addr =/= null ->
                                                [Addr|Acc];
                                            _ ->
                                                Acc
                                        end
                                    end, [], RealIds),
                            Args#{IdName => RealIds, <<"gateways">> => Gateways}
                    end,
                Res1 =
                    case alinkdata_dao:query_no_count(DaoId, NArgs) of
                        ok ->
                            response(alinkdata_ajax_result:success_result(), Req);
                        {error, Reason} ->
                            Msg = list_to_binary(io_lib:format("~p", [Reason])),
                            response(alinkdata_ajax_result:error_result(Msg), Req)
                    end,
                {Res1, NArgs};
            <<"DELETE">> when Table =:= <<"scene_device">> ->
                NArgs =
                    case maps:get(IdName, Args, <<>>) of
                        <<>> ->
                            Args#{<<"devices">> => []};
                        IdValue ->
                            RealIds = binary:split(IdValue, <<",">>, [global]),
                            DeviceIds =
                                lists:foldl(
                                    fun(RealId, Acc) ->
                                        case alinkdata_dao:query_no_count('QUERY_scene_device', #{<<"id">> => RealId}) of
                                            {ok, [#{<<"device">> := DeviceId}|_]} ->
                                                [DeviceId|Acc];
                                            _ ->
                                                Acc
                                        end
                                    end, [], RealIds),
                            Args#{IdName => RealIds, <<"devices">> => DeviceIds}
                    end,
                Res1 =
                    case alinkdata_dao:query_no_count(DaoId, NArgs) of
                        ok ->
                            response(alinkdata_ajax_result:success_result(), Req);
                        {error, Reason} ->
                            Msg = list_to_binary(io_lib:format("~p", [Reason])),
                            response(alinkdata_ajax_result:error_result(Msg), Req)
                    end,
                {Res1, NArgs};
            <<"DELETE">> ->
                NArgs =
                    case maps:get(IdName, Args, <<>>) of
                        <<>> ->
                            Args;
                        IdValue ->
                            Args#{IdName => binary:split(IdValue, <<",">>, [global])}
                    end,
                Res1 =
                    case alinkdata_dao:query_no_count(DaoId, NArgs) of
                        ok ->
                            response(alinkdata_ajax_result:success_result(), Req);
                        {error, Reason} ->
                            Msg = list_to_binary(io_lib:format("~p", [Reason])),
                            response(alinkdata_ajax_result:error_result(Msg), Req)
                    end,
                {Res1, NArgs};
            <<"EXPORT">> ->
                {ok,Vs} = alinkdata_dao:query_no_count(binary_to_atom(<<"QUERY_", Table/binary>>, latin1), Args),
                FileName = <<Table/binary, ".xlsx">>,
                {ok, Content} = alinkdata_xlsx:to_xlsx_file_content(FileName, Table, Vs),
                Headers = alinkdata_common:file_headers(Content, <<"\"", FileName/binary, "\"">>),
                {{200, Headers, Content}, Args};
            <<"QUERY">> ->
                {NDaoId, NArgs} =
                    case maps:is_key(<<"leftJoin[table]">>, Args)
                    andalso maps:is_key(<<"leftJoin[on]">>, Args) of
                        true ->
                            #{<<"leftJoin[on]">> := LeftJoinOn} = Args,
                            [A, B] = binary:split(LeftJoinOn, <<"=">>, [global]),
                            Args1 = Args#{
                                <<"leftJoin[aParam]">> => A,
                                <<"leftJoin[bParam]">> => B
                            },
                            DaoBin = atom_to_binary(DaoId),
                            {binary_to_atom(<<"LEFT_JOIN_", DaoBin/binary>>), Args1};
                        _ ->
                            {DaoId, Args}
                    end,
                NArgs1 = Args#{<<"dataScope">> => generated_data_scope(OperationID, NArgs, Context, Req)},
%%                NArgs2 = maps:remove(<<"ownerDept">>, maps:remove(<<"owner">>, NArgs1)),
                Res1 = alinkdata_common_service:handle_with_dao(NDaoId, OperationID, NArgs1, Context, Req),
                {Res1, Args};
            _ when Table =:= <<"device">> ->
                NArgs =
                    case maps:is_key(<<"addr">>, Args) of
                        true ->
                            Args;
                        _ ->
                            case maps:get(<<"id">>, Args, undefined) of
                                undefined ->
                                    Args;
                                DeviceId ->
                                    case alinkdata_dao:query_no_count('QUERY_device', #{<<"id">> => DeviceId}) of
                                        {ok, [#{<<"addr">> := Addr}|_]} ->
                                            Args#{<<"addr">> => Addr};
                                        _ ->
                                            Args
                                    end
                            end
                    end,
                Res1 = alinkdata_common_service:handle_with_dao(DaoId, OperationID, NArgs, Context, Req),
                {Res1, NArgs};
            _ when Table =:= <<"product">> ->
                NArgs =
                    case maps:get(<<"id">>, Args, undefined) of
                        undefined ->
                            Args;
                        ProductId ->
                            case alinkdata_dao:query_no_count('QUERY_product', #{<<"id">> => ProductId}) of
                                {ok, [#{<<"pk">> := Pk, <<"ps">> := Ps}|_]} when Pk =/= <<>>
                                                                            andalso Pk =/= null
                                                                            andalso Ps =/= <<>>
                                                                            andalso Ps =/= null ->
                                    Args#{<<"pkss">> => [#{<<"pk">> => Pk, <<"ps">> => Ps}]};
                                _ ->
                                    Args
                            end
                    end,
                Res1 = alinkdata_common_service:handle_with_dao(DaoId, OperationID, NArgs, Context, Req),
                {Res1, NArgs};
            _ when Table =:= <<"sub_device">> ->
                NArgs =
                    case maps:is_key(<<"gateway">>, Args) of
                        true ->
                            Args#{<<"gateways">> => [maps:get(<<"gateway">>, Args)]};
                        _ ->
                            Args#{<<"gateways">> => []}
                    end,
                Res1 = alinkdata_common_service:handle_with_dao(DaoId, OperationID, NArgs, Context, Req),
                {Res1, NArgs};
            _ when Table =:= <<"scene_device">> ->
                NArgs =
                    case maps:is_key(<<"devices">>, Args) of
                        true ->
                            Args#{<<"devices">> => [alinkutil_type:to_integer(maps:get(<<"device">>, Args))]};
                        _ ->
                            Args#{<<"devices">> => []}
                    end,
                Res1 = alinkdata_common_service:handle_with_dao(DaoId, OperationID, NArgs, Context, Req),
                {Res1, NArgs};
            _ ->
                Res1 = alinkdata_common_service:handle_with_dao(DaoId, OperationID, Args, Context, Req),
                {Res1, Args}
        end,
    run_hooks(DaoId, InputArgs),
    Res.

parse_extend(#{extend := #{<<"action">> := Action, <<"table">> := Table} = Extend}) ->
    {binary_to_atom(<<Action/binary, "_", Table/binary>>, latin1), Table, Action, maps:get(<<"id">>, Extend, undefined)};
parse_extend(_) ->
    {undefined, undefined}.
%%%===================================================================
%%% Internal functions
%%%===================================================================
response(Result, Req) ->
    Header = #{},
    {maps:get(<<"code">>, Result, 500), Header, Result, Req}.


run_hooks(DaoId, InputArgs) ->
    try
        ehttpd_hook:run(DaoId, [DaoId, InputArgs], #{})
    catch
        Error:Reason:Stacktrace ->
            logger:error("Failed to execute ~0p", [{Error, Reason, Stacktrace}]),
            ok
    end.



generated_data_scope(_OperationID, Args, #{token := Token}, _Req) ->
    #{<<"user">> := User, <<"roles">> := Roles} = ehttpd_auth:get_session(Token),
    case alinkdata_common:is_admin(maps:get(<<"userId">>, User)) of
        true ->
            <<>>;
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
                        SubSql = <<" OR owner_dept IN ( SELECT dept_id FROM sys_role_dept WHERE role_id = ", RoleId/binary, " ) ">>,
                        {[DataScope|Contains], <<Sql/binary, SubSql/binary>>};
                    <<"3">> ->
                        DeptId = alinkdata_common:to_binary(maps:get(<<"deptId">>, User)),
                        SubSql = <<" OR owner_dept = ", DeptId/binary, " ">>,
                        {[DataScope|Contains], <<Sql/binary, SubSql/binary>>};
                    <<"4">> ->
                        DeptId = alinkdata_common:to_binary(maps:get(<<"deptId">>, User)),
                        SubSql = <<" OR owner_dept IN ( SELECT dept_id FROM sys_dept WHERE dept_id = ",
                            DeptId/binary, " or find_in_set( ",
                            DeptId/binary, " , ancestors ) )">>,
                        {[DataScope|Contains], <<Sql/binary, SubSql/binary>>};
                    <<"5">> ->
                        UserId = alinkdata_common:to_binary(maps:get(<<"userId">>, User)),
                        SubSql = <<" OR owner = ", UserId/binary, " ">>,
                        {[DataScope|Contains], <<Sql/binary, SubSql/binary>>}
                end
                end,
            {_, AppendSql} = lists:foldl(F, {[], <<>>}, Roles),
            case AppendSql of
                <<>> ->
                    <<>>;
                _ ->
                    S = binary:part(AppendSql, {4, byte_size(AppendSql) - 4}),
                    <<" AND (", S/binary, ")">>
            end
    end;
generated_data_scope(_OperationID, _Args, _Context, _Req) ->
    <<>>.