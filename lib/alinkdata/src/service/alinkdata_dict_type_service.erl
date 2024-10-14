%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 25. 8月 2022 上午12:24
%%%-------------------------------------------------------------------
-module(alinkdata_dict_type_service).
-author("yqfclid").

%% API
-export([
    query_dict_type/4,
    add_dict_type/4,
    edit_dict_type/4,
    remove_dict_type/4,
    export/4
]).

%%%===================================================================
%%% API
%%%===================================================================
query_dict_type(_OperationID, Args, _Context, Req) ->
    Res =
        case alinkdata_dao:query_no_count(select_dict_type_by_id, Args) of
            {ok, [DictType]} ->
                alinkdata_ajax_result:success_result(DictType);
            {ok, []} ->
                alinkdata_ajax_result:error_result(<<"not found">>);
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).


add_dict_type(_OperationID, Args, #{token := Token} = _Context, Req) ->
    Res =
        case check_dict_type_unique(Args) of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                NArgs = Args#{<<"createBy">> => SelfUserName},
                ok = alinkdata_dao:query_no_count(insert_dict_type, NArgs),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


edit_dict_type(_OperationID, Args, #{token := Token} = _Context, Req) ->
    Res =
        case check_dict_type_unique(Args) of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                ok = alinkdata_dao:query_no_count(update_dict_type, Args#{<<"updateBy">> => SelfUserName}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).

remove_dict_type(_OperationID, #{<<"dictId">> := <<"refreshCache">>} = _Args, _Context, Req) ->
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req);
remove_dict_type(_OperationID, Args, _Context, Req) ->
    DictIds = binary:split(maps:get(<<"dictId">>, Args, []), <<",">>, [global]),
    Check =
        lists:foldl(
            fun(DictId, ok) ->
                {ok, [DictInfo]} = alinkdata_dao:query_no_count(select_dict_type_by_id, #{<<"dictId">> => DictId}),
                case alinkdata_dao:query_no_count(count_dict_data_by_type, #{<<"dictId">> => DictId}) of
                    {ok, [#{<<"count(1)">> := 0}]} ->
                        ok;
                    {ok, _} ->
                        DictName = maps:get(<<"dictName">>, DictInfo, <<>>),
                        {error, <<DictName/binary, "已分配,不能删除"/utf8>>};
                    {error, Reason} ->
                        {error, Reason}
                end;
                (_, {error, Reason}) ->
                    {error, Reason}
            end , ok, DictIds),
    Res =
        case Check of
            ok ->
                ok = alinkdata_dao:query_no_count(delete_dict_type_by_ids, #{<<"dictIds">> => DictIds}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).



export(_OperationID, Args, _Context, _Req) ->
    {ok,Dicts} = alinkdata_dao:query_no_count(select_dict_type_list, Args),
    Content = alinkdata_common:to_file_content(Dicts),
    Headers = alinkdata_common:file_headers(Content, <<"\"dictType_data.csv\""/utf8>>),
    {200, Headers, Content}.
%%%===================================================================
%%% Internal functions
%%%===================================================================
check_dict_type_unique(Args) ->
    DictId  = maps:get(<<"dictId">>, Args, -1),
    case alinkdata_dao:query_no_count(check_dict_type_unique, Args) of
        {ok, []} ->
            ok;
        {ok, [#{<<"dictId">> := DictId}]} ->
            ok;
        {ok, _} ->
            DictName = maps:get(<<"dictName">>, Args, <<>>),
            {error, <<"修改字典'"/utf8, DictName/binary, "'失败，字典类型已存在"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.
