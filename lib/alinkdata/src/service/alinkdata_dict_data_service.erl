%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 25. 8月 2022 上午12:25
%%%-------------------------------------------------------------------
-module(alinkdata_dict_data_service).
-author("yqfclid").

%% API
-export([
    query_dict_data/4,
    add_dict_data/4,
    edit_dict_data/4,
    remove_dict_data/4,
    export/4
]).

%%%===================================================================
%%% API
%%%===================================================================
query_dict_data(_OperationID, Args, _Context, Req) ->
    Res =
        case alinkdata_dao:query_no_count(select_dict_data_by_id, Args) of
            {ok, [DictData]} ->
                alinkdata_ajax_result:success_result(DictData);
            {ok, []} ->
                alinkdata_ajax_result:error_result(<<"not found">>);
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).


add_dict_data(_OperationID, Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
    NArgs = Args#{<<"createBy">> => SelfUserName},
    ok = alinkdata_dao:query_no_count(insert_dict_data, NArgs),
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).


edit_dict_data(_OperationID, Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
    ok = alinkdata_dao:query_no_count(update_dict_data, Args#{<<"updateBy">> => SelfUserName}),
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).


remove_dict_data(_OperationID, Args, _Context, Req) ->
    DictCodes = binary:split(maps:get(<<"dictCode">>, Args, []), <<",">>, [global]),
    ok = alinkdata_dao:query_no_count(delete_dict_data_by_ids, #{<<"dictCodes">> => DictCodes}),
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).



export(_OperationID, Args, _Context, _Req) ->
    {ok,Dicts} = alinkdata_dao:query_no_count(select_dict_data_list, Args),
    Content = alinkdata_common:to_file_content(Dicts),
    Headers = alinkdata_common:file_headers(Content, <<"\"dictData_data.csv\""/utf8>>),
    {200, Headers, Content}.
%%%===================================================================
%%% Internal functions
%%%===================================================================