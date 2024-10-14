%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 8月 2022 下午11:13
%%%-------------------------------------------------------------------
-module(alinkdata_notice_service).
-author("yqfclid").

%% API
-export([
    query_notice/4,
    add_notice/4,
    edit_notice/4,
    remove_notice/4
]).

%%%===================================================================
%%% API
%%%===================================================================
query_notice(_OperationID, Args, _Context, Req) ->
    Res =
        case alinkdata_dao:query_no_count(select_notice_by_id, Args) of
            {ok, [Notice]} ->
                ok = alinkdata_dao:query_no_count(update_notice, Args#{<<"status">> => <<"1">>}),
                alinkdata_ajax_result:success_result(Notice);
            {ok, []} ->
                alinkdata_ajax_result:error_result(<<"not found">>);
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).

add_notice(_OperationID, Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
    NArgs = Args#{<<"createBy">> => SelfUserName},
    ok = alinkdata_dao:query_no_count(insert_notice, NArgs),
    alinkdata_ajax_result:success_result(),
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).


edit_notice(_OperationID, Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
    ok = alinkdata_dao:query_no_count(update_notice, Args#{<<"updateBy">> => SelfUserName}),
    alinkdata_ajax_result:success_result(),
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).


remove_notice(_OperationID, Args, _Context, Req) ->
    NoticeIds = binary:split(maps:get(<<"noticeId">>, Args, []), <<",">>, [global]),
    ok = alinkdata_dao:query_no_count(delete_notice_by_ids, #{<<"noticeIds">> => NoticeIds}),
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================