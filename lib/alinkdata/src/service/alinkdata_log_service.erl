%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 06. 4月 2023 下午1:34
%%%-------------------------------------------------------------------
-module(alinkdata_log_service).

%% API
-export([
    export_oper_log/4,
    remove_oper_log/4,
    export_loginfo/4,
    remove_loginfo/4
]).

%%%===================================================================
%%% API
%%%===================================================================
export_oper_log(_OperationID, Args, _Context, _Req) ->
    {ok,Logs} = alinkdata_dao:query_no_count(select_oper_log_list, Args),
    Content = alinkdata_common:to_file_content(Logs),
    Headers = alinkdata_common:file_headers(Content, <<"\"oper_log.csv\""/utf8>>),
    {200, Headers, Content}.


remove_oper_log(_OperationID, Args, _Context, Req) ->
    case maps:get(<<"operId">>, Args, []) of
        <<"clean">> ->
            ok = alinkdata_dao:query_no_count(clean_oper_log, #{});
        OId ->
            OperIds = binary:split(OId, <<",">>, [global]),
            ok = alinkdata_dao:query_no_count(delete_oper_log_by_ids, #{<<"operIds">> => OperIds})

    end,
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).



export_loginfo(_OperationID, Args, _Context, _Req) ->
    {ok,Logs} = alinkdata_dao:query_no_count(select_oper_log_list, Args),
    Content = alinkdata_common:to_file_content(Logs),
    Headers = alinkdata_common:file_headers(Content, <<"\"logininfo.csv\""/utf8>>),
    {200, Headers, Content}.


remove_loginfo(_OperationID, Args, _Context, Req) ->
    case maps:get(<<"infoId">>, Args, []) of
        <<"clean">> ->
            ok = alinkdata_dao:query_no_count(clean_logininfor, #{});
        IId ->
            InfoIds = binary:split(IId, <<",">>, [global]),
            ok = alinkdata_dao:query_no_count(delete_logininfor_by_ids, #{<<"infoIds">> => InfoIds})

    end,
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================