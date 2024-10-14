%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 25. 8月 2022 上午12:16
%%%-------------------------------------------------------------------
-module(alinkdata_config_service).
-author("yqfclid").

%% API
-export([
    query_config/4,
    add_config/4,
    edit_config/4,
    remove_config/4,
    export/4
]).

%%%===================================================================
%%% API
%%%===================================================================
query_config(_OperationID, Args,  #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userId">> := UserId}} = ehttpd_auth:get_session(Token),
    Res =
        case alinkdata_dao:query_no_count(select_config, Args#{<<"owner">> => UserId}) of
            {ok, [Config]} ->
                alinkdata_ajax_result:success_result(Config);
            {ok, []} ->
                alinkdata_ajax_result:error_result(<<"not found">>);
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).

add_config(_OperationID, Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userId">> := UserId}} = ehttpd_auth:get_session(Token),
    NArgs = Args#{<<"owner">> => UserId},
    ok = alinkdata_dao:query_no_count(insert_config, NArgs),
    Res = alinkdata_ajax_result:success_result(),
    alinkdata_common_service:response(Res, Req).


edit_config(_OperationID, Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userId">> := UserId}} = ehttpd_auth:get_session(Token),
    ok = alinkdata_dao:query_no_count(update_config, Args#{<<"owner">> => UserId}),
    Res = alinkdata_ajax_result:success_result(),
    alinkdata_common_service:response(Res, Req).

remove_config(_OperationID, #{<<"configId">> := <<"refreshCache">>} = _Args, _Context, Req) ->
    alinkdata_wechat:fresh_wechat_env(),
    alinkdata_sms:fresh_sms_env(),
    alinkdata_wechat_mini:fresh_wechat_mini_env(),
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req);
remove_config(_OperationID, Args, _Context, Req) ->
    ConfigIds = binary:split(maps:get(<<"configId">>, Args, []), <<",">>, [global]),
    ok = alinkdata_dao:query_no_count(delete_config_by_ids, #{<<"configIds">> => ConfigIds}),
    Res = alinkdata_ajax_result:success_result(),
    alinkdata_common_service:response(Res, Req).

export(_OperationID, Args, _Context, _Req) ->
    {ok,Configs} = alinkdata_dao:query_no_count(select_config_list, Args),
    FileName = <<"config_data.xlsx">>,
    FileNameH = <<"\"", FileName/binary, "\"">>,
    {ok, Content} = alinkdata_xlsx:to_xlsx_file_content(FileName, <<"config">>, Configs),
    Headers = alinkdata_common:file_headers(Content, FileNameH),
    {200, Headers, Content}.
%%%===================================================================
%%% Internal functions
%%%===================================================================

