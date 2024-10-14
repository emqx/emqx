%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 05. 12月 2023 下午8:17
%%%-------------------------------------------------------------------
-module(alinkdata_project_service).
-author("yqfclid").

%% API
-export([
    sync_data/4
]).

%%%===================================================================
%%% API
%%%===================================================================
sync_data(_OperationID, #{<<"id">> := Id, <<"event">> := Event}, _Context, Req) ->
    Res =
        case alinkdata_dao:query_no_count('QUERY_app', #{<<"project">> => Id}) of
            {ok, Apps} ->
                lists:foreach(
                    fun(#{<<"appid">> := AppId}) ->
                        ProjectIdB = alinkutil_type:to_binary(Id),
                        AppIdB = alinkutil_type:to_binary(AppId),
                        Topic = <<"app/in/", ProjectIdB/binary, "/", AppIdB/binary>>,
                        Data = #{<<"msgType">> => <<"sync">>, <<"data">> => #{<<"event">> => Event}},
                        emqx:publish(emqx_message:make(Topic, jiffy:encode(Data))),
                        alinkdata_app_service:set_sync_status(AppId, <<"wait">>)
                end, Apps),
                alinkdata_ajax_result:success_result();
            {error, Reason} ->
                logger:error("query appid ~p failed ~p", [Id, Reason]),
                alinkdata_ajax_result:error_result(<<"Internal Error">>)
        end,
    alinkdata_common_service:response(Res, Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================