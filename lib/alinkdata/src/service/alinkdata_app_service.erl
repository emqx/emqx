%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 17. 5月 2023 下午4:16
%%%-------------------------------------------------------------------
-module(alinkdata_app_service).

%% API
-export([
    login_with_appid/4,
    sync_data/4,
    set_sync_status/2
]).

%%%===================================================================
%%% API
%%%===================================================================
login_with_appid(_OperationID, #{<<"appId">> := AppId, <<"appSecret">> := AppSecret}, _Context, Req) ->
    Res =
        case alinkdata_dao:query_no_count('QUERY_app', #{<<"appid">> => AppId}) of
            {ok, [#{<<"secret">> := AppSecret, <<"owner">> := UserId, <<"project">> := ProjectId} = Map |_ ]} ->
                case alinkdata_user:query_user_by_id(UserId) of
                    {ok, #{<<"user">> := #{<<"userName">> := UserName}} = UserInfo} ->
                        Ts = os:system_time(second),
                        Token = ehttpd_utils:md5(lists:concat([binary_to_list(UserName), Ts])),
                        TTL = maps:get(<<"expire">>, Map, ehttpd_server:get_env(default, expire, 1800)),
                        ehttpd_auth:put_session(Token, UserInfo#{<<"appid">> => AppId}, TTL),
                        (alinkdata_ajax_result:success_result())#{token => Token, expire => TTL, <<"projectId">> => ProjectId};
                    {error, Reason} ->
                        alinkdata_ajax_result:error_result(Reason)
                end;
            {ok, _} ->
                alinkdata_ajax_result:error_result(<<"错误的appid或者appsecert"/utf8>>);
            {error, Reason} ->
                logger:error("query appid ~p failed ~p", [AppId, Reason]),
                alinkdata_ajax_result:error_result(<<"Internal Error">>)
        end,
    alinkdata_common_service:response(Res, Req).

sync_data(_OperationID, #{<<"id">> := Id, <<"event">> := Event}, _Context, Req) ->
    Res =
        case alinkdata_dao:query_no_count('QUERY_app', #{<<"id">> => Id}) of
            {ok, [#{<<"project">> := ProjectId, <<"appid">> := AppId} |_ ]} ->
                ProjectIdB = alinkutil_type:to_binary(ProjectId),
                Topic = <<"app/in/", ProjectIdB/binary, "/", AppId/binary>>,
                Data = #{<<"msgType">> => <<"sync">>, <<"data">> => #{<<"event">> => Event}},
                emqx:publish(emqx_message:make(Topic, jiffy:encode(Data))),
                set_sync_status(AppId, <<"wait">>),
                alinkdata_ajax_result:success_result();
            {ok, _} ->
                alinkdata_ajax_result:error_result(<<"错误的appid"/utf8>>);
            {error, Reason} ->
                logger:error("query appid ~p failed ~p", [Id, Reason]),
                alinkdata_ajax_result:error_result(<<"Internal Error">>)
        end,
    alinkdata_common_service:response(Res, Req).


set_sync_status(AppId, Status) ->
    Sql = <<"update sys_app set sync_status= '", Status/binary, "' where appid = '", AppId/binary, "'">>,
    case alinkdata:query_mysql_format_map(default, Sql) of
        ok ->
            ok;
        {error, Reason} ->
            logger:error("set sync status ~p ~p failed:~p", [AppId, Status, Reason])
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================