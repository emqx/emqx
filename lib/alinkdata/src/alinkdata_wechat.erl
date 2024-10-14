%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc wechat util
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(alinkdata_wechat).

-behaviour(gen_server).

-export([
    login_notice/2,
    get_open_id/1,
    fresh_access_token/0,
    do_flush_access_token/2,
    send_message/2,
    send_message/3,
    handle_event/1,
    get_qrcode/0,
    get_access_token/0,
    send_msg_by_user_id/2,
    get_user_info/1,
    set_session/1,
    get_session/1,
    fresh_wechat_env/0,
    timestamp2localtime_str/1,
    get_mini_session/1,
    set_mini_session/2
]).

-export([start_link/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-define(WECHAT_CACHE, ?MODULE).

-define(QRUUID_CACHE, qr_scene_id_cache).

-define(QR_CODE_EXPIRE_SECONDS, 1800).

-define(QRUUID_CACHE_MAX_SZIE, 10240).

-record(state, {flush_timer = erlang:make_ref(), flush_interval}).

%%%===================================================================
%%% API
%%%===================================================================



fresh_access_token() ->
    gen_server:call(?SERVER, flush_access_token).



handle_event(#{
    <<"FromUserName">> := OpenId,
    <<"Ticket">> := _Ticket,
    <<"EventKey">> := EventKey,
    <<"MsgType">> := <<"event">>,
    <<"Event">> := EventType
}) when (EventType =:= <<"subscribe">> orelse EventType =:= <<"SCAN">>)
    andalso EventKey =/= <<>> ->
    SceneId = event_key_scene_id(EventKey),
    Now = erlang:system_time(second),
    case ets:lookup(?QRUUID_CACHE, SceneId) of
        [{_, ExpireAt, _}] when Now > ExpireAt ->
            ets:delete(?QRUUID_CACHE, SceneId),
            ok;
        [{_, ExpireAt, _}] ->
            ets:insert(?QRUUID_CACHE, {SceneId, ExpireAt, OpenId});
        [] ->
            ok;
        {error, Reason} ->
            logger:error("handle scene event failed :~p", [Reason]),
            ok
    end;
handle_event(WechatEvent) ->
     logger:info("get another wechat event ~p", [WechatEvent]).


get_access_token() ->
    case ets:lookup(?WECHAT_CACHE, access_token) of
        [{_, Token}] ->
            {ok, Token};
        [] ->
            fresh_access_token();
        {error, Reason} ->
            {error, Reason}
    end.

get_user_info(OpenId) ->
    F =
        fun(Token) ->
            Url = <<"https://api.weixin.qq.com/cgi-bin/user/info?access_token=",
                Token/binary,
                "&openid=",
                OpenId/binary,
                "&lang=zh_CN">>,
            case alinkdata_http:request(get, Url, [], <<>>, [{ssl_options, [{depth, 2}]}]) of
                {ok, 200, _Headers, Body} ->
                    case jiffy:decode(Body, [return_maps]) of
                        #{<<"openid">> := OpenId} = UserInfo ->
                            {ok, UserInfo#{<<"open_id">> => OpenId}};
                        Err ->
                            {error, Err}
                    end;
                {ok, StatusCode, RespHeaders, Body} ->
                    {error, {StatusCode, RespHeaders, Body}};
                {error, Reason} ->
                    {error, Reason}
            end
        end,
    request_with_token(F).


set_mini_session(SessionKey, MiniInfo) ->
    Now = erlang:system_time(second),
    ExpireAt = Now + 1800,
    ets:insert(?QRUUID_CACHE, {{mini_session, SessionKey}, ExpireAt, MiniInfo}),
    SessionKey.

get_mini_session(SessionKey) ->
    Now = erlang:system_time(second),
    case ets:lookup(?QRUUID_CACHE, {mini_session, SessionKey}) of
        [{_, ExpireAt, WechatInfo}] when Now =< ExpireAt->
            {ok, WechatInfo};
        [SessionInfo] ->
            ets:delete_object(?QRUUID_CACHE, SessionInfo),
            {error, expired};
        [] ->
            {error, not_found}
    end.


set_session(WechatInfo) ->
    Now = erlang:system_time(second),
    TimeB = integer_to_binary(erlang:system_time()),
    Rand = alinkutil_type:to_binary(rand:uniform(10000)),
    SessionKey = <<Rand/binary, "_", TimeB/binary>>,
    ExpireAt = Now + 1800,
    ets:insert(?QRUUID_CACHE, {{session, SessionKey}, ExpireAt, WechatInfo}),
    SessionKey.

get_session(Session) ->
    Now = erlang:system_time(second),
    case ets:lookup(?QRUUID_CACHE, {session, Session}) of
        [{_, ExpireAt, WechatInfo}] when Now =< ExpireAt->
            {ok, WechatInfo};
        [SessionInfo] ->
            ets:delete_object(?QRUUID_CACHE, SessionInfo),
            {error, expired};
        [] ->
            {error, not_found}
    end.

get_open_id(SceneId) ->
    Now = erlang:system_time(second),
    case ets:lookup(?QRUUID_CACHE, SceneId) of
        [{_, ExpireAt, _}] when Now > ExpireAt ->
            ets:delete(?QRUUID_CACHE, SceneId),
            {error, expired};
        [{_, _, undefined}] ->
            {error, not_subscribe};
        [{_, _, OpenId}] ->
            ets:delete(?QRUUID_CACHE, SceneId),
            {ok, OpenId};
        [] ->
            {error, expired};
        {error, Reason} ->
            logger:error("check scene id failed :~p", [Reason]),
            {error, expired}
    end.


get_qrcode() ->
    request_with_token(
        fun(Token) ->
            Url = <<"https://api.weixin.qq.com/cgi-bin/qrcode/create?access_token=", Token/binary>>,
            RandSceneId = alinkutil_type:to_binary(alinkutil_time:timestamp(nano)),
            PostData = #{
                <<"expire_seconds">> => ?QR_CODE_EXPIRE_SECONDS,
                <<"action_name">> => <<"QR_STR_SCENE">>,
                <<"action_info">> => #{<<"scene">> => #{<<"scene_str">> => RandSceneId}}
            },
            case alinkdata_http:request(post, Url, [], jiffy:encode(PostData), [{ssl_options, [{depth, 2}]}]) of
                {ok, 200, _RespHeaders, Body} ->
                    case catch jiffy:decode(Body, [return_maps]) of
                        #{
                            <<"ticket">> := Ticket,
                            <<"expire_seconds">> := _ExpireSeconds,
                            <<"url">> := _QrCodeUrl
                        } = _Ret->
                            case ets:info(?QRUUID_CACHE, size) of
                                Size when Size > ?QRUUID_CACHE_MAX_SZIE ->
                                    ets:delete_all_objects(?QRUUID_CACHE_MAX_SZIE);
                                _ ->
                                    ok
                            end,
                            ExpireAt = erlang:system_time(second) + ?QR_CODE_EXPIRE_SECONDS,
                            ets:insert(?QRUUID_CACHE, {RandSceneId, ExpireAt, undefined}),
                            {ok, RandSceneId, Ticket};
                        Ret ->
                            {error, Ret}
                    end;
                {ok, StatusCode, RespHeaders, Body} ->
                    {error, {qrcode_failed, StatusCode, RespHeaders, Body}};
                {error, Reason} ->
                    {error, {qrcode_failed, Reason}}
            end
        end).

send_msg_by_user_id(UserId, MsgDetail) ->
    case alinkdata_dao:query_user_by_user_id(#{<<"userId">> => UserId}) of
        {ok, [#{<<"openId">> := OpenId}]} when OpenId =/= <<>> ->
            send_message(OpenId, MsgDetail);
        {ok, []} ->
            {error, user_not_found};
        {ok, _} ->
            {error, user_not_bind};
        {error, Reason} ->
            {error, Reason}
    end.

login_notice(OpenId, MsgDetail) ->
    NoticeTemplateId  = get_from_wechat_env(notice_tpl_id),
    Fun =
        fun(Token) ->
            Url = <<"https://api.weixin.qq.com/cgi-bin/message/template/send?access_token=", Token/binary>>,
            PostData = #{
                <<"touser">> => OpenId,
                <<"template_id">> => NoticeTemplateId,
                <<"data">> =>
                #{
                    <<"first">> => #{
                        <<"value">> => maps:get(<<"title">>, MsgDetail, <<""/utf8>>)
                    },
                    <<"time">> => #{
                        <<"value">> => timestamp2localtime_str(maps:get(<<"time">>, MsgDetail, erlang:system_time(second)))
                    },
                    <<"ip">> => #{
                        <<"value">> => maps:get(<<"ip">>, MsgDetail, <<>>)
                    },
                    <<"reason">> => #{
                        <<"value">> => maps:get(<<"reason">>, MsgDetail, <<"如果本次登录不是您本人所为，请到平台处理账号密码。"/utf8>>)
                    }
                }
            },
            case alinkdata_http:request(post, Url, [], jiffy:encode(PostData), [{ssl_options, [{depth, 2}]}]) of
                {ok, 200, _RespHeaders, Body} ->
                    case catch jiffy:decode(Body, [return_maps]) of
                        #{<<"errcode">> := 0, <<"msgid">> := MsgId} ->
                            logger:info("send msg to ~p ~p msgid ~p success", [OpenId, MsgDetail, MsgId]),
                            ok;
                        RetJson ->
                            {error, RetJson}
                    end;
                {error, Reason} ->
                    {error, {send_msg_failed, Reason}}
            end
        end,
    request_with_token(Fun).

send_message(OpenId, MsgDetail) ->
    TemplateId = get_from_wechat_env(alert_tpl_id),
    send_message(OpenId, TemplateId, MsgDetail).


send_message(OpenId, TemplateId, MsgDetail) ->
    request_with_token(
        fun(Token) ->
            Url = <<"https://api.weixin.qq.com/cgi-bin/message/template/send?access_token=", Token/binary>>,
            PostData = #{
                <<"touser">> => OpenId,
                <<"template_id">> => TemplateId,
                <<"data">> =>
                #{
                    <<"first">> => #{
                        <<"value">> => maps:get(<<"title">>, MsgDetail, <<"尊敬的用户，发生了故障报警！"/utf8>>)
                    },
                    <<"keyword1">> => #{
                        <<"value">> => timestamp2localtime_str(maps:get(<<"time">>, MsgDetail, erlang:system_time(second)))
                    },
                    <<"keyword2">> => #{
                        <<"value">> => maps:get(<<"location">>, MsgDetail, <<>>)
                    },
                    <<"keyword3">> => #{
                        <<"value">> => maps:get(<<"device">>, MsgDetail, <<>>)
                    },
                    <<"keyword4">> => #{
                        <<"value">> => maps:get(<<"status">>, MsgDetail, <<>>)
                    },
                    <<"remark">> => #{
                        <<"value">> => maps:get(<<"remark">>, MsgDetail, <<"请及时检查并处理!"/utf8>>)
                    }
                }
            },
            case alinkdata_http:request(post, Url, [], jiffy:encode(PostData), [{ssl_options, [{depth, 2}]}]) of
                {ok, 200, _RespHeaders, Body} ->
                    case catch jiffy:decode(Body, [return_maps]) of
                        #{<<"errcode">> := 0, <<"msgid">> := MsgId} ->
                            logger:info("send msg to ~p ~p msgid ~p success", [OpenId, MsgDetail, MsgId]),
                            ok;
                        RetJson ->
                            {error, RetJson}
                    end;
                {error, Reason} ->
                    {error, {send_msg_failed, Reason}}
            end
        end).

request_with_token(Fun) ->
    case get_access_token() of
        {ok, Token} ->
            case Fun(Token) of
                {error, #{ <<"errcode">> := Errcode}} = Err->
                    case lists:member(Errcode, [40001, 40029]) of
                        true ->
                            case fresh_access_token() of
                                {ok, NewToken} ->
                                    Fun(NewToken);
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        false ->
                            Err
                    end;
                Other ->
                    Other
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    Opts = application:get_env(alinkdata, wechat_server, []),
    ets:new(?WECHAT_CACHE, [set, public, named_table]),
    ets:new(?QRUUID_CACHE, [set, public, named_table]),
    FlushSeconds = proplists:get_value(flush_interval, Opts, 7000),
    FlushMicroSeconds = 1000 * FlushSeconds,
    State = #state{
        flush_interval = FlushMicroSeconds
    },
    {ok, State}.

handle_call(flush_access_token, _From, State) ->
    #state{
        flush_timer = Ref,
        flush_interval = FLushInterval
    } = State,
    erlang:cancel_timer(Ref),
    AppId = get_from_wechat_env(appid),
    AppSecret = get_from_wechat_env(secret),
    case flush_access_token(AppId, AppSecret) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {ok, Token} ->
            NState = State#state{
                flush_timer = erlang:start_timer(FLushInterval, self(), flush_access_token)
            },
            {reply, {ok, Token}, NState}
    end;
handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info({timeout, _Ref, flush_access_token}, State) ->
    #state{
        flush_interval = FLushInterval
    } = State,
    AppId = get_from_wechat_env(appid),
    AppSecret = get_from_wechat_env(secret),
    flush_access_token(AppId, AppSecret),
    NState = State#state{
        flush_timer = erlang:start_timer(FLushInterval, self(), flush_access_token)
    },
    {noreply, NState};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
flush_access_token(AppId, AppSecret) ->
    case do_flush_access_token(AppId, AppSecret) of
        {ok, AccessToken} ->
            {ok, AccessToken};
        {error, Reason} ->
            logger:error("get access token failed: ~p", [Reason]),
            {error, Reason}
    end.

do_flush_access_token(AppId, AppSecret) ->
    Url = <<"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=",
            AppId/binary,
            "&secret=",
            AppSecret/binary>>,
    case alinkdata_http:request(get, Url, [], <<>>, [{ssl_options, [{depth, 2}]}]) of
        {ok, 200, _Headers, Body} ->
            case jiffy:decode(Body, [return_maps]) of
                #{<<"access_token">> := AccessToken,
                  <<"expires_in">> := ExpireIn} ->
                    ets:insert(?WECHAT_CACHE, {access_token, AccessToken}),
                    ets:insert(?WECHAT_CACHE, {access_token_expire_in, ExpireIn}),
                    {ok, AccessToken};
                _ ->
                    {error, Body}
            end;
        {ok, StatusCode, RespHeaders, Body} ->
            {error, {StatusCode, RespHeaders, Body}};
        {error, Reason} ->
            {error, Reason}
    end.



event_key_scene_id(<<"qrscene_", SceneId/binary>>) ->
    SceneId;
event_key_scene_id(SceneId) ->
    SceneId.





timestamp2localtime_str(TimeStamp) ->
    {{Y, M, D}, {H, Mi, S}} = calendar:gregorian_seconds_to_datetime(TimeStamp + 3600 *8 + calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})),
    list_to_binary(lists:flatten(io_lib:format("~w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",[Y,M,D,H,Mi,S]))).



get_from_wechat_env(ConfigKey) ->
    WechatEnv = application:get_env(alinkdata, wechat, []),
    ConfigKeyAtom = alinkutil_type:to_atom(ConfigKey),
    proplists:get_value(ConfigKeyAtom , WechatEnv, <<>>).


fresh_wechat_env() ->
    case alinkdata_dao:query_no_count('select_config', #{<<"configKey">> => <<"wechat">>}) of
        {ok, [#{<<"configValue">> := ConfigValue}]} ->
            ConfigMap =  jiffy:decode(ConfigValue, [return_maps]),
            SmsConfig = maps:fold(
                fun(K, V, Acc) ->
                    KAtom = alinkutil_type:to_atom(K),
                    [{KAtom, V}|Acc]
                end, [], ConfigMap),
            application:set_env(alinkdata, wechat, SmsConfig);
        {ok, _} ->
            ok;
        {error, Reason} ->
            logger:error("get config key sms failed :~p", [Reason])
    end.
