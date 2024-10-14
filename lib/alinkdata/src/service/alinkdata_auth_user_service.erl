%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 05. 9月 2022 下午11:19
%%%-------------------------------------------------------------------
-module(alinkdata_auth_user_service).
-author("yqfclid").

%% API
-export([
    select_allocated_list/4,
    select_unallocated_list/4,
    cancel/4,
    cancel_all/4,
    select_all/4,
    login_with_wechat/4,
    wechat_qr/4,
    bind_wechat/4,
    verify_wechat/4,
    unbind_wechat/4,
    login_with_phone/4,
    get_verify_code/4,
    login_with_mini/4,
    bind_mini_phone_number/4,
    bind_mini/4
]).

%%%===================================================================
%%% API
%%%===================================================================
select_allocated_list(OperationID, Args, #{token := Token} = Context, Req) ->
    NArgs = alinkdata_common_service:handle_data_scope(Args, Token, <<"d">>, <<"u">>),
    alinkdata_common_service:common_handle(OperationID, NArgs, Context, Req).

select_unallocated_list(OperationID, Args, #{token := Token} = Context, Req) ->
    NArgs = alinkdata_common_service:handle_data_scope(Args, Token, <<"d">>, <<"u">>),
    alinkdata_common_service:common_handle(OperationID, NArgs, Context, Req).


cancel(_OperationID, Args, _Context, Req) ->
    ok = alinkdata_dao:query_no_count(delete_user_role_info, Args),
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).


cancel_all(_OperationID, #{<<"userIds">> := UserIdsString} = Args, _Context, Req) ->
    UserIds = binary:split(UserIdsString, <<",">>, [global]),
    ok = alinkdata_dao:query_no_count(delete_user_role_infos, Args#{<<"userIds">> => UserIds}),
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(), Req).


select_all(_OperationID, #{<<"roleId">> := RoleId, <<"userIds">> := UserIdsString}, #{token := Token} = _Context, Req) ->
    UserIds = binary:split(UserIdsString, <<",">>, [global]),
    Res =
        case alinkdata_role_service:check_role_data_scope(RoleId, Token) of
            ok ->
                Insert =
                    lists:map(
                        fun(UserId) -> #{<<"roleId">> => RoleId, <<"userId">> => UserId}
                    end, UserIds),
                ok = alinkdata_dao:query_no_count(batch_user_role, #{<<"iterms">> => Insert}),
                alinkdata_ajax_result:success_result();
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).


%% todo
login_with_wechat(_OperationID, #{<<"wechatSession">> := WechatSession}, _Context, Req) ->
    Log = ehttpd_utils:get_log(Req),
    Res =
        case alinkdata_wechat:get_session(WechatSession) of
            {ok, #{<<"open_id">> := OpenId}} ->
                case alinkdata_user:query_user_by_open_id(OpenId) of
                    {ok, #{<<"user">> := #{<<"userName">> := UserName} = User} = UserInfo} ->
                        wechat_loigin_success_handle(User, OpenId, Log),
                        Ts = os:system_time(second),
                        Token = ehttpd_utils:md5(lists:concat([binary_to_list(UserName), Ts])),
                        TTL = ehttpd_server:get_env(default, expire, 1800),
                        ehttpd_auth:put_session(Token, UserInfo, TTL),
                        (alinkdata_ajax_result:success_result())#{token => Token, exists => true};
                    {error, <<"登陆用户不存在"/utf8>>} ->
                        (alinkdata_ajax_result:success_result())#{ exists => false};
                    {error, Reason} ->
                        alinkdata_ajax_result:error_result(Reason)
                end;
            {error, expired} ->
                alinkdata_ajax_result:error_result(<<"微信凭证已过期"/utf8>>);
            {error, not_found} ->
                alinkdata_ajax_result:error_result(<<"微信凭证错误"/utf8>>)
        end,
    alinkdata_common_service:response(Res, Req).

wechat_qr(_OperationID, _Args,  _Context, Req) ->
    Res = case alinkdata_wechat:get_qrcode() of
              {ok, SceneId, Ticket} ->
                  alinkdata_ajax_result:success_result(#{<<"ticket">> => Ticket, <<"sceneId">> => SceneId});
              {error, Reason} ->
                  alinkdata_ajax_result:error_result(Reason)
          end,
    alinkdata_common_service:response(Res, Req).



verify_wechat(_OperationID, #{<<"sceneId">> := SceneId}, _Context, Req) ->
    Res =
        case alinkdata_wechat:get_open_id(SceneId) of
             {ok, OpenId} ->
%%                 ok = alinkdata_dao:query_no_count(update_user, #{<<"userId">> => UserId, <<"openId">> => OpenId}),
%%                 TTL = ehttpd_server:get_env(default, expire, 1800),
%%                 ehttpd_auth:put_session(Token, UserInfo#{<<"user">> => User#{<<"openId">> => OpenId}}, TTL),
                 case alinkdata_wechat:get_user_info(OpenId) of
                     {ok, WechatInfo} ->
                         Session = alinkdata_wechat:set_session(WechatInfo),
                         alinkdata_ajax_result:success_result(#{<<"verified">> => true, <<"wechatSession">> => Session});
                     {error, Reason} ->
                         loggger:error("get wechat info ~p failed:~p", [OpenId, Reason]),
                         alinkdata_ajax_result:error_result(<<"Internal Error"/utf8>>)
                 end;
             {error, not_subscribe} ->
                 alinkdata_ajax_result:success_result(#{<<"verified">> => false});
             {error, expired} ->
                 alinkdata_ajax_result:error_result(<<"二维码已过期，请刷新重试"/utf8>>)
         end,
    alinkdata_common_service:response(Res, Req).


bind_wechat(_OperationID, #{<<"wechatSession">> := WechatSession}, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userId">> := UserId} = User} = UserInfo = ehttpd_auth:get_session(Token),
    Res =
        case alinkdata_wechat:get_session(WechatSession) of
            {ok, #{<<"open_id">> := OpenId}} ->
                case get_or_create_wechat(OpenId) of
                    {ok, WechatId} ->
                        ok = alinkdata_dao:query_no_count('update_user', #{<<"userId">> => UserId, <<"wechat">> => WechatId}),
                        TTL = ehttpd_server:get_env(default, expire, 1800),
                        ehttpd_auth:put_session(Token, UserInfo#{<<"user">> => User#{<<"wechat">> => WechatId}}, TTL),
                        alinkdata_ajax_result:success_result();
                    {error, Reason} ->
                        logger:error("get or create error:~p", [Reason]),
                        alinkdata_ajax_result:error_result(<<"Internal Error"/utf8>>)
                end;
            {error, not_found} ->
                alinkdata_ajax_result:error_result(<<"凭证已过期"/utf8>>);
            {error, expired} ->
                alinkdata_ajax_result:error_result(<<"请重新登录"/utf8>>)
        end,
    alinkdata_common_service:response(Res, Req).



unbind_wechat(_OperationID, _Args, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userId">> := UserId} = User} = UserInfo = ehttpd_auth:get_session(Token),
    Res =
        case alinkdata_dao:query_no_count(select_user_by_id, #{<<"userId">> => UserId}) of
            {ok, [#{<<"wechat">> := _WechatId}]} ->
                ok = alinkdata_dao:query_no_count(update_user, #{<<"userId">> => UserId, <<"wechat">> => 0}),
                TTL = ehttpd_server:get_env(default, expire, 1800),
                ehttpd_auth:put_session(Token, UserInfo#{<<"user">> => User#{<<"wechat">> => 0}}, TTL),
                alinkdata_ajax_result:success_result();
            {ok, []} ->
                alinkdata_ajax_result:error_result(<<"用户未绑定微信"/utf8>>);
            {ok, _} ->
                alinkdata_ajax_result:error_result(<<"微信账号未输对"/utf8>>);
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).


get_verify_code(_OperationID, #{<<"phone">> := Phone}, _Context, Req) ->
    Key = <<"verify_code#", Phone/binary>>,
    Res =
        case ehttpd_cache:get_with_ttl(Key) of
            PhoneCode when PhoneCode =/= expired
                    andalso PhoneCode =/= undefined->
                alinkdata_ajax_result:error_result(<<"获取验证码太过频繁，请稍后重试"/utf8>>);
            _ ->
                VerifyCode = generate_verify_code(6),
                ExpiredInterval = 3 * 60,
                ehttpd_cache:set_with_ttl(Key, VerifyCode, ExpiredInterval),
                alinkdata_sms:send_verify_code(Phone, VerifyCode, ExpiredInterval),
                alinkdata_ajax_result:success_result(#{<<"expire">> => ExpiredInterval})
        end,
    alinkdata_common_service:response(Res, Req).


login_with_phone(_OperationID, #{<<"verifyCode">> := VerifyCode, <<"phone">> := Phone}, _Context, Req) ->
    Log = ehttpd_utils:get_log(Req),
    Key = <<"verify_code#", Phone/binary>>,
    Res =
        case ehttpd_cache:get_with_ttl(Key) of
            VerifyCode ->
                ehttpd_cache:delete(Key),
                case alinkdata_user:query_user_by_phone_number(Phone) of
                    {ok, #{<<"user">> := #{<<"userName">> := UserName} = User} = UserInfo} ->
                        phone_login_success_handle(User, Log),
                        Ts = os:system_time(second),
                        Token = ehttpd_utils:md5(lists:concat([binary_to_list(UserName), Ts])),
                        TTL = ehttpd_server:get_env(default, expire, 1800),
                        ehttpd_auth:put_session(Token, UserInfo, TTL),
                        (alinkdata_ajax_result:success_result())#{token => Token, exists => true};
                    {error, <<"登陆用户不存在"/utf8>>} ->
                        (alinkdata_ajax_result:success_result())#{ exists => false};
                    {error, Reason} ->
                        alinkdata_ajax_result:error_result(Reason)
                end;
            undefined ->
                alinkdata_ajax_result:error_result(<<"验证码已失效"/utf8>>);
            expired ->
                alinkdata_ajax_result:error_result(<<"验证码已失效"/utf8>>);
            _ ->
                alinkdata_ajax_result:error_result(<<"验证码错误，请重试"/utf8>>)
        end,
    alinkdata_common_service:response(Res, Req).



login_with_mini(_OperationID, #{<<"code">> := Code}, _Context, Req) ->
    Res =
        case alinkdata_wechat_mini:request_mini_program_login(Code) of
            {ok, #{<<"openId">> := OpenId, <<"sessionKey">> := SessionKey} = R} ->
                case alinkdata_dao:query_no_count('QUERY_wechat_mini', #{<<"openId">> => OpenId}) of
                    {ok, [#{<<"id">> := MiniId} = MiniInfo]}  ->
                        case alinkdata_user:query_user_by_mini(MiniId) of
                            {ok, #{<<"user">> := #{<<"userName">> := UserName}} = UserInfo} ->
                                Ts = os:system_time(second),
                                Token = ehttpd_utils:md5(lists:concat([binary_to_list(UserName), Ts])),
                                TTL = ehttpd_server:get_env(default, expire, 1800),
                                ehttpd_auth:put_session(Token, UserInfo, TTL),
                                alinkdata_wechat:set_mini_session(SessionKey, MiniInfo),
                                (alinkdata_ajax_result:success_result())#{exists => true, token => Token, <<"sessionKey">> => SessionKey};
                            {error, <<"登陆用户不存在"/utf8>>} ->
                                alinkdata_wechat:set_mini_session(SessionKey, MiniInfo),
                                (alinkdata_ajax_result:success_result())#{ <<"sessionKey">> => SessionKey ,exists => false};
                            {error, Reason} ->
                                alinkdata_ajax_result:error_result(Reason)
                        end;
                    {ok, []} ->
                        InsertArgs = #{<<"openId">> => OpenId, <<"unionId">> => maps:get(<<"unionId">>, R, <<>>)},
                        ok = alinkdata_dao:query_no_count('POST_wechat_mini', InsertArgs),
                        {ok, [MiniInfo]} = alinkdata_dao:query_no_count('QUERY_wechat_mini', #{<<"openId">> => OpenId}),
                        alinkdata_wechat:set_mini_session(SessionKey, MiniInfo),
                        (alinkdata_ajax_result:success_result())#{ <<"sessionKey">> => SessionKey ,exists => false};
                    {error, Reason} ->
                        alinkdata_ajax_result:error_result(Reason)
                end;
            {error, Reason} ->
                logger:error("login with wechat mini failed:~p", [Reason]),
                alinkdata_ajax_result:error_result(<<"Internal Error">>)
        end,
    alinkdata_common_service:response(Res, Req).


bind_mini_phone_number(_OperationID, #{<<"code">> := Code, <<"wechatMiniId">> := MiniId}, _Context, Req) ->
    Res =
        case alinkdata_wechat_mini:request_mini_program_phone(Code) of
            {ok, PhoneNumber} ->
                InsertArgs = #{<<"id">> => MiniId, <<"phone">> => PhoneNumber},
                ok = alinkdata_dao:query_no_count('POST_wechat_mini', InsertArgs),
                case alinkdata_user:query_user_by_phone_number(PhoneNumber) of
                    {ok, #{<<"user">> := #{<<"userName">> := UserName}} = UserInfo} ->
                        Ts = os:system_time(second),
                        Token = ehttpd_utils:md5(lists:concat([binary_to_list(UserName), Ts])),
                        TTL = ehttpd_server:get_env(default, expire, 1800),
                        ehttpd_auth:put_session(Token, UserInfo, TTL),
                        (alinkdata_ajax_result:success_result())#{token => Token, login => success};
                    {error, <<"登陆用户不存在"/utf8>>} ->
                        (alinkdata_ajax_result:success_result())#{ login => register, <<"phone">> => PhoneNumber};
                    {error, Reason} ->
                        alinkdata_ajax_result:error_result(Reason)
                end;
            {error, Reason} ->
                logger:error("login with wechat mini phone failed:~p", [Reason]),
                alinkdata_ajax_result:error_result(<<"Internal Error">>)
        end,
    alinkdata_common_service:response(Res, Req).


bind_mini(_OperationID, #{<<"sessionKey">> := SessionKey}, #{token := Token} = _Context, Req) ->
    #{<<"user">> := #{<<"userId">> := UserId} = User} = UserInfo = ehttpd_auth:get_session(Token),
    Res =
        case alinkdata_wechat:get_mini_session(SessionKey) of
            {ok, #{<<"id">> := MiniId}} ->
                ok = alinkdata_dao:query_no_count('update_user', #{<<"userId">> => UserId, <<"mini">> => MiniId}),
                TTL = ehttpd_server:get_env(default, expire, 1800),
                ehttpd_auth:put_session(Token, UserInfo#{<<"user">> => User#{<<"mini">> => MiniId}}, TTL),
                alinkdata_ajax_result:success_result();
            {error, not_found} ->
                alinkdata_ajax_result:error_result(<<"凭证已过期"/utf8>>);
            {error, expired} ->
                alinkdata_ajax_result:error_result(<<"请重新登录"/utf8>>)
        end,
    alinkdata_common_service:response(Res, Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================
check_is_bind_wechat(UserId) ->
    {ok, [#{<<"openId">> := OpenId}]} = alinkdata_dao:query_no_count(select_user_by_id, #{<<"userId">> => UserId}),
    case OpenId of
        <<"">> ->
            false;
        _ ->
            true
    end.

get_or_create_wechat(OpenId) ->
    case alinkdata_dao:query_no_count('QUERY_wechat', #{<<"openId">> => OpenId}) of
        {ok, [#{<<"id">> := WechatId}]} ->
            {ok, WechatId};
        {ok, []} ->
            case alinkdata_dao:query_no_count('POST_wechat', #{<<"openId">> => OpenId}) of
                ok ->
                    case alinkdata_dao:query_no_count('QUERY_wechat', #{<<"openId">> => OpenId}) of
                        {ok, [#{<<"id">> := WechatId}]} ->
                            {ok, WechatId};
                        Err ->
                            {error, Err}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


wechat_loigin_success_handle(User, OpenId, Log) ->
    #{<<"userName">> := UserName, <<"userId">> := UserId} = User,
    Fun =
        fun() ->
            IpAddr =
                case binary:split(maps:get(<<"Peer">>, Log, <<":">>), <<":">>, [global]) of
                    [Ip|_] ->
                        Ip;
                    _ ->
                        <<>>
                end,
            WechatNoticeData =
                #{
                    <<"title">> => <<"您好，您的帐号"/utf8, UserName/binary, "被登录"/utf8>>,
                    <<"time">> => erlang:system_time(second),
                    <<"ip">> => IpAddr
                },
            alinkdata_wechat:login_notice(OpenId, WechatNoticeData),
            case alinkdata_common:is_admin(UserId) of
                true ->
                    ok;
                _ ->
                    LoginNoticeData =
                        #{
                            <<"title">> => <<"您好，有用户使用微信进行了登录，用户名为"/utf8, UserName/binary>>,
                            <<"time">> => erlang:system_time(second),
                            <<"ip">> => IpAddr,
                            <<"reason">> => <<"如果本次登录时异常的，请及时到平台进行处理"/utf8>>
                        },
                    case alinkdata_user:get_admin_open_id() of
                        {ok, AdminOpenId} ->
                            alinkdata_wechat:login_notice(AdminOpenId, LoginNoticeData);
                        {error, Reason} ->
                            logger:error("find admin wechat openid failed ~p", [Reason])
                    end
            end
        end,
    alinkdata_async_worker:do(Fun),
    alinkdata_user:record_login_info(true, UserName, Log, <<"登录成功"/utf8>>).



phone_login_success_handle(User, Log) ->
    #{<<"userName">> := UserName, <<"userId">> := UserId} = User,
    Fun =
        fun() ->
            IpAddr =
                case binary:split(maps:get(<<"Peer">>, Log, <<":">>), <<":">>, [global]) of
                    [Ip|_] ->
                        Ip;
                    _ ->
                        <<>>
                end,
            case alinkdata_common:is_admin(UserId) of
                true ->
                    ok;
                _ ->
                    LoginNoticeData =
                        #{
                            <<"title">> => <<"您好，有用户使用手机号进行了登录，用户名为"/utf8, UserName/binary>>,
                            <<"time">> => erlang:system_time(second),
                            <<"ip">> => IpAddr,
                            <<"reason">> => <<"如果本次登录时异常的，请及时到平台进行处理"/utf8>>
                        },
                    case alinkdata_user:get_admin_open_id() of
                        {ok, AdminOpenId} ->
                            alinkdata_wechat:login_notice(AdminOpenId, LoginNoticeData);
                        {error, Reason} ->
                            logger:error("find admin wechat openid failed ~p", [Reason])
                    end
            end
        end,
    alinkdata_async_worker:do(Fun),
    alinkdata_user:record_login_info(true, UserName, Log, <<"登录成功"/utf8>>).



generate_verify_code(Num) ->
    lists:foldl(
        fun(_, Acc) ->
            IntB = alinkutil_type:to_binary(rand:uniform(10) - 1),
            <<Acc/binary, IntB/binary>>
    end, <<>>, lists:seq(1, Num)).
