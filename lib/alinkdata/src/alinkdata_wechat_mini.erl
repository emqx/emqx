%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 05. 7月 2023 下午3:08
%%%-------------------------------------------------------------------
-module(alinkdata_wechat_mini).

%% API
-export([
    fresh_wechat_mini_env/0,
    request_mini_program_login/1,
    request_mini_program_phone/1
]).

%%%===================================================================
%%% API
%%%===================================================================

fresh_wechat_mini_env() ->
    case alinkdata_dao:query_no_count('select_config', #{<<"configKey">> => <<"wechat_mini">>}) of
        {ok, [#{<<"configValue">> := ConfigValue}]} ->
            ConfigMap =  jiffy:decode(ConfigValue, [return_maps]),
            SmsConfig = maps:fold(
                fun(K, V, Acc) ->
                    KAtom = alinkutil_type:to_atom(K),
                    [{KAtom, V}|Acc]
                end, [], ConfigMap),
            application:set_env(alinkdata, wechat_mini, SmsConfig);
        {ok, _} ->
            ok;
        {error, Reason} ->
            logger:error("get config key sms failed :~p", [Reason])
    end.



request_mini_program_phone(Code) ->
    F =
        fun(Token) ->
            Url = <<"https://api.weixin.qq.com/wxa/business/getuserphonenumber?access_token=", Token/binary>>,
            case alinkdata_http:request(post, Url, [], jiffy:encode(#{<<"code">> => Code}), [{ssl_options, [{depth, 2}]}]) of
                {ok, 200, _Headers, Body} ->
                    case jiffy:decode(Body, [return_maps]) of
                        #{
                            <<"errcode">> := 0,
                            <<"phone_info">> := #{<<"phoneNumber">> := Phone}
                        } ->
                            {ok, Phone};
                        #{<<"errcode">> := _ErrCode} = Err->
                            {error, Err};
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


request_mini_program_login(Code) ->
    AppId = get_from_wechat_mini_env(appid),
    AppSecret = get_from_wechat_mini_env(app_secret),
    Url = <<"https://api.weixin.qq.com/sns/jscode2session?grant_type=authorization_code&js_code=",
        Code/binary, "&appid=", AppId/binary, "&secret=", AppSecret/binary>>,
    case alinkdata_http:request(get, Url, [], <<>>, [{ssl_options, [{depth, 2}]}]) of
        {ok, 200, _Headers, Body} ->
            case jiffy:decode(Body, [return_maps]) of
                #{
                    <<"openid">> := OpenId,
                    <<"session_key">> := SessionKey
                } ->
                    Data =
                        #{
                            <<"openId">> => OpenId,
                            <<"sessionKey">> => SessionKey
                        },
                    {ok, Data};
                #{<<"errcode">> := ErrCode} ->
                    {error, {code, ErrCode}};
                Err ->
                    {error, Err}
            end;
        {ok, StatusCode, RespHeaders, Body} ->
            {error, {StatusCode, RespHeaders, Body}};
        {error, Reason} ->
            {error, Reason}
    end.


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


get_access_token() ->
    case ets:lookup(alinkdata_wechat, mini_access_token) of
        [{_, Token}] ->
            {ok, Token};
        [] ->
            fresh_access_token();
        {error, Reason} ->
            {error, Reason}
    end.


fresh_access_token() ->
    AppId = get_from_wechat_mini_env(appid),
    AppSecret = get_from_wechat_mini_env(app_secret),
    Url = <<"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=",
        AppId/binary,
        "&secret=",
        AppSecret/binary>>,
    case alinkdata_http:request(get, Url, [], <<>>, [{ssl_options, [{depth, 2}]}]) of
        {ok, 200, _Headers, Body} ->
            case jiffy:decode(Body, [return_maps]) of
                #{<<"access_token">> := AccessToken,
                    <<"expires_in">> := ExpireIn} ->
                    ets:insert(alinkdata_wechat, {mini_access_token, AccessToken}),
                    ets:insert(alinkdata_wechat, {mini_access_token_expire_in, ExpireIn}),
                    {ok, AccessToken};
                _ ->
                    {error, Body}
            end;
        {ok, StatusCode, RespHeaders, Body} ->
            {error, {StatusCode, RespHeaders, Body}};
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_from_wechat_mini_env(ConfigKey) ->
    WechatMiniEnv = application:get_env(alinkdata, wechat_mini, []),
    ConfigKeyAtom = alinkutil_type:to_atom(ConfigKey),
    proplists:get_value(ConfigKeyAtom , WechatMiniEnv, <<>>).