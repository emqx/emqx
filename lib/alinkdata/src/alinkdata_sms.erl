%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 28. 4月 2023 下午8:07
%%%-------------------------------------------------------------------
-module(alinkdata_sms).

%% API
-export([
    send_verify_code/3,
    send_alert_sms/2,
    send_sms/3,
    fresh_sms_env/0
]).

%%%===================================================================
%%% API
%%%===================================================================
send_verify_code(PhoneNumber, VerifyCode, ExpireInterval) ->
    Interval = alinkutil_type:to_binary(round(ExpireInterval / 60)),
    Params =
        [
            VerifyCode,
            Interval
        ],
    TplId = get_from_sms_env(verify_tpl_id),
    case send_sms(PhoneNumber, TplId, Params) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            logger:error("send sms to phone ~p with param ~p failed:~p", [PhoneNumber, Params, Reason])
    end.

send_alert_sms(PhoneNumber, AlertInfo) ->
    Params =
        [
            maps:get(<<"device">>, AlertInfo, <<>>),
            maps:get(<<"alert_level">>, AlertInfo, <<>>),
            maps:get(<<"status">>, AlertInfo, <<>>),
            maps:get(<<"location">>, AlertInfo, <<>>)
        ],
    TplId = get_from_sms_env(alert_tpl_id),
    case send_sms(PhoneNumber, TplId, Params) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            logger:error("send sms to phone ~p with param ~p failed:~p", [PhoneNumber, Params, Reason])
    end.


send_sms(Mobile, TplId, Params) ->
    send_sms(<<"+86">>, Mobile, TplId, Params).
send_sms(NationCode, Mobile, TplId, Params) ->
    send_sms(NationCode, Mobile, TplId, Params, <<>>).
send_sms(NationCode, Mobile, TplId, Params, Ext) ->
    RandomB = integer_to_binary(1000 + rand:uniform(1000)),
    AppId = get_from_sms_env(appid),
    AppKey = get_from_sms_env(appkey),
    Sign = get_from_sms_env(sign),
    case re:run(NationCode, <<"\\+(\\d{1,3})">>, [{capture, all, binary}]) of
        {match, [_, NationCode1]} ->
            Now = erlang:system_time(second),
            NowB = alinkutil_type:to_binary(Now),
            SigStr =
                <<"appkey=", AppKey/binary, "&random=", RandomB/binary,
                    "&time=", NowB/binary, "&mobile=", Mobile/binary>>,
            Sig = string:to_lower(binary_to_list(<<<<Y>> || <<X:4>> <= crypto:hash(sha256, SigStr), Y <- integer_to_list(X, 16)>>)),
            SigB = alinkutil_type:to_binary(Sig),
            Data = #{
                <<"tpl_id">> => TplId,
                <<"ext">> => Ext,
                <<"extend">> => <<>>,
                <<"params">> => Params,
                <<"sign">> => Sign,
                <<"tel">> => #{
                    <<"mobile">> => alinkutil_type:to_binary(Mobile),
                    <<"nationcode">> => NationCode1
                },
                <<"time">> => Now,
                <<"sig">> => SigB
            },
            Url = <<"https://yun.tim.qq.com/v5/tlssmssvr/sendsms?sdkappid=", AppId/binary, "&random=", RandomB/binary>>,
            Headers = [{<<"Content-Type">>, <<"application/json">>}],
            case catch alinkdata_http:request(post, Url, Headers, jiffy:encode(Data), [{ssl_options, [{depth, 2}]}]) of
                {ok, 200, _RespHeader, ResBody} ->
                    case jiffy:decode(ResBody, [return_maps]) of
                        #{<<"result">> := 0, <<"ext">> := Ext} ->
                            {ok, Ext};
                        #{<<"errmsg">> := ErrMsg, <<"result">> := Code} ->
                            {error, #{code => Code, error => ErrMsg}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            {error, #{code => 1, error => <<"NationCode is illegality">>}}
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================
get_from_sms_env(ConfigKey) ->
    SmsEnv = application:get_env(alinkdata, sms, []),
    ConfigKeyAtom = alinkutil_type:to_atom(ConfigKey),
    proplists:get_value(ConfigKeyAtom , SmsEnv, <<>>).


fresh_sms_env() ->
    case alinkdata_dao:query_no_count('select_config', #{<<"configKey">> => <<"sms">>}) of
        {ok, [#{<<"configValue">> := ConfigValue}]} ->
            ConfigMap =  jiffy:decode(ConfigValue, [return_maps]),
            SmsConfig = maps:fold(
                fun(K, V, Acc) ->
                    KAtom = alinkutil_type:to_atom(K),
                    [{KAtom, V}|Acc]
            end, [], ConfigMap),
            application:set_env(alinkdata, sms, SmsConfig);
        {ok, _} ->
            ok;
        {error, Reason} ->
            logger:error("get config key sms failed :~p", [Reason])
    end.