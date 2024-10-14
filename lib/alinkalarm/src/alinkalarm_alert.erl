%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 11. 3月 2023 下午9:59
%%%-------------------------------------------------------------------
-module(alinkalarm_alert).
-author("yqfclid").

%% API
-export([
    alert/4,
    recover/3,
    send_alert/2
]).

%%%===================================================================
%%% API
%%%===================================================================
alert(#{<<"groupId">> := GroupId, <<"status">> := <<"1">>} = Stat, WrongInfos, RuleInfo, NotifyTypeB) when is_integer(GroupId) ->
    #{<<"device_stat">> := #{ <<"deviceAddr">> := Addr}} = Stat,
    AlertInfo = build_alert_info(Stat, WrongInfos, RuleInfo),
    record_alarm(Stat, RuleInfo, AlertInfo),
    NotifyTypes = (get_notify_types(NotifyTypeB)) ++ [<<"online">>],
%%    run_hook(RuleInfo),
    lists:foreach(
        fun(<<"wechat">>) ->
            lists:foreach(
                fun(OpenId) ->
                    send_alert(OpenId, AlertInfo)
                end, get_alert_user_openids(GroupId));
            (<<"sms">>) ->
                lists:foreach(
                    fun(PhoneNumber) ->
                        alinkdata_sms:send_alert_sms(PhoneNumber, AlertInfo)
                    end, get_alert_user_phone_numbers(GroupId));
            (<<"app">>) ->
                alinkalarm_device_alarm:alert(Addr, Stat, RuleInfo, WrongInfos);
            (<<"online">>) ->
                lists:foreach(
                    fun(Username) ->
                        online_alert(Username, AlertInfo)
                    end, get_alert_username(GroupId));
            (_) ->
                ok
        end, lists:usort(NotifyTypes));
alert(Stat, WrongInfos, RuleInfo, NotifyTypeB) ->
    #{<<"device_stat">> := #{ <<"deviceAddr">> := Addr}} = Stat,
    AlertInfo = build_alert_info(Stat, WrongInfos, RuleInfo),
    record_alarm(Stat, RuleInfo, AlertInfo),
    NotifyTypes = (get_notify_types(NotifyTypeB)),
%%    run_hook(RuleInfo),
    lists:foreach(
        fun(<<"app">>) ->
                alinkalarm_device_alarm:alert(Addr, Stat, RuleInfo, WrongInfos);
            (_) ->
                ok
        end, lists:usort(NotifyTypes)),
    ok.


recover(#{<<"groupId">> := GroupId} = Stat, RuleInfo, NotifyTypeB) ->
    RecoverInfo = build_recover_info(Stat, RuleInfo),
    NotifyTypes = (get_notify_types(NotifyTypeB)) ++ [<<"online">>],
    #{<<"device_stat">> := #{ <<"deviceAddr">> := Addr}} = Stat,
    lists:foreach(
        fun(<<"wechat">>) ->
            lists:foreach(
                fun(OpenId) ->
                    send_alert(OpenId, RecoverInfo)
                end, get_alert_user_openids(GroupId));
            (<<"online">>) ->
                lists:foreach(
                    fun(Username) ->
                        online_alert(Username, RecoverInfo)
                    end, get_alert_username(GroupId));
            (<<"app">>) ->
                alinkalarm_device_alarm:recover(Addr, Stat, RuleInfo);
           (<<"sms">>) ->
            ok;
           (_) ->
            ok
    end, lists:usort(NotifyTypes)).



send_alert(OpenId, AlertInfo) ->
    case alinkdata_wechat:send_message(OpenId, AlertInfo) of
        ok ->
            ok;
        {error, Reason} ->
            logger:error("send alert to ~p failed:~p", [OpenId, Reason])
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================
get_alert_user_openids(GroupId) ->
    case alinkdata_dao:query_no_count('QUERY_group', #{<<"id">> => GroupId}) of
        {ok, [#{<<"users">> := Users}]} when is_binary(Users) andalso Users =/= <<>> ->
            lists:foldl(
                fun(#{<<"userId">> := UserId}, Acc) ->
                    case alinkdata_dao:query_user_by_user_id(#{<<"userId">> => UserId}) of
                        {ok, [#{<<"wechat">> := WechatId}]} when WechatId > 0->
                            case alinkdata_dao:query_no_count('QUERY_wechat', #{<<"id">> => WechatId}) of
                                {ok, [#{<<"openId">> := OpenId}]} ->
                                    [OpenId|Acc];
                                {ok, _} ->
                                    Acc;
                                {error, Reason} ->
                                    logger:error("get wechat ~p openid error ~p", [WechatId, Reason]),
                                    Acc
                            end;
                        {ok, _} ->
                            Acc;
                        Ret ->
                            logger:error("get user ~p wechat error ~p", [UserId, Ret]),
                            Acc
                    end
            end, [], jiffy:decode(Users, [return_maps]));
        Ret ->
            logger:error("get group ~p user error ~p", [GroupId, Ret]),
            []
    end.

get_alert_user_phone_numbers(GroupId) ->
    case alinkdata_dao:query_no_count('QUERY_group', #{<<"id">> => GroupId}) of
        {ok, [#{<<"users">> := Users}]} when is_binary(Users) andalso Users =/= <<>> ->
            lists:foldl(
                fun(#{<<"userId">> := UserId}, Acc) ->
                    case alinkdata_dao:query_user_by_user_id(#{<<"userId">> => UserId}) of
                        {ok, [#{<<"phonenumber">> := Phone}]} when Phone =/= <<>>
                                                            andalso Phone =/= null->
                            [Phone|Acc];
                        {ok, _} ->
                            Acc;
                        Ret ->
                            logger:error("get user ~p wechat error ~p", [UserId, Ret]),
                            Acc
                    end
                end, [], jiffy:decode(Users, [return_maps]));
        Ret ->
            logger:error("get group ~p user error ~p", [GroupId, Ret]),
            []
    end.


get_alert_username(GroupId) ->
    case alinkdata_dao:query_no_count('QUERY_group', #{<<"id">> => GroupId}) of
        {ok, [#{<<"users">> := Users}]} when is_binary(Users) andalso Users =/= <<>> ->
            lists:foldl(
                fun(#{<<"userId">> := UserId}, Acc) ->
                    case alinkdata_dao:query_user_by_user_id(#{<<"userId">> => UserId}) of
                        {ok, [#{<<"userName">> := Username}]} when Username =/= <<>>
                            andalso Username =/= null->
                            [Username|Acc];
                        {ok, _} ->
                            Acc;
                        Ret ->
                            logger:error("get user ~p wechat error ~p", [UserId, Ret]),
                            Acc
                    end
                end, [], jiffy:decode(Users, [return_maps]));
        Ret ->
            logger:error("get group ~p user error ~p", [GroupId, Ret]),
            []
    end.

build_alert_info(Stat, WrongInfos, RuleInfo) ->
    #{
        <<"device_info">> := DeviceInfo,
        <<"product_info">> := ProductInfo,
        <<"device_stat">> := #{<<"stat">> := DeviceStat, <<"deviceAddr">> := Addr}
    } = Stat,
    Status =
        lists:foldl(
            fun(WrongInfo, Acc) ->
                WrongStatus = build_stat(Stat, WrongInfo),
                case Acc of
                    <<>> ->
                        <<"当前设备状态为"/utf8, WrongStatus/binary>>;
                    _ ->
                        <<Acc/binary, ", "/utf8, WrongStatus/binary>>
                end
        end, <<>>, WrongInfos),
    AlertType =
        case maps:get(<<"type">>, RuleInfo, <<"1">>) of
            <<"1">> ->
                <<"告警"/utf8>>;
            <<"2">> ->
                <<"事件"/utf8>>
        end,
    AlertName = maps:get(<<"name">>, RuleInfo, <<>>),
    AlertLevel =
        case maps:get(<<"level">>, RuleInfo, <<"4">>) of
            <<"1">> ->
                <<"一级告警"/utf8>>;
            <<"2">> ->
                <<"二级告警"/utf8>>;
            <<"3">> ->
                <<"三级告警"/utf8>>;
            <<"4">> ->
                <<"四级告警"/utf8>>;
            _ ->
                <<"普通事件"/utf8>>
        end,
    ProductName = maps:get(<<"name">>, ProductInfo, <<>>),
    Content =
        case maps:get(<<"content">>, RuleInfo, <<>>) of
            <<>> ->
                <<"请及时检查并处理!"/utf8>>;
            C ->
                C
        end,
    Remark =
        <<
            "设备编号："/utf8, Addr/binary, "\n",
            "触发告警规则："/utf8, AlertName/binary, "\n",
            "所属产品："/utf8, ProductName/binary, "\n",
            "告警类型："/utf8, AlertType/binary, "\n",
            "告警级别："/utf8, AlertLevel/binary, "\n",
            Content/binary
        >>,
    #{
        <<"time">> => maps:get(<<"ts">>, DeviceStat, erlang:system_time(second)),
        <<"device">> => maps:get(<<"name">>, DeviceInfo, <<>>),
        <<"status">> => Status,
        <<"location">> => maps:get(<<"location">>, DeviceInfo, <<>>),
        <<"remark">> => Remark,
        <<"alert_level">> => AlertLevel
    }.


build_stat(_Stat, WrongInfo) ->
    #{
        <<"thing_config">> := #{<<"title">> := Title} = NameThing,
        <<"stat">> := #{<<"value">> := V}
    } = WrongInfo,
    case V of
        {<<"alink_ignore_sync_data_timeout">>, FailCount} ->
            FailCountB = alinkutil_type:to_binary(FailCount),
            <<"连续抄数据失败"/utf8, FailCountB/binary, "次"/utf8>>;
        _ ->
            Unit = maps:get(<<"unit">>, NameThing, <<>>),
            VB = format_stat_value(NameThing, V),
            <<Title/binary, VB/binary, Unit/binary>>
    end.

build_recover_info(Stat0, RuleInfo) ->
    #{
        <<"device_info">> := DeviceInfo,
        <<"product_info">> := ProductInfo,
        <<"device_stat">> := #{<<"stat">> := DeviceStat, <<"deviceAddr">> := Addr}
    } = Stat0,
    AlertName = maps:get(<<"name">>, RuleInfo, <<>>),
    ProductName = maps:get(<<"name">>, ProductInfo, <<>>),
    Thing = maps:get(<<"thing">>, ProductInfo, #{}),
    Status =
        maps:fold(
            fun(Name, #{<<"title">> := Title} = NameThing, Acc) ->
                case maps:get(Name, DeviceStat, undefined) of
                    #{<<"value">> := {<<"alink_ignore_sync_data_timeout">>, FailCount}} ->
                        FailCountB = alinkutil_type:to_binary(FailCount),
                        case Acc of
                            <<>> ->
                                <<"当前设备状态为"/utf8, "连续抄数据失败"/utf8, FailCountB/binary, "次"/utf8>>;
                            _ ->
                                <<Acc/binary, ", 连续抄数据失败"/utf8, FailCountB/binary, "次"/utf8>>
                        end;
                    #{<<"value">> := <<"alink_ignore_", _/binary>>} ->
                        Acc;
                    #{<<"value">> := NameV} ->
                        NameVB = format_stat_value(NameThing, NameV),
                        Unit = maps:get(<<"unit">>, NameThing, <<>>),
                        SubStatB = <<Title/binary, NameVB/binary, Unit/binary>>,
                        case Acc of
                            <<>> ->
                                <<"当前设备状态为"/utf8, SubStatB/binary>>;
                            _ ->
                                <<Acc/binary, ", "/utf8, SubStatB/binary>>
                        end;
                    _ ->
                        Acc
                end
            end, <<>>, Thing),
    Content =
        case maps:get(<<"content">>, RuleInfo, <<>>) of
            <<>> ->
                <<"设备已恢复正常"/utf8>>;
            C ->
                C
        end,
    Remark =
        <<
            "设备编号："/utf8, Addr/binary, "\n",
            "触发告警规则："/utf8, AlertName/binary, "\n",
            "所属产品："/utf8, ProductName/binary, "\n",
            Content/binary
        >>,
    #{
        <<"title">> => <<"尊敬的用户，设备已恢复正常"/utf8>>,
        <<"time">> => maps:get(<<"ts">>, DeviceStat, erlang:system_time(second)),
        <<"device">> => maps:get(<<"name">>, DeviceInfo, <<>>),
        <<"status">> => <<Status/binary, "，已恢复"/utf8>>,
        <<"location">> => maps:get(<<"location">>, DeviceInfo, <<>>),
        <<"remark">> => Remark
    }.


record_alarm(Stat, RuleInfo, AlertInfo) ->
    case catch do_record_alarm(Stat, RuleInfo, AlertInfo) of
        ok ->
            ok;
        Err ->
            logger:error("record alarm ~p ~p failed:~p", [Stat, RuleInfo, Err])
    end.
do_record_alarm(Stat, RuleInfo, AlertInfo) ->
    Content = jiffy:encode(AlertInfo),
    #{
        <<"device_stat">> := #{<<"stat">> := DeviceStat, <<"deviceAddr">> := Addr},
        <<"device_info">> := DeviceInfo
    } = Stat,
    Ts = maps:get(<<"ts">>, DeviceStat, erlang:system_time(second)),
    Args = #{
        <<"title">> => maps:get(<<"name">>, RuleInfo, <<>>),
        <<"rule">> => maps:get(<<"id">>, RuleInfo, 0),
        <<"level">> => maps:get(<<"level">>, RuleInfo, <<"4">>),
        <<"type">> => maps:get(<<"type">>, RuleInfo, <<"1">>),
        <<"addr">> => Addr,
        <<"createTime">> => timestamp2localtime_str(Ts),
        <<"content">> => Content,
        <<"confirm">> => 0,
        <<"owner">> => maps:get(<<"owner">>, DeviceInfo, 1),
        <<"ownerDept">> => maps:get(<<"ownerDept">>, DeviceInfo, 0)
    },
    case alinkdata_dao:query_no_count('POST_alarm', Args) of
        ok ->
            ok;
        {error, Reason} ->
            logger:error("record alarm ~p error:~p", [Args, Reason])
    end.


format_stat_value(NameThing, V) when is_float(V) ->
    Precision = maps:get(<<"precision">>, NameThing, 2),
    float_to_binary(V, [{decimals, Precision}]);
format_stat_value(_, V)  ->
    alinkutil_type:to_binary(V).


timestamp2localtime_str(TimeStamp) ->
    {{Y, M, D}, {H, Mi, S}} = calendar:gregorian_seconds_to_datetime(TimeStamp + 3600 *8 + calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})),
    list_to_binary(lists:flatten(io_lib:format("~w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",[Y,M,D,H,Mi,S]))).



get_notify_types(null) ->
    [];
get_notify_types(<<>>) ->
    [];
get_notify_types(NotifyTypeB) ->
    case jiffy:decode(NotifyTypeB) of
        NotifyTypeList when is_list(NotifyTypeList) ->
            NotifyTypeList;
        _ ->
            logger:error("wrong notify type ~p", [NotifyTypeB]),
            throw({wrong_notify_type, NotifyTypeB})
    end.



online_alert(Username, AlertInfo) ->
    Topic = <<"p/system/", Username/binary>>,
    AlertInfoBin = jiffy:encode(AlertInfo),
    emqx:publish(emqx_message:make(Topic, AlertInfoBin)).


run_hook(RuleInfo) ->
    alinkdata_hooks:run('alinkiot.metrics', [#{event => <<"alarm_trig">>, type => maps:get(<<"level">>, RuleInfo, <<"4">>)}]).
