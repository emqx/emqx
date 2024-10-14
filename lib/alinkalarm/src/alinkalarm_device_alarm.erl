%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 11. 12月 2023 下午6:03
%%%-------------------------------------------------------------------
-module(alinkalarm_device_alarm).
-author("yqfclid").

%% API
-export([
    alert/4,
    recover/3
]).

%%%===================================================================
%%% API
%%%===================================================================
alert(Addr, Stat, RuleInfo, WrongInfos) ->
    #{<<"device_stat">> := #{<<"stat">> := DeviceStat}} = Stat,
    Data = #{
        <<"addr">> => Addr,
        <<"time">> => erlang:system_time(second),
        <<"data">> => #{
            <<"stat">> => DeviceStat,
            <<"rule">> => RuleInfo,
            <<"wrongInfos">> => WrongInfos,
            <<"type">> => <<"alarm">>
        }
    },
    send_alarm(Addr, Data).


recover(Addr, Stat, RuleInfo) ->
    #{<<"device_stat">> := #{<<"stat">> := DeviceStat}} = Stat,
    Data = #{
        <<"addr">> => Addr,
        <<"time">> => erlang:system_time(second),
        <<"data">> => #{
            <<"stat">> => DeviceStat,
            <<"rule">> => RuleInfo,
            <<"type">> => <<"recover">>
        }
    },
    send_alarm(Addr, Data).


send_alarm(Addr, Data) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, #{<<"project">> := Project, <<"id">> := DeviceId}} when Project =/= 0 ->
            ProjectB = alinkutil_type:to_binary(Project),
            case alinkcore_cache:query_device_scene(DeviceId) of
                {ok, Scenes} ->
                    lists:foreach(
                        fun(#{<<"scene">> := Scene}) ->
                            SceneB = alinkutil_type:to_binary(Scene),
                            Topic1 = <<"s/out/", ProjectB/binary, "/", SceneB/binary, "/", Addr/binary>>,
                            Msg1 =
                                #{
                                    <<"msgType">> => <<"deviceAlarm">>,
                                    <<"data">> => Data
                                },
                            emqx:publish(emqx_message:make(Topic1, jiffy:encode(Msg1)))
                        end, Scenes);
                _ ->
                    ok
            end;
        _ ->
            skip
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================