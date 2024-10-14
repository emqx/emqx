%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 11. 12月 2023 下午6:03
%%%-------------------------------------------------------------------
-module(alinkcore_device_event).
-author("yqfclid").

%% API
-export([
    online/1,
    offline/2
]).

%%%===================================================================
%%% API

%%%===================================================================
online(Addr) ->
    NAddrs =
        case alinkcore_cache:query_children(Addr) of
            {ok, Children} ->
                [Addr|lists:map(fun(#{<<"addr">> := A}) -> A end, Children)];
            _ ->
                [Addr]
        end,
    lists:foreach(
        fun(A) ->
            Data = #{
                <<"addr">> => A,
                <<"time">> => erlang:system_time(second),
                <<"status">> => <<"1">>
            },
            send_event(A, Data)
    end, NAddrs).


offline(Addr, Reason) ->
    Data = #{
        <<"addr">> => Addr,
        <<"time">> => erlang:system_time(second),
        <<"status">> => <<"0">>,
        <<"reason">> => Reason
    },
    send_event(Addr, Data).


send_event(Addr, Data) ->
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
                                    <<"msgType">> => <<"deviceEvent">>,
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