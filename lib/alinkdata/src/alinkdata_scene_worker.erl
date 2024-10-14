%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2024, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 9月 2024 下午4:29
%%%-------------------------------------------------------------------
-module(alinkdata_scene_worker).

-behaviour(gen_server).

-export([
    start/1,
    start_link/1
]).


-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {scene_id, device_data = #{}, calc_interval, hole_deepth}).
%%%===================================================================
%%% API
%%%===================================================================
start(SceneId) ->
    case supervisor:start_child(alinkdata_scene_worker_sup, [SceneId]) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.



start_link(SceneId) ->
    gen_server:start_link(?MODULE, [SceneId], []).


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
init([SceneId]) ->
    Pid = self(),
    alinkdata_scene_data:add_scene(SceneId, Pid),
    SceneConfig =
        case alinkdata_dao:query_no_count('QUERY_scene', #{<<"id">> => SceneId}) of
            {ok, [#{<<"config">> := C}]} when C =/= <<>> ->
                jiffy:decode(C, [return_maps]);
            {ok, _} ->
                [];
            {error, Reason} ->
                logger:error("get scene config ~p failed ~p", [SceneId, Reason]),
                []
        end,
    CalcInterval = get_config_from_raw(<<"calc_interval">>, SceneConfig, 10),
    HoleDeepth = get_config_from_raw(<<"hole_deepth">>, SceneConfig, 26.0),
    erlang:send_after(CalcInterval * 1000, self(), calc),
    {ok, #state{scene_id = SceneId, calc_interval = CalcInterval, hole_deepth = HoleDeepth}}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Request, State = #state{}) ->
    {noreply, State}.



handle_info(calc, #state{scene_id = SceneId, device_data =  DeviceData} = State) ->
    SceneConfig =
        case alinkdata_dao:query_no_count('QUERY_scene', #{<<"id">> => SceneId}) of
            {ok, [#{<<"config">> := C}]} when C =/= <<>> ->
                jiffy:decode(C, [return_maps]);
            {ok, _} ->
                [];
            {error, Reason} ->
                logger:error("get scene config ~p failed ~p", [SceneId, Reason]),
                []
        end,
    CalcInterval = get_config_from_raw(<<"calc_interval">>, SceneConfig, 10),
    HoleDeepth = get_config_from_raw(<<"hole_deepth">>, SceneConfig, 26.0),
    erlang:send_after(CalcInterval * 60 * 1000, self(), calc),
    Now = erlang:system_time(second),
    RealTimeDeviceData =
        maps:fold(
            fun(K, V, Acc) ->
                DataTime = maps:get(<<"ts">>, V, 0),
                case Now - DataTime > 3 * 60 * CalcInterval of
                    true ->
                        Acc;
                    _ ->
                        Acc#{K => V}
                end
        end, #{}, DeviceData),
    deal_device_data(SceneId, HoleDeepth, maps:to_list(RealTimeDeviceData)),
    {noreply, State#state{device_data = RealTimeDeviceData, calc_interval = CalcInterval, hole_deepth = HoleDeepth}};
handle_info({data, _ProductId, Addr, Data}, #state{device_data = OldDeviceData} = State) ->
    {noreply, State#state{device_data = OldDeviceData#{Addr => Data}}};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{scene_id = SceneId}) ->
    Pid = self(),
    alinkdata_scene_data:delete_scene({SceneId, Pid}),
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
deal_device_data(_, _, []) ->
    ok;
deal_device_data(SceneId, HoleDeepth, RealTimeDeviceData0) ->
    RealTimeDeviceData =
        lists:map(
            fun({Addr, DeviceData}) ->
                Length =
                    case alinkcore_cache:query_device(Addr) of
                        {ok, #{<<"config">> := Config}} ->
                            LenB = maps:get(<<"length">>, Config, 0),
                            case catch alinkutil_type:to_float(LenB) of
                                {'EXIT', _} ->
                                    alinkutil_type:to_integer(LenB) * 1.0;
                                Len ->
                                    Len
                            end;
                        _ ->
                            0
                    end,
                {Addr, DeviceData#{<<"length">> => #{<<"value">> => Length}}}
        end, RealTimeDeviceData0),
    CalcData = alinkdata_hole_deepth:calc(HoleDeepth, RealTimeDeviceData),
    Time = now_time(),
    Data = jiffy:encode(#{<<"data">> => CalcData}),
    Insert = #{<<"scene_type">> => <<"chart">>, <<"scene">> => SceneId, <<"data">> => Data, <<"create_time">> => Time},
    case alinkdata_dao:query_no_count('POST_scene_view', Insert) of
        ok ->
            ok;
        {error, Reason} ->
            logger:error("update scene view ~p failed:~p", [SceneId, Reason])
    end.


get_config_from_raw(_, [], Default) ->
    Default;
get_config_from_raw(Key, [#{<<"key">> := Key, <<"value">> := Value}|_], _) ->
    Value;
get_config_from_raw(Key, [_|T], Default) ->
    get_config_from_raw(Key, T, Default).


now_time() ->
    {{Y, M, D}, {H, Mi, S}} = erlang:localtime(),
    list_to_binary(lists:flatten(io_lib:format("~w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",[Y,M,D,H,Mi,S]))).