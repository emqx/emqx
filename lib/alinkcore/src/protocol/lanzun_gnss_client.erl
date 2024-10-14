%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 8月 2023 下午12:55
%%%-------------------------------------------------------------------
-module(lanzun_gnss_client).

-behaviour(gen_server).

-export([
    start/3,
    stop/1
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

-record(state, {
    product_id,
    addr,
    session,
    last_time,
    continue_failed_time = 0,
    device_id
}).

-include("alinkcore.hrl").

-define(GET_DATA_INTERVAL, 10 * 60 * 1000).
%%%===================================================================
%%% API
%%%===================================================================
start(ProductId, Addr, DeviceId) ->
    gen_server:start(?MODULE, [ProductId, Addr, DeviceId], []).


stop(Pid) ->
    gen_server:cast(Pid, stop).
%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
init([ProductId, Addr, DeviceId]) ->
    Session = #session{
        pid = self(),
        node = node(),
        connect_time = erlang:system_time(second)
    },
    LastTime = get_last_time(Addr),
    alinkcore_cache:save(session, Addr, Session),
    erlang:send_after(?GET_DATA_INTERVAL, self(), sync_data),
    {ok, #state{
        session = Session,
        product_id = ProductId,
        addr = Addr,
        device_id = DeviceId,
        last_time = LastTime
    }}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.


handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Request, State = #state{}) ->
    {noreply, State}.


handle_info(sync_data, #state{continue_failed_time = FailTime, addr = Addr} = State) ->
    case sync_data(State) of
        {ok, LastTime} ->
            erlang:send_after(?GET_DATA_INTERVAL, self(), sync_data),
            {noreply, State#state{last_time = LastTime, continue_failed_time = 0}};
        {error, Reason} ->
            logger:error("~p sync_data failed:~p", [Addr,Reason]),
            case FailTime + 1 >= 5 of
                true ->
                    logger:error("~p sync_data continue failed ~p time, should stop", [Addr, FailTime + 1]),
                    {stop, normal, State};
                _ ->
                    erlang:send_after(?GET_DATA_INTERVAL, self(), sync_data),
                    {noreply, State#state{continue_failed_time = FailTime + 1}}
            end
    end;
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{session = Session, addr = Addr}) ->
    alinkcore_cache:delete_object(session, Addr, Session),
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_last_time(Addr) ->
    T0 = binary:replace(Addr, <<"-">>, <<"_">>, [global]),
    Table = <<"device_", T0/binary>>,
    case alinkiot_tdengine:request(<<"describe ", Table/binary>>) of
        {ok, _, _} ->
            case alinkiot_tdengine:request(<<"select last(TIMEDIFF(time, 0, 1a)) as time from ", Table/binary>>) of
                {ok, 1, [#{<<"time">> := Time}]} ->
                    Time;
                {ok, 0, []} ->
                    0;
                {error, Reason} ->
                    logger:error("query td ~p lanzun gnss last time failed:~p", [Addr, Reason]),
                    0
            end;
        {error, _Reason} ->
            0
    end.


sync_data(#state{product_id = ProductId, device_id = DeviceId, last_time = LastTime, addr = Addr}) ->
    NowTime = erlang:system_time(millisecond),
    case alinkdata_lanzun_gnss:get_data(ProductId, DeviceId, LastTime, NowTime) of
        {ok, RawData} ->
            {ok, handle_raw_data(RawData, ProductId, Addr)};
        {error, Reason} ->
            {error, Reason}
    end.




handle_raw_data(RawData, ProductId, Addr) ->
    {TsValueMap, NLastTime} =
        maps:fold(
            fun(K, Kvs, {Acc0, Acc1}) ->
                lists:foldl(
                    fun(#{<<"ts">> := Ts, <<"value">> := RV}, {Accc0, Accc1}) ->
                        NK = transform_k(K),
                        V = transform_value(RV),
                        OldTsV = maps:get(Ts, Acc0, #{}),
                        SecondTs = Ts div 1000,
                        NTsV = OldTsV#{<<"ts">> => SecondTs, NK => #{<<"value">> => V, <<"ts">> => SecondTs}},
                        NAccc0 = Accc0#{Ts => NTsV},
                        NAccc1 =
                            case Accc1 > Ts of
                                true ->
                                    Accc1;
                                _ ->
                                    Ts
                            end,
                        {NAccc0, NAccc1}
                end, {Acc0, Acc1}, Kvs)
        end, {#{}, 0}, RawData),
    maps:fold(
        fun(_, Data, _) ->
            alinkcore_data:handle(ProductId, Addr, Data)
    end, 0, TsValueMap),
    NLastTime.




transform_value(RV) ->
    case binary:split(RV, <<".">>, [global]) of
        [_] ->
            binary_to_integer(RV);
        _ ->
            binary_to_float(RV)
    end.


transform_k(<<"X">>) -> <<"x">>;
transform_k(<<"XI">>) -> <<"xi">>;
transform_k(<<"Y">>) -> <<"y">>;
transform_k(<<"YI">>) -> <<"yi">>;
transform_k(<<"H">>) -> <<"h">>;
transform_k(<<"HI">>) -> <<"hi">>;
transform_k(<<"status">>) -> <<"stat">>;
transform_k(<<"satNum">>) -> <<"satnum">>.