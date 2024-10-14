%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(alinkiot_tdengine_worker).

-behaviour(gen_server).

-export([
    connect/1,
    start_workers/0,
    stop_workers/0,
    insert/1,
    insert/2
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    url,
    authorization,
    flush_interval,
    flush_msg_len,
    flush_time_ref,
    cache_msgs
}).

%%%===================================================================
%%% API
%%%===================================================================
%% #{<<"addr">> => Addr, <<"productId">> => Addr, <<"ts">> => Ts, <<"N">> => V}
insert(DeviceStat) ->
    ecpool:with_client(alinkiot_tdengine_worker_pool, fun(Pid) -> alinkiot_tdengine_worker:insert(Pid, DeviceStat) end).


start_workers() ->
    Opts = application:get_all_env(alinkiot_tdengine),
    AllOpts = [{pool_type, round_robin},{auto_reconnect, 3}|Opts],
    ecpool_sup:start_pool(alinkiot_tdengine_worker_pool, alinkiot_tdengine_worker, AllOpts).

stop_workers() ->
    ecpool_sup:stop_pool(alinkiot_tdengine_worker_pool).

insert(Pid, Query) ->
    gen_server:cast(Pid, {insert, Query}).

connect(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

init([Opts]) ->
    Host = alinkutil_type:to_binary(proplists:get_value(host, Opts, "127.0.0.1")),
    Port = alinkutil_type:to_binary(proplists:get_value(port, Opts, 6041)),
    UserName = alinkutil_type:to_binary(proplists:get_value(username, Opts, "root")),
    Password = alinkutil_type:to_binary(proplists:get_value(password, Opts, "taosdata")),
    DataBase = alinkutil_type:to_binary(proplists:get_value(database, Opts, <<>>)),
    Url = alinkiot_tdengine_rest:build_url(Host, Port, DataBase),
    Token = base64:encode(<<UserName/binary, ":", Password/binary>>),
    Authorization = <<"Basic ", Token/binary>>,
    FlushInterval = proplists:get_value(flush_interval, Opts, 0),
    FlushMsgLen = proplists:get_value(flush_msg_len, Opts, 0),
    hackney_pool:start_pool(alinkiot_tdengine_worker_pool, [{max_connections, 20}]),
    State = #state{
        url = Url,
        authorization = Authorization,
        flush_interval = FlushInterval,
        flush_msg_len = FlushMsgLen,
        flush_time_ref = start_timer(FlushInterval),
        cache_msgs = []
    },
    {ok, State}.


handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast({insert, Msg}, #state{cache_msgs = CacheMsgs,
                                  flush_msg_len = FlushMsgLen} = State) when length(CacheMsgs) >= FlushMsgLen - 1 ->
    write([Msg|CacheMsgs], State),
    {noreply, State#state{cache_msgs = []}};
handle_cast({insert, Data}, #state{cache_msgs = CacheMsgs} = State) ->
    {noreply, State#state{cache_msgs = [Data|CacheMsgs]}};
handle_cast(_Request, State = #state{}) ->
    {noreply, State}.


handle_info({timeout, _TimerRef, flush}, #state{cache_msgs = CacheMsgs, flush_interval = FlushInterval} = State) ->
    write(CacheMsgs, State),
    {noreply, State#state{cache_msgs = [], flush_time_ref = start_timer(FlushInterval)}};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_timer(0) -> erlang:make_ref();
start_timer(FlushInterval) -> erlang:start_timer(FlushInterval, self(), flush).

write([], _State) ->
    ignore;
write(Msgs, State) ->
    #state{url = Url,
        authorization = Authorization} = State,
    Sql =  build_sql(Msgs),
    QueryOpts =
        #{
            pool => alinkiot_tdengine_worker_pool,
            authorization => Authorization,
            retry => 3
        },
    case alinkiot_tdengine_rest:query(Url, Sql, QueryOpts) of
        {ok, _, _} ->
            ok;
        {error, Reason} ->
            logger:error("Data ~p State ~p send data to tdengine failed:~p", [Msgs, State, Reason])
    end.


build_sql(Msgs) ->
    Sql0 = <<"insert into">>,
    Sql1 =
        lists:foldl(
            fun(Msg, Acc) ->
                SubSql = build_sub_sql(Msg),
                <<Acc/binary, SubSql/binary>>
        end, <<>>, Msgs),
    <<Sql0/binary, Sql1/binary>>.


build_sub_sql(Msg) ->
    #{
        <<"addr">> := Addr,
        <<"productId">> := ProductId
    } = Msg,
    AlarmRuleB =
        case maps:get(<<"alarm_rule">>, Msg, null) of
            null ->
                <<"NULL">>;
            AlarmRule ->
                AB = alinkutil_type:to_binary(AlarmRule),
                <<"'", AB/binary, "'">>
        end,

    Table = binary:replace(<<"device_", Addr/binary>>, <<"-">>, <<"_">>, [global]),
    ProductIdB = alinkutil_type:to_binary(ProductId),
    STable = <<"alinkiot_", ProductIdB/binary>>,
    AddrB = alinkiot_tdengine_common:to_sql_bin(Addr),
    Time = maps:get(<<"ts">>, Msg, erlang:system_time(second)),
    TimeB = alinkutil_type:to_binary(Time),
    MilliTimeB =
        case byte_size(TimeB) of
            10 ->
                <<TimeB/binary, "000">>;
            _ ->
                TimeB
        end,

    {Sql0, Sql1} =
        maps:fold(
            fun(K, RV, {Acc0, Acc1}) ->
                case lists:member(K, [<<"addr">>, <<"product_id">>, <<"ts">>, <<"productId">>, <<"alarm_rule">>]) of
                    true ->
                        {Acc0, Acc1};
                    _ ->
                        #{<<"value">> := V} = RV,
                        KB = alinkutil_type:to_binary(K),
                        TdF = alinkiot_tdengine_common:to_td_field_name(KB),
                        VB = alinkiot_tdengine_common:to_sql_bin(V),
                        {<<Acc0/binary, ",", TdF/binary>>, <<Acc1/binary, ",", VB/binary>>}
                end
            end, {<<"time, alarm_rule">>, <<MilliTimeB/binary, ",", AlarmRuleB/binary>>}, Msg),
    Sql2 = <<") using ", STable/binary, "(addr, product_id) tags(",
        AddrB/binary, ",", ProductIdB/binary, ") values (">>,
    <<" ", Table/binary, "(", Sql0/binary, Sql2/binary, Sql1/binary, ")">>.