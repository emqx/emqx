%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 21. 8月 2023 下午5:22
%%%-------------------------------------------------------------------
-module(gnss_sub_device).

-behaviour(gen_server).

-export([
    start_link/5,
    handle_data/3,
    connect/1
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
    last_time = 0,
    session,
    addr,
    db_connect_pid,
    db_opts,
    is_connected = false,
    owner,
    product_id,
    sub_addr,
    query_time
}).
-include("alinkcore.hrl").


-define(QUERY_INTERVAL, 60 * 1000).

-define(RECONNECT_INTERVAL, 10 * 1000).

-define(DIFF_SECONDS_0000_1970, 	62167219200).   % Gregorian格林高利秒 与 Greenwich格林威治秒 的时间差
%%%===================================================================
%%% API
%%%===================================================================
start_link(Owner, ProductId, Addr, SubAddr, DbAOpts) ->
    gen_server:start_link(?MODULE, [Owner, ProductId, Addr, SubAddr, DbAOpts], []).


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
init([Owner, ProductId, Addr, SubAddr, Config]) ->
    process_flag(trap_exit, true),
    Session = #session{
        pid = self(),
        node = node(),
        connect_time = erlang:system_time(second)
    },
    case connect(Config) of
        {ok, Pid} ->
            alinkcore_cache:save(session, Addr, Session),
            QueryTime = maps:get(<<"retry">>, Config, ?QUERY_INTERVAL),
            State = #state{
                session = Session,
                addr = Addr,
                db_opts = Config,
                db_connect_pid = Pid,
                is_connected = true,
                owner = Owner,
                product_id = ProductId,
                sub_addr = SubAddr,
                query_time = QueryTime
            },
            LastTime = sync(State),
            erlang:start_timer(QueryTime, self(), query),
            {ok, State#state{last_time = LastTime}};
        {error, Reason} ->
            logger:error("~p stoped by connecting failed:~p", [Reason]),
            {stop, normal, #state{}}
    end.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info({timeout, _Ref, query}, #state{is_connected = true, addr = Addr, last_time = L} = State) ->
    {Connected, LastTime} =
        case catch query(State) of
            {'EXIT', Reason} ->
                logger:error("~p query exit failed:~p", [Addr, Reason]),
                erlang:start_timer(?RECONNECT_INTERVAL, self(), reconnect),
                {false, L};
            {ok, L1} ->
                {true, L1};
            {error, Reason} ->
                logger:error("~p query failed:~p", [Addr, Reason]),
                {true, L}
        end,
    erlang:start_timer(State#state.query_time, self(), query),
    {noreply, State#state{is_connected = Connected, last_time = LastTime}};

handle_info({timeout, _Ref, reconnect}, #state{db_opts = DbOpts, addr = Addr} = State) ->
    case connect(DbOpts) of
        {ok, NPid} ->
            {noreply, State#state{db_connect_pid = NPid}};
        {error, Reason} ->
            logger:error("~p db reconnect failed:~p", [Addr, Reason]),
            erlang:start_timer(?RECONNECT_INTERVAL, self(), reconnect),
            {noreply, State#state{is_connected = false}}
    end;

handle_info({'EXIT', Owner, _Why}, #state{owner = Owner} = State) ->
    {stop, normal, State};
handle_info({'EXIT', Pid, Why}, #state{db_connect_pid = Pid, db_opts = DbOpts, addr = Addr} = State) ->
    logger:error("~p db pid exit:~p", [Addr, Why]),
    case connect(DbOpts) of
        {ok, NPid} ->
            {noreply, State#state{db_connect_pid = NPid, is_connected = true}};
        {error, Reason} ->
            logger:error("~p db reconnect failed:~p", [Addr, Reason]),
            erlang:start_timer(?RECONNECT_INTERVAL, self(), reconnect),
            {noreply, State#state{is_connected = false}}
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
connect(Config) ->
    case maps:get(<<"gnss">>, Config, undefined) of
        undefined ->
            {error, gnss_server_not_found};
        Key ->
            case alinkcore_config:find_by_name(Key, [{format, json}]) of
                {error, Reason} ->
                    {error, Reason};
                {ok, [DbMap|_]} ->
                    Opts =
                        [
                            {host, binary_to_list(maps:get(<<"host">>, DbMap, <<>>))},
                            {port, maps:get(<<"port">>, DbMap, <<>>)},
                            {user, binary_to_list(maps:get(<<"user">>, DbMap, <<>>))},
                            {password, binary_to_list(maps:get(<<"password">>, DbMap, <<>>))},
                            {database, binary_to_list(maps:get(<<"db">>, DbMap, <<>>))},
                            {keep_alive, true}
                        ],
                    case mysql:start_link(Opts) of
                        {ok, Pid} ->
                            {ok, Pid};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
    end.



query(#state{last_time = LastTime, db_connect_pid = Pid, addr = Addr, product_id = ProductId}) ->
    Sql = <<"select * from navi_cloud_deform.data_gnss_current where station_name = '", Addr/binary, "'">>,
    case format_result_to_map(mysql:query(Pid, Sql)) of
        {ok, [#{<<"time">> := LastTime}|_]} ->
            {ok, LastTime};
        {ok, [#{<<"time">> := Time} = GnssData|_]} ->
            handle_data(ProductId, Addr, GnssData),
            {ok, Time};
        {ok, []} ->
            {ok, LastTime};
        {error, Reason} ->
            {error, Reason}
    end.

sync(#state{last_time = LastTime, db_connect_pid = Pid, addr = Addr, product_id = ProductId, sub_addr = SubAddr}) ->
    {{LastY, LastM, LastD}, {Lasth, Lastm, Lasts}} = LastTime = get_last_data_time(Addr),
    {{NowY, NowM, _}, _} = erlang:localtime(),
    NeedSyncMonths = lack_months({NowY, NowM}, {LastY, LastM}),
    SyncedTables =
        lists:map(
            fun({Y, M}) ->
                YB = alinkutil_type:to_binary(Y),
                MB0 = alinkutil_type:to_binary(M),
                MB =
                    case byte_size(MB0) of
                        1 ->
                            <<"0", MB0/binary>>;
                        _ ->
                            MB0
                    end,
                <<"data_gnss_", YB/binary, MB/binary>>
        end, NeedSyncMonths),
    {ok, _, RawTables} = mysql:query(Pid, <<"show tables;">>),
    Tables = lists:flatten(RawTables),
    lists:foldl(
        fun(SyncedTable, Acc) ->
            case lists:member(SyncedTable, Tables) of
                true ->
                    TimeStrB = list_to_binary(lists:flatten(io_lib:format("~w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",[LastY, LastM, LastD, Lasth, Lastm, Lasts]))),
                    Sql = <<"select * from ", SyncedTable/binary, " where station_name = '",
                            SubAddr/binary, "'and time > '", TimeStrB/binary, "'">>,
                    case format_result_to_map(mysql:query(Pid, Sql)) of
                        {ok, GnssDatas} ->
                            lists:foldl(
                                fun(#{<<"time">> := GTime} = G, Acc1) ->
                                    handle_data(ProductId, Addr, G),
                                    case GTime > Acc1 of
                                        true ->
                                            GTime;
                                        _ ->
                                            Acc1
                                    end
                            end, Acc, GnssDatas);
                        {error, Reason} ->
                            logger:error("query ~p failed", [SyncedTable, Reason]),
                            Acc
                    end;
                _ ->
                    ok
            end
    end, {{2023, 8, 1}, {0, 0, 0}}, SyncedTables).

get_last_data_time(Addr) ->
    T0 = binary:replace(Addr, <<"-">>, <<"_">>, [global]),
    Table = <<"device_", T0/binary>>,
    case alinkiot_tdengine:request(<<"describe ", Table/binary>>) of
        {ok, _, _} ->
            case alinkiot_tdengine:request(<<"select last(TIMEDIFF(time, 0, 1s)) as time from ", Table/binary>>) of
                {ok, 1, [#{<<"time">> := Time}]} ->
                    seconds_to_datetime(Time);
                {ok, 0, []} ->
                    {{2023, 8, 1}, {0, 0, 0}};
                {error, Reason} ->
                    logger:error("query td ~p gnss last time failed:~p", [Addr, Reason]),
                    {{2023, 8, 1}, {0, 0, 0}}
            end;
        {error, _Reason} ->
            {{2023, 8, 1}, {0, 0, 0}}
    end.


handle_data(ProductId, Addr, #{<<"time">> := Time} = GnssData) ->
    TimeS = datetime_to_seconds(Time),
    Data =
        #{
            <<"ts">> => TimeS,
            <<"b">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"b">>, GnssData, 0)
            },
            <<"l">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"l">>, GnssData, 0)
            },
            <<"h">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"h">>, GnssData, 0)
            },
            <<"x">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"x">>, GnssData, 0)
            },
            <<"y">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"y">>, GnssData, 0)
            },
            <<"sx">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"sx">>, GnssData, 0)
            },
            <<"sy">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"sy">>, GnssData, 0)
            },
            <<"sh">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"sh">>, GnssData, 0)
            },
            <<"ax">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"ax">>, GnssData, 0)
            },
            <<"ay">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"ay">>, GnssData, 0)
            },
            <<"ah">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"ah">>, GnssData, 0)
            },
            <<"station_name">> => #{
                <<"ts">> => TimeS, <<"value">> => maps:get(<<"station_name">>, GnssData, <<>>)
            }
        },
    alinkcore_data:handle(ProductId, Addr, Data).


datetime_to_seconds(DateTime) ->
    [UniversalTime] = calendar:local_time_to_universal_time_dst(DateTime),
    calendar:datetime_to_gregorian_seconds(UniversalTime) - ?DIFF_SECONDS_0000_1970 + 3600 * 8.


seconds_to_datetime(Seconds) ->
    DateTime = calendar:gregorian_seconds_to_datetime(Seconds + ?DIFF_SECONDS_0000_1970),
    calendar:universal_time_to_local_time(DateTime).



format_result_to_map(ok) ->
    ok;
format_result_to_map({ok, ColNames, Datas}) ->
    NDatas =
        lists:map(
            fun(Data) ->
                maps:from_list(lists:zip(ColNames, Data))
            end, Datas),
    {ok, NDatas};
format_result_to_map({ok, QueryDatas}) ->
    NQueryDatas =
        lists:map(
            fun({ColNames, Datas}) ->
                lists:map(
                    fun(Data) ->
                        maps:from_list(lists:zip(ColNames, Data))
                    end, Datas)
            end, QueryDatas),
    {ok, NQueryDatas};
format_result_to_map(Other) ->
    Other.



lack_months({NowY, NowM}, {LastY, LastM}) ->
    lack_months({NowY, NowM}, {LastY, LastM}, []).

lack_months({NowY, NowM}, {NowY, NowM}, Acc) ->
    [{NowY, NowM}|Acc];
lack_months({NowY, NowM}, {LastY, LastM}, Acc) ->
    case LastM of
        12 ->
            lack_months({NowY, NowM}, {LastY + 1, 1}, [{LastY, LastM}|Acc]);
        _ ->
            lack_months({NowY, NowM}, {LastY, LastM + 1}, [{LastY, LastM}|Acc])
    end.


