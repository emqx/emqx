%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 27. 5月 2023 下午1:54
%%%-------------------------------------------------------------------
-module(alinkdata_metrics).

-behaviour(gen_server).

-export([
    start_link/0,
    record_td/0,
    collect/1,
    init_td_table/0,
    record_device_log/1,
    delete_out_of_date/0
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

-record(state, {}).

-define(COLLECT_INTERVAL, 3600 * 1000).
-define(OUT_OF_DATE_INTERVAL, 24 * 3600 * 1000).
%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%collect(#{event := <<"device_metrics">>, type := <<"login">>}) ->
%%    ets:update_counter(?MODULE, device_online, {2, 1}, {device_online, 0}),
%%    ok;
%%collect(#{event := <<"device_metrics">>, type := <<"logout">>}) ->
%%    ets:update_counter(?MODULE, device_online, {2, -1}, {device_online, 1}),
%%    ok;
%%collect(#{event := <<"alarm_trig">>, type := Level}) ->
%%    ets:update_counter(?MODULE, {alarm_trig, Level}, {2, 1}, {{alarm_trig, Level}, 0}),
%%    ok;
collect(_) ->
    ok.


record_device_log(Args) ->
    alinkalarm_consumer:device_log_notify(Args),
    alinkiot_tdengine_metrics_worker:insert_device_log(Args#{<<"metrics_type">> => <<"device_log">>}).


record_td() ->
    {ok, [#{<<"count(id)">> := DeviceCount}]} =
        alinkdata:query_mysql_format_map(default, <<"select count(id) from sys_device">>),
    {ok, [#{<<"count(id)">> := DeviceActiveCount}]} =
        alinkdata:query_mysql_format_map(default, <<"select count(id) from sys_device where status = '0'">>),
    DeviceOnline = alinkcore_cache:sessions_count(),
    NowB = alinkutil_type:to_binary(erlang:system_time(millisecond)),
    DeviceCountB = alinkutil_type:to_binary(DeviceCount),
    DeviceActiveCountB = alinkutil_type:to_binary(DeviceActiveCount),
    DeviceOnlineB = alinkutil_type:to_binary(DeviceOnline),
    NodeB = alinkutil_type:to_binary(node()),
    DeviceSubTable = alinkutil_type:to_binary(erlang:phash2({NodeB})),
    DeviceMetricsSql =
        <<"insert into metrics_device_", DeviceSubTable/binary,
            "(time, all_device, active_device, online_device) ",
            "using metrics_device(node) tags('", NodeB/binary, "') values(",
            NowB/binary, ",", DeviceCountB/binary, ",", DeviceActiveCountB/binary, ",", DeviceOnlineB/binary,
           ")">>,
    case alinkiot_tdengine:request(DeviceMetricsSql) of
        {ok, _, _} ->
            ok;
        {error, Reason0} ->
            logger:error("write to device metrics table failed ~p", [Reason0])
    end,
    {ok, AlarmCounts0} =
        alinkdata:query_mysql_format_map(default, <<"select count(id), level from sys_alarm group by level">>),
    AllAlarmCount = lists:foldl(fun(#{<<"count(id)">> := Triged}, Acc) -> Acc + Triged end, 0, AlarmCounts0),
    AlarmCounts = [#{<<"count(id)">> => AllAlarmCount, <<"level">> => <<"-1">>}|AlarmCounts0],
    lists:foreach(
        fun(#{<<"count(id)">> := Triged,<<"level">> := Level}) ->
            LevelB = alinkutil_type:to_binary(Level),
            TrigedB = alinkutil_type:to_binary(Triged),
            TableS = alinkutil_type:to_binary(erlang:phash2({NodeB, Level})),
            AlarmSql =
                <<"insert into metrics_alarm_", TableS/binary, "(time, triged)",
                    " using metrics_alarm(node, level) tags('",
                    NodeB/binary, "',", LevelB/binary, ") values(",
                    NowB/binary, ",", TrigedB/binary,
                    ")">>,
            case alinkiot_tdengine:request(AlarmSql) of
                {ok, _, _} ->
                    ok;
                {error, Reason1} ->
                    logger:error("write to alarm metrics table failed ~p", [Reason1])
            end;
           (_) ->
               ok
    end, AlarmCounts).

init_td_table() ->
    Sql0 = <<"create stable if not exists metrics_device",
            "(time TIMESTAMP,all_device INT, active_device INT, online_device INT) ",
            "tags(node VARCHAR(100))">>,
    case alinkiot_tdengine:request(Sql0) of
        {ok, _, _} ->
            ok;
        {error, Reason0} ->
            logger:error("init device metrics table failed:~p", [Reason0])
    end,
    Sql1 = <<"create stable if not exists metrics_alarm",
        "(time TIMESTAMP,triged INT) ",
        "tags(node VARCHAR(100), level INT)">>,
    case alinkiot_tdengine:request(Sql1) of
        {ok, _, _} ->
            ok;
        {error, Reason1} ->
            logger:error("init alarm metrics table failed:~p", [Reason1])
    end,
    Sql2 = <<"create stable if not exists metrics_device_log",
        "(time TIMESTAMP,data VARCHAR(4096), event VARCHAR(100), ",
        "reason VARCHAR(4096), result VARCHAR(4), ip VARCHAR(30)) ",
        "tags(node VARCHAR(100), addr VARCHAR(100), product_id INT)">>,
    case alinkiot_tdengine:request(Sql2) of
        {ok, _, _} ->
            ok;
        {error, Reason2} ->
            logger:error("init device_log table failed:~p", [Reason2])
    end.


delete_out_of_date() ->
    NowB = alinkutil_type:to_binary(erlang:system_time(millisecond) - 7 * 24 * 3600 * 1000),
    Sql1 = <<"delete from metrics_device_log where time < ", NowB/binary>>,
    case alinkiot_tdengine:request(Sql1) of
        {ok, _, _} ->
            ok;
        {error, Reason1} ->
            logger:error("delete from metrics_device_log failed:~p", [Reason1])
    end,
    Sql2 = <<"delete from metrics_device where time < ", NowB/binary>>,
    case alinkiot_tdengine:request(Sql2) of
        {ok, _, _} ->
            ok;
        {error, Reason2} ->
            logger:error("delete from metrics_device failed:~p", [Reason2])
    end,
    Sql3 = <<"delete from metrics_alarm where time < ", NowB/binary>>,
    case alinkiot_tdengine:request(Sql3) of
        {ok, _, _} ->
            ok;
        {error, Reason3} ->
            logger:error("delete from metrics_alarm failed:~p", [Reason3])
    end.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
init([]) ->
    erlang:start_timer(?COLLECT_INTERVAL, self(), collect),
    erlang:start_timer(?OUT_OF_DATE_INTERVAL, self(), delete_out_of_date),
    ets:new(?MODULE, [public, set, named_table, {write_concurrency, true}]),
%%    alinkdata_hooks:add('alinkiot.metrics', {?MODULE, collect, []}),
    init_td_table(),
    alinkdata_hooks:add('device.log', {?MODULE, record_device_log, []}),
    {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.


handle_cast(_Request, State = #state{}) ->
    {noreply, State}.


handle_info({timeout, _Ref, delete_out_of_date}, State) ->
    delete_out_of_date(),
    erlang:start_timer(?OUT_OF_DATE_INTERVAL, self(), delete_out_of_date),
    {noreply, State};
handle_info({timeout, _Ref, collect}, State) ->
    record_td(),
    erlang:start_timer(?COLLECT_INTERVAL, self(), collect),
    {noreply, State};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
