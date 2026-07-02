%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_buffer_mon).

-behaviour(gen_server).

-include("emqx_cm.hrl").
-include("logger.hrl").

-export([
    start_link/0,
    update/1,
    maybe_log/3,
    run_top/1,
    cancel_top/0,
    top_status/0,
    local_top/1,
    local_top/2
]).

-export_type([
    sort_by/0,
    row/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ifdef(TEST).
-export([
    scan_local/2,
    csv_rows/1,
    write_csv/2
]).
-endif.

-define(CONF_KEY, {?MODULE, conf}).
-define(DEFAULT_CONF, #{buffered_payload_high_watermark => 0}).
-define(LOG_MSG, session_buffer_high_watermark).
-define(TOP_TIMEOUT, 300000).
-define(DEFAULT_SCAN_BATCH_SIZE, 1000).
-define(DEFAULT_SCAN_SLEEP_MS, 1).
-define(SESSION_TOP_EXTRA_STATS, [mqueue_len, total_payload_bytes, inflight_cnt]).

-type stats() :: emqx_types:stats() | map().
-type sort_by() :: mqueue_length | total_payload_bytes.
-type top_status() ::
    #{status := idle}
    | #{
        status := running,
        pid => pid(),
        out => file:name_all(),
        count := pos_integer(),
        sort := sort_by(),
        batch_size := pos_integer(),
        sleep_ms := non_neg_integer(),
        started_at := integer(),
        initiator := node(),
        progress := map()
    }
    | #{
        status := completed,
        out := file:name_all(),
        rows := non_neg_integer()
    }
    | #{
        status := completed,
        scan_id := term(),
        initiator := node(),
        started_at := integer(),
        completed_at := integer(),
        rows := non_neg_integer(),
        scanned := non_neg_integer(),
        total := non_neg_integer()
    }
    | #{
        status := failed,
        out := file:name_all(),
        reason := term()
    }
    | #{
        status := cancelled,
        out := file:name_all(),
        reason := cancelled
    }
    | #{
        status := cancelled,
        scan_id := term(),
        initiator := node(),
        started_at := integer(),
        reason := term(),
        scanned := non_neg_integer(),
        total := non_neg_integer()
    }.
-type row() :: #{
    clientid := emqx_types:clientid(),
    pid := pid() | undefined,
    node := node(),
    mqueue_length := non_neg_integer(),
    total_payload_bytes := non_neg_integer(),
    inflight_count := non_neg_integer()
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec update(map() | undefined) -> ok.
update(Conf0) ->
    Conf = put_conf(Conf0),
    gen_server:cast(?MODULE, {update, Conf}).

-spec maybe_log(emqx_types:clientid(), pid(), stats()) -> ok.
maybe_log(ClientId, ChanPid, Stats) ->
    Conf = persistent_term:get(?CONF_KEY, ?DEFAULT_CONF),
    case is_logging_enabled(Conf) of
        {true, HighWatermark} ->
            TotalPayloadBytes = stat_value(total_payload_bytes, Stats, 0),
            case TotalPayloadBytes > HighWatermark of
                true ->
                    do_log(ClientId, ChanPid, Stats, TotalPayloadBytes, HighWatermark);
                false ->
                    ok
            end;
        false ->
            ok
    end.

do_log(ClientId, ChanPid, Stats, TotalPayloadBytes, HighWatermark) ->
    ?SLOG_THROTTLE(
        warning,
        #{
            msg => ?LOG_MSG,
            clientid => ClientId,
            pid => ChanPid,
            mqueue_length => stat_value(mqueue_len, Stats, 0),
            inflight_count => stat_value(inflight_cnt, Stats, 0),
            total_payload_bytes => TotalPayloadBytes,
            buffered_payload_high_watermark => HighWatermark
        },
        #{clientid => ClientId}
    ).

is_logging_enabled(#{buffered_payload_high_watermark := HighWatermark}) when HighWatermark > 0 ->
    {true, HighWatermark};
is_logging_enabled(_Conf) ->
    false.

-spec run_top(#{count := pos_integer(), sort := sort_by(), out := file:name_all()}) ->
    {ok, pid()} | {error, busy | eexist}.
run_top(Opts) ->
    gen_server:call(?MODULE, {run_top, Opts}, infinity).

-spec cancel_top() -> {ok, cancelled} | {error, not_running}.
cancel_top() ->
    gen_server:call(?MODULE, cancel_top, infinity).

-spec top_status() -> top_status().
top_status() ->
    gen_server:call(?MODULE, top_status, infinity).

-spec local_top(map()) -> [row()] | {error, term()}.
local_top(#{count := _Count, sort := _Sort} = Opts) ->
    scan_tool_rows(Opts);
local_top(_Opts) ->
    {error, badarg}.

-spec local_top
    (map(), term()) -> [row()] | {error, term()};
    (pos_integer(), sort_by()) -> [row()].
local_top(Opts, _Compat) when is_map(Opts) ->
    local_top(Opts);
local_top(Count, SortBy) ->
    scan_local(Count, SortBy).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Conf = put_conf(emqx_config:get([sysmon, session], ?DEFAULT_CONF)),
    {ok, #{
        conf => Conf,
        top => undefined,
        top_status => #{status => idle}
    }}.

handle_call({run_top, Opts0}, _From, State = #{top := undefined}) ->
    Opts = normalize_top_opts(Opts0),
    case ensure_out_file_absent(maps:get(out, Opts)) of
        ok ->
            RunningNodes = emqx:running_nodes(),
            NodesTotal = length(RunningNodes),
            Server = self(),
            {Pid, MRef} =
                erlang:spawn_monitor(fun() ->
                    ok = gen_server:call(
                        Server, {top_scan_result, self(), do_run_top(Opts)}, infinity
                    )
                end),
            Top = #{pid => Pid, mref => MRef, opts => Opts},
            Status = top_running_status(Pid, Opts, NodesTotal),
            {reply, {ok, Pid}, State#{top := Top, top_status := Status}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({run_top, _Opts}, _From, State) ->
    {reply, {error, busy}, State};
handle_call(cancel_top, _From, State = #{top := #{pid := Pid, opts := Opts}}) ->
    exit(Pid, shutdown),
    Status = top_cancelled_status(Opts),
    {reply, {ok, cancelled}, State#{top := undefined, top_status := Status}};
handle_call(cancel_top, _From, State) ->
    {reply, {error, not_running}, State};
handle_call({top_scan_result, Pid, Result}, _From, State = #{top := Top = #{pid := Pid}}) ->
    {reply, ok, State#{top := Top#{result => Result}}};
handle_call({top_scan_result, _Pid, _Result}, _From, State) ->
    {reply, ok, State};
handle_call(top_status, _From, State) ->
    {Status, State1} = take_top_status(State),
    {reply, Status, State1};
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

%% update/1 already normalizes and stores the config synchronously.
handle_cast({update, Conf}, State = #{conf := Conf}) ->
    {noreply, State};
handle_cast({update, Conf}, State) ->
    {noreply, State#{conf := Conf}};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(
    {'DOWN', MRef, process, Pid, Reason},
    State = #{top := Top = #{pid := Pid, mref := MRef, opts := Opts}}
) ->
    maybe_log_scan_down(Reason),
    Status = maps:get(result, Top, top_scan_failed_status(Opts, Reason)),
    {noreply, State#{top := undefined, top_status := Status}};
handle_info(
    _Info,
    State
) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, ensure_top_status(State)}.

%%--------------------------------------------------------------------
%% Top-K scan
%%--------------------------------------------------------------------

do_run_top(
    #{
        count := _Count,
        sort := SortBy,
        out := OutFile,
        batch_size := _BatchSize,
        sleep_ms := _SleepMs
    } = Opts
) ->
    Rows = session_top_rows(
        emqx_session_tool:cluster_top_by(sort_metric(SortBy), session_tool_opts(Opts))
    ),
    case write_csv(OutFile, Rows) of
        ok ->
            ?SLOG(info, #{
                msg => session_top_written,
                file => OutFile,
                rows => length(Rows)
            }),
            #{
                status => completed,
                out => OutFile,
                rows => length(Rows)
            };
        {error, Reason} ->
            ?SLOG(error, #{
                msg => session_top_write_failed,
                file => OutFile,
                reason => Reason
            }),
            #{
                status => failed,
                out => OutFile,
                reason => Reason
            }
    end.

-spec scan_local(pos_integer(), sort_by()) -> [row()].
scan_local(Count, _SortBy) when Count =< 0 ->
    [];
scan_local(Count, SortBy) ->
    scan_tool_rows(#{count => Count, sort => SortBy}).

scan_tool_rows(#{count := _Count, sort := _Sort} = Opts) ->
    session_top_rows(emqx_session_tool:scan(session_tool_opts(Opts))).

session_tool_opts(Opts) ->
    #{
        metric => sort_metric(maps:get(sort, Opts)),
        top_k => maps:get(count, Opts),
        min_value => 0,
        chunk => maps:get(batch_size, Opts, ?DEFAULT_SCAN_BATCH_SIZE),
        sleep_ms => maps:get(sleep_ms, Opts, ?DEFAULT_SCAN_SLEEP_MS),
        extra_stats => ?SESSION_TOP_EXTRA_STATS,
        rpc_timeout => ?TOP_TIMEOUT
    }.

session_top_rows(Rows) ->
    [session_top_row(Row) || Row <- Rows].

session_top_row(Row) ->
    #{
        clientid => maps:get(clientid, Row),
        pid => maps:get(pid, Row, undefined),
        node => maps:get(node, Row),
        mqueue_length => stat_value(mqueue_len, Row),
        total_payload_bytes => stat_value(total_payload_bytes, Row),
        inflight_count => stat_value(inflight_cnt, Row)
    }.

stat_value(Key, #{extras := Extras} = Row) when is_map(Extras) ->
    case maps:get(Key, Extras, undefined) of
        undefined -> stat_metric_value(Key, Row);
        Value -> Value
    end;
stat_value(Key, Row) ->
    stat_metric_value(Key, Row).

stat_metric_value(mqueue_len, #{metric := mqueue_len, value := Value}) ->
    Value;
stat_metric_value(total_payload_bytes, #{metric := total_payload_bytes, value := Value}) ->
    Value;
stat_metric_value(inflight_cnt, #{metric := inflight_cnt, value := Value}) ->
    Value;
stat_metric_value(_Key, _Row) ->
    0.

sort_metric(mqueue_length) ->
    mqueue_len;
sort_metric(total_payload_bytes) ->
    total_payload_bytes.

write_csv(OutFile, Rows) ->
    case file:open(OutFile, [write, exclusive, raw, binary]) of
        {ok, IoDev} ->
            try
                write_csv_chunks(IoDev, [
                    <<"clientid,pid,node,mqueue_length,total_payload_bytes,inflight_count\n">>,
                    csv_rows(Rows)
                ])
            after
                _ = file:close(IoDev)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

write_csv_chunks(_IoDev, []) ->
    ok;
write_csv_chunks(IoDev, [Chunk | More]) ->
    case file:write(IoDev, Chunk) of
        ok -> write_csv_chunks(IoDev, More);
        {error, Reason} -> {error, Reason}
    end.

csv_rows(Rows) ->
    [csv_row(Row) || Row <- Rows].

csv_row(Row) ->
    [
        csv_cell(maps:get(clientid, Row)),
        <<",">>,
        csv_cell(maps:get(pid, Row)),
        <<",">>,
        csv_cell(maps:get(node, Row)),
        <<",">>,
        integer_to_binary(maps:get(mqueue_length, Row)),
        <<",">>,
        integer_to_binary(maps:get(total_payload_bytes, Row)),
        <<",">>,
        integer_to_binary(maps:get(inflight_count, Row)),
        $\n
    ].

csv_cell(Value) ->
    Bin = to_binary(Value),
    case needs_quote(Bin) of
        true -> [$", binary:replace(Bin, <<"\"">>, <<"\"\"">>, [global]), $"];
        false -> Bin
    end.

needs_quote(Bin) ->
    lists:any(
        fun(Pattern) -> binary:match(Bin, Pattern) =/= nomatch end,
        [<<",">>, <<"\"">>, <<"\n">>, <<"\r">>]
    ).

to_binary(Value) when is_binary(Value) ->
    Value;
to_binary(Value) when is_atom(Value) ->
    atom_to_binary(Value, utf8);
to_binary(Value) when is_pid(Value) ->
    list_to_binary(pid_to_list(Value)).

maybe_log_scan_down(normal) ->
    ok;
maybe_log_scan_down(shutdown) ->
    ok;
maybe_log_scan_down(Reason) ->
    ?SLOG(error, #{msg => session_top_scan_failed, reason => Reason}).

normalize_top_opts(Opts) ->
    StartedAt = maps:get(started_at, Opts, erlang:system_time(millisecond)),
    Opts#{
        batch_size => maps:get(batch_size, Opts, ?DEFAULT_SCAN_BATCH_SIZE),
        sleep_ms => maps:get(sleep_ms, Opts, ?DEFAULT_SCAN_SLEEP_MS),
        scan_id => maps:get(scan_id, Opts, {node(), StartedAt, erlang:unique_integer([positive])}),
        initiator => maps:get(initiator, Opts, node()),
        started_at => StartedAt
    }.

top_running_status(Pid, Opts, NodesTotal) ->
    #{
        status => running,
        pid => Pid,
        out => maps:get(out, Opts),
        count => maps:get(count, Opts),
        sort => maps:get(sort, Opts),
        batch_size => maps:get(batch_size, Opts),
        sleep_ms => maps:get(sleep_ms, Opts),
        scan_id => maps:get(scan_id, Opts),
        initiator => maps:get(initiator, Opts),
        started_at => maps:get(started_at, Opts),
        progress => #{nodes_total => NodesTotal, nodes_done => 0}
    }.

top_cancelled_status(Opts) ->
    #{
        status => cancelled,
        out => maps:get(out, Opts),
        reason => cancelled
    }.

top_scan_failed_status(Opts, Reason) ->
    #{
        status => failed,
        out => maps:get(out, Opts),
        reason => Reason
    }.

take_top_status(State = #{top_status := TopStatus}) ->
    Status = maps:get(status, TopStatus, idle),
    {TopStatus, maybe_reset_top_status(State, Status)}.

maybe_reset_top_status(State, Status) ->
    case is_terminal_top_status(Status) of
        true ->
            State#{top_status := #{status => idle}};
        false ->
            State
    end.

is_terminal_top_status(completed) -> true;
is_terminal_top_status(failed) -> true;
is_terminal_top_status(cancelled) -> true;
is_terminal_top_status(_) -> false.

ensure_out_file_absent(OutFile) ->
    case file:read_file_info(OutFile) of
        {ok, _Info} -> {error, eexist};
        {error, enoent} -> ok;
        {error, Reason} -> {error, Reason}
    end.

ensure_top_status(State = #{top_status := _}) ->
    State#{
        top => maps:get(top, State, maps:get(scan, State, undefined))
    };
ensure_top_status(State) ->
    State#{
        top => maps:get(top, State, maps:get(scan, State, undefined)),
        top_status => #{status => idle}
    }.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

put_conf(Conf0) ->
    Conf = normalize_conf(Conf0),
    case persistent_term:get(?CONF_KEY, undefined) of
        Conf ->
            ok;
        _ ->
            persistent_term:put(?CONF_KEY, Conf)
    end,
    Conf.

normalize_conf(undefined) ->
    ?DEFAULT_CONF;
normalize_conf(Conf) when is_map(Conf) ->
    maps:merge(?DEFAULT_CONF, maps:filter(fun(_Key, Value) -> Value =/= undefined end, Conf)).

stat_value(Key, Stats, Default) when is_map(Stats) ->
    maps:get(Key, Stats, Default);
stat_value(Key, Stats, Default) when is_list(Stats) ->
    proplists:get_value(Key, Stats, Default).
