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
    start_top_scan/1,
    cancel_top_scan/1,
    top_scan_result/3,
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
-define(TOP_CONTROL_TIMEOUT, 5000).
-define(TOP_BPAPI_VSN, 1).
-define(DEFAULT_SCAN_BATCH_SIZE, 1000).
-define(DEFAULT_SCAN_SLEEP_MS, 1).
-define(SESSION_TOP_EXTRA_STATS, [mqueue_len, total_payload_bytes, inflight_cnt]).

-type stats() :: emqx_types:stats() | map().
-type sort_by() :: mqueue_length | total_payload_bytes.
-type top_status() ::
    #{status := idle}
    | #{
        status := running,
        role := collector | worker,
        out => file:name_all(),
        count := pos_integer(),
        sort := sort_by(),
        batch_size := pos_integer(),
        sleep_ms := non_neg_integer(),
        scan_id := term(),
        started_at := integer(),
        initiator := node(),
        collector := node(),
        progress := map()
    }
    | #{
        status := completed,
        role => collector | worker,
        out => file:name_all(),
        scan_id => term(),
        initiator => node(),
        collector => node(),
        started_at => integer(),
        completed_at => integer(),
        rows := non_neg_integer(),
        partial => boolean(),
        bad_replies => [{node(), term()}]
    }
    | #{
        status := failed,
        role => collector | worker,
        out => file:name_all(),
        scan_id => term(),
        initiator => node(),
        collector => node(),
        started_at => integer(),
        reason := term(),
        partial => boolean(),
        bad_replies => [{node(), term()}]
    }
    | #{
        status := cancelled,
        role => collector | worker,
        out => file:name_all(),
        scan_id => term(),
        initiator => node(),
        collector => node(),
        started_at => integer(),
        reason := term()
    }.
-type row() :: #{
    clientid := emqx_types:clientid(),
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
    {ok, term()} | {error, busy | eexist | term()}.
run_top(Opts) ->
    gen_server:call(?MODULE, {run_top, Opts}, infinity).

-spec cancel_top() -> {ok, cancelled} | {error, not_running}.
cancel_top() ->
    gen_server:call(?MODULE, cancel_top, infinity).

-spec top_status() -> top_status().
top_status() ->
    gen_server:call(?MODULE, top_status, infinity).

-spec start_top_scan(map()) -> {ok, accepted} | {error, busy | term()}.
start_top_scan(Opts) ->
    gen_server:call(?MODULE, {start_top_scan, Opts}, infinity).

-spec cancel_top_scan(term()) -> {ok, cancelled} | {error, not_running}.
cancel_top_scan(ScanId) ->
    gen_server:call(?MODULE, {cancel_top_scan, ScanId}, infinity).

-spec top_scan_result(term(), node(), {ok, [row()]} | {error, term()}) -> ok.
top_scan_result(ScanId, Node, Result) ->
    gen_server:cast(?MODULE, {top_scan_result, ScanId, Node, Result}).

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
        ok -> start_collector_top(Opts, State);
        {error, Reason} -> {reply, {error, Reason}, State}
    end;
handle_call({run_top, _Opts}, _From, State) ->
    {reply, {error, busy}, State};
handle_call({start_top_scan, Opts0}, _From, State = #{top := undefined}) ->
    Opts = normalize_top_opts(Opts0),
    Top = #{
        role => worker,
        scan_id => maps:get(scan_id, Opts),
        opts => Opts,
        nodes => [node()],
        local => new_local_top_scan(Opts)
    },
    State1 = State#{
        top := Top,
        top_status := top_running_status(Top)
    },
    {reply, {ok, accepted}, schedule_top_tick(State1, 0)};
handle_call({start_top_scan, _Opts}, _From, State) ->
    {reply, {error, busy}, State};
handle_call(cancel_top, _From, State = #{top := Top = #{role := collector, scan_id := ScanId}}) ->
    cancel_remote_top_scans(maps:get(pending, Top, []) -- [node()], ScanId),
    {reply, {ok, cancelled}, cancel_top_scan_state(State, cancelled, false)};
handle_call(cancel_top, _From, State = #{top := #{role := worker}}) ->
    {reply, {ok, cancelled}, cancel_top_scan_state(State, cancelled, true)};
handle_call(cancel_top, _From, State) ->
    {reply, {error, not_running}, State};
handle_call({cancel_top_scan, ScanId}, _From, State = #{top := #{scan_id := ScanId}}) ->
    {reply, {ok, cancelled}, cancel_top_scan_state(State, cancelled, true)};
handle_call({cancel_top_scan, _ScanId}, _From, State) ->
    {reply, {error, not_running}, State};
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
handle_cast(
    {top_scan_result, ScanId, Node, Result},
    State = #{top := #{role := collector, scan_id := ScanId}}
) ->
    {noreply, complete_collector_node(Node, Result, State)};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({top_scan_next, ScanId}, State = #{top := #{scan_id := ScanId}}) ->
    advance_top_scan(State);
handle_info(
    {top_scan_timeout, ScanId},
    State = #{top := #{role := collector, scan_id := ScanId} = Top}
) ->
    {noreply, finish_collector_top(State#{top := timeout_collector_top(Top)})};
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

top_scan_nodes() ->
    try emqx_bpapi:nodes_supporting_bpapi_version(emqx_session_buffer_mon, ?TOP_BPAPI_VSN) of
        Nodes -> Nodes
    catch
        _:_ -> emqx:running_nodes()
    end.

start_remote_top_scans([], _Req) ->
    {[], []};
start_remote_top_scans(Nodes, Req) ->
    Replies = emqx_session_buffer_mon_proto_v1:start_top_scan(Nodes, Req, ?TOP_CONTROL_TIMEOUT),
    lists:foldl(
        fun({Node, Reply}, {Started, Bad}) ->
            case top_start_reply(Reply) of
                ok -> {[Node | Started], Bad};
                {error, Reason} -> {Started, [{Node, Reason} | Bad]}
            end
        end,
        {[], []},
        lists:zip(Nodes, Replies)
    ).

top_start_reply({ok, {ok, accepted}}) -> ok;
top_start_reply({ok, {error, Reason}}) -> {error, Reason};
top_start_reply({error, Reason}) -> {error, Reason};
top_start_reply(Reply) -> {error, Reply}.

start_collector_top(Opts, State) ->
    Nodes = lists:usort([node() | top_scan_nodes()]),
    RemoteNodes = Nodes -- [node()],
    CollectorOpts = Opts#{collector => node(), nodes => Nodes},
    RemoteReq = maps:without([out], CollectorOpts),
    {StartedRemoteNodes, BadReplies} = start_remote_top_scans(RemoteNodes, RemoteReq),
    PendingNodes = lists:usort([node() | StartedRemoteNodes]),
    Top = #{
        role => collector,
        scan_id => maps:get(scan_id, Opts),
        opts => CollectorOpts,
        nodes => Nodes,
        pending => PendingNodes,
        rows_by_node => #{},
        bad_replies => BadReplies,
        local => new_local_top_scan(Opts),
        timeout_timer => undefined
    },
    Top1 = schedule_top_timeout(Top),
    State1 = State#{
        top := Top1,
        top_status := top_running_status(Top1)
    },
    {reply, {ok, maps:get(scan_id, Opts)}, schedule_top_tick(State1, 0)}.

cancel_remote_top_scans([], _ScanId) ->
    ok;
cancel_remote_top_scans(Nodes, ScanId) ->
    _ = emqx_session_buffer_mon_proto_v1:cancel_top_scan(Nodes, ScanId, ?TOP_CONTROL_TIMEOUT),
    ok.

new_local_top_scan(Opts) ->
    #{
        acc => emqx_session_tool:scan_acc_new(session_tool_opts(Opts)),
        batches_done => 0,
        timer => undefined
    }.

schedule_top_tick(State = #{top := Top = #{scan_id := ScanId, local := Local}}, Delay) ->
    TRef = erlang:send_after(Delay, self(), {top_scan_next, ScanId}),
    State#{top := Top#{local := Local#{timer := TRef}}}.

schedule_top_timeout(Top = #{scan_id := ScanId}) ->
    TRef = erlang:send_after(?TOP_TIMEOUT, self(), {top_scan_timeout, ScanId}),
    Top#{timeout_timer := TRef}.

advance_top_scan(State = #{top := Top = #{local := Local0}}) ->
    #{acc := Acc0, batches_done := Batches0} = Local0,
    try emqx_session_tool:scan_acc(Acc0) of
        {continue, Acc1} ->
            _ = erlang:garbage_collect(),
            Local = Local0#{acc := Acc1, batches_done := Batches0 + 1, timer := undefined},
            Top1 = Top#{local := Local},
            State1 = State#{
                top := Top1,
                top_status := top_running_status(Top1)
            },
            {noreply, schedule_top_tick(State1, top_sleep_ms(Top1))};
        {done, Acc1} ->
            Local = Local0#{acc := Acc1, batches_done := Batches0 + 1, timer := undefined},
            Rows = session_top_rows(emqx_session_tool:scan_acc_rows(Acc1)),
            complete_local_top(Rows, State#{top := Top#{local := Local}})
    catch
        Class:Reason:Stack ->
            ?SLOG(error, #{
                msg => session_top_scan_failed,
                class => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            complete_local_top({error, {Class, Reason}}, State)
    end.

complete_local_top(
    {error, Reason}, State = #{top := Top = #{role := worker, scan_id := ScanId, opts := Opts}}
) ->
    notify_collector(Opts, ScanId, {error, Reason}),
    {noreply, State#{top := undefined, top_status := top_failed_status(Top, Reason)}};
complete_local_top(
    Rows, State = #{top := Top = #{role := worker, scan_id := ScanId, opts := Opts}}
) ->
    notify_collector(Opts, ScanId, result_reply(Rows)),
    Status = top_completed_status(Top, row_count(Rows)),
    {noreply, State#{top := undefined, top_status := Status}};
complete_local_top(Rows, State = #{top := #{role := collector}}) ->
    {noreply, complete_collector_node(node(), result_reply(Rows), State)}.

result_reply({error, Reason}) -> {error, Reason};
result_reply(Rows) -> {ok, Rows}.

row_count({error, _Reason}) -> 0;
row_count(Rows) -> length(Rows).

notify_collector(Opts, ScanId, Result) ->
    Collector = maps:get(collector, Opts, node()),
    ok = emqx_session_buffer_mon_proto_v1:top_scan_result(Collector, ScanId, node(), Result).

complete_collector_node(
    Node,
    Result,
    State = #{top := Top = #{pending := Pending0, rows_by_node := RowsByNode0, bad_replies := Bad0}}
) ->
    case lists:member(Node, Pending0) of
        false ->
            State;
        true ->
            Pending = lists:delete(Node, Pending0),
            {RowsByNode, Bad} =
                case Result of
                    {ok, Rows0} ->
                        Rows = normalize_result_rows(Rows0),
                        {RowsByNode0#{Node => Rows}, Bad0};
                    {error, Reason} ->
                        {RowsByNode0, [{Node, Reason} | Bad0]}
                end,
            Top1 = Top#{
                pending := Pending,
                rows_by_node := RowsByNode,
                bad_replies := Bad
            },
            case Pending of
                [] -> finish_collector_top(State#{top := Top1});
                _ -> State#{top := Top1, top_status := top_running_status(Top1)}
            end
    end.

finish_collector_top(State = #{top := Top = #{opts := Opts, rows_by_node := RowsByNode}}) ->
    cancel_top_scan_timers(Top),
    Rows0 = lists:append(maps:values(RowsByNode)),
    Rows = top_rows(Rows0, maps:get(sort, Opts), maps:get(count, Opts)),
    OutFile = maps:get(out, Opts),
    case write_csv(OutFile, Rows) of
        ok ->
            ?SLOG(info, #{
                msg => session_top_written,
                file => OutFile,
                rows => length(Rows)
            }),
            State#{
                top := undefined,
                top_status := with_bad_replies(top_completed_status(Top, length(Rows)), Top)
            };
        {error, Reason} ->
            ?SLOG(error, #{
                msg => session_top_write_failed,
                file => OutFile,
                reason => Reason
            }),
            State#{
                top := undefined,
                top_status := with_bad_replies(top_failed_status(Top, Reason), Top)
            }
    end.

top_rows(Rows, SortBy, Count) ->
    Sorted = lists:sort(
        fun(RowA, RowB) ->
            maps:get(SortBy, RowA) >= maps:get(SortBy, RowB)
        end,
        Rows
    ),
    lists:sublist(Sorted, Count).

normalize_result_rows(Rows) ->
    [normalize_result_row(Row) || Row <- Rows].

normalize_result_row(#{mqueue_length := _, total_payload_bytes := _, inflight_count := _} = Row) ->
    Row;
normalize_result_row(Row) ->
    session_top_row(Row).

timeout_collector_top(Top = #{pending := Pending, bad_replies := BadReplies}) ->
    Top#{
        pending := [],
        bad_replies := [{Node, timeout} || Node <- Pending] ++ BadReplies
    }.

cancel_top_scan_state(State = #{top := Top}, Reason, NotifyCollector) ->
    cancel_top_scan_timers(Top),
    case NotifyCollector of
        true -> notify_collector_cancelled(Top, Reason);
        false -> ok
    end,
    State#{
        top := undefined,
        top_status := top_cancelled_status(Top, Reason)
    }.

cancel_top_scan_timers(Top) ->
    cancel_local_top_timer(Top),
    cancel_top_timeout_timer(Top).

cancel_local_top_timer(#{local := #{timer := undefined}}) ->
    ok;
cancel_local_top_timer(#{local := #{timer := TRef}}) ->
    _ = erlang:cancel_timer(TRef),
    ok.

cancel_top_timeout_timer(#{timeout_timer := undefined}) ->
    ok;
cancel_top_timeout_timer(#{timeout_timer := TRef}) ->
    _ = erlang:cancel_timer(TRef),
    ok;
cancel_top_timeout_timer(_Top) ->
    ok.

notify_collector_cancelled(#{role := worker, scan_id := ScanId, opts := Opts}, Reason) ->
    notify_collector(Opts, ScanId, {error, Reason});
notify_collector_cancelled(_Top, _Reason) ->
    ok.

top_sleep_ms(#{opts := Opts}) ->
    maps:get(sleep_ms, Opts, ?DEFAULT_SCAN_SLEEP_MS).

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
    case open_new_csv_file(OutFile) of
        {ok, IoDev} ->
            try
                write_csv_chunks(IoDev, [
                    <<"clientid,node,mqueue_length,total_payload_bytes,inflight_count\n">>,
                    csv_rows(Rows)
                ])
            after
                _ = file:close(IoDev)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

open_new_csv_file(OutFile) ->
    file:open(OutFile, [write, exclusive, raw, binary]).

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
    atom_to_binary(Value, utf8).

normalize_top_opts(Opts) ->
    StartedAt = maps:get(started_at, Opts, erlang:system_time(millisecond)),
    Opts#{
        batch_size => maps:get(batch_size, Opts, ?DEFAULT_SCAN_BATCH_SIZE),
        sleep_ms => maps:get(sleep_ms, Opts, ?DEFAULT_SCAN_SLEEP_MS),
        scan_id => maps:get(scan_id, Opts, {node(), StartedAt, erlang:unique_integer([positive])}),
        initiator => maps:get(initiator, Opts, node()),
        collector => maps:get(collector, Opts, node()),
        started_at => StartedAt
    }.

top_running_status(Top = #{role := Role, opts := Opts}) ->
    Local = maps:get(local, Top, #{}),
    Nodes = maps:get(nodes, Top, [node()]),
    Pending = maps:get(pending, Top, Nodes),
    BadReplies = maps:get(bad_replies, Top, []),
    Base0 = #{
        status => running,
        role => Role,
        count => maps:get(count, Opts),
        sort => maps:get(sort, Opts),
        batch_size => maps:get(batch_size, Opts),
        sleep_ms => maps:get(sleep_ms, Opts),
        scan_id => maps:get(scan_id, Opts),
        initiator => maps:get(initiator, Opts),
        collector => maps:get(collector, Opts, node()),
        started_at => maps:get(started_at, Opts),
        progress => top_progress(Role, Nodes, Pending, Local, BadReplies)
    },
    with_optional_out(Base0, Opts).

top_progress(
    collector,
    Nodes,
    Pending,
    _Local,
    BadReplies
) ->
    Progress = #{
        nodes_total => length(Nodes),
        nodes_done => length(Nodes) - length(Pending),
        nodes_pending => length(Pending)
    },
    with_progress_problem(Progress, BadReplies);
top_progress(worker, _Nodes, _Pending, Local, _BadReplies) ->
    #{batches_done => maps:get(batches_done, Local, 0)}.

with_progress_problem(Progress, []) ->
    Progress;
with_progress_problem(Progress, BadReplies) ->
    Progress#{bad_replies => lists:reverse(BadReplies)}.

top_completed_status(#{role := Role, opts := Opts}, Rows) ->
    Status0 = #{
        status => completed,
        role => Role,
        scan_id => maps:get(scan_id, Opts),
        initiator => maps:get(initiator, Opts),
        collector => maps:get(collector, Opts, node()),
        started_at => maps:get(started_at, Opts),
        completed_at => erlang:system_time(millisecond),
        rows => Rows
    },
    with_optional_out(Status0, Opts).

top_cancelled_status(#{role := Role, opts := Opts}, Reason) ->
    Status0 = #{
        status => cancelled,
        role => Role,
        scan_id => maps:get(scan_id, Opts),
        initiator => maps:get(initiator, Opts),
        collector => maps:get(collector, Opts, node()),
        started_at => maps:get(started_at, Opts),
        reason => Reason
    },
    with_optional_out(Status0, Opts).

top_failed_status(Top, Reason) ->
    #{opts := Opts} = Top,
    Status0 = #{
        status => failed,
        role => maps:get(role, Top),
        scan_id => maps:get(scan_id, Opts),
        initiator => maps:get(initiator, Opts),
        collector => maps:get(collector, Opts, node()),
        started_at => maps:get(started_at, Opts),
        reason => Reason
    },
    with_optional_out(Status0, Opts).

with_optional_out(Status, #{out := OutFile}) ->
    Status#{out => OutFile};
with_optional_out(Status, _Opts) ->
    Status.

with_bad_replies(Status, #{bad_replies := []}) ->
    Status;
with_bad_replies(Status, #{bad_replies := BadReplies}) ->
    Status#{
        partial => true,
        bad_replies => lists:reverse(BadReplies)
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
