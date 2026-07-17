%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_top_scanner).

-behaviour(gen_server).

-include("logger.hrl").

-export([
    start_link/0,
    start_scan/1,
    cancel/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(DEFAULT_SCAN_BATCH_SIZE, 1000).
-define(DEFAULT_SCAN_SLEEP_MS, 1).
-define(SESSION_TOP_EXTRA_STATS, [mqueue_len, total_payload_bytes, inflight_cnt]).
-define(CALL_TIMEOUT, 5000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec start_scan(map()) -> {ok, accepted} | {error, {busy, node()}}.
start_scan(Opts) ->
    gen_server:call(?MODULE, {start_scan, Opts}, ?CALL_TIMEOUT).

-spec cancel(term()) -> {ok, cancelled}.
cancel(ScanId) ->
    gen_server:call(?MODULE, {cancel, ScanId}, ?CALL_TIMEOUT).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{scan => undefined}}.

handle_call({start_scan, Opts0}, _From, State = #{scan := undefined}) ->
    Opts = normalize_opts(Opts0),
    Collector = maps:get(collector, Opts),
    Scan = #{
        scan_id => maps:get(scan_id, Opts),
        opts => Opts,
        acc => emqx_session_tool:scan_acc_new(session_tool_opts(Opts)),
        collector_monitor => erlang:monitor(
            process, {emqx_session_top_collector, Collector}
        )
    },
    {reply, {ok, accepted}, schedule_tick(State#{scan := Scan}, 0)};
handle_call({start_scan, _Opts}, _From, State = #{scan := #{opts := CurrentOpts}}) ->
    {reply, {error, {busy, maps:get(collector, CurrentOpts)}}, State};
handle_call({cancel, ScanId}, _From, State = #{scan := #{scan_id := ScanId}}) ->
    {reply, {ok, cancelled}, cancel_scan(State, cancelled)};
handle_call({cancel, _ScanId}, _From, State) ->
    {reply, {ok, cancelled}, State};
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({scan_next, ScanId}, State = #{scan := #{scan_id := ScanId}}) ->
    advance_scan(State);
handle_info(
    {'DOWN', Monitor, process, {emqx_session_top_collector, Collector}, Reason},
    State = #{scan := #{collector_monitor := Monitor}}
) ->
    ?SLOG(info, #{
        msg => session_top_collector_down,
        collector => Collector,
        reason => Reason
    }),
    {noreply, abort_scan(State)};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Incremental local scan
%%--------------------------------------------------------------------

advance_scan(State = #{scan := Scan0}) ->
    #{acc := Acc0} = Scan0,
    try emqx_session_tool:scan_acc(Acc0) of
        {continue, Acc1} ->
            _ = erlang:garbage_collect(),
            Scan = Scan0#{acc := Acc1},
            {noreply, schedule_tick(State#{scan := Scan}, sleep_ms(Scan))};
        {done, Acc1} ->
            Rows = emqx_session_top_collector:normalize_rows(
                emqx_session_tool:scan_acc_rows(Acc1)
            ),
            {noreply, complete_scan(State, {ok, Rows})}
    catch
        Class:Reason:Stack ->
            ?SLOG(error, #{
                msg => session_top_scan_failed,
                class => Class,
                reason => Reason,
                stacktrace => Stack
            }),
            {noreply, complete_scan(State, {error, {Class, Reason}})}
    end.

complete_scan(State = #{scan := Scan = #{scan_id := ScanId, opts := Opts}}, Result) ->
    demonitor_collector(Scan),
    notify_collector(Opts, ScanId, Result),
    State#{scan := undefined}.

cancel_scan(State = #{scan := Scan = #{scan_id := ScanId, opts := Opts}}, Reason) ->
    cancel_timer(Scan),
    demonitor_collector(Scan),
    notify_collector(Opts, ScanId, {error, Reason}),
    State#{scan := undefined}.

abort_scan(State = #{scan := Scan}) ->
    cancel_timer(Scan),
    State#{scan := undefined}.

notify_collector(Opts, ScanId, Result) ->
    Collector = maps:get(collector, Opts, node()),
    ok = emqx_session_top_proto_v1:top_scan_result(Collector, ScanId, node(), Result).

schedule_tick(State = #{scan := Scan = #{scan_id := ScanId}}, Delay) ->
    TRef = erlang:send_after(Delay, self(), {scan_next, ScanId}),
    State#{scan := Scan#{timer => TRef}}.

cancel_timer(#{timer := TRef}) ->
    _ = erlang:cancel_timer(TRef),
    ok.

demonitor_collector(#{collector_monitor := Monitor}) ->
    _ = erlang:demonitor(Monitor, [flush]),
    ok.

sleep_ms(#{opts := Opts}) ->
    maps:get(sleep_ms, Opts, ?DEFAULT_SCAN_SLEEP_MS).

session_tool_opts(Opts) ->
    #{
        metric => sort_metric(maps:get(sort, Opts)),
        top_k => maps:get(count, Opts),
        min_value => 0,
        chunk => maps:get(batch_size, Opts, ?DEFAULT_SCAN_BATCH_SIZE),
        sleep_ms => maps:get(sleep_ms, Opts, ?DEFAULT_SCAN_SLEEP_MS),
        extra_stats => ?SESSION_TOP_EXTRA_STATS
    }.

sort_metric(mqueue_length) -> mqueue_len;
sort_metric(total_payload_bytes) -> total_payload_bytes.

normalize_opts(Opts) ->
    ScanId = maps:get(
        scan_id,
        Opts,
        {node(), erlang:system_time(millisecond), erlang:unique_integer([positive])}
    ),
    Opts#{
        batch_size => maps:get(batch_size, Opts, ?DEFAULT_SCAN_BATCH_SIZE),
        sleep_ms => maps:get(sleep_ms, Opts, ?DEFAULT_SCAN_SLEEP_MS),
        scan_id => ScanId,
        collector => maps:get(collector, Opts, node())
    }.
