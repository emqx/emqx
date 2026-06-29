%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_tool).

-moduledoc """
Operator-facing diagnostics for finding the top-K sessions by a session
gauge or counter (e.g. `mqueue_len', `mqueue_dropped', `inflight_cnt').

Intended use is from the remote console on a live cluster to locate the
small set of clients with a backlog or that are dropping messages, in
deployments with tens of thousands of connections, without paging through
the full client list.

Safe to run on a live cluster:
* Streams the `emqx_channel_info' ets table in chunks (via
  `emqx_utils_stream:ets/1'); never builds a whole-table list.
* Keeps only a fixed-size (top-K) max-heap while scanning; output is
  memory-bounded regardless of how many sessions exist.
* Yields between batches; chunk size and sleep are tunable.
* Reads the metric from the cached per-channel stats already stored in
  the registry; it never messages a channel process, so the scan adds no
  load to the connection processes themselves.

Freshness: the value read for each session is the last stats snapshot the
connection process published. Connections refresh their cached stats on
the per-zone stats timer (the idle timeout), so a value may lag live
state by up to that interval. When `stats.enable' is false for a zone its
connections only publish the snapshot taken at registration time; for
those sessions the gauge reflects connect-time state.

Scope: only `emqx_session_mem' sessions registered in the local channel
registry are scanned. Persistent (durable) sessions whose state lives in
DS rather than the channel registry are not yet covered (TODO: tag rows
with `engine => mem | persistent_ds' once supported).
""".

-include("emqx_cm.hrl").
-include("emqx_channel.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([
    top_by/1,
    top_by/2,
    scan/1,
    cluster_top_by/1,
    cluster_top_by/2,
    available_metrics/0
]).

%% Incremental scan engine (for a caller that owns the cursor/state).
-export([
    scan_acc_new/1,
    scan_acc/1,
    scan_acc_rows/1
]).

-export_type([scan_opts/0, row/0, scan_acc/0]).

-type metric() ::
    %% session gauges (see ?SESSION_STATS_KEYS)
    subscriptions_cnt
    | subscriptions_max
    | inflight_cnt
    | inflight_max
    | mqueue_len
    | mqueue_max
    | mqueue_dropped
    | awaiting_rel_cnt
    | awaiting_rel_max
    %% channel packet/message counters (see ?CHANNEL_METRICS)
    | recv_pkt
    | recv_msg
    | 'recv_msg.qos0'
    | 'recv_msg.qos1'
    | 'recv_msg.qos2'
    | 'recv_msg.dropped'
    | 'recv_msg.dropped.await_pubrel_timeout'
    | send_pkt
    | send_msg
    | 'send_msg.qos0'
    | 'send_msg.qos1'
    | 'send_msg.qos2'
    | 'send_msg.dropped'
    | 'send_msg.dropped.expired'
    | 'send_msg.dropped.queue_full'
    | 'send_msg.dropped.too_large'.

-type scan_opts() :: #{
    %% which cached session gauge/counter to rank by; required by scan/1
    %% (enforced at runtime), injected by top_by/2 and cluster_top_by/2
    metric => metric(),
    %% number of rows to return (default 20)
    top_k => pos_integer(),
    %% exclude sessions whose value is below this (default 1, i.e. skip 0)
    min_value => non_neg_integer(),
    %% rows per ets:select batch before sleeping (default 1000)
    chunk => pos_integer(),
    %% ms to sleep after each batch (default 50)
    sleep_ms => non_neg_integer(),
    %% extra cached info keys to attach to each result row, resolved from
    %% the cached session/clientinfo/conninfo maps, e.g. created_at,
    %% username, peername, connected_at, proto_ver
    extra_keys => [atom()],
    %% per-node RPC timeout (ms) for cluster_top_by/2 (default 30000);
    %% ignored by the single-node scan/1
    rpc_timeout => timeout()
}.

-type row() :: #{
    clientid := emqx_types:clientid(),
    node := node(),
    metric := metric(),
    value := number(),
    extras => #{atom() => term()}
}.

-type heap_entry() :: {number(), emqx_types:clientid(), pid()}.

%% One projected ets row: {ClientId, ChanPid, cached stats proplist}.
-type row_input() :: {emqx_types:clientid(), pid(), list()}.

%% Accumulating scan state: the bounded top-K heap plus a lazy stream over
%% the channel registry (the stream, not the accumulator, carries the ets
%% cursor) and the resolved options. Opaque to callers; advanced with
%% scan_acc/1.
-opaque scan_acc() :: #{
    metric := metric(),
    min_value := non_neg_integer(),
    top_k := pos_integer(),
    chunk := pos_integer(),
    extra_keys := [atom()],
    heap := gb_sets:set(heap_entry()),
    stream := emqx_utils_stream:stream(row_input())
}.

-define(DEFAULT_TOP_K, 20).
-define(DEFAULT_MIN_VALUE, 1).
-define(DEFAULT_CHUNK, 1000).
-define(DEFAULT_SLEEP_MS, 50).
-define(DEFAULT_RPC_TIMEOUT, 30_000).

%% Rankable session-level gauges, the meaningful subset of `?STATS_KEYS'
%% in emqx_session_mem. Kept in sync manually: that macro is
%% module-private and not exported via a header. Excluded on purpose:
%% `durable' (a boolean, not a gauge) and `next_pkt_id' (a wrapping
%% packet-id counter, an identifier rather than a magnitude) — ranking by
%% either is meaningless.
-define(SESSION_STATS_KEYS, [
    subscriptions_cnt,
    subscriptions_max,
    inflight_cnt,
    inflight_max,
    mqueue_len,
    mqueue_max,
    mqueue_dropped,
    awaiting_rel_cnt,
    awaiting_rel_max
]).

%%--------------------------------------------------------------------
%% Single-node scan
%%--------------------------------------------------------------------

-doc "Equivalent to `top_by(Metric, #{})'.".
-spec top_by(metric()) -> [row()].
top_by(Metric) ->
    top_by(Metric, #{}).

-doc """
Scan the local node and return the top-K sessions ranked by `Metric',
highest first. `Metric' must be one of `available_metrics/0'.
""".
-spec top_by(metric(), scan_opts()) -> [row()].
top_by(Metric, Opts) ->
    scan(Opts#{metric => Metric}).

-doc """
Scan the local node's sessions and return the top-K rows ranked by the
configured metric, highest value first.

Streams the channel registry in batches of `chunk' rows, sleeping
`sleep_ms' milliseconds between batches, while keeping only a top-K heap.
Sessions whose value is below `min_value' (default 1) are excluded, so a
session with a 0 gauge is never reported by default.
""".
-spec scan(scan_opts()) -> [row()].
scan(Opts) ->
    SleepMs = maps:get(sleep_ms, Opts, ?DEFAULT_SLEEP_MS),
    Acc = scan_to_end(scan_acc_new(Opts), SleepMs),
    scan_acc_rows(Acc).

%% All-at-once driver: advance the cursor to exhaustion, sleeping between
%% batches. A gen_server would instead drive scan_acc/1 from its own
%% timer/event loop (see the scan_acc/1 docs).
scan_to_end(Acc, SleepMs) ->
    case scan_acc(Acc) of
        {done, Acc1} ->
            Acc1;
        {continue, Acc1} ->
            %% Reclaim the just-processed batch (the ets:select result and
            %% its intermediate terms) at the yield point, so a long scan
            %% keeps a flat heap. The incremental engine leaves GC to the
            %% owning process on purpose.
            _ = erlang:garbage_collect(),
            ok = maybe_sleep(SleepMs),
            scan_to_end(Acc1, SleepMs)
    end.

%%--------------------------------------------------------------------
%% Incremental scan engine
%%--------------------------------------------------------------------

-doc """
Build the initial accumulator for an incremental scan from `Opts' (same
options as `scan/1', except `sleep_ms' which is the driver's concern).
Drive it with `scan_acc/1' and finalize with `scan_acc_rows/1'.
""".
-spec scan_acc_new(scan_opts()) -> scan_acc().
scan_acc_new(Opts) ->
    Chunk = maps:get(chunk, Opts, ?DEFAULT_CHUNK),
    #{
        metric => get_required_metric(Opts),
        min_value => maps:get(min_value, Opts, ?DEFAULT_MIN_VALUE),
        top_k => maps:get(top_k, Opts, ?DEFAULT_TOP_K),
        chunk => Chunk,
        extra_keys => maps:get(extra_keys, Opts, []),
        heap => gb_sets:empty(),
        stream => session_stream(Chunk)
    }.

-doc """
Advance an incremental scan by one `chunk'-sized batch.

Returns `{continue, Acc}' when there may be more rows (call again) or
`{done, Acc}' once the underlying stream is exhausted. The accumulator
holds the top-K heap and the remaining `emqx_utils_stream' (which carries
the ets cursor in its tail), so a caller (e.g. a gen_server) can hold it
between events, advance one batch per tick, and abort simply by dropping
it — calling `scan_acc_rows/1' on the partially advanced state still
yields the best rows seen so far.
""".
-spec scan_acc(scan_acc()) -> {continue | done, scan_acc()}.
scan_acc(#{stream := Stream, chunk := Chunk, heap := Heap0} = Acc0) ->
    Fold = fun({ClientId, ChanPid, Stats}, Heap) ->
        consider(ClientId, ChanPid, Stats, Acc0, Heap)
    end,
    {Heap, Rest} = emqx_utils_stream:fold(Fold, Heap0, Chunk, Stream),
    Acc = Acc0#{heap := Heap, stream := Rest},
    case Rest of
        [] -> {done, Acc};
        _ -> {continue, Acc}
    end.

-doc """
Finalize an incremental scan: turn the accumulated top-K heap into the
ranked rows (highest value first), resolving any `extra_keys'. Safe to
call at any point, including after an early abort, to read the partial
result.
""".
-spec scan_acc_rows(scan_acc()) -> [row()].
scan_acc_rows(#{heap := Heap, metric := Metric, extra_keys := ExtraKeys}) ->
    %% gb_sets:to_list is ascending; reverse for highest-first.
    Candidates = lists:reverse(gb_sets:to_list(Heap)),
    [resolve_row(C, Metric, ExtraKeys) || C <- Candidates].

%% Lazy stream over the channel registry, scanned in `Chunk'-sized ets
%% batches. Each row is projected to {ClientId, ChanPid, Stats}; Stats is
%% the cached stats proplist (the metric lives here). The larger info map
%% is left in the table and only fetched for the K winners (resolve_row/3).
-spec session_stream(pos_integer()) -> emqx_utils_stream:stream(row_input()).
session_stream(Chunk) ->
    MatchSpec = ets:fun2ms(
        fun({{ClientId, ChanPid}, _Info, Stats}) -> {ClientId, ChanPid, Stats} end
    ),
    emqx_utils_stream:ets(fun
        (undefined) -> ets:select(?CHAN_INFO_TAB, MatchSpec, Chunk);
        (Cont) -> ets:select(Cont)
    end).

%% Read the metric from the cached stats proplist and, if it qualifies,
%% offer it to the bounded heap.
consider(ClientId, ChanPid, Stats, #{metric := Metric, min_value := MinValue, top_k := TopK}, Heap) ->
    case read_metric(Metric, Stats) of
        Value when is_number(Value), Value >= MinValue ->
            heap_offer({Value, ClientId, ChanPid}, TopK, Heap);
        _ ->
            Heap
    end.

read_metric(Metric, Stats) when is_list(Stats) ->
    proplists:get_value(Metric, Stats, 0);
read_metric(_Metric, _Stats) ->
    0.

%% Bounded max-heap as a gb_sets ordered on {Value, ClientId, ChanPid}.
%% While below capacity, always insert. Once full, evict the current
%% smallest only when the candidate is larger.
heap_offer(Candidate, TopK, Heap) ->
    case gb_sets:size(Heap) < TopK of
        true ->
            gb_sets:add(Candidate, Heap);
        false ->
            {{SmallestValue, _, _}, Heap1} = gb_sets:take_smallest(Heap),
            {CandValue, _, _} = Candidate,
            case CandValue > SmallestValue of
                true -> gb_sets:add(Candidate, Heap1);
                false -> Heap
            end
    end.

maybe_sleep(SleepMs) when is_integer(SleepMs), SleepMs > 0 ->
    timer:sleep(SleepMs);
maybe_sleep(_SleepMs) ->
    ok.

%% Build the result row. Extras are resolved here, once per winner, by
%% re-reading the cached info map. If the session has since disconnected
%% the info row is gone and extras come back empty (best effort).
resolve_row({Value, ClientId, ChanPid}, Metric, ExtraKeys) ->
    Base = #{
        clientid => ClientId,
        node => node(),
        metric => Metric,
        value => Value
    },
    case ExtraKeys of
        [] ->
            Base;
        _ ->
            Info = lookup_info(ClientId, ChanPid),
            Base#{extras => maps:from_list([{K, read_extra(K, Info)} || K <- ExtraKeys])}
    end.

lookup_info(ClientId, ChanPid) ->
    case ets:lookup(?CHAN_INFO_TAB, {ClientId, ChanPid}) of
        [{_Chan, Info, _Stats}] when is_map(Info) -> Info;
        _ -> #{}
    end.

%% Resolve an extra key against the cached channel info map. The info map
%% nests `session', `clientinfo' and `conninfo' sub-maps; search those
%% (most specific first) then the top level.
read_extra(Key, Info) ->
    Session = maps:get(session, Info, #{}),
    ClientInfo = maps:get(clientinfo, Info, #{}),
    ConnInfo = maps:get(conninfo, Info, #{}),
    find_first(Key, [Session, ClientInfo, ConnInfo, Info]).

find_first(_Key, []) ->
    undefined;
find_first(Key, [Map | Rest]) ->
    case is_map(Map) andalso maps:is_key(Key, Map) of
        true -> maps:get(Key, Map);
        false -> find_first(Key, Rest)
    end.

%%--------------------------------------------------------------------
%% Cluster-wide scan
%%--------------------------------------------------------------------

-doc "Equivalent to `cluster_top_by(Metric, #{})'.".
-spec cluster_top_by(metric()) -> [row()].
cluster_top_by(Metric) ->
    cluster_top_by(Metric, #{}).

-doc """
Scan every running cluster node and return the cluster-wide top-K rows
ranked by `Metric', highest value first.

Each node scans its own session set (no cross-node ets traversal); the
per-node top-K heaps are then merged here and re-trimmed to a single
top-K. Nodes whose scan fails are skipped (their rows are simply absent
from the result).
""".
-spec cluster_top_by(metric(), scan_opts()) -> [row()].
cluster_top_by(Metric, Opts0) ->
    Opts = Opts0#{metric => Metric},
    TopK = maps:get(top_k, Opts, ?DEFAULT_TOP_K),
    Timeout = maps:get(rpc_timeout, Opts, ?DEFAULT_RPC_TIMEOUT),
    Nodes = emqx:running_nodes(),
    Results = emqx_session_tool_proto_v1:scan(Nodes, Opts, Timeout),
    Rows = lists:append([NodeRows || {ok, NodeRows} <- Results, is_list(NodeRows)]),
    Sorted = lists:sort(fun(#{value := A}, #{value := B}) -> A >= B end, Rows),
    lists:sublist(Sorted, TopK).

%%--------------------------------------------------------------------
%% Metrics
%%--------------------------------------------------------------------

-doc """
Return the metric keys accepted by `scan/1' and friends: the session
stats (`mqueue_len', `mqueue_dropped', `inflight_cnt', ...) plus the
per-channel packet/message counters (`recv_msg', `send_msg', the
`*.dropped.*' counters, ...).
""".
-spec available_metrics() -> [metric()].
available_metrics() ->
    ?SESSION_STATS_KEYS ++ ?CHANNEL_METRICS.

get_required_metric(Opts) ->
    case maps:find(metric, Opts) of
        {ok, Metric} when is_atom(Metric) ->
            assert_supported(Metric),
            Metric;
        _ ->
            error({missing_required_option, metric})
    end.

assert_supported(Metric) ->
    case lists:member(Metric, available_metrics()) of
        true -> ok;
        false -> error({unsupported_metric, Metric, available_metrics()})
    end.
