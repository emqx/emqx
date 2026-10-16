%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_intake).

%% L1 acceptance cache: a bounded, node-local, ordered ETS queue holding
%% fully-resolved BatchPub QoS=1 requests. API workers insert in parallel
%% (lock-free) and the HTTP 200 is sent right after the insert; the
%% per-core promoter drains batches in order and promotes them into mria
%% (the durability point). Entries still in the queue are lost on node
%% crash by contract ("queued, not yet promoted, may be lost").

-behaviour(gen_server).

-export([start_link/0]).
-export([
    enqueue/1,
    requeue/1,
    backoff_ms/1,
    sweep_deferred/0,
    take_batch/2,
    delete_batch/1,
    depth/0,
    deferred_depth/0,
    admission_depth/0,
    reset/0
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export_type([entry/0]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

%% One fully-resolved QoS=1 request:
%%   payload        :: binary()          -- full message payload (refc binary,
%%                                          shared by reference, no copy)
%%   hash           :: binary()          -- sha256(payload)
%%   api_msg_id     :: binary()          -- API-facing MessageId (UUID string)
%%   msg_id         :: binary()          -- internal message id
%%   delivery_id    :: binary()          -- unique per request
%%   product_key    :: binary()
%%   topic_template :: binary()
%%   devices        :: [binary()]
%%   created_at     :: non_neg_integer() -- seconds (intake time)
%%   expires_at     :: non_neg_integer() -- seconds
%%   not_before     :: non_neg_integer() -- ms, monotonic; set only while the
%%                                          entry waits out a retry backoff
%%   defer_attempts :: non_neg_integer() -- how many times it was deferred
-type entry() :: #{
    payload := binary(),
    hash := binary(),
    api_msg_id := binary(),
    msg_id := binary(),
    delivery_id := binary(),
    product_key := binary(),
    topic_template := binary(),
    devices := [binary()],
    created_at := non_neg_integer(),
    expires_at := non_neg_integer(),
    not_before => non_neg_integer(),
    defer_attempts => non_neg_integer()
}.

%% Backoff of a batch that the promoter could not append to the per-device
%% index after ?APPEND_RETRY_MAX in-worker attempts. It is never dropped: the
%% batch is already committed in mria, so dropping it would leave devices
%% permanently without an index entry (accepted requests would never be
%% delivered). Doubling from 100ms and capped at 30s keeps a permanently
%% unreachable shard retried forever without occupying a promoter worker -
%% the failure mode where N permanently failing batches pinned every worker
%% and the intake queue stopped draining.
-define(DEFER_BACKOFF_BASE_MS, 100).
-define(DEFER_BACKOFF_MAX_MS, 30000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    _ = ets:new(?TAB_INT_Q, [
        named_table, ordered_set, public, {write_concurrency, true}, {read_concurrency, true}
    ]),
    _ = ets:new(?TAB_INT_SEQ, [named_table, set, public, {write_concurrency, true}]),
    _ = ets:new(?TAB_INT_DEFER, [
        named_table, ordered_set, public, {write_concurrency, true}, {read_concurrency, true}
    ]),
    true = ets:insert(?TAB_INT_SEQ, {seq, 0}),
    {ok, #{}}.

%% Insert one request. Returns {ok, Seq} or `full` when the bounded queue
%% is at capacity; the caller turns `full` into HTTP 429 backpressure. The
%% queue never grows unboundedly.
%%
%% The depth check and the insert are separate operations, so concurrent
%% enqueues can overshoot ?INTAKE_QUEUE_DEPTH by at most (concurrent enqueues -
%% 1): the bound exists to bound memory and to turn sustained overload into
%% 429s, not to be exact, and an atomic reservation per request would put a
%% contended counter on the acceptance path for that. The drain side bounds the
%% queue anyway (a bounded batch per worker per round).
-spec enqueue(entry()) -> {ok, non_neg_integer()} | full.
enqueue(Entry) ->
    MaxDepth = ?INTAKE_QUEUE_DEPTH,
    case admission_depth() >= MaxDepth of
        true ->
            emqx_bcast_metrics:intake_rejected(),
            full;
        false ->
            Seq = ets:update_counter(?TAB_INT_SEQ, seq, 1),
            true = ets:insert(?TAB_INT_Q, {Seq, Entry}),
            emqx_bcast_metrics:intake_enqueued(),
            {ok, Seq}
    end.

%% Everything accepted and not yet promoted on this node: the ready queue plus
%% the batches the promoter handed back for a later retry. The deferred
%% entries count towards the bound on purpose. A batch only reaches the
%% deferred table after a worker took it out of the ready queue, so without
%% them in the total an unavailable index shard would keep the ready queue
%% shallow - acceptance would keep answering 200 for work that can never be
%% indexed, and the deferred table (payload included) would grow without limit
%% instead of the overload turning into 429 backpressure.
-spec admission_depth() -> non_neg_integer().
admission_depth() ->
    queue_size() + deferred_depth().

%% Put a batch that the promoter already took (and already committed in mria)
%% back for a later retry, delaying each entry by a doubling backoff. The
%% bounded-depth check of enqueue/1 is deliberately skipped: a batch that was
%% already accepted must never fail to come back, or it would be dropped. The
%% bound is enforced where it belongs - on new work, via admission_depth/0,
%% which counts these entries, so a node whose deferred set is over the bound
%% answers 429 to new requests until it drains.
-spec requeue([entry()]) -> non_neg_integer().
requeue(Entries) ->
    Now = erlang:monotonic_time(millisecond),
    lists:foldl(
        fun(Entry, N) ->
            Attempts = maps:get(defer_attempts, Entry, 0) + 1,
            NotBefore = Now + backoff_ms(Attempts),
            Seq = ets:update_counter(?TAB_INT_SEQ, seq, 1),
            true = ets:insert(
                ?TAB_INT_DEFER,
                {{NotBefore, Seq}, Entry#{not_before => NotBefore, defer_attempts => Attempts}}
            ),
            N + 1
        end,
        0,
        Entries
    ).

%% Doubling from ?DEFER_BACKOFF_BASE_MS, capped at ?DEFER_BACKOFF_MAX_MS.
-spec backoff_ms(pos_integer()) -> pos_integer().
backoff_ms(Attempts) ->
    min(?DEFER_BACKOFF_BASE_MS bsl (Attempts - 1), ?DEFER_BACKOFF_MAX_MS).

%% Move every entry whose backoff elapsed back onto the ready queue, keeping
%% the queue ordered by the order they woke up. Cheap when nothing is due: one
%% ets:first/1 on an empty (or far-future) deferred table.
-spec sweep_deferred() -> non_neg_integer().
sweep_deferred() ->
    sweep_due(ets:first(?TAB_INT_DEFER), erlang:monotonic_time(millisecond), 0).

sweep_due('$end_of_table', _Now, N) ->
    N;
sweep_due({NotBefore, _Seq} = Key, Now, N) when NotBefore =< Now ->
    case ets:take(?TAB_INT_DEFER, Key) of
        [{Key, Entry}] ->
            Seq = ets:update_counter(?TAB_INT_SEQ, seq, 1),
            true = ets:insert(?TAB_INT_Q, {Seq, Entry}),
            sweep_due(ets:first(?TAB_INT_DEFER), Now, N + 1);
        [] ->
            sweep_due(ets:first(?TAB_INT_DEFER), Now, N)
    end;
sweep_due(_Key, _Now, N) ->
    N.

%% Atomically take the head batch (ets:take per key): the entries leave
%% the queue here and the promoter promotes them afterwards. Concurrent
%% promoters (N workers per core) get disjoint batches. A crash between the
%% take and the mnesia commit loses the entries - within the crash-volatile
%% contract (queued, not yet promoted, may be lost); the promoter retries
%% the taken batch on transient failures and its idempotence guards
%% (already_promoted, append dedup) keep retries safe.
-spec take_batch(pos_integer(), pos_integer()) -> [{non_neg_integer(), entry()}].
take_batch(MaxEntries, MaxKeys) ->
    case ets:first(?TAB_INT_Q) of
        '$end_of_table' ->
            [];
        First ->
            take_from(First, MaxEntries, MaxKeys, 0, [])
    end.

take_from('$end_of_table', _MaxEntries, _MaxKeys, _Keys, Acc) ->
    lists:reverse(Acc);
take_from(_Key, MaxEntries, _MaxKeys, _Keys, Acc) when length(Acc) >= MaxEntries ->
    lists:reverse(Acc);
take_from(Key, MaxEntries, MaxKeys, Keys, Acc) ->
    case ets:take(?TAB_INT_Q, Key) of
        [{Key, Entry}] ->
            NKeys = Keys + length(maps:get(devices, Entry, [])),
            case NKeys > MaxKeys andalso Acc =/= [] of
                true ->
                    %% The entry was already removed from the queue by the
                    %% atomic take above; put it back so the over-limit
                    %% entry is not silently lost (a fresh take must not
                    %% miss it, or promotion skips it forever).
                    true = ets:insert(?TAB_INT_Q, {Key, Entry}),
                    lists:reverse(Acc);
                false ->
                    take_from(
                        ets:next(?TAB_INT_Q, Key), MaxEntries, MaxKeys, NKeys, [{Key, Entry} | Acc]
                    )
            end;
        [] ->
            take_from(ets:next(?TAB_INT_Q, Key), MaxEntries, MaxKeys, Keys, Acc)
    end.

%% Remove committed batch entries (idempotent).
-spec delete_batch([non_neg_integer()]) -> ok.
delete_batch(Seqs) ->
    lists:foreach(fun(Seq) -> ets:delete(?TAB_INT_Q, Seq) end, Seqs),
    ok.

-spec depth() -> non_neg_integer().
depth() ->
    queue_size().

%% Entries waiting out an append-retry backoff (not yet takeable).
-spec deferred_depth() -> non_neg_integer().
deferred_depth() ->
    case ets:info(?TAB_INT_DEFER, size) of
        undefined -> 0;
        N -> N
    end.

queue_size() ->
    case ets:info(?TAB_INT_Q, size) of
        undefined -> 0;
        N -> N
    end.

%% Test/maintenance reset: drop all queued entries and restart the seq
%% counter. Entries in the queue are crash-volatile by contract, so this
%% loses nothing durable.
-spec reset() -> ok.
reset() ->
    ets:delete_all_objects(?TAB_INT_Q),
    ets:delete_all_objects(?TAB_INT_DEFER),
    ets:delete_all_objects(?TAB_INT_SEQ),
    true = ets:insert(?TAB_INT_SEQ, {seq, 0}),
    ok.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.
terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
