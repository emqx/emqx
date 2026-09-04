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
-export([enqueue/1, take_batch/2, delete_batch/1, depth/0, reset/0]).
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
    expires_at := non_neg_integer()
}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    _ = ets:new(?TAB_INT_Q, [
        named_table, ordered_set, public, {write_concurrency, true}, {read_concurrency, true}
    ]),
    _ = ets:new(?TAB_INT_SEQ, [named_table, set, public, {write_concurrency, true}]),
    true = ets:insert(?TAB_INT_SEQ, {seq, 0}),
    {ok, #{}}.

%% Insert one request. Returns {ok, Seq} or `full` when the bounded queue
%% is at capacity; the caller turns `full` into HTTP 429 backpressure. The
%% queue never grows unboundedly.
-spec enqueue(entry()) -> {ok, non_neg_integer()} | full.
enqueue(Entry) ->
    MaxDepth = emqx_bcast_config:get(intake_queue_depth, 20000),
    case queue_size() >= MaxDepth of
        true ->
            emqx_bcast_metrics:intake_rejected(),
            full;
        false ->
            Seq = ets:update_counter(?TAB_INT_SEQ, seq, 1),
            true = ets:insert(?TAB_INT_Q, {Seq, Entry}),
            emqx_bcast_metrics:intake_enqueued(),
            {ok, Seq}
    end.

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
