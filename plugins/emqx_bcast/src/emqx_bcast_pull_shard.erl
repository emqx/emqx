%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_pull_shard).

-behaviour(gen_server).

%% Per-node device-pull shards: each shard gen_server is the single-writer
%% owner of one per-client state row (claim round / unacked window) for
%% its partition of devices; heavy work (claim, deliver, release) is
%% off-boxed to emqx_bcast_pull_worker_pool. Hooks cast directly into the
%% shard gen_server with the protocol tuples handled below.
%%
%% The three legacy tables (bcast_unacked / bcast_want_next_stage /
%% bcast_claim_inflight) are merged into ONE per-client state table per
%% shard (bcast_client_state_<S>), so every client event costs one ETS
%% lookup + one update instead of up to three lookups. The same row
%% carries the per-device in-flight window (window=1) and the single
%% outstanding claim round, which keeps the window bookkeeping atomic in a
%% single writer. Ack confirmations carry a remaining-queued flag from the
%% core, so a freed window slot refills immediately instead of waiting for
%% the next trigger.

-export([
    start_link/1,
    shard_count/0,
    shard_name/1,
    shard_of/2,
    cast_client/3,
    tab/2,
    qos0_deliver_local/4,
    qos1_core_trigger_local/3,
    inflight_entries/1,
    begin_pools_restart/0,
    worker_pools_restarted/1,
    ack_applied/1,
    deliver_results_remote/3
]).

%% Worker tasks.
-export([
    do_want_next/3,
    do_find_qos0_targets/5,
    do_commit_deliveries/2,
    do_deliver_qos0/1,
    do_deliver_qos0_and_ack/7,
    do_release_claim/3,
    do_release_client_claims/3,
    do_release_tags_confirm/2
]).

%% gen_server callbacks.
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

%% Node-local pull shards. One partition per scheduler keeps the per-shard
%% reduction budget comfortably below the process wall; on machines with
%% more schedulers than the former fixed count each partition owns fewer
%% rows and the sweep full-table scan (and the GC it triggers) shrinks,
%% while on fewer-scheduler machines the partitions are wider and the
%% scan per shard grows (total sweep work per node is unchanged - every
%% row is scanned once per sweep). Keep in sync with emqx_bcast_ack_shard
%% so acks from one pull partition stay together.

-define(TAB_STATE(Shard), tab(Shard, bcast_client_state)).
%% Per-shard bookkeeping table for the flush path. Counting claim rounds
%% by scanning the state table (one row per online client on the
%% partition) made every claim/ack/refill event a full-table pass and
%% wedged the shard mailbox at 800k connected devices. The claim counter
%% is maintained with O(1) ets:update_counter on every claim/clear and
%% reconciled from the row table by the periodic sweep.
-define(TAB_CNT(Shard), tab(Shard, bcast_pull_counters)).
-define(WORKER_POOL, emqx_bcast_pull_worker_pool).
-define(POOL_RESTART_WATCHDOG_MS, 30000).
-define(POOL_RESTART_RETRY_MS, 1000).

%% Max pending claim entries one flush tick submits; the remainder stays
%% queued for the next tick. Small batches keep the claim task quantum
%% (and with it the core server-pool mailbox depth) small: a 2000-entry
%% flush made one claim task occupy a pool worker for the whole per-shard
%% serialization of the batch.
-define(FLUSH_MAX_ENTRIES, 500).

%% Claim flush cadence: 2ms (was 10ms). With per-device windows the
%% ack-to-refill latency is the per-device drain rate limiter.
-define(FLUSH_MS, 2).

%% Ceiling on outstanding claim rounds per shard (rows with a claim set):
%% claim_next refuses new claims at this cap, so the core server pool
%% never receives more claim tasks than it can digest - the mailbox
%% backlog that wedged the pool under load is bounded by design.
-define(CLAIM_ROUND_CAP, 2000).

%% Retry cadence when the worker pool is unavailable (flush gate).
-define(FLUSH_RETRY_BACKOFF_MS, 1000).

%% Backoff after a flush round whose task submissions were dropped (pool
%% wedged): retry the still-pending batch after this delay instead of
%% letting the immediate re-arm spin a no-op flush loop that starves the
%% rest of the mailbox.
-define(FLUSH_FAIL_BACKOFF_MS, 50).

%% Sweep cadence for claim rounds whose async result never came back
%% (dropped cast, core node down, claim worker killed mid-flight): 5s
%% with a 10s stale age (the claim legs themselves time out at 5s). The
%% sweep also runs the periodic full-scan reconcile of the claim counter,
%% so the interval balances recovery latency against the O(partition
%% rows) pass.
-define(CLAIM_STALE_SWEEP_MS, 5000).
-define(CLAIM_STALE_AGE_MS, 10000).

%% An ack-in-flight mark whose core-applied confirmation never arrives
%% would hold the per-device window closed forever. The sweep expires
%% marks older than this TTL, releases the core claim and re-claims the
%% client (at-least-once redelivery). Set far above the normal ack round
%% trip so only a lost confirmation ever triggers it.
-define(ACK_INFLIGHT_TTL_MS, 30_000).

%% Release accumulation: releases are collected in the gen_server state
%% and flushed as grouped casts (one per index shard) either when the
%% batch reaches this size or on a short timer - never one spawn/cast
%% per release.
-define(RELEASE_FLUSH_COUNT, 128).
-define(RELEASE_FLUSH_MS, 3).

-record(state, {
    shard = 0,
    flush_timer = undefined,
    %% Claim entries written to their rows but not yet submitted to the
    %% claim workers; the flush drains this list in bounded batches.
    pending = [],
    mons = #{},
    pools_restarting = false,
    deferred_deliveries = [],
    restart_watchdog = undefined,
    restart_owner = undefined,
    deferred_retry = undefined,
    deferred_retry_count = 0,
    release_timer = undefined,
    release_tags = [],
    release_claims = []
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(non_neg_integer()) -> gen_server:start_ret().
start_link(Shard) ->
    gen_server:start_link({local, shard_name(Shard)}, ?MODULE, [Shard], []).

-spec shard_count() -> pos_integer().
shard_count() ->
    erlang:system_info(schedulers_online).

-spec shard_name(non_neg_integer()) -> atom().
shard_name(Shard) ->
    list_to_atom("emqx_bcast_pull_shard_" ++ integer_to_list(Shard)).

%% Route a client to its node-local shard: phash2 over {ProductKey,
%% DeviceName} (the same key tuple the index owner hashes, but with a
%% node-local range of schedulers_online). Every event for one client
%% (hooks, ack, trigger, deliver_results) must reach the same local shard
%% so the window and the claim round stay single-writer.
-spec shard_of(binary(), binary()) -> non_neg_integer().
shard_of(ProductKey, ClientId) ->
    erlang:phash2({ProductKey, ClientId}, shard_count()).

-spec cast_client(binary(), binary(), term()) -> ok.
cast_client(ProductKey, ClientId, Msg) ->
    gen_server:cast(shard_name(shard_of(ProductKey, ClientId)), Msg).

%% Per-shard ETS table name.
-spec tab(non_neg_integer(), atom()) -> atom().
tab(Shard, Base) ->
    list_to_atom(atom_to_list(Base) ++ "_" ++ integer_to_list(Shard)).

-spec qos0_deliver_local(binary(), [binary()] | undefined, binary(), binary()) -> ok.
qos0_deliver_local(ProductKey, DeviceNames, TopicTemplate, Payload) ->
    lists:foreach(
        fun({Shard, Sub}) ->
            gen_server:cast(
                shard_name(Shard),
                {qos0_deliver, ProductKey, Sub, TopicTemplate, Payload}
            )
        end,
        group_devices(ProductKey, DeviceNames)
    ).

-spec qos1_core_trigger_local(binary(), [binary()], binary()) -> ok.
qos1_core_trigger_local(ProductKey, DeviceNames, TopicTemplate) ->
    lists:foreach(
        fun({Shard, Sub}) ->
            gen_server:cast(
                shard_name(Shard),
                {qos1_core_trigger, ProductKey, Sub, TopicTemplate}
            )
        end,
        group_devices(ProductKey, DeviceNames)
    ).

%% Split a device list into per-shard groups. undefined (PubBroadcast) is
%% handled by ONE shard: the product-wide device scan cannot be
%% partitioned cheaply, so broadcasting undefined to all shards
%% multiplied the full registry scan by the shard count. PubBroadcast is
%% a low-frequency entry, so one shard's sequential scan is fine.
group_devices(_ProductKey, undefined) ->
    [{0, undefined}];
group_devices(ProductKey, DeviceNames) ->
    lists:foldr(
        fun(DN, Acc) ->
            Shard = shard_of(ProductKey, DN),
            case lists:keyfind(Shard, 1, Acc) of
                {Shard, List} -> lists:keyreplace(Shard, 1, Acc, {Shard, [DN | List]});
                false -> [{Shard, [DN]} | Acc]
            end
        end,
        [],
        DeviceNames
    ).

-spec begin_pools_restart() ->
    {ok, [{binary(), pos_integer(), binary()}]} | {error, restart_in_progress}.
begin_pools_restart() ->
    Results = [
        begin
            try gen_server:call(shard_name(Shard), begin_pools_restart, infinity) of
                {ok, Marks} -> {ok, Marks};
                {error, restart_in_progress} = E -> E
            catch
                exit:{noproc, _} -> {ok, []};
                exit:{normal, _} -> {ok, []}
            end
        end
     || Shard <- lists:seq(0, shard_count() - 1)
    ],
    case [E || {error, _} = E <- Results] of
        [] ->
            {ok, lists:append([M || {ok, M} <- Results])};
        [E | _] ->
            lists:foreach(
                fun
                    ({Shard, {ok, _Marks}}) ->
                        gen_server:cast(shard_name(Shard), {abort_pools_restart});
                    ({_Shard, _}) ->
                        ok
                end,
                lists:zip(lists:seq(0, shard_count() - 1), Results)
            ),
            E
    end.

%% Snapshot of the claim rounds in flight on this shard (rows with a
%% claim set) - used by begin_pools_restart so no mark can slip in
%% between the snapshot and worker termination.
-spec inflight_entries(non_neg_integer()) -> [{binary(), pos_integer(), binary()}].
inflight_entries(Shard) ->
    try
        [
            {Row#bcast_client_state.clientid, Tag, Row#bcast_client_state.product_key}
         || Row <- ets:tab2list(?TAB_STATE(Shard)),
            {Tag, _Ts} <- [Row#bcast_client_state.claim],
            Tag =/= undefined
        ]
    catch
        error:badarg -> []
    end.

-spec worker_pools_restarted([{binary(), pos_integer(), binary()}]) -> ok.
worker_pools_restarted(Marks) ->
    Groups = lists:foldr(
        fun({ClientId, _Tag, ProductKey} = Mark, Acc) ->
            Shard = shard_of(ProductKey, ClientId),
            case lists:keyfind(Shard, 1, Acc) of
                {Shard, List} -> lists:keyreplace(Shard, 1, Acc, {Shard, [Mark | List]});
                false -> [{Shard, [Mark]} | Acc]
            end
        end,
        [],
        Marks
    ),
    lists:foreach(
        fun({Shard, SubMarks}) ->
            gen_server:cast(shard_name(Shard), {worker_pools_restarted, SubMarks})
        end,
        Groups
    ).

%%--------------------------------------------------------------------
%% Worker tasks
%%--------------------------------------------------------------------

-spec do_want_next(non_neg_integer(), node(), [map()]) -> ok.
do_want_next(Shard, Core, Entries) ->
    %% Subscription filters are read from the per-client state row's cache
    %% (maintained by the session.subscribed/unsubscribed/resumed hooks),
    %% so the claim no longer reads EMQX's global subscription tables per
    %% claim. The topics travel with the claim request so the core claim tx
    %% can skip entries the device is not subscribed to. An empty cache
    %% yields no topics and the claim returns no_more.
    Entries1 = [
        M#{topics => cached_topics(Shard, M)}
     || M <- Entries
    ],
    Marks = [inflight_mark(M) || M <- Entries],
    %% Fire-and-forget want_next: the pull worker must not block on the
    %% whole claim round trip. Claim execution stays in the core's server
    %% worker pool; its result is routed back to this node's pull_shard
    %% by deliver_results_remote/3 through the same deliver_results +
    %% generation-mark path.
    try
        case Core =:= node() of
            true ->
                emqx_bcast_pull_server_pool:want_next_async(Shard, node(), Entries1, Marks);
            false ->
                emqx_rpc:cast(
                    Core,
                    emqx_bcast_pull_server_pool,
                    want_next_async,
                    [Shard, node(), Entries1, Marks]
                )
        end
    catch
        Error:Reason ->
            ?SLOG(warning, #{
                msg => "bcast_want_next_async_submit_failed",
                exception => Error,
                reason => Reason
            }),
            %% The request never left; release the batch's marks so the
            %% client window is not stalled (the entries are re-claimed by
            %% the recovery paths).
            gen_server:cast(shard_name(Shard), {deliver_results, [], Marks})
    end,
    ok.

%% [{TopicFilter, Qos}] read from the per-client state row. The cache is
%% maintained by the session.subscribed/unsubscribed hooks (incremental)
%% and re-synced from emqx_broker:subscriptions/1 on session.resumed.
%%
%% An empty cache is ambiguous: it can mean "not subscribed yet" or "the
%% cache lagged the authoritative subscription state" (pre-existing
%% subscription at plugin start, or a subscribe path that did not fire
%% session.subscribed). On empty, read the authoritative subscriptions and
%% write them back into the row (only the topics field) so the next claim
%% hits the cache instead of re-reading the global subscription tables.
cached_topics(Shard, #{clientid := ClientId, product_key := ProductKey, pid := Pid}) ->
    case ets:lookup(?TAB_STATE(Shard), {ProductKey, ClientId}) of
        [#bcast_client_state{topics = []}] ->
            Topics = subscription_topics(Pid),
            _ = ets:update_element(
                ?TAB_STATE(Shard),
                {ProductKey, ClientId},
                {#bcast_client_state.topics, Topics}
            ),
            Topics;
        [#bcast_client_state{topics = Topics}] ->
            Topics;
        [] ->
            subscription_topics(Pid)
    end.

%% [{TopicFilter, Qos}] from EMQX's own subscription tables for a channel
%% pid. Used only to re-sync the cache on session resume, when the restored
%% subscriptions do not re-fire session.subscribed.
subscription_topics(Pid) ->
    case is_process_alive(Pid) of
        false ->
            [];
        true ->
            [
                {Filter, maps:get(qos, SubOpts, 0)}
             || {Filter, SubOpts} <- emqx_broker:subscriptions(Pid)
            ]
    end.

%% Add or replace one cached subscription filter (keyed on the filter).
upsert_topic(Filter, Qos, Topics) ->
    case lists:keyfind(Filter, 1, Topics) of
        {Filter, _} -> lists:keyreplace(Filter, 1, Topics, {Filter, Qos});
        false -> [{Filter, Qos} | Topics]
    end.

%% Deliver one QoS1 delivery descriptor. The descriptor is a transient
%% worker-side map; the state table only keeps the much smaller unacked
%% window ({Did, AckInFlight} entries) needed for PUBACK matching and
%% release.
deliver_pending_one(#{
    clientid := ClientId,
    delivery_id := DeliveryId,
    product_key := ProductKey,
    topic := Topic,
    payload := Payload,
    pid := Pid,
    attempts := Attempts
}) ->
    case session_holds_channel(ClientId, Pid) of
        true ->
            case is_process_alive(Pid) of
                false ->
                    fail_pending_delivery(ClientId, DeliveryId, ProductKey);
                true ->
                    Msg = emqx_message:make(
                        DeliveryId,
                        ClientId,
                        ?QOS_1,
                        Topic,
                        Payload,
                        #{},
                        #{
                            ?BCAST_DELIVERY_ID => DeliveryId,
                            ?BCAST_PRODUCT_KEY => ProductKey
                        }
                    ),
                    Pid ! #deliver{topic = Topic, message = Msg},
                    emqx_bcast_metrics:qos1_delivered(),
                    case Attempts >= 2 of
                        true -> emqx_bcast_metrics:qos1_redelivered();
                        false -> ok
                    end
            end;
        _ ->
            %% Client gone, dead, or taken over by another channel: do not
            %% count a delivery that cannot reach the current session.
            fail_pending_delivery(ClientId, DeliveryId, ProductKey)
    end.

fail_pending_delivery(ClientId, DeliveryId, ProductKey) ->
    gen_server:cast(
        shard_name(shard_of(ProductKey, ClientId)),
        {deliver_failed, ProductKey, ClientId, DeliveryId}
    ),
    ok.

-spec do_deliver_qos0([{pid(), binary(), binary(), binary(), binary()}]) -> ok.
do_deliver_qos0(Targets) ->
    lists:foreach(
        fun({Pid, _ProductKey, ClientId, Topic, Payload}) ->
            case session_holds_channel(ClientId, Pid) of
                true ->
                    Msg = emqx_message:make(ClientId, ?QOS_0, Topic, Payload),
                    Pid ! #deliver{topic = Topic, message = Msg};
                false ->
                    %% The session was taken over (or disconnected) after
                    %% the target scan; only the current channel holder may
                    %% receive the direct #deliver.
                    ok
            end
        end,
        Targets
    ).

%% Resolve QoS0 fanout targets in a worker: for each target device, look
%% up its channel pid and check the subscription by reading EMQX's own
%% subscription tables (emqx_broker:subscriptions/1) instead of a plugin
%% mirror. DeviceNames = undefined means product-wide (PubBroadcast),
%% handled by ONE shard (group_devices/2 sends it only to shard 0).
-spec do_find_qos0_targets(
    non_neg_integer(), binary(), [binary()] | undefined, binary(), binary()
) -> [{pid(), binary(), binary(), binary(), binary()}].
do_find_qos0_targets(_Shard, ProductKey, DeviceNames, TopicTemplate, Payload) ->
    Devices =
        case DeviceNames of
            undefined ->
                emqx_bcast:lookup_devices_by_product(ProductKey);
            _ ->
                lists:filtermap(
                    fun(DeviceName) ->
                        case emqx_bcast:lookup_device({ProductKey, DeviceName}) of
                            {ok, Pid} -> {true, {DeviceName, Pid}};
                            {error, not_found} -> false
                        end
                    end,
                    DeviceNames
                )
        end,
    %% Expand the productKey placeholder once for the whole fanout; only
    %% the per-device deviceName replacement runs inside the loop.
    Partial = emqx_bcast_utils:replace_product_key(TopicTemplate, ProductKey),
    lists:filtermap(
        fun({DeviceName, Pid}) ->
            Topic = emqx_bcast_utils:expand_topic_partial(Partial, DeviceName),
            case sub_match(Pid, Topic) of
                {ok, _SubQos} ->
                    {true, {Pid, ProductKey, DeviceName, Topic, Payload}};
                false ->
                    false
            end
        end,
        Devices
    ).

%% Session-holder check against EMQX's authoritative channel registry.
%% emqx_cm:lookup_channels/1 is global, so this also covers a takeover
%% where the new channel lives on another node while the local device
%% registry still contains the old pid.
session_holds_channel(ClientId, Pid) ->
    lists:member(Pid, emqx_cm:lookup_channels(ClientId)).

%% Subscription check against EMQX's own subscription state. Returns
%% {ok, Qos} with a matching subscription QoS, or false.
sub_match(Pid, Topic) ->
    case is_process_alive(Pid) of
        false ->
            false;
        true ->
            try
                lists:foreach(
                    fun({Filter, SubOpts}) ->
                        case emqx_topic:match(Topic, Filter) of
                            true -> throw({bcast_sub_match, maps:get(qos, SubOpts, 0)});
                            false -> ok
                        end
                    end,
                    emqx_broker:subscriptions(Pid)
                ),
                false
            catch
                {bcast_sub_match, Qos} -> {ok, Qos}
            end
    end.

-spec do_deliver_qos0_and_ack(
    binary(), pid(), binary(), binary(), binary(), binary(), pos_integer()
) ->
    ok.
do_deliver_qos0_and_ack(ClientId, Pid, Topic, Payload, DeliveryId, ProductKey, Attempts) ->
    Msg = emqx_message:make(ClientId, ?QOS_0, Topic, Payload),
    Pid ! #deliver{topic = Topic, message = Msg},
    %% The QoS0-subscription delivery is an actual PUBLISH send too, so it
    %% counts toward delivered (and redelivered when attempt >= 2) exactly
    %% like the QoS1 send path; auto_acked records the self-confirmation.
    emqx_bcast_metrics:qos1_delivered(),
    case Attempts >= 2 of
        true -> emqx_bcast_metrics:qos1_redelivered();
        false -> ok
    end,
    emqx_bcast_metrics:qos1_auto_acked(),
    %% Route through the same pull {ack} entry point: the core-applied
    %% confirmation unblocks the next delivery.
    emqx_bcast_pull_shard:cast_client(
        ProductKey, ClientId, {ack, ClientId, DeliveryId, ProductKey}
    ).

-spec do_release_claim(binary(), binary(), binary()) -> ok.
do_release_claim(ProductKey, ClientId, DeliveryId) ->
    %% Synchronous release of one claim (runs in a pull worker; used by
    %% the commit-time failure paths that must release before any retry
    %% claim of the same delivery).
    _ = emqx_bcast_index_owner:release_claim(ProductKey, ClientId, DeliveryId),
    ok.

-spec do_release_client_claims(binary(), binary(), pos_integer()) -> ok.
do_release_client_claims(ProductKey, ClientId, ClaimTag) ->
    %% Synchronous tag release used by the release-then-reclaim recovery
    %% paths (sweep, pool restart, failed claim rounds).
    _ = emqx_bcast_index_owner:release_client_claims_sync([{ProductKey, ClientId, ClaimTag}]),
    ok.

%% Worker task: release a batch of claim rounds by tag (synchronously, so
%% a later claim round cannot race the release), then signal the shard to
%% re-claim the clients whose rounds are gone. Runs on the pull worker
%% pool - never inside the shard gen_server.
-spec do_release_tags_confirm(non_neg_integer(), [{binary(), pos_integer(), binary()}]) -> ok.
do_release_tags_confirm(Shard, Marks) ->
    Tags = [{PK, C, Tag} || {C, Tag, PK} <- Marks],
    _ = emqx_bcast_index_owner:release_client_claims_sync(Tags),
    %% Batch re-arm cast (one message per release batch, not per mark).
    gen_server:cast(
        shard_name(Shard),
        {claim_released_batch, [{C, PK} || {C, _Tag, PK} <- Marks]}
    ),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Shard]) ->
    ok = ensure_state_table(?TAB_STATE(Shard)),
    ok = ensure_counter_table(Shard),
    _ = erlang:send_after(?CLAIM_STALE_SWEEP_MS, self(), sweep_stale_claims),
    {ok, #state{shard = Shard}}.

handle_call(begin_pools_restart, _From, State = #state{pools_restarting = true}) ->
    {reply, {error, restart_in_progress}, State};
handle_call(begin_pools_restart, {Caller, _Tag}, State) ->
    %% Atomically stop new flushes and snapshot the current claim rounds.
    %% No mark can be created between this snapshot and worker
    %% termination. Monitor the restart caller so an early death
    %% self-heals; the timer is only a backstop.
    ok = cancel_deferred_retry(State#state.deferred_retry),
    State0 = State#state{deferred_retry = undefined, deferred_retry_count = 0},
    MRef = monitor(process, Caller),
    Timer = erlang:send_after(
        ?POOL_RESTART_WATCHDOG_MS, self(), pools_restart_watchdog
    ),
    {reply, {ok, inflight_entries(State#state.shard)}, State0#state{
        pools_restarting = true,
        restart_watchdog = Timer,
        restart_owner = {MRef, Caller}
    }};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({client_connected, ClientId, Pid, ProductKey}, State) ->
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    %% (Re)create the client state row when a fresh session starts.
    ensure_row(ProductKey, ClientId, Pid, State#state.shard),
    Ref = monitor(process, Pid),
    Mons = maps:put(Ref, {Pid, ClientId, ProductKey}, State#state.mons),
    {noreply, State#state{mons = Mons}};
handle_cast({client_disconnected, ClientId, Pid, ProductKey}, State) ->
    cleanup_client(ClientId, Pid, ProductKey, State),
    {noreply, State};
handle_cast({subscribe, ClientId, Pid, ProductKey}, State) ->
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    {noreply, maybe_claim(ProductKey, ClientId, Pid, State)};
handle_cast({topic_added, ClientId, Pid, ProductKey, TopicFilter, Qos}, State) ->
    %% session.subscribed fired (post-commit): cache the filter so the
    %% claim path no longer reads emqx_broker:subscriptions/1 per claim.
    Shard = State#state.shard,
    case ets:lookup(?TAB_STATE(Shard), {ProductKey, ClientId}) of
        [#bcast_client_state{} = Row] ->
            Topics = upsert_topic(TopicFilter, Qos, Row#bcast_client_state.topics),
            row_put(Shard, Row#bcast_client_state{pid = Pid, topics = Topics});
        [] ->
            row_put(
                Shard,
                #bcast_client_state{
                    key = {ProductKey, ClientId},
                    product_key = ProductKey,
                    clientid = ClientId,
                    pid = Pid,
                    topics = [{TopicFilter, Qos}]
                }
            )
    end,
    {noreply, State};
handle_cast({topic_removed, ClientId, Pid, ProductKey, TopicFilter}, State) ->
    %% session.unsubscribed fired (post-commit): drop the cached filter.
    Shard = State#state.shard,
    case ets:lookup(?TAB_STATE(Shard), {ProductKey, ClientId}) of
        [#bcast_client_state{} = Row] ->
            Topics = lists:keydelete(TopicFilter, 1, Row#bcast_client_state.topics),
            row_put(Shard, Row#bcast_client_state{pid = Pid, topics = Topics});
        [] ->
            ok
    end,
    {noreply, State};
handle_cast({resume, ClientId, Pid, ProductKey}, State) ->
    %% session.resumed: re-register and re-sync the cached filters (resume
    %% restores subscriptions without re-firing session.subscribed), then
    %% re-arm a want_next so a resumed backlog keeps draining.
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    Shard = State#state.shard,
    case ets:lookup(?TAB_STATE(Shard), {ProductKey, ClientId}) of
        [#bcast_client_state{} = Row] ->
            row_put(
                Shard,
                Row#bcast_client_state{pid = Pid, topics = subscription_topics(Pid)}
            );
        [] ->
            ensure_row(ProductKey, ClientId, Pid, Shard)
    end,
    {noreply, maybe_claim(ProductKey, ClientId, Pid, State)};
handle_cast({unsubscribe, ClientId, _Pid, ProductKey}, State) ->
    %% The client is no longer subscribed: release every unacked delivery
    %% instead of leaving it in the window until the 60s lease or a
    %% disconnect. The core entries go back to stored and can be claimed
    %% again if the client resubscribes.
    Shard = State#state.shard,
    case ets:lookup(?TAB_STATE(Shard), {ProductKey, ClientId}) of
        [#bcast_client_state{} = Row] ->
            Release = [
                {ProductKey, ClientId, Did}
             || {Did, false} <- Row#bcast_client_state.inflight
            ],
            State1 = release_claims_later(State, Release),
            {noreply, row_delete(State1, Row)};
        [] ->
            {noreply, State}
    end;
handle_cast({ping, ClientId, Pid, ProductKey}, State) ->
    %% Keepalive pings refresh the device registry only; they must NOT
    %% claim a want_next. At 800k online devices every keepalive would
    %% otherwise drive a claim -> flush probe cycle against the
    %% index shards even when every queue is empty (each probe returns
    %% no_more), saturating the pull shard mailboxes and the core index
    %% shards with zero deliveries in flight. New content is signalled by
    %% the promoter trigger; subscribe / session.resumed / ack refill /
    %% release-then-reclaim cover every re-arm path. register_device is
    %% idempotent (skips when the entry already holds this pid), so the
    %% ping itself is a cheap registry refresh.
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    {noreply, State};
handle_cast({ack, ClientId, DeliveryId, ProductKey}, State) ->
    %% Pull is the ack entry point and forwards to core only AFTER the
    %% local window entry is transitioned to ack-in-flight (real PUBACK).
    %% The entry then stays until the core-applied confirmation comes
    %% back, so duplicate PUBACKs can never double-count. Auto-ack from
    %% the QoS0 path has no window entry and does not create one.
    _ = mark_ack_in_flight(ProductKey, ClientId, DeliveryId),
    emqx_bcast_ack_shard:ack(ProductKey, ClientId, DeliveryId),
    {noreply, State};
handle_cast({ack_applied_batch, Pairs}, State) ->
    Shard = State#state.shard,
    State1 = lists:foldl(
        fun({ClientId, ProductKey, DeliveryId, RemQueued}, St) ->
            case take_ack_in_flight(ProductKey, ClientId, DeliveryId) of
                counted ->
                    %% A real PUBACK was confirmed applied at core exactly
                    %% once: count it here (device node) and free the slot.
                    emqx_bcast_metrics:qos1_acked(),
                    refill(ProductKey, ClientId, RemQueued, Shard, St);
                _ ->
                    %% No matching window entry: an auto-ack (QoS0 path)
                    %% or stale confirmation. Do not count; still advance
                    %% the client when the core says more is queued.
                    refill(ProductKey, ClientId, RemQueued, Shard, St)
            end
        end,
        State,
        Pairs
    ),
    {noreply, State1};
handle_cast({qos0_deliver, ProductKey, DeviceNames, TopicTemplate, Payload}, State) ->
    %% The per-device online + subscription check runs in a worker: it
    %% reads emqx_broker:subscriptions(Pid) per device and must not block
    %% the gen_server on a large fanout.
    Shard = State#state.shard,
    submit_to_worker(fun() ->
        Targets = do_find_qos0_targets(Shard, ProductKey, DeviceNames, TopicTemplate, Payload),
        case Targets of
            [] ->
                ok;
            _ ->
                emqx_bcast_metrics:qos0_delivery_count(length(Targets)),
                do_deliver_qos0(Targets)
        end
    end),
    {noreply, State};
handle_cast({qos1_core_trigger, ProductKey, DeviceNames, _TopicTemplate}, State) ->
    %% Claim want_next from the (PK, DN) list. The only per-device work is
    %% one local device-registry lookup plus one state-row claim update;
    %% subscription matching is intentionally deferred to the claim path.
    State1 = lists:foldl(
        fun(DeviceName, St) ->
            case emqx_bcast:lookup_device({ProductKey, DeviceName}) of
                {ok, Pid} ->
                    claim_next(DeviceName, Pid, ProductKey, St);
                {error, not_found} ->
                    St
            end
        end,
        State,
        DeviceNames
    ),
    {noreply, maybe_flush_buffer3(State1)};
handle_cast({deliver_results, Results, Marks}, State = #state{pools_restarting = true}) ->
    %% Workers are being torn down; keep this batch for replay after the
    %% replacement pools are running instead of clearing marks and then
    %% dropping the fill/deliver submissions into a dead pool.
    Deferred = State#state.deferred_deliveries,
    {noreply, State#state{deferred_deliveries = [{results, Results, Marks} | Deferred]}};
handle_cast({deliver_results, Results, Marks}, State) ->
    {noreply, dispatch_deliver_results(Results, Marks, State)};
handle_cast({worker_pools_restarted, Marks}, State) ->
    ok = emqx_bcast_utils:cancel_timer(State#state.restart_watchdog),
    ok = cancel_deferred_retry(State#state.deferred_retry),
    ok = cancel_restart_owner(State#state.restart_owner),
    State0 = State#state{
        pools_restarting = false,
        restart_watchdog = undefined,
        restart_owner = undefined,
        deferred_retry = undefined,
        deferred_retry_count = 0
    },
    %% The worker supervisors were restarted while one or more want_next
    %% workers may have been killed mid-RPC. Release those generations by
    %% tag and re-claim after the release has completed on core, so the
    %% replay claim cannot see the old entry still pending.
    Shard = State#state.shard,
    Live = [
        Mark
     || Mark = {C, Tag, PK} <- Marks,
        clear_row_claim(PK, C, Tag) =:= ok
    ],
    case Live of
        [] ->
            ok;
        _ ->
            submit_to_worker(fun() -> do_release_tags_confirm(Shard, Live) end)
    end,
    State1 = maybe_replay_deferred(State0),
    {noreply, maybe_flush_buffer3(State1)};
handle_cast({abort_pools_restart}, State = #state{pools_restarting = true}) ->
    %% Reset a begin_pools_restart that was aborted because a sibling
    %% shard reported restart_in_progress (the restart owner owns the
    %% marks snapshot then; we must not release anything here). Replay
    %% any deferred deliver_results batches.
    ok = emqx_bcast_utils:cancel_timer(State#state.restart_watchdog),
    ok = cancel_restart_owner(State#state.restart_owner),
    ok = cancel_deferred_retry(State#state.deferred_retry),
    State0 = State#state{
        pools_restarting = false,
        restart_watchdog = undefined,
        restart_owner = undefined,
        deferred_retry = undefined,
        deferred_retry_count = 0
    },
    State1 = maybe_replay_deferred(State0),
    {noreply, maybe_flush_buffer3(State1)};
handle_cast({abort_pools_restart}, State) ->
    {noreply, State};
handle_cast({claim_released, ClientId, ProductKey}, State) ->
    %% A claim round was released (pool restart / sweep / failed round):
    %% re-claim the client so its backlog keeps draining.
    Shard = State#state.shard,
    {noreply, reclaim_online(ProductKey, ClientId, Shard, State)};
handle_cast({claim_released_batch, Clients}, State) ->
    %% Batch re-arm after a multi-mark release: ONE cast for the whole
    %% batch instead of one {claim_released} cast per mark - a failed
    %% claim round at scale used to flood the shard mailbox with one
    %% re-arm message per client.
    Shard = State#state.shard,
    State1 = lists:foldl(
        fun({ClientId, ProductKey}, St) ->
            reclaim_online(ProductKey, ClientId, Shard, St)
        end,
        State,
        Clients
    ),
    {noreply, State1};
handle_cast({deliver_failed, ProductKey, ClientId, DeliveryId}, State) ->
    %% The deliver worker found the channel pid dead: drop the window
    %% entry (if it is still the same delivery) and release the core
    %% claim so it does not sit as an unacked tombstone blocking the
    %% window forever.
    Shard = State#state.shard,
    case ets:lookup(?TAB_STATE(Shard), {ProductKey, ClientId}) of
        [#bcast_client_state{} = Row] ->
            case lists:keytake(DeliveryId, 1, Row#bcast_client_state.inflight) of
                {value, {DeliveryId, false}, Rest} ->
                    State1 = release_claims_later(
                        State, [{ProductKey, ClientId, DeliveryId}]
                    ),
                    row_put(Shard, Row#bcast_client_state{inflight = Rest}),
                    {noreply, State1};
                _ ->
                    {noreply, State}
            end;
        [] ->
            {noreply, State}
    end;
handle_cast({retry_client, ClientId, ProductKey}, State) ->
    %% A delivery could not be committed (payload lag, session race): its
    %% claim was released synchronously by the worker; re-claim the
    %% client so the next claim round retries it.
    Shard = State#state.shard,
    {noreply, reclaim_online(ProductKey, ClientId, Shard, State)};
handle_cast(Msg, State) ->
    ?SLOG(warning, #{msg => "bcast_pull_shard_unexpected_cast", message => Msg}),
    {noreply, State}.

handle_info(pools_restart_watchdog, State = #state{pools_restarting = true}) ->
    ?SLOG(error, #{
        msg => "bcast_pools_restart_watchdog_fired",
        deferred_deliveries => length(State#state.deferred_deliveries)
    }),
    ok = cancel_restart_owner(State#state.restart_owner),
    ok = cancel_deferred_retry(State#state.deferred_retry),
    State0 = State#state{
        pools_restarting = false,
        restart_watchdog = undefined,
        restart_owner = undefined,
        deferred_retry = undefined,
        deferred_retry_count = 0
    },
    State1 = maybe_replay_deferred(State0),
    {noreply, maybe_flush_buffer3(State1)};
handle_info({retry_deferred, Token}, State = #state{deferred_retry = {Token, _Timer}}) ->
    {noreply, maybe_replay_deferred(State#state{deferred_retry = undefined})};
handle_info({retry_deferred, _StaleToken}, State) ->
    {noreply, State};
handle_info(flush_buffer3, State = #state{pools_restarting = true}) ->
    %% Keep pending entries until the replacement pools are running.
    ok = emqx_bcast_utils:cancel_timer(State#state.flush_timer),
    {noreply, State#state{flush_timer = undefined}};
handle_info(flush_buffer3, State) ->
    ok = emqx_bcast_utils:cancel_timer(State#state.flush_timer),
    %% Gate the flush on pool availability BEFORE taking entries: if the
    %% worker pool is down (restart window) the submitted tasks would be
    %% dropped and the claim rounds taken here would never be cleared
    %% (window stall + state leak). Keep everything pending and re-arm.
    case emqx_bcast_utils:pool_available(?WORKER_POOL) of
        false ->
            ?SLOG(warning, #{
                msg => "bcast_flush_deferred_pool_unavailable",
                pending => length(State#state.pending)
            }),
            {noreply, State#state{
                flush_timer = erlang:send_after(
                    ?FLUSH_RETRY_BACKOFF_MS, self(), flush_buffer3
                )
            }};
        true ->
            flush_buffer3_available(State)
    end;
handle_info(flush_releases, State) ->
    {noreply, flush_releases(State#state{release_timer = undefined})};
handle_info(
    {'DOWN', MRef, process, Pid, Reason},
    State = #state{restart_owner = {MRef, Pid}}
) ->
    ?SLOG(error, #{
        msg => "bcast_pools_restart_caller_died",
        reason => Reason
    }),
    ok = emqx_bcast_utils:cancel_timer(State#state.restart_watchdog),
    ok = cancel_deferred_retry(State#state.deferred_retry),
    State0 = State#state{
        pools_restarting = false,
        restart_watchdog = undefined,
        restart_owner = undefined,
        deferred_retry = undefined,
        deferred_retry_count = 0
    },
    State1 = maybe_replay_deferred(State0),
    {noreply, maybe_flush_buffer3(State1)};
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case maps:take(Ref, State#state.mons) of
        {{Pid, ClientId, ProductKey}, Mons} ->
            cleanup_client(ClientId, Pid, ProductKey, State),
            emqx_bcast_ack_shard:client_down(ProductKey, ClientId),
            {noreply, State#state{mons = Mons}};
        error ->
            {noreply, State}
    end;
handle_info(sweep_stale_claims, State = #state{pools_restarting = false}) ->
    %% Release claim rounds whose async result never came back (dropped
    %% emqx_rpc cast, core node down, claim worker killed between submit
    %% and reply). Release-then-reclaim keeps the backlog draining.
    _ = erlang:send_after(?CLAIM_STALE_SWEEP_MS, self(), sweep_stale_claims),
    {noreply, sweep_stale_marks(State)};
handle_info(sweep_stale_claims, State) ->
    %% Defer during a pool restart: worker_pools_restarted replays the
    %% marks.
    _ = erlang:send_after(?CLAIM_STALE_SWEEP_MS, self(), sweep_stale_claims),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Client state rows
%%--------------------------------------------------------------------

ensure_state_table(Name) ->
    emqx_bcast_utils:ensure_ets(Name, [
        named_table,
        public,
        set,
        {keypos, #bcast_client_state.key},
        {read_concurrency, true},
        {write_concurrency, true}
    ]).

ensure_counter_table(Shard) ->
    Name = ?TAB_CNT(Shard),
    case ets:info(Name) of
        undefined ->
            _ = ets:new(Name, [named_table, public, set, {write_concurrency, true}]),
            true = ets:insert(Name, [{claim, 0}]),
            ok;
        _ ->
            ok
    end.

cnt_get(Shard, Key) ->
    try ets:lookup_element(?TAB_CNT(Shard), Key, 2) of
        N when is_integer(N) -> N;
        _ -> 0
    catch
        error:badarg -> 0
    end.

cnt_inc(Shard, Key, Delta) ->
    try ets:update_counter(?TAB_CNT(Shard), Key, {2, Delta}) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

cnt_put(Shard, Key, Value) ->
    try ets:insert(?TAB_CNT(Shard), {Key, Value}) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

bump_claim(Shard, Delta) -> cnt_inc(Shard, claim, Delta).

ensure_row(ProductKey, ClientId, Pid, Shard) ->
    Key = {ProductKey, ClientId},
    case ets:member(?TAB_STATE(Shard), Key) of
        true ->
            ok;
        false ->
            ets:insert_new(
                ?TAB_STATE(Shard),
                #bcast_client_state{
                    key = Key,
                    product_key = ProductKey,
                    clientid = ClientId,
                    pid = Pid
                }
            ),
            ok
    end.

row_put(Shard, Row) ->
    ets:insert(?TAB_STATE(Shard), Row),
    ok.

row_delete(State, Row) ->
    Shard = State#state.shard,
    drop_row_indexes(Shard, Row),
    ets:delete(?TAB_STATE(Shard), Row#bcast_client_state.key),
    State.

%% Remove a row's claim bookkeeping (called before the row is deleted or
%% rewritten with claim=undefined).
drop_row_indexes(Shard, Row) ->
    case Row#bcast_client_state.claim of
        undefined -> ok;
        _ -> bump_claim(Shard, -1)
    end,
    ok.

window_size() ->
    1.

%%--------------------------------------------------------------------
%% Window / claim round transitions
%%--------------------------------------------------------------------

%% maybe_claim/4 is the subscribe entry: claim the client when the pid is
%% alive and the window has a free slot.
maybe_claim(ProductKey, ClientId, Pid, State) ->
    case is_process_alive(Pid) of
        true ->
            claim_next(ClientId, Pid, ProductKey, State);
        false ->
            State
    end.

%% Claim one client (if eligible): write the claim round directly to the
%% row and queue the entry for the flush to submit. Returns the State with
%% the entry appended to the pending list when a new claim was recorded.
claim_next(ClientId, Pid, ProductKey, State) ->
    Shard = State#state.shard,
    Key = {ProductKey, ClientId},
    case ets:lookup(?TAB_STATE(Shard), Key) of
        [] ->
            claim_row(
                Shard,
                #bcast_client_state{
                    key = Key,
                    product_key = ProductKey,
                    clientid = ClientId,
                    pid = Pid
                },
                State
            );
        [Row] ->
            case eligible_to_claim(Row) of
                true ->
                    claim_row(Shard, Row#bcast_client_state{pid = Pid}, State);
                false ->
                    State
            end
    end.

%% Write the claim round to the row and queue it for submission.
claim_row(Shard, Row, State) ->
    case claim_round_count(Shard) >= ?CLAIM_ROUND_CAP of
        true ->
            %% Claim-round cap reached (core server pool backpressure):
            %% leave the client idle; a later trigger/refill re-claims it
            %% once the sweep releases stale rounds and frees headroom.
            State;
        false ->
            Tag = next_claim_tag(),
            Row1 = Row#bcast_client_state{claim = {Tag, erlang:system_time(millisecond)}},
            ets:insert(?TAB_STATE(Shard), Row1),
            bump_claim(Shard, 1),
            Entry = #{
                clientid => Row1#bcast_client_state.clientid,
                product_key => Row1#bcast_client_state.product_key,
                pid => Row1#bcast_client_state.pid,
                claim_tag => Tag
            },
            %% Arm the flush here so every claim path (trigger, subscribe,
            %% refill, release, retry) submits its pending entries; the
            %% timer is a single-armed 2ms cadence regardless of caller.
            maybe_flush_buffer3(State#state{pending = [Entry | State#state.pending]})
    end.

eligible_to_claim(Row) ->
    Row#bcast_client_state.claim =:= undefined andalso
        length(Row#bcast_client_state.inflight) < window_size().

%% Re-claim a client after an ack freed a window slot when the core still
%% has queued entries for it (RemQueued=true) - the drain refill path
%% that eliminates empty no_more claims.
refill(ProductKey, ClientId, true, Shard, State) ->
    reclaim_online(ProductKey, ClientId, Shard, State);
refill(_ProductKey, _ClientId, false, _Shard, State) ->
    State.

%% Claim the client when it is still online (registry lookup), returning
%% the updated State (the flush timer arms on a new claim).
reclaim_online(ProductKey, ClientId, _Shard, State) ->
    case emqx_bcast:lookup_device({ProductKey, ClientId}) of
        {ok, Pid} when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true ->
                    claim_next(ClientId, Pid, ProductKey, State);
                false ->
                    State
            end;
        _ ->
            State
    end.

maybe_flush_buffer3(State = #state{pending = []}) ->
    State#state{flush_timer = undefined};
maybe_flush_buffer3(State) ->
    %% One flush cadence for every pending level. The old immediate
    %% self() ! flush_buffer3 arm for Staged >= FLUSH_MAX_ENTRIES enqueued
    %% one flush message PER re-arm call; a release / refill storm that
    %% re-arms once per client piled unbounded flush_buffer3 messages into
    %% the mailbox on top of the casts (a self-sustaining queue even with
    %% no new content). The 2ms timer caps the flush rate - 500 entries
    %% per 2ms far exceeds any drain need - and keeps at most one flush
    %% message pending at any time.
    case State#state.flush_timer of
        undefined ->
            State#state{
                flush_timer = erlang:send_after(?FLUSH_MS, self(), flush_buffer3)
            };
        T ->
            State#state{flush_timer = T}
    end.

claim_round_count(Shard) ->
    cnt_get(Shard, claim).

next_claim_tag() ->
    erlang:unique_integer([monotonic, positive]).

inflight_mark(#{clientid := ClientId, product_key := ProductKey, claim_tag := Tag}) ->
    {ClientId, Tag, ProductKey}.

%%--------------------------------------------------------------------
%% Flush
%%--------------------------------------------------------------------

%% Bounded want_next flush: submit at most FLUSH_MAX_ENTRIES pending claim
%% entries per tick. The claim-round cap is enforced at claim time (see
%% claim_next), so the pending list only ever holds at most CLAIM_ROUND_CAP
%% already-claimed entries that the core server pool is about to digest.
take_pending(State, Max) ->
    Pending = State#state.pending,
    {Take, Rest} = lists:split(min(Max, length(Pending)), Pending),
    {Take, State#state{pending = Rest}}.
flush_buffer3_available(State) ->
    Shard = State#state.shard,
    {Entries, State1} = take_pending(State, ?FLUSH_MAX_ENTRIES),
    case Entries of
        [] ->
            {noreply, maybe_flush_buffer3(State1#state{flush_timer = undefined})};
        _ ->
            %% Group entries by the core that owns their index shard
            %% (shard_owner(shard_of({PK, DN}))). The claim then arrives
            %% on the one core that can execute it with LOCAL
            %% device-shard calls.
            Groups = lists:foldr(
                fun(M, Acc) ->
                    PK = maps:get(product_key, M),
                    C = maps:get(clientid, M),
                    Core = emqx_bcast_index_owner:shard_owner(
                        emqx_bcast_index_owner:shard_of({PK, C})
                    ),
                    case lists:keyfind(Core, 1, Acc) of
                        {Core, List} ->
                            lists:keyreplace(Core, 1, Acc, {Core, [M | List]});
                        false ->
                            [{Core, [M]} | Acc]
                    end
                end,
                [],
                Entries
            ),
            Failed = lists:foldl(
                fun({Core, Group}, Acc) ->
                    case submit_to_worker(fun() -> do_want_next(Shard, Core, Group) end) of
                        ok ->
                            Acc;
                        {error, Reason} ->
                            %% A dropped task would strand its claim
                            %% rounds: keep them claimed and re-queue for
                            %% the next tick (the rows still carry the
                            %% claim tag; re-submitting is idempotent).
                            ?SLOG(warning, #{
                                msg => "bcast_flush_submit_failed_rollback",
                                reason => Reason,
                                entries => length(Group)
                            }),
                            Group ++ Acc
                    end
                end,
                [],
                Groups
            ),
            case Failed of
                [] ->
                    {noreply, maybe_flush_buffer3(State1#state{flush_timer = undefined})};
                _ ->
                    %% The pool dropped tasks this round: back off the
                    %% retry so a wedged pool cannot spin a tight flush
                    %% loop that starves the rest of the mailbox.
                    {noreply, State1#state{
                        pending = Failed ++ State1#state.pending,
                        flush_timer = erlang:send_after(
                            ?FLUSH_FAIL_BACKOFF_MS, self(), flush_buffer3
                        )
                    }}
            end
    end.

%% Clear a row's claim round iff the tag still matches. Returns ok when
%% cleared, stale when the row is gone or a newer round owns the mark.
clear_row_claim(ProductKey, ClientId, Tag) ->
    Shard = shard_of(ProductKey, ClientId),
    Tab = ?TAB_STATE(Shard),
    case ets:lookup(Tab, {ProductKey, ClientId}) of
        [#bcast_client_state{claim = {Tag, _Ts}} = Row] ->
            ets:insert(Tab, Row#bcast_client_state{claim = undefined}),
            bump_claim(Shard, -1),
            ok;
        _ ->
            stale
    end.

%%--------------------------------------------------------------------
%% Deliver-result dispatch (gen_server side)
%%--------------------------------------------------------------------

%% Results: [{ClientId, {ok, [ClaimMap]} | no_more | {error, _}}].
%% Fresh claim results are committed by ONE worker task per batch (the
%% gen_server is out of the per-message drain path). Marks whose result
%% is missing/error/stale have their claim released first
%% (synchronously, by the worker) and their client re-claimed on
%% completion, so a failed round cannot strand the backlog.
dispatch_deliver_results(Results, Marks, State) ->
    Shard = State#state.shard,
    {FreshResults, ReleaseMarks} = split_deliver_results(Results, Marks),
    State1 =
        case ReleaseMarks of
            [] ->
                State;
            _ ->
                case
                    submit_to_worker(fun() ->
                        do_release_tags_confirm(Shard, ReleaseMarks)
                    end)
                of
                    ok ->
                        State;
                    {error, _} ->
                        State
                end
        end,
    case FreshResults of
        [] ->
            State1;
        _ ->
            submit_commit(FreshResults, Marks, State1)
    end.

submit_commit(FreshResults, Marks, State) ->
    Shard = State#state.shard,
    case submit_to_worker(fun() -> do_commit_deliveries(Shard, FreshResults) end) of
        ok ->
            State;
        {error, Reason} ->
            %% Pool unavailable: hold the batch for the deferred retry
            %% (same path as a pool restart) instead of dropping it or
            %% delivering inline in the gen_server. Claims stay held, so
            %% the window is not violated.
            ?SLOG(warning, #{
                msg => "bcast_deliver_results_deferred_pool_unavailable",
                reason => Reason,
                results => length(FreshResults)
            }),
            schedule_deferred_retry(
                State#state{
                    %% Keep the marks with the deferred batch so the replay
                    %% can resolve each client result against its claim
                    %% round again (empty marks silently dropped the
                    %% batch's fresh results on replay).
                    deferred_deliveries = [
                        {results, FreshResults, Marks}
                        | State#state.deferred_deliveries
                    ]
                }
            )
    end.

%% Worker task: commit every fresh claim result of one batch. Per client
%% the results carry up to the window size of claimed deliveries, which
%% are prepared (payload fetched from the local mria copy), inserted into
%% the state row in claim order, and delivered in order.
-spec do_commit_deliveries(non_neg_integer(), [tuple()]) -> ok.
do_commit_deliveries(Shard, FreshResults) ->
    lists:foreach(
        fun({ClientId, Result, {_C, Tag, PK}}) ->
            try commit_delivery_result(Shard, ClientId, Result, PK, Tag) of
                _ -> ok
            catch
                Error:Reason:Stacktrace ->
                    ?SLOG(warning, #{
                        msg => "bcast_commit_delivery_result_failed",
                        exception => Error,
                        reason => Reason,
                        stacktrace => Stacktrace,
                        clientid => ClientId,
                        product_key => PK,
                        claim_tag => Tag
                    }),
                    _ = clear_row_claim(PK, ClientId, Tag),
                    do_release_client_claims(PK, ClientId, Tag)
            end
        end,
        FreshResults
    ).

%% One claim result committed by the delivery worker.
commit_delivery_result(Shard, ClientId, {ok, ClaimMaps}, PK, Tag) ->
    Key = {PK, ClientId},
    case ets:lookup(?TAB_STATE(Shard), Key) of
        [] ->
            %% Client disconnected/cleaned up while the claim was in
            %% flight: the cleanup already released the round by tag.
            ok;
        [#bcast_client_state{claim = {Tag, _Ts}} = Row] ->
            OldInflight = Row#bcast_client_state.inflight,
            case prepare_and_commit(Shard, PK, ClientId, ClaimMaps) of
                {ok, Added, Sends} ->
                    case commit_window_update(Shard, PK, ClientId, Tag, OldInflight, Added) of
                        true ->
                            bump_claim(Shard, -1),
                            %% Send in claim order after the window rows are
                            %% in place (window invariant: no new claim can
                            %% open a slot before the sends).
                            lists:foreach(fun run_send/1, Sends);
                        false ->
                            commit_lost_update_fallback(Shard, PK, ClientId, Tag)
                    end;
                {retry, Added} ->
                    %% Payload lag or session race: window entries for the
                    %% claimable part were committed, the failed dids were
                    %% released synchronously; re-claim the client for the
                    %% next round.
                    case commit_window_update(Shard, PK, ClientId, Tag, OldInflight, Added) of
                        true ->
                            bump_claim(Shard, -1),
                            gen_server:cast(shard_name(Shard), {retry_client, ClientId, PK});
                        false ->
                            commit_lost_update_fallback(Shard, PK, ClientId, Tag)
                    end
            end;
        [#bcast_client_state{}] ->
            %% Stale generation: a newer round owns the row. Release this
            %% round by tag (idempotent).
            do_release_client_claims(PK, ClientId, Tag)
    end;
commit_delivery_result(_Shard, ClientId, no_more, PK, Tag) ->
    %% Core explicitly returned no_more: it did NOT mark any claim
    %% inflight, so there is nothing to release. Only the local claim
    %% round mark is cleared.
    _ = clear_row_claim(PK, ClientId, Tag),
    ok;
commit_delivery_result(_Shard, _ClientId, {error, _Reason}, _PK, _Tag) ->
    %% Handled by the release path in split_deliver_results (marks of
    %% failed rounds are released by tag there); nothing to do here.
    ok.
%% Prepare every claimed delivery and commit the claimable part into the
%% window. Failed dids (offline / dead pid / payload lag / sub race)
%% release their core claims synchronously BEFORE any retry can claim
%% them again (FIFO safety). The sends are returned in claim order:
%% {qos1, EntryMap} for window deliveries, {qos0, ...} for the
%% auto-acked QoS0-subscription path. Returns {ok, Row2, Sends} when at
%% least one delivery is ready to send, {retry, Row2} otherwise.
prepare_and_commit(Shard, PK, ClientId, ClaimMaps) ->
    {SendsRev, Fails, Any} = lists:foldl(
        fun(Map, {Sends, Fl, Any1}) ->
            case prepare_delivery_entry(Shard, PK, ClientId, Map) of
                {pending, Entry} ->
                    {[{qos1, Entry} | Sends], Fl, true};
                {qos0, Entry0} ->
                    {[{qos0, Entry0} | Sends], Fl, true};
                {fail, Did} ->
                    %% Release the core claim NOW (synchronously) so a
                    %% retry claim of the same did cannot race it.
                    _ = emqx_bcast_index_owner:release_claim(PK, ClientId, Did),
                    {Sends, [Did | Fl], Any1};
                {fail_retry, Did} ->
                    _ = emqx_bcast_index_owner:release_claim(PK, ClientId, Did),
                    {Sends, [Did | Fl], Any1}
            end
        end,
        {[], [], false},
        ClaimMaps
    ),
    _ = {Fails, Shard},
    Sends = lists:reverse(SendsRev),
    Added = [
        {maps:get(delivery_id, E), false}
     || {qos1, E} <- Sends
    ],
    case Any of
        false ->
            {retry, Added};
        true ->
            {ok, Added, Sends}
    end.

%% Atomically close the claim round {Tag, _} and append the new window
%% entries, but ONLY if the row's claim and inflight are still exactly
%% what the worker read before preparing the sends. A full-row write
%% would clobber fields owned by the shard gen_server or other writers
%% (topics from subscribe/unsubscribe hooks, pid, an ack that landed
%% mid-commit); the guarded replace preserves them.
commit_window_update(Shard, PK, ClientId, Tag, OldInflight, Added) ->
    Tab = ?TAB_STATE(Shard),
    NewInflight = OldInflight ++ Added,
    %% Match-spec bodies cannot hold literal tuples (a tuple in expression
    %% position is parsed as a BIF call), so each inflight entry is
    %% emitted with tuple-construction syntax.
    InflightExpr = [{{Did, Acked}} || {Did, Acked} <- NewInflight],
    MS = [
        {
            #bcast_client_state{
                key = {PK, ClientId},
                product_key = '_',
                clientid = '_',
                pid = '$1',
                claim = {Tag, '_'},
                inflight = OldInflight,
                topics = '$2'
            },
            [],
            [
                {{bcast_client_state, {{PK, ClientId}}, PK, ClientId, '$1', undefined, InflightExpr,
                    '$2'}}
            ]
        }
    ],
    ets:select_replace(Tab, MS) =:= 1.

%% The row changed under the worker (ack landed, subscription update,
%% cleanup): do not clobber it with a stale write. Release the claim
%% round by tag (idempotent on the core) and re-claim the client.
commit_lost_update_fallback(Shard, PK, ClientId, Tag) ->
    _ = clear_row_claim(PK, ClientId, Tag),
    do_release_client_claims(PK, ClientId, Tag),
    gen_server:cast(shard_name(Shard), {retry_client, ClientId, PK}),
    ok.

run_send({qos1, Entry}) ->
    deliver_entry_committed(Entry);
run_send(
    {qos0, #{
        clientid := ClientId,
        delivery_id := DeliveryId,
        product_key := ProductKey,
        topic := Topic,
        payload := Payload,
        pid := Pid,
        attempts := Attempts
    }}
) ->
    do_deliver_qos0_and_ack(ClientId, Pid, Topic, Payload, DeliveryId, ProductKey, Attempts).

%% Prepare one claimed delivery (worker side): expand topic, fetch
%% payload, session checks. Returns {pending, EntryMap} / {qos0, Map} /
%% {fail, DeliveryId} / {fail_retry, DeliveryId}.
prepare_delivery_entry(Shard, PK, ClientId, ClaimMap) ->
    DeliveryId = maps:get(delivery_id, ClaimMap),
    Tpl = maps:get(topic_template, ClaimMap),
    SubQos = maps:get(sub_qos, ClaimMap, 1),
    Attempts = maps:get(attempt, ClaimMap, 1),
    case emqx_bcast:lookup_device({PK, ClientId}) of
        {error, not_found} ->
            {fail, DeliveryId};
        {ok, Pid} when is_pid(Pid) ->
            case is_process_alive(Pid) of
                false ->
                    {fail, DeliveryId};
                true ->
                    case fetch_payload(Shard, PK, ClientId, maps:get(msg_id, ClaimMap)) of
                        {ok, Payload} ->
                            Topic = emqx_bcast_utils:expand_topic(Tpl, PK, ClientId),
                            case SubQos >= 1 of
                                true ->
                                    {pending, #{
                                        clientid => ClientId,
                                        delivery_id => DeliveryId,
                                        product_key => PK,
                                        topic => Topic,
                                        payload => Payload,
                                        pid => Pid,
                                        attempts => Attempts
                                    }};
                                false ->
                                    {qos0, #{
                                        clientid => ClientId,
                                        delivery_id => DeliveryId,
                                        product_key => PK,
                                        topic => Topic,
                                        payload => Payload,
                                        pid => Pid,
                                        attempts => Attempts
                                    }}
                            end;
                        {error, payload_unavailable} ->
                            %% The claim core validated the message row at
                            %% claim time; the local mria copy lags (or
                            %% the row disappeared in a race). Release and
                            %% let the next round retry.
                            {fail_retry, DeliveryId}
                    end
            end
    end.
%% Payload for a claimed delivery: read the local mria copy of
%% bcast_message (every node - core or replicant - keeps a copy served by
%% rlog); on a transient miss fall back to the claim core's copy, which
%% validated the row at claim time. Returns {ok, Payload} |
%% {error, payload_unavailable}.
fetch_payload(Shard, PK, ClientId, MsgId) ->
    _ = {Shard, PK, ClientId},
    case local_payload(MsgId) of
        {ok, Payload} ->
            {ok, Payload};
        {error, _} ->
            Core = emqx_bcast_index_owner:shard_owner(
                emqx_bcast_index_owner:shard_of({PK, ClientId})
            ),
            remote_payload(Core, MsgId)
    end.

local_payload(MsgId) ->
    try
        case mnesia:dirty_read(?TAB_MSG, MsgId) of
            [#bcast_message{payload = Payload}] ->
                {ok, Payload};
            [] ->
                {error, not_found}
        end
    catch
        _:_ ->
            {error, not_found}
    end.

remote_payload(Core, MsgId) ->
    %% emqx_rpc:call/6 carries the caller module as the gen_rpc key
    %% (same shape the index shard routing uses).
    case
        emqx_rpc:call(
            ?MODULE,
            Core,
            emqx_bcast_storage,
            lookup_message,
            [MsgId],
            ?BCAST_RPC_CALL_TIMEOUT_MS
        )
    of
        {ok, #bcast_message{payload = Payload}} ->
            {ok, Payload};
        {badrpc, _} ->
            {error, payload_unavailable};
        _ ->
            {error, payload_unavailable}
    end.

%% Deliver one window entry, mirroring the per-entry safety net (a
%% malformed entry must not kill the worker and strand the rest of the
%% batch).
deliver_entry_committed(
    #{
        clientid := ClientId,
        delivery_id := DeliveryId,
        product_key := ProductKey
    } = Entry
) ->
    try deliver_pending_one(Entry) of
        _ -> ok
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(warning, #{
                msg => "bcast_deliver_entry_committed_failed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace,
                clientid => ClientId,
                delivery_id => DeliveryId
            }),
            fail_pending_delivery(ClientId, DeliveryId, ProductKey)
    end.

%% Results are consumed only for marks whose generation is still current.
%% Fresh marks are NOT cleared here: do_commit_deliveries clears them
%% once the window entries are committed, keeping the window intact
%% across the async worker hop. Every other mark goes through the
%% release-then-reclaim path.
split_deliver_results(Results, Marks) ->
    ResultsByClient = deliver_results_map(Results),
    lists:foldl(
        fun({ClientId, Tag, ProductKey} = Mark, {FreshAcc, ReleaseAcc}) ->
            Result = maps:get(ClientId, ResultsByClient, undefined),
            case {mark_current(ProductKey, ClientId, Tag), Result} of
                {true, {ok, _} = OkResult} ->
                    {[{ClientId, OkResult, Mark} | FreshAcc], ReleaseAcc};
                {true, no_more} ->
                    %% Nothing claimed at core; only the local round mark
                    %% is cleared (no release RPC needed).
                    _ = clear_row_claim(ProductKey, ClientId, Tag),
                    {FreshAcc, ReleaseAcc};
                {true, {error, _}} ->
                    %% The round failed at core; clear the local mark and
                    %% release by tag (it may have committed).
                    _ = clear_row_claim(ProductKey, ClientId, Tag),
                    {FreshAcc, [Mark | ReleaseAcc]};
                {true, undefined} ->
                    %% Claim failed / empty results: core may have
                    %% committed, so release by tag to be safe.
                    _ = clear_row_claim(ProductKey, ClientId, Tag),
                    {FreshAcc, [Mark | ReleaseAcc]};
                {false, _} ->
                    %% Stale generation: a newer round owns the mark now.
                    %% The old round may have committed; release by tag is
                    %% idempotent.
                    {FreshAcc, [Mark | ReleaseAcc]}
            end
        end,
        {[], []},
        Marks
    ).

%% Build the per-client result map without ever crashing the shard on a
%% malformed batch. Worker legs normalize their failures to
%% {error, claim_unavailable} at the source, but a stray {'EXIT', _} tail
%% or an improper list must not take down the gen_server (and with it
%% every state row of the partition): marks that end up without a result
%% fall through to the release-by-tag path in the fold above.
deliver_results_map(Results) ->
    try maps:from_list(Results) of
        M when is_map(M) -> M
    catch
        error:badarg ->
            ?SLOG(warning, #{
                msg => "bcast_malformed_deliver_results",
                entries => length_safe(Results)
            }),
            maps:from_list(collect_result_pairs(Results))
    end.

collect_result_pairs(Results) ->
    collect_result_pairs(Results, []).

collect_result_pairs([{ClientId, _} = Pair | Rest], Acc) when is_binary(ClientId) ->
    collect_result_pairs(Rest, [Pair | Acc]);
collect_result_pairs([_Other | Rest], Acc) ->
    %% Drop elements that are not {ClientId, Result} pairs; the matching
    %% mark is resolved as a failed round and released by tag.
    collect_result_pairs(Rest, Acc);
collect_result_pairs(_Tail, Acc) ->
    Acc.

length_safe(L) ->
    try length(L) of
        N -> N
    catch
        _:_ -> 0
    end.

mark_current(ProductKey, ClientId, Tag) ->
    Tab = ?TAB_STATE(shard_of(ProductKey, ClientId)),
    case ets:lookup(Tab, {ProductKey, ClientId}) of
        [#bcast_client_state{claim = {Tag, _Ts}}] -> true;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% Client lifecycle cleanup
%%--------------------------------------------------------------------

cleanup_client(ClientId, Pid, ProductKey, State) ->
    Shard = State#state.shard,
    Key = {ProductKey, ClientId},
    case ets:lookup(?TAB_STATE(Shard), Key) of
        [] ->
            ok;
        %% A disconnect/DOWN for a pid that no longer owns the row is
        %% stale (the client already reconnected and the row belongs to
        %% the new session): do not touch claim/window bookkeeping.
        [#bcast_client_state{pid = RowPid}] when RowPid =/= Pid ->
            ok;
        [#bcast_client_state{} = Row] ->
            %% Split the window: deliveries whose ack is already in flight
            %% must keep their row until the core-applied confirmation
            %% (acked is counted exactly once); everything else is
            %% released and dropped.
            {Keep, Release} = lists:partition(
                fun({_Did, AckInFlight}) -> AckInFlight end,
                Row#bcast_client_state.inflight
            ),
            State1 = release_claims_later(
                State, [{ProductKey, ClientId, Did} || {Did, false} <- Release]
            ),
            State2 =
                case Row#bcast_client_state.claim of
                    undefined ->
                        State1;
                    {Tag, _Ts} ->
                        release_tags_later(State1, [{ProductKey, ClientId, Tag}])
                end,
            case Keep of
                [] ->
                    _ = row_delete(State2, Row),
                    ok;
                _ ->
                    drop_row_indexes(Shard, Row),
                    row_put(
                        Shard,
                        Row#bcast_client_state{
                            pid = Pid,
                            claim = undefined,
                            inflight = Keep
                        }
                    ),
                    ok
            end
    end,
    emqx_bcast:unregister_device(ProductKey, ClientId, Pid),
    ok.

%%--------------------------------------------------------------------
%% Ack / window accounting
%%--------------------------------------------------------------------

%% Transition the window entry to ack-in-flight when a real PUBACK
%% matches a delivery. The entry then stays until the core-applied
%% confirmation deletes it, so duplicate PUBACKs never double-count.
mark_ack_in_flight(ProductKey, ClientId, DeliveryId) ->
    Tab = ?TAB_STATE(shard_of(ProductKey, ClientId)),
    case ets:lookup(Tab, {ProductKey, ClientId}) of
        [#bcast_client_state{} = Row] ->
            case lists:keytake(DeliveryId, 1, Row#bcast_client_state.inflight) of
                {value, {DeliveryId, false}, Rest} ->
                    %% Rewrite only the inflight field (window=1 makes it a
                    %% one-element list) instead of the whole row.
                    Now = erlang:system_time(millisecond),
                    ets:update_element(
                        Tab,
                        {ProductKey, ClientId},
                        {#bcast_client_state.inflight, [{DeliveryId, {true, Now}} | Rest]}
                    ),
                    true;
                _ ->
                    false
            end;
        [] ->
            false
    end.

%% Core has applied the ack; delete the ack-in-flight entry. Returns
%% \`counted\` only when this node actually transitioned that delivery to
%% ack-in-flight (so acked is counted exactly once per logical delivery).
take_ack_in_flight(ProductKey, ClientId, DeliveryId) ->
    Tab = ?TAB_STATE(shard_of(ProductKey, ClientId)),
    case ets:lookup(Tab, {ProductKey, ClientId}) of
        [#bcast_client_state{} = Row] ->
            case lists:keytake(DeliveryId, 1, Row#bcast_client_state.inflight) of
                {value, {DeliveryId, {true, _Ts}}, Rest} ->
                    %% Rewrite only the inflight field instead of the whole row.
                    ets:update_element(
                        Tab,
                        {ProductKey, ClientId},
                        {#bcast_client_state.inflight, Rest}
                    ),
                    counted;
                _ ->
                    false
            end;
        [] ->
            false
    end.
%%--------------------------------------------------------------------
%% Ack routing (core-applied confirmations back to the origin shards)
%%--------------------------------------------------------------------

%% Pairs: [{ClientId, ProductKey, DeliveryId, RemQueued}]
-spec ack_applied([{binary(), binary(), binary(), boolean()}]) -> ok.
ack_applied(Pairs) ->
    lists:foreach(
        fun({Shard, Sub}) ->
            gen_server:cast(shard_name(Shard), {ack_applied_batch, Sub})
        end,
        group_ack_pairs(Pairs)
    ),
    ok.

%% Claim results routed back from the core's server worker pool after an
%% asynchronous want_next.
-spec deliver_results_remote(non_neg_integer(), list(), list()) -> ok.
deliver_results_remote(Shard, Results, Marks) ->
    gen_server:cast(shard_name(Shard), {deliver_results, Results, Marks}),
    ok.

group_ack_pairs(Pairs) ->
    lists:foldl(
        fun(Pair = {ClientId, ProductKey, _Did, _Rem}, Acc) ->
            Shard = shard_of(ProductKey, ClientId),
            case lists:keyfind(Shard, 1, Acc) of
                {Shard, List} -> lists:keyreplace(Shard, 1, Acc, {Shard, [Pair | List]});
                false -> [{Shard, [Pair]} | Acc]
            end
        end,
        [],
        Pairs
    ).

%%--------------------------------------------------------------------
%% Stale claim-round sweep
%%--------------------------------------------------------------------

%% Release claim rounds older than the stale age and re-claim the clients
%% (their backlog keeps draining even after a dropped cast). The claim
%% counter is reconciled from the authoritative row table in the same pass
%% (backstop for a crash between a row update and its counter update; the
%% flush path itself is O(pending) thanks to the per-shard counter). The
%% pass also expires ack-in-flight marks older than their TTL (a lost
%% core-applied confirmation would otherwise hold the window closed
%% forever). One ets:foldl traversal replaces the former ets:tab2list
%% materialization plus two list comprehensions plus the stale filter,
%% which allocated every partition row up to three times per sweep tick.
sweep_stale_marks(State) ->
    Shard = State#state.shard,
    Now = erlang:system_time(millisecond),
    try
        {Claimed, StaleRev, ExpiredRev} = ets:foldl(
            fun(#bcast_client_state{} = Row, {C, Acc, ExpAcc}) ->
                C1 =
                    case Row#bcast_client_state.claim of
                        undefined -> C;
                        _ -> C + 1
                    end,
                Acc1 =
                    case Row#bcast_client_state.claim of
                        {Tag, Ts} when Now - Ts > ?CLAIM_STALE_AGE_MS ->
                            [
                                {
                                    Row#bcast_client_state.product_key,
                                    Row#bcast_client_state.clientid,
                                    Tag
                                }
                                | Acc
                            ];
                        _ ->
                            Acc
                    end,
                ExpAcc1 = lists:foldl(
                    fun
                        ({Did, {true, ATs} = Mark}, EA) when
                            Now - ATs > ?ACK_INFLIGHT_TTL_MS
                        ->
                            [
                                {
                                    Row#bcast_client_state.product_key,
                                    Row#bcast_client_state.clientid,
                                    Did,
                                    Mark
                                }
                                | EA
                            ];
                        (_, EA) ->
                            EA
                    end,
                    ExpAcc,
                    Row#bcast_client_state.inflight
                ),
                {C1, Acc1, ExpAcc1}
            end,
            {0, [], []},
            ?TAB_STATE(Shard)
        ),
        cnt_put(Shard, claim, Claimed),
        State1 = expire_ack_in_flight(Shard, lists:reverse(ExpiredRev), State),
        case StaleRev of
            [] ->
                State1;
            _ ->
                Live = [
                    Mark
                 || Mark = {C, Tag, PK} <- lists:reverse(StaleRev),
                    clear_row_claim(PK, C, Tag) =:= ok
                ],
                case Live of
                    [] ->
                        State1;
                    _ ->
                        ?SLOG(warning, #{
                            msg => "bcast_stale_claim_released",
                            marks => length(Live)
                        }),
                        case
                            submit_to_worker(fun() ->
                                do_release_tags_confirm(Shard, Live)
                            end)
                        of
                            ok ->
                                State1;
                            {error, _} ->
                                State1
                        end
                end
        end
    catch
        error:badarg ->
            State
    end.

%% Expire ack-in-flight marks whose core-applied confirmation was lost:
%% remove the window entry (atomically, so a row recreated between the
%% scan and the write is untouched), release the core claim so the
%% delivery is re-queued, and re-claim the client (at-least-once). A
%% confirmation that lands between the scan and the take simply wins:
%% the entry is gone, the take is a no-op, and the release is idempotent
%% on the core side (the claim is already consumed).
expire_ack_in_flight(_Shard, [], State) ->
    State;
expire_ack_in_flight(Shard, Expired, State) ->
    lists:foldl(
        fun({PK, DN, Did, Mark}, St) ->
            case take_expired_ack_in_flight(PK, DN, Did, Mark) of
                true ->
                    ?SLOG(warning, #{
                        msg => "bcast_ack_in_flight_expired",
                        product_key => PK,
                        clientid => DN,
                        delivery_id => Did
                    }),
                    reclaim_online(
                        PK, DN, Shard, release_claims_later(St, [{PK, DN, Did}])
                    );
                false ->
                    St
            end
        end,
        State,
        Expired
    ).

%% Atomically drop the expired mark from the row's window (returns true
%% when the row still carried exactly this mark). The match spec guards
%% on the whole inflight field so a row recreated after the scan (new
%% session, new window entries) is never clobbered.
take_expired_ack_in_flight(PK, DN, Did, Mark) ->
    Tab = ?TAB_STATE(shard_of(PK, DN)),
    Old = [{Did, Mark}],
    Rest = lists:keydelete(Did, 1, Old),
    %% The replacement body must be a double-wrapped tuple: a bare tuple
    %% in a match spec body is parsed as a function call. The field order
    %% mirrors the bcast_client_state record.
    MS = [
        {
            #bcast_client_state{
                key = {PK, DN},
                %% Unmentioned record fields expand to their record
                %% default (undefined), not '_': name them explicitly.
                product_key = '_',
                clientid = '_',
                pid = '$1',
                claim = '$2',
                inflight = Old,
                topics = '$3'
            },
            [],
            [{{bcast_client_state, {{PK, DN}}, PK, DN, '$1', '$2', Rest, '$3'}}]
        }
    ],
    try
        ets:select_replace(Tab, MS) =:= 1
    catch
        error:badarg -> false
    end.

%%--------------------------------------------------------------------
%% Pool restart helpers
%%--------------------------------------------------------------------

maybe_replay_deferred(State = #state{deferred_deliveries = []}) ->
    State;
maybe_replay_deferred(State = #state{deferred_deliveries = Deferred}) ->
    case emqx_bcast_utils:pool_available(?WORKER_POOL) of
        true ->
            lists:foldl(
                fun({results, Results, Marks}, AccState) ->
                    dispatch_deliver_results(Results, Marks, AccState)
                end,
                State#state{deferred_deliveries = [], deferred_retry_count = 0},
                lists:reverse(Deferred)
            );
        false ->
            Count = State#state.deferred_retry_count + 1,
            case Count rem 30 of
                0 ->
                    ?SLOG(warning, #{
                        msg => "bcast_deferred_delivery_retry",
                        deferred_deliveries => length(Deferred),
                        retries => Count
                    });
                _ ->
                    ok
            end,
            schedule_deferred_retry(State#state{deferred_retry_count = Count})
    end.

schedule_deferred_retry(State) ->
    Token = make_ref(),
    Timer = erlang:send_after(
        ?POOL_RESTART_RETRY_MS, self(), {retry_deferred, Token}
    ),
    State#state{deferred_retry = {Token, Timer}}.

cancel_deferred_retry(undefined) ->
    ok;
cancel_deferred_retry({_Token, Timer}) ->
    emqx_bcast_utils:cancel_timer(Timer).

cancel_restart_owner(undefined) ->
    ok;
cancel_restart_owner({MRef, _Pid}) ->
    erlang:demonitor(MRef, [flush]),
    ok.

submit_to_worker(Fun) ->
    case emqx_bcast_utils:submit_pool(?WORKER_POOL, Fun) of
        ok ->
            ok;
        {error, Reason} = Error ->
            %% Never run the task inline in the pull_shard gen_server: the
            %% task may contain long RPCs. Propagate the error instead of
            %% swallowing it.
            ?SLOG(warning, #{
                msg => "bcast_worker_pool_submit_failed",
                pool => ?WORKER_POOL,
                reason => Reason
            }),
            Error
    end.

%%--------------------------------------------------------------------
%% Release accumulation & batched flush
%%--------------------------------------------------------------------

%% Batch did-releases; flushed as grouped async casts (one per index
%% shard) on the release timer/size - never one spawn or RPC per release.
%% Used where no immediate reclaim follows (client gone / unsubscribed),
%% so ordering against new claims does not matter: the claim lease is the
%% backstop.
release_claims_later(State, []) ->
    State;
release_claims_later(State, Claims) ->
    S = State#state{release_claims = Claims ++ State#state.release_claims},
    arm_release_flush(S).

release_tags_later(State, []) ->
    State;
release_tags_later(State, Tags) ->
    S = State#state{release_tags = Tags ++ State#state.release_tags},
    arm_release_flush(S).

arm_release_flush(State = #state{release_timer = undefined}) ->
    Total = length(State#state.release_tags) + length(State#state.release_claims),
    case Total >= ?RELEASE_FLUSH_COUNT of
        true ->
            self() ! flush_releases,
            State;
        false ->
            State#state{
                release_timer = erlang:send_after(?RELEASE_FLUSH_MS, self(), flush_releases)
            }
    end;
arm_release_flush(State) ->
    State.

flush_releases(State = #state{release_tags = [], release_claims = []}) ->
    State;
flush_releases(State) ->
    Tags = State#state.release_tags,
    Claims = State#state.release_claims,
    case {Tags, Claims} of
        {[], []} ->
            State;
        _ ->
            _ = emqx_bcast_index_owner:release_client_claims_async(Tags),
            _ = emqx_bcast_index_owner:release_claims_async(Claims),
            State#state{release_tags = [], release_claims = []}
    end.
