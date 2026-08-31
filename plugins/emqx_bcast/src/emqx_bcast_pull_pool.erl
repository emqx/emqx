%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_pull_pool).

-behaviour(gen_server).

%% API for hooks and sibling pools. Hooks cast directly into the gen_server
%% with the protocol tuples handled below; the exported wrappers used to
%% construct different tuples and were silently swallowed by the catch-all
%% so they have been removed.
-export([
    start_link/1,
    shard_count/0,
    pool_name/1,
    shard_of/1,
    cast_client/2,
    tab/2,
    qos0_deliver_local/4,
    qos1_core_trigger_local/3,
    inflight_entries/1,
    begin_pools_restart/0,
    worker_pools_restarted/1
]).

%% Worker tasks.
-export([
    do_want_next/3,
    do_find_qos0_targets/5,
    do_find_trigger_devices/3,
    do_deliver_pending/1,
    do_deliver_qos0/1,
    do_deliver_qos0_and_ack/6,
    do_release_claim/3,
    do_release_client_claims/3
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

-define(PULL_POOL_SHARDS, 4).

-define(TAB_A(Shard), tab(Shard, bcast_buffer_a)).
-define(TAB_B(Shard), tab(Shard, bcast_buffer_b)).
-define(TAB_BUF3(Shard), tab(Shard, bcast_buffer3)).
-define(TAB_INFLIGHT(Shard), tab(Shard, bcast_pull_inflight)).
-define(WORKER_POOL, emqx_bcast_pull_worker_pool).
-define(POOL_RESTART_WATCHDOG_MS, 30000).
-define(POOL_RESTART_RETRY_MS, 1000).

%% Max staged want_next entries one flush tick submits for claiming; the
%% remainder stays staged for the next tick (unbounded batches made each
%% flush O(whole backlog) and fed a lag spiral on the coordinator). 2000
%% with the 10ms flush cadence keeps the claim pipeline fed without
%% unbounded batches.
-define(FLUSH_MAX_ENTRIES, 2000).

%% Retry cadence when the worker pool is unavailable (flush gate).
-define(FLUSH_RETRY_BACKOFF_MS, 1000).

-record(state, {
    shard = 0,
    flush_timer = undefined,
    mons = #{},
    pools_restarting = false,
    deferred_deliveries = [],
    restart_watchdog = undefined,
    restart_owner = undefined,
    deferred_retry = undefined,
    deferred_retry_count = 0
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(non_neg_integer()) -> gen_server:start_ret().
start_link(Shard) ->
    gen_server:start_link({local, pool_name(Shard)}, ?MODULE, [Shard], []).

-spec shard_count() -> pos_integer().
shard_count() ->
    ?PULL_POOL_SHARDS.

-spec pool_name(non_neg_integer()) -> atom().
pool_name(Shard) ->
    list_to_atom("emqx_bcast_pull_pool_" ++ integer_to_list(Shard)).

%% Route a client to its shard: phash2 over the device name. Every event
%% for one client (hooks, ack, trigger, deliver_results) must reach the
%% same shard so window=1 and the inflight mark stay single-writer.
-spec shard_of(binary()) -> non_neg_integer().
shard_of(ClientId) ->
    erlang:phash2(ClientId, ?PULL_POOL_SHARDS).

-spec cast_client(binary(), term()) -> ok.
cast_client(ClientId, Msg) ->
    gen_server:cast(pool_name(shard_of(ClientId)), Msg).

%% Per-shard ETS table name.
-spec tab(non_neg_integer(), atom()) -> atom().
tab(Shard, Base) ->
    list_to_atom(atom_to_list(Base) ++ "_" ++ integer_to_list(Shard)).

-spec qos0_deliver_local(binary(), [binary()] | undefined, binary(), binary()) -> ok.
qos0_deliver_local(ProductKey, DeviceNames, TopicTemplate, Payload) ->
    lists:foreach(
        fun({Shard, Sub}) ->
            gen_server:cast(
                pool_name(Shard),
                {qos0_deliver, ProductKey, Sub, TopicTemplate, Payload}
            )
        end,
        group_devices(DeviceNames)
    ).

-spec qos1_core_trigger_local(binary(), [binary()], binary()) -> ok.
qos1_core_trigger_local(ProductKey, DeviceNames, TopicTemplate) ->
    lists:foreach(
        fun({Shard, Sub}) ->
            gen_server:cast(
                pool_name(Shard),
                {qos1_core_trigger, ProductKey, Sub, TopicTemplate}
            )
        end,
        group_devices(DeviceNames)
    ).

%% Split a device list into per-shard groups. undefined (PubBroadcast) is
%% handled by ONE shard: the product-wide device scan cannot be partitioned
%% cheaply, so broadcasting undefined to all shards multiplied the full
%% registry scan 4x (each shard scanned everything, then the shard filter
%% kept only its own devices - correct, but 4x the scan CPU). PubBroadcast
%% is a low-frequency entry, so one shard's sequential scan is fine (the
%% shard id is still passed through to do_find_qos0_targets/5 for
%% symmetry, but the undefined branch delivers to every device).
group_devices(undefined) ->
    [{0, undefined}];
group_devices(DeviceNames) ->
    lists:foldr(
        fun(DN, Acc) ->
            Shard = shard_of(DN),
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
            try gen_server:call(pool_name(Shard), begin_pools_restart, infinity) of
                {ok, Marks} -> {ok, Marks};
                {error, restart_in_progress} = E -> E
            catch
                exit:{noproc, _} -> {ok, []};
                exit:{normal, _} -> {ok, []}
            end
        end
     || Shard <- lists:seq(0, ?PULL_POOL_SHARDS - 1)
    ],
    case [E || {error, _} = E <- Results] of
        [] ->
            {ok, lists:append([M || {ok, M} <- Results])};
        [E | _] ->
            %% A partial failure (one shard already in a restart) must
            %% not leave the successfully-armed shards frozen in
            %% pools_restarting until the 30s watchdog - reset them so the
            %% next begin can run cleanly.
            lists:foreach(
                fun
                    ({Shard, {ok, _Marks}}) ->
                        gen_server:cast(pool_name(Shard), {abort_pools_restart});
                    ({_Shard, _}) ->
                        ok
                end,
                lists:zip(lists:seq(0, ?PULL_POOL_SHARDS - 1), Results)
            ),
            E
    end.

%% Per-shard snapshot only. Each shard's begin_pools_restart reply
%% used to scan ALL shards' inflight tables, so the aggregated mark list
%% carried 4 copies of every mark (4x release RPCs at restart time).
-spec inflight_entries(non_neg_integer()) -> [{binary(), pos_integer(), binary()}].
inflight_entries(Shard) ->
    try
        [
            {ClientId, Tag, ProductKey}
         || {ClientId, Tag, ProductKey, _Ts} <- ets:tab2list(?TAB_INFLIGHT(Shard))
        ]
    catch
        error:badarg -> []
    end.

-spec worker_pools_restarted([{binary(), pos_integer(), binary()}]) -> ok.
worker_pools_restarted(Marks) ->
    Groups = lists:foldr(
        fun({ClientId, _Tag, _ProductKey} = Mark, Acc) ->
            Shard = shard_of(ClientId),
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
            gen_server:cast(pool_name(Shard), {worker_pools_restarted, SubMarks})
        end,
        Groups
    ).

%%--------------------------------------------------------------------
%% Worker tasks
%%--------------------------------------------------------------------

-spec do_want_next(non_neg_integer(), node(), [map()]) -> ok.
do_want_next(Shard, Core, Entries) ->
    %% Subscription filters are resolved here, in the worker, by reading
    %% EMQX's own subscription tables for each channel pid. The topics
    %% travel with the claim request so the core claim tx can skip entries
    %% the device is not subscribed to. An empty subscription (or a dead
    %% pid) yields no topics and the claim will return no_more.
    Entries1 = [
        M#{topics => subscription_topics(maps:get(pid, M))}
     || M <- Entries
    ],
    Results0 =
        case Core =:= node() of
            true ->
                try
                    emqx_bcast_pull_server_pool:want_next(Entries1)
                catch
                    Error:Reason ->
                        ?SLOG(warning, #{
                            msg => "bcast_want_next_local_failed",
                            exception => Error,
                            reason => Reason
                        }),
                        []
                end;
            false ->
                try
                    emqx_rpc:call(
                        ?MODULE,
                        Core,
                        emqx_bcast_pull_server_pool,
                        want_next,
                        [Entries1],
                        ?BCAST_RPC_CALL_TIMEOUT_MS
                    )
                of
                    R when is_list(R) ->
                        R;
                    {error, ClaimReason} ->
                        ?SLOG(warning, #{
                            msg => "bcast_want_next_claim_failed",
                            reason => ClaimReason
                        }),
                        [];
                    Other ->
                        ?SLOG(warning, #{
                            msg => "bcast_want_next_unexpected_result",
                            result => Other
                        }),
                        []
                catch
                    Error:Reason ->
                        ?SLOG(warning, #{
                            msg => "bcast_want_next_rpc_failed",
                            exception => Error,
                            reason => Reason
                        }),
                        []
                end
        end,
    Results =
        case Results0 of
            L when is_list(L) -> L;
            _ -> []
        end,
    %% Every mark travels with its claim generation. The gen_server clears
    %% an inflight mark only when the generation still matches, and can
    %% release-by-tag when the RPC timed out (the core transaction may have
    %% committed) or when a stale result races a newer claim.
    Marks = [inflight_mark(M) || M <- Entries],
    gen_server:cast(pool_name(Shard), {deliver_results, Results, Marks}).

%% [{TopicFilter, Qos}] from EMQX's own subscription tables for a channel
%% pid. This is the single source of truth; no plugin-side mirror exists.
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

-spec do_deliver_pending([#bcast_buffer_entry{}]) -> ok.
do_deliver_pending(Entries) ->
    lists:foreach(
        fun(Entry) ->
            try deliver_pending_one(Entry) of
                ok -> ok
            catch
                Error:Reason:Stacktrace ->
                    %% A malformed entry must not kill the worker mid-foreach
                    %% and leave the rest of the active buffer blocking
                    %% window=1 forever.
                    ?SLOG(warning, #{
                        msg => "bcast_deliver_pending_entry_failed",
                        exception => Error,
                        reason => Reason,
                        stacktrace => Stacktrace,
                        clientid => Entry#bcast_buffer_entry.clientid,
                        delivery_id => Entry#bcast_buffer_entry.delivery_id
                    }),
                    fail_pending_delivery(Entry)
            end
        end,
        Entries
    ).

deliver_pending_one(
    #bcast_buffer_entry{
        clientid = ClientId,
        delivery_id = DeliveryId,
        product_key = ProductKey,
        topic = Topic,
        payload = Payload,
        pid = Pid
    }
) ->
    case session_holds_channel(ClientId, Pid) of
        true ->
            case is_process_alive(Pid) of
                false ->
                    fail_pending_delivery(#bcast_buffer_entry{
                        clientid = ClientId,
                        delivery_id = DeliveryId,
                        product_key = ProductKey
                    });
                true ->
                    %% The topic was expanded once in prepare_delivery and
                    %% stored in the buffer entry; no per-delivery re-expansion.
                    %% The subscription was matched at claim time
                    %% (claim_check returned sub_qos) - the final re-check is
                    %% dropped. A client that unsubscribes in the few ms
                    %% between claim and delivery still receives this
                    %% message, matching EMQX's own in-flight semantics.
                    %% This also removes the per-message subscription query
                    %% (emqx_broker:subscriptions/1) from the drain hot path.
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
                    emqx_bcast_metrics:qos1_delivered()
            end;
        _ ->
            %% Client gone, dead, or taken over by another channel: do not
            %% count a delivery that cannot reach the current session.
            %% Remove the buffer entry and release the claim.
            fail_pending_delivery(#bcast_buffer_entry{
                clientid = ClientId,
                delivery_id = DeliveryId,
                product_key = ProductKey
            })
    end.

fail_pending_delivery(#bcast_buffer_entry{
    clientid = ClientId,
    delivery_id = DeliveryId,
    product_key = ProductKey
}) ->
    gen_server:cast(pool_name(shard_of(ClientId)), {deliver_failed, ClientId, DeliveryId}),
    do_release_claim(ProductKey, ClientId, DeliveryId).

-spec do_deliver_qos0([{pid(), binary(), binary(), binary(), binary()}]) -> ok.
do_deliver_qos0(Targets) ->
    lists:foreach(
        fun({Pid, _ProductKey, ClientId, Topic, Payload}) ->
            case session_holds_channel(ClientId, Pid) of
                true ->
                    Msg = emqx_message:make(ClientId, ?QOS_0, Topic, Payload),
                    Pid ! #deliver{topic = Topic, message = Msg};
                false ->
                    %% The session was taken over (or disconnected) after the
                    %% target scan; only the current channel holder may
                    %% receive the direct #deliver.
                    ok
            end
        end,
        Targets
    ).

%% Resolve QoS0 fanout targets in a worker: for each target device, look up
%% its channel pid and check the subscription by reading EMQX's own
%% subscription tables (emqx_broker:subscriptions/1) instead of a plugin
%% mirror. DeviceNames = undefined means product-wide (PubBroadcast).
%% DeviceNames = undefined (PubBroadcast) is handled by ONE shard
%% (group_devices/1 sends it only to shard 0), which delivers to EVERY
%% device of the product - no shard filter here. The earlier version
%% fanned undefined out to all shards and each shard filtered to its own
%% partition, which delivered every device 4 times (a duplicate-delivery bug) and
%% multiplied the product-wide scan 4x.
-spec do_find_qos0_targets(non_neg_integer(), binary(), [binary()] | undefined, binary(), binary()) ->
    [{pid(), binary(), binary(), binary(), binary()}].
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
    %% Expand \${productKey} once for the whole fanout; only the
    %% per-device \${deviceName} replacement runs inside the loop.
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

%% Resolve which devices a QoS1 trigger applies to, in a worker. Returns
%% [{DeviceName, Pid}] for devices that are online and subscribed.
-spec do_find_trigger_devices(binary(), [binary()], binary()) -> [{binary(), pid()}].
do_find_trigger_devices(ProductKey, DeviceNames, TopicTemplate) ->
    %% Single \${productKey} expansion for the whole trigger fanout.
    Partial = emqx_bcast_utils:replace_product_key(TopicTemplate, ProductKey),
    lists:filtermap(
        fun(DeviceName) ->
            case emqx_bcast:lookup_device({ProductKey, DeviceName}) of
                {ok, Pid} ->
                    Topic = emqx_bcast_utils:expand_topic_partial(Partial, DeviceName),
                    case sub_match(Pid, Topic) of
                        {ok, _} -> {true, {DeviceName, Pid}};
                        false -> false
                    end;
                {error, not_found} ->
                    false
            end
        end,
        DeviceNames
    ).

%% Session-holder check against EMQX's authoritative channel registry.
%% emqx_cm:lookup_channels/1 is global, so this also covers a takeover where
%% the new channel lives on another node while the local device registry
%% still contains the old pid.
session_holds_channel(ClientId, Pid) ->
    lists:member(Pid, emqx_cm:lookup_channels(ClientId)).

%% Subscription check against EMQX's own subscription state. Returns
%% {ok, Qos} with a matching subscription QoS, or false.
%% Early-exit on the first matching filter (throw/catch) instead of
%% folding over the whole subscription list; every call site only tests
%% {ok, _} vs false, so the first-match QoS is sufficient.
%% The per-message final re-check in deliver_pending_one was dropped
%% (claim-time matching is authoritative); sub_match now runs only in the
%% target-resolution paths that CONSTRUCT a delivery (do_find_trigger_
%% devices / do_find_qos0_targets). Note: a match-first variant that
%% avoids building the full [{Topic, SubOpts}] list would need
%% emqx_broker to expose the plain topic list, which is not available.
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

-spec do_deliver_qos0_and_ack(binary(), pid(), binary(), binary(), binary(), binary()) -> ok.
do_deliver_qos0_and_ack(ClientId, Pid, Topic, Payload, DeliveryId, ProductKey) ->
    Msg = emqx_message:make(ClientId, ?QOS_0, Topic, Payload),
    Pid ! #deliver{topic = Topic, message = Msg},
    emqx_bcast_metrics:qos1_auto_acked(),
    emqx_bcast_ack_pool:ack(ClientId, DeliveryId, ProductKey).

-spec do_release_claim(binary(), binary(), binary()) -> ok.
do_release_claim(ProductKey, ClientId, DeliveryId) ->
    _ = emqx_bcast:rpc_core(emqx_bcast_storage, release_claim, [ProductKey, ClientId, DeliveryId]),
    ok.

-spec do_release_client_claims(binary(), binary(), pos_integer()) -> ok.
do_release_client_claims(ProductKey, ClientId, ClaimTag) ->
    _ = emqx_bcast:rpc_core(
        emqx_bcast_storage, release_client_claims, [ProductKey, ClientId, ClaimTag]
    ),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Shard]) ->
    ok = ensure_buffer_table(?TAB_A(Shard), #bcast_buffer_entry.clientid),
    ok = ensure_buffer_table(?TAB_B(Shard), #bcast_buffer_entry.clientid),
    ok = ensure_buffer_table(?TAB_BUF3(Shard), #bcast_buffer3.clientid),
    ok = ensure_buffer_table(?TAB_INFLIGHT(Shard), 1),
    {ok, #state{shard = Shard}}.

handle_call(begin_pools_restart, _From, State = #state{pools_restarting = true}) ->
    {reply, {error, restart_in_progress}, State};
handle_call(begin_pools_restart, {Caller, _Tag}, State) ->
    %% Atomically stop new flushes and snapshot the current inflight marks.
    %% No mark can be created between this snapshot and worker termination.
    %% Monitor the restart caller so an early death self-heals; the timer is
    %% only a backstop for a hung caller and is sized above the worst-case
    %% three-pool shutdown budget.
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
    Ref = monitor(process, Pid),
    Mons = maps:put(Ref, {Pid, ClientId, ProductKey}, State#state.mons),
    {noreply, State#state{mons = Mons}};
handle_cast({client_disconnected, ClientId, Pid, ProductKey}, State) ->
    cleanup_client(ClientId, Pid, ProductKey),
    {noreply, State};
handle_cast({subscribe, ClientId, Pid, ProductKey}, State) ->
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    {noreply, trigger_want_next(ClientId, Pid, ProductKey, State)};
handle_cast({unsubscribe, ClientId, Pid, ProductKey}, State) ->
    %% The client is no longer subscribed: release an unacked delivery
    %% instead of leaving it in the active buffer until the 60s lease or a
    %% disconnect. The core entry goes back to stored and can be claimed
    %% again if the client resubscribes.
    case ets:lookup(?TAB_A(State#state.shard), ClientId) of
        [
            #bcast_buffer_entry{
                pid = Pid, product_key = ProductKey, delivery_id = DeliveryId
            }
        ] ->
            ets:delete(?TAB_A(State#state.shard), ClientId),
            submit_to_worker(fun() -> do_release_claim(ProductKey, ClientId, DeliveryId) end);
        _ ->
            ok
    end,
    {noreply, State};
handle_cast({ping, ClientId, Pid, ProductKey}, State) ->
    %% register_device is now idempotent (skips when the entry already
    %% holds this pid), so the 1500/s keepalive pings do not rewrite the
    %% registry.
    emqx_bcast:register_device(ProductKey, ClientId, Pid),
    {noreply, trigger_want_next(ClientId, Pid, ProductKey, State)};
handle_cast({ack, ClientId, DeliveryId, ProductKey}, State) ->
    {Pid, State0} =
        case take_pending(ClientId, DeliveryId) of
            {ok, PendingPid} ->
                emqx_bcast_metrics:qos1_acked(),
                {PendingPid, State};
            none ->
                %% Duplicate PUBACK: core accounting is idempotent, and the
                %% metric is intentionally not incremented here.
                case emqx_bcast:lookup_device({ProductKey, ClientId}) of
                    {ok, CurrentPid} -> {CurrentPid, State};
                    {error, not_found} -> {undefined, State}
                end
        end,
    State2 =
        case Pid of
            undefined ->
                State0;
            _ ->
                trigger_want_next(ClientId, Pid, ProductKey, State0)
        end,
    {noreply, State2};
handle_cast({qos0_deliver, ProductKey, DeviceNames, TopicTemplate, Payload}, State) ->
    %% The per-device online + subscription check runs in a worker: it reads
    %% emqx_broker:subscriptions(Pid) per device and must not block the
    %% gen_server on a large fanout. The shard id travels with the task;
    %% DeviceNames=undefined (PubBroadcast) is delivered to every device by
    %% the single shard that group_devices/1 sends it to.
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
handle_cast({qos1_core_trigger, ProductKey, DeviceNames, TopicTemplate}, State) ->
    %% Resolve online+subscribed devices in a worker AND stage the
    %% want_next entries there too (the buffer3 table is public with
    %% write_concurrency; per-client key, so concurrent staging is safe).
    %% The gen_server only receives a count notification to arm the flush
    %% timer - no more inline fold of a 10k-device trigger inside one cast
    %% (which stalled every ack/deliver_results behind it).
    Shard = State#state.shard,
    submit_to_worker(fun() ->
        Triggers = do_find_trigger_devices(ProductKey, DeviceNames, TopicTemplate),
        Staged = lists:foldl(
            fun({DeviceName, Pid}, N) ->
                case stage_want_next(DeviceName, Pid, ProductKey, Shard) of
                    true -> N + 1;
                    false -> N
                end
            end,
            0,
            Triggers
        ),
        case Staged of
            0 -> ok;
            _ -> gen_server:cast(pool_name(Shard), {buffer3_staged, Staged})
        end
    end),
    {noreply, State};
handle_cast({buffer3_staged, _Staged}, State) ->
    %% Staged-count notification; no metric (avoids counter churn).
    {noreply, maybe_flush_buffer3(State)};
handle_cast({deliver_results, Results, Marks}, State = #state{pools_restarting = true}) ->
    %% Workers are being torn down; keep this batch for replay after the
    %% replacement pools are running instead of clearing marks and then
    %% dropping the fill/deliver submissions into a dead pool.
    Deferred = State#state.deferred_deliveries,
    {noreply, State#state{deferred_deliveries = [{results, Results, Marks} | Deferred]}};
handle_cast({deliver_results, Results, Marks}, State) ->
    {noreply, dispatch_deliver_results(Results, Marks, State)};
handle_cast({prepared, Prepared}, State = #state{pools_restarting = true}) ->
    %% A prepare worker finished across a pool restart: hold the decisions
    %% for replay after the replacement pools are running (marks stay held).
    Deferred = State#state.deferred_deliveries,
    {noreply, State#state{deferred_deliveries = [{prepared, Prepared} | Deferred]}};
handle_cast({prepared, Prepared}, State) ->
    {noreply, handle_prepared(Prepared, State)};
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
    %% workers may have been killed mid-RPC. Release those generations
    %% by tag and replay the trigger only after the release has completed on
    %% core, so the replay claim cannot see the old entry still pending.
    Shard = State#state.shard,
    lists:foreach(
        fun({C, Tag, PK} = _Mark) ->
            case clear_inflight_mark(C, Tag) of
                ok ->
                    submit_to_worker(fun() ->
                        do_release_client_claims(PK, C, Tag),
                        gen_server:cast(pool_name(Shard), {claim_released, C, PK})
                    end);
                stale ->
                    submit_release_mark(PK, C, Tag)
            end
        end,
        Marks
    ),
    State1 = maybe_replay_deferred(State0),
    {noreply, maybe_flush_buffer3(State1)};
handle_cast({abort_pools_restart}, State = #state{pools_restarting = true}) ->
    %% Reset a begin_pools_restart that was aborted because a sibling
    %% shard reported restart_in_progress (the restart owner owns the
    %% marks snapshot then; we must not release anything here).
    %% Replay any deferred deliver_results/prepared batches - the old
    %% code cancelled the retry timer but never replayed, so a shard that
    %% queued results during the (now aborted) restart window kept its
    %% held inflight marks forever and the affected clients' window=1
    %% stalled until a full restart happened to succeed. Mirrors the
    %% watchdog and worker_pools_restarted recovery paths.
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
    case emqx_bcast:lookup_device({ProductKey, ClientId}) of
        {ok, Pid} ->
            {noreply, trigger_want_next(ClientId, Pid, ProductKey, State)};
        {error, not_found} ->
            {noreply, State}
    end;
handle_cast({deliver_failed, ClientId, DeliveryId}, State) ->
    %% The deliver worker found the channel pid dead: drop the buffer entry
    %% (if it is still the same delivery) so it does not sit in the active
    %% buffer as an unacked tombstone blocking window=1 forever.
    case ets:lookup(?TAB_A(State#state.shard), ClientId) of
        [#bcast_buffer_entry{delivery_id = DeliveryId}] ->
            ets:delete(?TAB_A(State#state.shard), ClientId);
        _ ->
            ok
    end,
    {noreply, State};
handle_cast(Msg, State) ->
    ?SLOG(warning, #{msg => "bcast_pull_pool_unexpected_cast", message => Msg}),
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
    %% The begin-time inflight snapshot is intentionally not released here;
    %% those marks remain covered by the pending lease if the caller died
    %% before worker_pools_restarted could run its tagged release.
    State1 = maybe_replay_deferred(State0),
    {noreply, maybe_flush_buffer3(State1)};
handle_info({retry_deferred, Token}, State = #state{deferred_retry = {Token, _Timer}}) ->
    {noreply, maybe_replay_deferred(State#state{deferred_retry = undefined})};
handle_info({retry_deferred, _StaleToken}, State) ->
    {noreply, State};
handle_info(flush_buffer3, State = #state{pools_restarting = true}) ->
    %% Keep staged entries until the replacement pools are running.
    ok = emqx_bcast_utils:cancel_timer(State#state.flush_timer),
    {noreply, State#state{flush_timer = undefined}};
handle_info(flush_buffer3, State) ->
    ok = emqx_bcast_utils:cancel_timer(State#state.flush_timer),
    %% Gate the flush on pool availability BEFORE taking entries: if the
    %% worker pool is down (restart window) the submitted tasks would be
    %% dropped and the inflight marks taken here would never be cleared
    %% (window=1 stall + bcast_pull_inflight leak). Keep everything staged
    %% and re-arm.
    case emqx_bcast_utils:pool_available(?WORKER_POOL) of
        false ->
            %% Fixed backoff instead of the immediate re-arm
            %% (maybe_flush_buffer3 self-messages instantly when staged >=
            %% 100 and re-arms every 10ms below that - either way a pool
            %% outage spun one warning per 10ms).
            ?SLOG(warning, #{
                msg => "bcast_flush_deferred_pool_unavailable",
                staged => ets:info(?TAB_BUF3(State#state.shard), size)
            }),
            {noreply, State#state{
                flush_timer = erlang:send_after(
                    ?FLUSH_RETRY_BACKOFF_MS, self(), flush_buffer3
                )
            }};
        true ->
            flush_buffer3_available(State)
    end;
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
            cleanup_client(ClientId, Pid, ProductKey),
            gen_server:cast(emqx_bcast_ack_pool, {client_down, ClientId}),
            {noreply, State#state{mons = Mons}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

trigger_want_next(ClientId, Pid, ProductKey, State) ->
    Shard = State#state.shard,
    case ets:lookup(?TAB_A(Shard), ClientId) of
        [_ | _] ->
            %% Window = 1: an unacked QoS1 delivery is already pending.
            State;
        [] ->
            case ets:lookup(?TAB_INFLIGHT(Shard), ClientId) of
                [{ClientId, _Tag, _ProductKey, _Ts}] ->
                    %% Window = 1: a want_next claim for this client is still
                    %% in flight (buffer3 flushed, deliver_results pending).
                    %% The mark is held until the prepared entry lands in
                    %% the buffer (handle_prepared), so without this guard a
                    %% second claim would take the NEXT delivery (the first
                    %% is pending on core) and the two results would race
                    %% each other, breaking the ack/take_pending
                    %% association.
                    State;
                [] ->
                    case ets:lookup(?TAB_BUF3(Shard), ClientId) of
                        [_ | _] ->
                            %% buffer3 dedup: one want_next per client per batch.
                            State;
                        [] ->
                            ets:insert(?TAB_BUF3(Shard), #bcast_buffer3{
                                clientid = ClientId,
                                product_key = ProductKey,
                                pid = Pid
                            }),
                            case State#state.pools_restarting of
                                true -> State;
                                false -> maybe_flush_buffer3(State)
                            end
                    end
            end
    end.

maybe_flush_buffer3(State) ->
    Timer = emqx_bcast_utils:maybe_batch_flush(
        ets:info(?TAB_BUF3(State#state.shard), size),
        State#state.flush_timer,
        flush_buffer3
    ),
    State#state{flush_timer = Timer}.

buffer3_to_map(#bcast_buffer3{
    clientid = ClientId,
    product_key = ProductKey,
    pid = Pid
}) ->
    #{clientid => ClientId, product_key => ProductKey, pid => Pid}.

next_claim_tag() ->
    erlang:unique_integer([monotonic, positive]).

inflight_record(#{clientid := ClientId, product_key := ProductKey}, Tag) ->
    {ClientId, Tag, ProductKey, erlang:system_time(millisecond)}.

inflight_mark(#{clientid := ClientId, product_key := ProductKey, claim_tag := Tag}) ->
    {ClientId, Tag, ProductKey}.

mark_current(ClientId, Tag) ->
    case ets:lookup(?TAB_INFLIGHT(shard_of(ClientId)), ClientId) of
        [{ClientId, Tag, _ProductKey, _Ts}] -> true;
        _ -> false
    end.

%% Single ets:take instead of lookup + delete. The mark is only
%% cleared (never kept) here, so taking it is safe; the fresh-result path
%% in split_deliver_results still uses mark_current/1 (lookup) because the
%% mark must STAY in the table until handle_prepared commits the buffer
%% entry (window=1 held-mark invariant).
clear_inflight_mark(ClientId, Tag) ->
    case ets:take(?TAB_INFLIGHT(shard_of(ClientId)), ClientId) of
        [{ClientId, Tag, _ProductKey, _Ts}] ->
            ok;
        _ ->
            stale
    end.

%% Deliver-result dispatch (gen_server side). The split is cheap and runs
%% inline; fresh inflight marks stay HELD until the prepared decision lands
%% in the buffer (handle_prepared clears them). Clearing a mark before the
%% buffer insert would open a window where a ping/subscribe-triggered
%% want_next claims a second delivery for the same client (window=1
%% violation: duplicate deliveries, ack/take_pending mismatch, exact-count
%% loyalty breakage). The per-entry preparation (device lookup + EMQX
%% subscription reads) runs in the worker pool.
dispatch_deliver_results(Results, Marks, State) ->
    {FreshResults, ReleaseMarks} = split_deliver_results(Results, Marks),
    [submit_release_mark(PK, C, Tag) || {C, Tag, PK} <- ReleaseMarks],
    case FreshResults of
        [] ->
            State;
        _ ->
            Shard = State#state.shard,
            case submit_to_worker(fun() -> do_prepare_deliveries(Shard, FreshResults) end) of
                ok ->
                    State;
                {error, Reason} ->
                    %% Pool unavailable: prepare inline so the held marks
                    %% cannot stall window=1 for the affected clients.
                    ?SLOG(warning, #{
                        msg => "bcast_prepare_inline_fallback",
                        reason => Reason
                    }),
                    handle_prepared(prepare_deliveries(FreshResults), State)
            end
    end.

%% Worker task: resolve every fresh claim result into a buffer entry or a
%% release/auto-ack action, then hand the decisions back to the shard
%% gen_server (the only process allowed to touch the buffer and the inflight
%% marks).
do_prepare_deliveries(Shard, FreshResults) ->
    try prepare_deliveries(FreshResults) of
        Prepared ->
            gen_server:cast(pool_name(Shard), {prepared, Prepared})
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "bcast_prepare_deliveries_failed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            %% Release the held marks so window=1 does not stall for these
            %% clients; the entries become claimable again via the lease.
            lists:foreach(
                fun({ClientId, _Result, {_C, Tag, PK}}) ->
                    _ = clear_inflight_mark(ClientId, Tag),
                    submit_release_mark(PK, ClientId, Tag)
                end,
                FreshResults
            )
    end.

prepare_deliveries(FreshResults) ->
    [
        {ClientId, prepare_result(ClientId, Result), Mark}
     || {ClientId, Result, Mark} <- FreshResults
    ].

prepare_result(_ClientId, no_more) ->
    no_more;
prepare_result(ClientId, {ok, DeliverMap}) ->
    prepare_delivery(ClientId, DeliverMap).

submit_release_mark(ProductKey, ClientId, Tag) ->
    submit_to_worker(fun() -> do_release_client_claims(ProductKey, ClientId, Tag) end).

%% Results are consumed only for marks whose generation is still current.
%% Fresh marks are deliberately NOT cleared here: handle_prepared clears
%% them once the prepared entry/action is committed, keeping window=1
%% intact across the async prepare hop. Every other mark has its claim
%% released by tag: that covers a timed-out claim RPC whose core
%% transaction may have committed and a late result from a previous client
%% generation racing a new claim.
split_deliver_results(Results, Marks) ->
    ResultsByClient = maps:from_list([{C, Result} || {C, Result} <- Results]),
    lists:foldl(
        fun({ClientId, Tag, _ProductKey} = Mark, {FreshAcc, ReleaseAcc}) ->
            Result = maps:get(ClientId, ResultsByClient, no_more),
            case {mark_current(ClientId, Tag), Result} of
                {true, {ok, _} = OkResult} ->
                    {[{ClientId, OkResult, Mark} | FreshAcc], ReleaseAcc};
                {true, _NoResult} ->
                    ok = clear_inflight_mark(ClientId, Tag),
                    {FreshAcc, [Mark | ReleaseAcc]};
                {false, _} ->
                    {FreshAcc, [Mark | ReleaseAcc]}
            end
        end,
        {[], []},
        Marks
    ).

cleanup_client(ClientId, Pid, ProductKey) ->
    Shard = shard_of(ClientId),
    release_pending(?TAB_A(Shard), ClientId, Pid),
    cleanup_buffer3(ClientId, Pid),
    release_inflight_claim(ClientId),
    ets:delete(?TAB_INFLIGHT(Shard), ClientId),
    emqx_bcast:unregister_device(ProductKey, ClientId, Pid).

%% A claim RPC may already have committed on core while this node thought
%% the client went away; release it by tag immediately instead of leaving it
%% pending for the full lease.
release_inflight_claim(ClientId) ->
    case ets:lookup(?TAB_INFLIGHT(shard_of(ClientId)), ClientId) of
        [{ClientId, Tag, ProductKey, _Ts}] ->
            submit_release_mark(ProductKey, ClientId, Tag);
        [] ->
            ok
    end.

%% The client went away with an unacked delivery in flight: drop the buffer
%% entry and release its claim so the delivery becomes claimable again
%% (e.g. after a reconnect) without waiting for the pending lease.
release_pending(Tab, ClientId, Pid) ->
    case ets:lookup(Tab, ClientId) of
        [
            #bcast_buffer_entry{
                pid = Pid, product_key = ProductKey, delivery_id = DeliveryId
            }
        ] ->
            ets:delete(Tab, ClientId),
            submit_to_worker(fun() -> do_release_claim(ProductKey, ClientId, DeliveryId) end);
        _ ->
            ok
    end.

cleanup_buffer3(ClientId, Pid) ->
    case ets:lookup(?TAB_BUF3(shard_of(ClientId)), ClientId) of
        [#bcast_buffer3{pid = Pid}] -> ets:delete(?TAB_BUF3(shard_of(ClientId)), ClientId);
        _ -> ok
    end.

take_pending(ClientId, DeliveryId) ->
    case ets:lookup(?TAB_A(shard_of(ClientId)), ClientId) of
        [#bcast_buffer_entry{delivery_id = DeliveryId, pid = Pid}] ->
            ets:delete(?TAB_A(shard_of(ClientId)), ClientId),
            {ok, Pid};
        _ ->
            none
    end.

%%--------------------------------------------------------------------
%% Internal: deliver-results processing and AB filling
%%--------------------------------------------------------------------

%% Commit prepared decisions (gen_server side). Entries land in the single
%% active buffer via insert_new (window=1: a client holds at most one
%% unacked entry; a conflicting insert is unreachable while marks are held
%% until here, and is released defensively if it ever happens). The
%% inflight mark is cleared only after the insert (or the release/auto-ack
%% action submission), so no window exists in which a concurrent
%% ping/subscribe-triggered want_next can double-claim.
handle_prepared(Prepared, State) ->
    {ToDeliver, Actions} = lists:foldl(
        fun({ClientId, PreparedResult, {_C, Tag, PK}}, {DeliverAcc, ActionAcc}) ->
            case PreparedResult of
                {pending, Entry} ->
                    case ets:insert_new(?TAB_A(shard_of(ClientId)), Entry) of
                        true ->
                            %% The entry landed in the active buffer; the
                            %% held inflight mark is cleared now (window=1
                            %% closes only after the buffer insert).
                            _ = clear_inflight_mark(ClientId, Tag),
                            {[Entry | DeliverAcc], ActionAcc};
                        false ->
                            %% Defensive: window=1 conflict (unreachable while
                            %% marks are held until fill). Drop the new claim
                            %% and release it so it becomes claimable again.
                            %% stale is fine here too: the mark is gone,
                            %% the claim is already released elsewhere.
                            _ = clear_inflight_mark(ClientId, Tag),
                            submit_to_worker(fun() ->
                                do_release_claim(
                                    PK, ClientId, Entry#bcast_buffer_entry.delivery_id
                                )
                            end),
                            {DeliverAcc, ActionAcc}
                    end;
                {qos0, ClientId0, Pid, Topic, Payload, DeliveryId, ProductKey} ->
                    _ = clear_inflight_mark(ClientId, Tag),
                    Action = fun() ->
                        do_deliver_qos0_and_ack(
                            ClientId0, Pid, Topic, Payload, DeliveryId, ProductKey
                        )
                    end,
                    {DeliverAcc, [Action | ActionAcc]};
                {no_match, ProductKey, ClientId0, DeliveryId} ->
                    _ = clear_inflight_mark(ClientId, Tag),
                    {DeliverAcc, [
                        fun() -> do_release_claim(ProductKey, ClientId0, DeliveryId) end
                        | ActionAcc
                    ]};
                {offline, ProductKey, ClientId0, DeliveryId} ->
                    %% Claimed on core but the device is not connected
                    %% here: release so the entry does not stay pending
                    %% until its lease expires.
                    _ = clear_inflight_mark(ClientId, Tag),
                    {DeliverAcc, [
                        fun() -> do_release_claim(ProductKey, ClientId0, DeliveryId) end
                        | ActionAcc
                    ]};
                no_more ->
                    _ = clear_inflight_mark(ClientId, Tag),
                    {DeliverAcc, ActionAcc}
            end
        end,
        {[], []},
        Prepared
    ),
    %% One worker task for all actions instead of one submit per
    %% action (each submit is a gproc pick + cast; a prepared batch with
    %% several releases/auto-acks no longer churns the pool).
    %% One failing action must not skip the rest of the batch.
    case Actions of
        [] ->
            ok;
        _ ->
            submit_to_worker(fun() ->
                lists:foreach(
                    fun(Action) ->
                        try Action() of
                            _ -> ok
                        catch
                            Error:Reason:Stacktrace ->
                                ?SLOG(warning, #{
                                    msg => "bcast_prepared_action_failed",
                                    exception => Error,
                                    reason => Reason,
                                    stacktrace => Stacktrace
                                })
                        end
                    end,
                    Actions
                )
            end)
    end,
    case ToDeliver of
        [] ->
            State;
        _ ->
            submit_to_worker(fun() -> do_deliver_pending(ToDeliver) end),
            State
    end.

%% Subscription QoS comes from the claim result (sub_qos), which was
%% resolved from emqx_broker:subscriptions in do_want_next and matched in
%% claim_check. No extra subscription read here; deliver_pending_one keeps
%% its final sub_match as the unsubscribe-race guard.
prepare_delivery(
    ClientId,
    #{
        delivery_id := DeliveryId,
        product_key := ProductKey,
        topic_template := TopicTemplate,
        payload := Payload
    } = ClaimMap
) ->
    %% Subscription QoS is carried by the claim result. Default to 1
    %% (QoS1 pending path) for callers that build claim maps without it
    %% (e.g. tests).
    SubQos = maps:get(sub_qos, ClaimMap, 1),
    case emqx_bcast:lookup_device({ProductKey, ClientId}) of
        {error, not_found} ->
            {offline, ProductKey, ClientId, DeliveryId};
        {ok, Pid} when is_pid(Pid) ->
            case is_process_alive(Pid) of
                false ->
                    {offline, ProductKey, ClientId, DeliveryId};
                true ->
                    %% Expand once here; deliver_pending_one reuses it.
                    Topic = emqx_bcast_utils:expand_topic(TopicTemplate, ProductKey, ClientId),
                    case SubQos >= 1 of
                        true ->
                            {pending, #bcast_buffer_entry{
                                clientid = ClientId,
                                delivery_id = DeliveryId,
                                product_key = ProductKey,
                                topic_template = TopicTemplate,
                                topic = Topic,
                                payload = Payload,
                                pid = Pid
                            }};
                        false ->
                            {qos0, ClientId, Pid, Topic, Payload, DeliveryId, ProductKey}
                    end
            end
    end.

%% Bounded want_next flush: take at most Max staged entries per tick via
%% ets:first/next + ets:take (the shard gen_server is the only writer of
%% its own buffer3 table, so the walk is race-free). A partial take leaves
%% the rest staged; the flush handler re-arms the timer via
%% maybe_flush_buffer3.
%% NOTE: ets:select_take/3 looked like the right one-op
%% replacement but only exists on OTP 26+; the plugin still runs on the
%% EMQX 6.1 testbed (OTP 25), where it is an undefined function. Keeping
%% the portable first/next + take walk.
take_buffer3_batch(Shard, Max) ->
    case ets:first(?TAB_BUF3(Shard)) of
        '$end_of_table' ->
            {[], 0};
        First ->
            take_buffer3_from(Shard, Max, First, 0, [])
    end.

take_buffer3_from(_Shard, _Max, '$end_of_table', _N, Acc) ->
    {lists:reverse(Acc), 0};
take_buffer3_from(Shard, Max, _Key, N, Acc) when N >= Max ->
    {lists:reverse(Acc), ets:info(?TAB_BUF3(Shard), size)};
take_buffer3_from(Shard, Max, Key, N, Acc) ->
    %% Capture the successor BEFORE the take: ets:next on a set table
    %% requires the key to still exist.
    Next = ets:next(?TAB_BUF3(Shard), Key),
    case ets:take(?TAB_BUF3(Shard), Key) of
        [Entry] -> take_buffer3_from(Shard, Max, Next, N + 1, [Entry | Acc]);
        [] -> take_buffer3_from(Shard, Max, Next, N, Acc)
    end.

maybe_replay_deferred(State = #state{deferred_deliveries = []}) ->
    State;
maybe_replay_deferred(State = #state{deferred_deliveries = Deferred}) ->
    case emqx_bcast_utils:pool_available(?WORKER_POOL) of
        true ->
            lists:foldl(
                fun
                    ({results, Results, Marks}, AccState) ->
                        dispatch_deliver_results(Results, Marks, AccState);
                    ({prepared, Prepared}, AccState) ->
                        handle_prepared(Prepared, AccState)
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
            %% Never run the task inline in the pull_pool gen_server: the task
            %% may contain a 15s RPC. Propagate the error instead of
            %% swallowing it - the flush rollback and the prepare
            %% inline-fallback branches match on {error, _} and were dead
            %% code while this function always returned ok (a pool death
            %% between the availability gate and the submit would strand
            %% inflight marks / drop staged entries).
            ?SLOG(warning, #{
                msg => "bcast_worker_pool_submit_failed",
                pool => ?WORKER_POOL,
                reason => Reason
            }),
            Error
    end.

flush_buffer3_available(State) ->
    {Entries, _Remaining} = take_buffer3_batch(State#state.shard, ?FLUSH_MAX_ENTRIES),
    case Entries of
        [] ->
            %% Nothing staged; a partial batch's remainder is covered by the
            %% maybe_flush_buffer3 re-arm below.
            {noreply, maybe_flush_buffer3(State#state{flush_timer = undefined})};
        _ ->
            Shard = State#state.shard,
            Maps = [
                begin
                    M0 = buffer3_to_map(E),
                    Tag = next_claim_tag(),
                    ets:insert(?TAB_INFLIGHT(Shard), inflight_record(M0, Tag)),
                    M0#{claim_tag => Tag}
                end
             || E <- Entries
            ],
            %% Resolve the sorted core list once per flush tick instead
            %% of calling core_for/1 (lists:sort + membership query) for
            %% every staged entry.
            CoreNodes = lists:sort(emqx_bcast:core_nodes()),
            Groups = lists:foldr(
                fun(M, Acc) ->
                    Core = core_of(maps:get(clientid, M), CoreNodes),
                    case lists:keyfind(Core, 1, Acc) of
                        {Core, List} ->
                            lists:keyreplace(Core, 1, Acc, {Core, [M | List]});
                        false ->
                            [{Core, [M]} | Acc]
                    end
                end,
                [],
                Maps
            ),
            lists:foreach(
                fun({Core, Group}) ->
                    case submit_to_worker(fun() -> do_want_next(Shard, Core, Group) end) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            %% A task dropped here would strand its
                            %% inflight marks. Roll back: clear the marks and
                            %% re-stage the entries for the next tick.
                            ?SLOG(warning, #{
                                msg => "bcast_flush_submit_failed_rollback",
                                reason => Reason,
                                entries => length(Group)
                            }),
                            lists:foreach(
                                fun(#{clientid := C, claim_tag := Tag}) ->
                                    _ = clear_inflight_mark(C, Tag)
                                end,
                                Group
                            ),
                            lists:foreach(
                                fun(#{clientid := C, product_key := PK, pid := Pid}) ->
                                    ets:insert(?TAB_BUF3(Shard), #bcast_buffer3{
                                        clientid = C,
                                        product_key = PK,
                                        pid = Pid
                                    })
                                end,
                                Group
                            )
                    end
                end,
                Groups
            ),
            {noreply, maybe_flush_buffer3(State#state{flush_timer = undefined})}
    end.

%% Stable core selection for one client given a pre-sorted node list
%% (Shared across a whole flush tick).
core_of(ClientId, SortedNodes) when SortedNodes =/= [] ->
    lists:nth(erlang:phash2(ClientId, length(SortedNodes)) + 1, SortedNodes);
core_of(_ClientId, []) ->
    node().

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal: buffer management
%%--------------------------------------------------------------------

ensure_buffer_table(Name, KeyPos) ->
    emqx_bcast_utils:ensure_ets(Name, [
        named_table,
        public,
        set,
        {keypos, KeyPos},
        {read_concurrency, true},
        {write_concurrency, true}
    ]).

%% Worker-side staging used by qos1_core_trigger. Pure ETS checks and
%% insert on the shard's public buffer3 table; returns true when a new
%% entry was staged so the caller can notify the shard to arm the flush
%% timer. Safe concurrently: the key is the clientid, so the last writer
%% wins and the shard is the only process that takes entries.
stage_want_next(ClientId, Pid, ProductKey, Shard) ->
    case ets:lookup(?TAB_A(Shard), ClientId) of
        [_ | _] ->
            false;
        [] ->
            case ets:lookup(?TAB_INFLIGHT(Shard), ClientId) of
                [{ClientId, _Tag, _ProductKey, _Ts}] ->
                    false;
                [] ->
                    case ets:lookup(?TAB_BUF3(Shard), ClientId) of
                        [_ | _] ->
                            false;
                        [] ->
                            ets:insert(?TAB_BUF3(Shard), #bcast_buffer3{
                                clientid = ClientId,
                                product_key = ProductKey,
                                pid = Pid
                            }),
                            true
                    end
            end
    end.
