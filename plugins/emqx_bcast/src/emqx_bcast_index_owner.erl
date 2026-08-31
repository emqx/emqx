%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_index_owner).

%% Authoritative per-device pending-delivery index, sharded across
%% ?SHARD_COUNT owner processes on the owner core node. Each shard owns a
%% disjoint partition of the {ProductKey, DeviceName} key space (by
%% phash2) and mutates only its own process-heap structures, so claims,
%% acks, appends and releases for DIFFERENT devices run in parallel while
%% every operation for the SAME device stays serialized in one process
%% (per-device FIFO holds, window=1, no lock machinery).
%%
%% Storage layout (process heap, no ETS):
%%   * queues    :: #{Key => queue:queue()}  per-device FIFO of Did
%%   * dids      :: #{Key3 => Ts}            existence + append timestamp
%%   * inflights :: #{Key => #{Key3 => {Ts, Tag}}}  claimed-not-acked
%%   * counts    :: #{Key => N}              pending count per device
%%   * reserves  :: #{Key => {Count, Ts}}    admission reservations
%% A claim pops the FIFO head (O(1)); acks/releases are O(1) map deletes;
%% lazy residuals (acked/removed entries still in the queue) are dropped
%% on the next claim pass. This replaces the old ETS layout (ordered_set
%% index + did/seq/count/reserve tables) whose allocator never returned
%% memory to the OS: 80M pending cost ~37.7GB of ETS memory (41.5GB RSS)
%% vs ~11GB of process heap with dynamic reclamation via fullsweep GC.
%%
%% The mria tables (bcast_msg deliveries, bcast_msg_meta counters,
%% bcast_message payloads) stay authoritative: each shard's heap index is
%% a derived, rebuildable cache. If the owner node dies, the surviving
%% core takes over and every shard rebuilds its partition from bcast_msg
%% (activate_partition); pending data committed to mria is not lost.
%%
%% Routing is two-dimensional:
%%   * per-device index state  -> shard_of({PK, DN})
%%   * per-delivery meta counter -> did_shard_of(DeliveryId)
%% The global pending counter lives in one shared ETS row
%% (bcast_quota_ets) updated with atomic update_counter from every shard.

-behaviour(gen_server).

-export([start_link/1]).
-export([shard_count/0, owner_node/0, is_owner/0]).
-export([
    append_batch/1,
    remove_batch/1,
    claim/1,
    ack_batch/1,
    release_claim/3,
    release_client_claims/3,
    check_quota/2,
    admit/2,
    release_admit/2,
    reserve_global_local/2,
    release_global_local/1,
    quota_update_local/1,
    device_deliveries/1,
    device_delivery_entries/1,
    pending_count/0,
    pending_count_for/1,
    pending_count_local/0,
    delete_delivery/1,
    delete_message/1,
    cleanup_expired/0,
    create_sync/2,
    create_delivery/6,
    rebuild_index/0,
    reset/0
]).
-export([local_handle/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

%% Fast activation poll: the first claim can arrive within ~50ms of app
%% start (CT topologies), so the leader must activate promptly. Polling a
%% non-owner node every 50ms is a trivial is_owner() check.
-define(OWNER_POLL_MS, 50).
-define(SYNC_TIMEOUT_MS, 30000).
-define(CLAIM_TIMEOUT_MS, 30000).

%% How many parallel index-owner processes the index is sharded into.
%% Shards are placed round-robin across the RUNNING core nodes (each core
%% runs all SHARD_COUNT processes, only its assigned subset activates), so
%% the index work fans out over all cores instead of one owner node.
%% 4 (2 per core) keeps the per-batch claim/ack groups coarse; 16 made
%% each 10ms batch split into ~4.5 entries per shard, multiplying the
%% per-message RPC/scheduling cost.
-define(SHARD_COUNT, 4).

%% How long a pending (claimed but not acked/released) entry stays excluded
%% from claims. Same value as the historical mnesia claim lease.
-define(PENDING_TTL_MS, 60000).

%% How long an admission reservation (accepted-but-not-yet-promoted) may
%% outlive its queue entry: reservations from a crashed intake node expire
%% after this window so the quota counter self-heals.
-define(RESERVE_TTL_MS, 60000).

%% Max index rows the orphan-repair scan inspects per cleanup tick.
-define(ORPHAN_SCAN_BUDGET, 2000).

%% Max expired deliveries the cleanup tick scans and removes. The
%% dirty_select limit bounds both the scan (projection) and the deletion
%% work per 60s tick, so a huge expiry backlog is drained across ticks
%% instead of one cleanup run holding the tables for minutes.
-define(CLEANUP_BUDGET, 10000).

%% Fresh index entries whose mnesia rows are not (yet) visible on this
%% node are skipped by claims instead of dropped: a concurrent promotion on
%% the peer core may still be replicating its transaction. Entries older
%% than this window with a missing row are genuinely stale and get dropped.
-define(REPLICATION_LAG_MS, 5000).

%% Poll interval for the conditional fullsweep GC that reclaims the
%% process heap after the pending backlog drains (dynamic memory return).
-define(GC_POLL_MS, 5000).

%% Heap threshold (words) above which a shrink triggers a major GC.
-define(GC_MIN_HEAP_WORDS, 16000000).

-spec start_link(integer()) -> gen_server:start_ret().
start_link(Shard) ->
    gen_server:start_link({local, shard_name(Shard)}, ?MODULE, [Shard], []).

-spec shard_count() -> pos_integer().
shard_count() ->
    ?SHARD_COUNT.

-spec shard_name(integer()) -> atom().
shard_name(Shard) ->
    list_to_atom("emqx_bcast_index_owner_" ++ integer_to_list(Shard)).

%% Device key partition: every operation for the same {PK, DN} lands on
%% the same shard, so per-device invariants are preserved.
-spec shard_of({binary(), binary()}) -> pos_integer().
shard_of(Key) ->
    erlang:phash2(Key, ?SHARD_COUNT).

%% Delivery counter partition: the bcast_msg_meta row is shared by every
%% device of a batch, so its counter is owned by one shard per Did.
-spec did_shard_of(binary()) -> pos_integer().
did_shard_of(Did) ->
    erlang:phash2(Did, ?SHARD_COUNT).

%% The global quota counter owner: the lexicographically smallest
%% running core node. All shards (wherever they run) update this one ETS
%% row through quota_update/1 (local update_counter or one emqx_rpc hop).
%% The single-node fallback keeps CT deployments working.
-spec owner_node() -> node().
owner_node() ->
    case emqx_bcast:core_nodes() of
        [] -> node();
        Nodes -> hd(lists:sort(Nodes))
    end.

-spec is_owner() -> boolean().
is_owner() ->
    owner_node() =:= node().

%% Shard placement: shard N lives on the (N mod core count)-th running
%% core. Every core runs all SHARD_COUNT shard processes (dormant until
%% assigned); when cores leave/join the allocation shifts automatically and
%% the activation leader (quota owner's shard 0) drives activation of the
%% newly assigned subset, rebuilding from mria.
-spec shard_owner(pos_integer()) -> node().
shard_owner(Shard) ->
    case emqx_bcast:core_nodes() of
        [] ->
            node();
        Nodes ->
            Sorted = lists:sort(Nodes),
            lists:nth((Shard rem length(Sorted)) + 1, Sorted)
    end.

init([Shard]) ->
    process_flag(trap_exit, true),
    State = #{
        shard => Shard,
        active => false,
        %% per-device FIFO: #{Key => queue:queue()} of Did
        queues => #{},
        %% existence + append timestamp: #{Key3 => Ts}
        dids => #{},
        %% claimed-not-acked: #{Key => #{Key3 => {Ts, ClaimTag}}}
        inflights => #{},
        %% pending count per device: #{Key => N}
        counts => #{},
        %% admission reservations: #{Key => {Count, Ts}}
        reserves => #{},
        %% peak pending size for conditional fullsweep
        peak => 0,
        %% bounded orphan-scan cursor
        orphan_cursor => 0
    },
    State1 = start_gc_timer(State),
    case Shard of
        0 ->
            %% Shard 0 is the activation leader (quota owner's shard 0).
            %% Activation is NOT driven from init (drive_activation()
            %% gen_server:calls the sibling shards, which may not be started
            %% yet); an immediate message runs inside the loop right after
            %% init, and the drive tolerates siblings that are still
            %% starting (retry at ?OWNER_POLL_MS). The quota table is only
            %% (re)initialized once this node actually is the quota owner.
            self() ! maybe_activate,
            {ok, State1};
        _ ->
            %% Non-leader shards activate when the leader drives them.
            {ok, State1}
    end.

%% Every shard runs the conditional fullsweep poll (memory reclamation
%% after the backlog drains).
start_gc_timer(State) ->
    erlang:send_after(?GC_POLL_MS, self(), maybe_gc),
    State.

ensure_quota_table() ->
    try ets:new(?TAB_QUOTA_ETS, [named_table, set, public, {read_concurrency, true}]) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

%%--------------------------------------------------------------------
%% Public API: every operation routes to the shard that owns its key.
%% Local callers go straight into the shard gen_server; remote callers use
%% one rpc hop, so per-key state is always mutated inside one process.
%%--------------------------------------------------------------------

append_batch(Entries) ->
    %% Report failures instead of swallowing them: with the atomic intake
    %% take the promoter cannot re-take a batch whose append failed, so a
    %% silent swallow would lose committed-but-unindexed deliveries.
    Results = parallel_map(
        fun({Shard, Sub}) ->
            route(Shard, {append_batch, Sub}, ?SYNC_TIMEOUT_MS)
        end,
        group_entries(Entries)
    ),
    %% A timed-out shard call surfaces as {'EXIT', _} from the catch in
    %% parallel_map; both shapes count as failure.
    Failed = fun
        ({error, _}) -> true;
        ({'EXIT', _}) -> true;
        (_) -> false
    end,
    case lists:any(Failed, Results) of
        true -> {error, append_failed};
        false -> ok
    end.

remove_batch(Entries) ->
    parallel_foreach(
        fun({Shard, Sub}) ->
            route(Shard, {remove_batch, Sub}, ?SYNC_TIMEOUT_MS)
        end,
        group_entries(Entries)
    ),
    ok.

claim(Entries) ->
    %% Per-shard calls run in parallel: the shards are independent, and a
    %% sequential fan-out multiplied the batch latency by the shard count
    %% (window=1 per client makes the drain latency-bound).
    lists:append(
        parallel_map(
            fun({Shard, Sub}) ->
                route(Shard, {claim, Sub}, ?CLAIM_TIMEOUT_MS)
            end,
            group_claim_entries(Entries)
        )
    ).

ack_batch(Acks) ->
    %% The index removal runs on the device shard; the meta counter
    %% decrement runs on the delivery shard. Groups run in parallel; each
    %% group keeps its device-then-delivery ordering. Results are returned
    %% in input order (callers only consume them for single-ack calls).
    Results0 = lists:append(
        parallel_map(
            fun({{DevShard, DidShard}, Sub}) ->
                Results = route(DevShard, {ack_index, Sub}, ?SYNC_TIMEOUT_MS),
                %% Only decrement the per-delivery meta counter for acks
                %% that actually removed an index entry (counted). A
                %% duplicate PUBACK (not_found) must NOT decrement again:
                %% after a claim-lease expiry redelivery the client acks
                %% the same delivery twice, and the second (duplicate) ack
                %% used to decrement the meta counter a second time -
                %% completing the delivery early, deleting the
                %% delivery/message rows while the OTHER devices of the
                %% batch still had pending entries (lost deliveries and
                %% quota skew).
                CountedDids = [
                    Did
                 || {Did, counted} <- lists:zip(
                        [Did || {_PK, _DN, Did} <- Sub],
                        Results
                    )
                ],
                ok = route(DidShard, {meta_dec_batch, CountedDids}, ?SYNC_TIMEOUT_MS),
                lists:zip(Sub, Results)
            end,
            group_acks(Acks)
        )
    ),
    %% O(n) map lookup instead of O(n^2) proplists scan per ack
    %% (a 500-ack batch was ~125k tuple comparisons before).
    ResultsMap = maps:from_list(Results0),
    [maps:get(Ack, ResultsMap, not_found) || Ack <- Acks].

%% Run independent shard calls concurrently and collect the results in
%% input order. Each call is a local gen_server call or a remote RPC; the
%% shards are disjoint, so the order between them does not matter.
parallel_map(Fun, List) ->
    Parent = self(),
    Refs = [
        begin
            Ref = make_ref(),
            spawn_link(fun() -> Parent ! {Ref, catch Fun(Item)} end),
            Ref
        end
     || Item <- List
    ],
    [
        receive
            {Ref, Result} -> Result
        end
     || Ref <- Refs
    ].

parallel_foreach(Fun, List) ->
    _ = parallel_map(Fun, List),
    ok.

release_claim(PK, DN, Did) ->
    route(shard_of({PK, DN}), {release_claim, PK, DN, Did}, ?SYNC_TIMEOUT_MS),
    ok.

release_client_claims(PK, DN, Tag) ->
    route(shard_of({PK, DN}), {release_client_claims, PK, DN, Tag}, ?SYNC_TIMEOUT_MS),
    ok.

%% Caller-side quota check (no shard-0 mailbox): the global counter
%% is read directly from the shared ETS row and the per-device checks fan
%% out to the shards in parallel.
check_quota(PK, DNs) ->
    GlobalMax = emqx_bcast_config:get(max_pending_deliveries),
    PerDeviceMax = emqx_bcast_config:get(max_pending_deliveries_per_device),
    check_quota_parallel(PK, DNs, #{global => GlobalMax, per_device => PerDeviceMax}).

%% Admission runs in the CALLER process, not through a coordinator:
%% the global pending counter is reserved atomically on the owner node
%% (update_counter with rollback on overshoot), then per-device checks
%% and reservations fan out to all shards in parallel. This removes the
%% shard-0 serialization that capped API acceptance at ~630 req/s (every
%% request used to take up to 9 sequential gen_server calls through shard
%% 0: 1 admit + 4 check + 4 reserve).
admit(PK, DNs) ->
    GlobalMax = emqx_bcast_config:get(max_pending_deliveries),
    PerDeviceMax = emqx_bcast_config:get(max_pending_deliveries_per_device),
    Len = length(DNs),
    case reserve_global(Len, GlobalMax) of
        ok ->
            Groups = group_devices(PK, DNs),
            Over = parallel_check_devices(PK, Groups, PerDeviceMax),
            case Over of
                [] ->
                    parallel_reserve_devices(PK, Groups),
                    ok;
                _ ->
                    release_global(Len),
                    {error, {quota_exceeded, Over}}
            end;
        {error, _} ->
            {error, {quota_exceeded, []}}
    end.

release_admit(PK, DNs) ->
    parallel_release_devices(PK, group_devices(PK, DNs)),
    ok.

%% Atomic global reservation on the owner node (the quota table lives
%% there); emqx_rpc is used when the caller is on another node.
reserve_global(Len, GlobalMax) ->
    case owner_node() =:= node() of
        true ->
            reserve_global_local(Len, GlobalMax);
        false ->
            emqx_rpc:call(
                ?MODULE,
                owner_node(),
                ?MODULE,
                reserve_global_local,
                [Len, GlobalMax],
                ?SYNC_TIMEOUT_MS
            )
    end.

release_global(Len) ->
    case owner_node() =:= node() of
        true ->
            _ = ets:update_counter(?TAB_QUOTA_ETS, global, {2, -Len}),
            ok;
        false ->
            _ = emqx_rpc:call(
                ?MODULE, owner_node(), ?MODULE, release_global_local, [Len], ?SYNC_TIMEOUT_MS
            ),
            ok
    end.

%% Runs on the owner node (exported for emqx_rpc).
reserve_global_local(Len, GlobalMax) ->
    NewGlobal = ets:update_counter(?TAB_QUOTA_ETS, global, {2, Len}),
    case NewGlobal > GlobalMax of
        true ->
            _ = ets:update_counter(?TAB_QUOTA_ETS, global, {2, -Len}),
            {error, quota_exceeded};
        false ->
            ok
    end.

%% Runs on the owner node (exported for emqx_rpc).
release_global_local(Len) ->
    _ = ets:update_counter(?TAB_QUOTA_ETS, global, {2, -Len}),
    ok.

%% Per-device quota checks across all shards in parallel (shard
%% gen_server calls). A dormant or failing shard degrades to "no
%% over-limit devices" (admission pressure defers to the bounded intake
%% queue), matching the old coordinator's degrade-to-accept behavior
%% during owner takeover.
parallel_check_devices(PK, Groups, Max) ->
    lists:append(
        parallel_map(
            fun({Shard, Sub}) ->
                case route(Shard, {check_devices, PK, Sub, Max}, ?SYNC_TIMEOUT_MS) of
                    {error, not_active} -> [];
                    {'EXIT', _} -> [];
                    Over -> Over
                end
            end,
            Groups
        )
    ).

%% Per-device reservations/releases keep going through the shard
%% gen_servers (parallel): reserve rows are read-modify-write and must be
%% serialized with append_entry's reserve_dec on the same shard process.
parallel_reserve_devices(PK, Groups) ->
    parallel_foreach(
        fun({Shard, Sub}) ->
            _ = route(Shard, {reserve_devices, PK, Sub}, ?SYNC_TIMEOUT_MS),
            ok
        end,
        Groups
    ).

parallel_release_devices(PK, Groups) ->
    parallel_foreach(
        fun({Shard, Sub}) ->
            _ = route(Shard, {release_devices, PK, Sub}, ?SYNC_TIMEOUT_MS),
            ok
        end,
        Groups
    ).

device_deliveries(Key) ->
    route(shard_of(Key), {device_deliveries, Key}, ?SYNC_TIMEOUT_MS).

device_delivery_entries(Key) ->
    route(shard_of(Key), {device_delivery_entries, Key}, ?SYNC_TIMEOUT_MS).

pending_count() ->
    route(0, {pending_count}, ?SYNC_TIMEOUT_MS).

pending_count_for(Key) ->
    route(shard_of(Key), {pending_count_for, Key}, ?SYNC_TIMEOUT_MS).

delete_delivery(Did) ->
    %% The delivery row carries the device list; read it locally (mria
    %% replica) and dispatch the index removal to the device shards, then
    %% delete the rows on the delivery shard.
    case mnesia:dirty_read(?TAB_MSG_REC, Did) of
        [] ->
            {error, not_found};
        [#bcast_msg{product_key = PK, device_names = DNs, msg_id = MsgId}] ->
            _ = remove_batch([{PK, DN, Did} || DN <- DNs]),
            route(did_shard_of(Did), {delete_delivery_rows, Did, MsgId}, ?SYNC_TIMEOUT_MS)
    end.

delete_message(ApiId) ->
    case mnesia:dirty_read(?TAB_MSG_API_ID, ApiId) of
        [] ->
            {error, not_found};
        [#bcast_message_api_id{msg_id = MsgId}] ->
            Deliveries = mnesia:dirty_match_object(
                ?TAB_MSG_REC, #bcast_msg{msg_id = MsgId, _ = '_'}
            ),
            _ = remove_batch([
                {D#bcast_msg.product_key, DN, D#bcast_msg.delivery_id}
             || D <- Deliveries,
                DN <- D#bcast_msg.device_names
            ]),
            DeliveryIds = [D#bcast_msg.delivery_id || D <- Deliveries],
            route(0, {delete_message_rows, ApiId, DeliveryIds}, ?SYNC_TIMEOUT_MS)
    end.

%% Run cleanup coordination in the CALLER (cleanup gen_server),
%% not inside shard 0's handle_call (which blocked that shard's
%% claim/ack/append for the whole scan + per-delivery transactions).
cleanup_expired() ->
    Now = emqx_bcast_utils:now_sec(),
    %% Projection-only scan: match spec returns just the keys we need
    %% (delivery_id, msg_id, product_key, device_names) instead of the full
    %% ~47KB bcast_msg row per expired delivery.
    Expired = scan_expired_deliveries(Now),
    %% Messages have no shard state: delete directly in the caller.
    cleanup_expired_messages_local(Now),
    %% Index removal routes per shard in parallel (remove_batch/1).
    dispatch_expired_index(Expired),
    %% Batched mnesia deletes: one transaction per chunk instead of one
    %% per expired delivery.
    delete_expired_deliveries_batched(Expired),
    %% Every shard runs its own orphan scan and stale-reservation cleanup.
    lists:foreach(
        fun(Shard) -> route(Shard, {cleanup_local}, ?SYNC_TIMEOUT_MS) end,
        lists:seq(0, ?SHARD_COUNT - 1)
    ),
    ok.

%% Synchronous creates run entirely in the CALLER process - the quota
%% check, the mnesia transaction and the per-shard append fan-out need no
%% shard-0 mailbox (mirrors admit/2). They used to be shard-0 handle_call
%% handlers that blocked that shard's claim/ack/append for the whole
%% transaction + sequential sibling fan-out. Test/legacy-only path in
%% production (BatchPub QoS1 goes through the async intake + promoter).
%% Declared semantic change - the old shard-0 handlers returned
%% {error, not_active} while dormant (owner takeover); the caller-side
%% version commits the mnesia rows regardless of activation state (the
%% per-shard index append is rebuilt at takeover if it races one). This is
%% acceptable for a test/legacy path, but is an intentional, documented
%% divergence from the old not_active rejection.
create_sync(Entry, Quota) ->
    PK = maps:get(product_key, Entry),
    DNs = maps:get(devices, Entry),
    case check_quota_parallel(PK, DNs, Quota) of
        ok ->
            case mnesia:transaction(fun() -> emqx_bcast_storage:promote_entry_tx(Entry) end, 20) of
                {atomic, {ok, ApiId, Delivery}} ->
                    Did = maps:get(delivery_id, Entry),
                    %% Surface an append failure instead of swallowing it
                    %% (the mnesia rows are already committed; a failed index
                    %% append used to be silently rebuilt only at takeover).
                    case append_batch([{PK, DN, Did} || DN <- DNs]) of
                        ok ->
                            {ok, ApiId, Delivery};
                        {error, Reason} ->
                            ?SLOG(error, #{
                                msg => "bcast_create_sync_index_append_failed",
                                delivery_id => Did,
                                reason => Reason
                            }),
                            {error, {index_append_failed, Reason}}
                    end;
                {atomic, {error, _} = Error} ->
                    Error;
                {aborted, Reason} ->
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
    end.

create_delivery(Did, MsgId, PK, Tpl, DNs, Target) ->
    Now = emqx_bcast_utils:now_sec(),
    TTL = emqx_bcast_utils:ttl(),
    Delivery = #bcast_msg{
        delivery_id = Did,
        msg_id = MsgId,
        product_key = PK,
        topic_template = Tpl,
        target_ack_count = Target,
        counter = 0,
        device_names = DNs,
        created_at = Now,
        expires_at = Now + TTL
    },
    case
        mnesia:transaction(
            fun() ->
                case mnesia:wread({?TAB_MSG_REC, Did}) of
                    [_] ->
                        {error, already_exists};
                    [] ->
                        mnesia:write(Delivery),
                        mnesia:write(#bcast_msg_meta{
                            delivery_id = Did,
                            msg_id = MsgId,
                            topic_template = Tpl,
                            counter = Target
                        }),
                        emqx_bcast_storage:inc_delivery_count_tx(MsgId)
                end
            end,
            20
        )
    of
        {atomic, ok} ->
            %% Surface an append failure instead of swallowing it.
            case append_batch([{PK, DN, Did} || DN <- DNs]) of
                ok ->
                    {ok, Delivery};
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "bcast_create_delivery_index_append_failed",
                        delivery_id => Did,
                        reason => Reason
                    }),
                    {error, {index_append_failed, Reason}}
            end;
        {atomic, {error, _} = Error} ->
            Error;
        {aborted, Reason} ->
            {error, Reason}
    end.

%% Caller-side quota check shared by check_quota/2 and create_sync/2.
%% The global pending counter is read from the OWNER node (the shared
%% ETS row lives there); a caller on any other node must not read its own
%% (missing) table.
check_quota_parallel(PK, DNs, Quota) ->
    GlobalMax = maps:get(global, Quota, infinity),
    PerDeviceMax = maps:get(per_device, Quota, infinity),
    GlobalCount = global_pending_count(),
    case exceeds_quota(GlobalCount + length(DNs), GlobalMax) of
        true ->
            {error, {quota_exceeded, []}};
        false ->
            Over = parallel_check_devices(PK, group_devices(PK, DNs), PerDeviceMax),
            case Over of
                [] -> ok;
                _ -> {error, {quota_exceeded, Over}}
            end
    end.

%% Global pending count from the quota owner node (the ETS row lives
%% there). Runs on the owner node (exported for emqx_rpc).
%% Residual: a failed RPC returns {badrpc,_}, which would otherwise
%% badarith the exceeds_quota comparison - degrade to 0 (admission then
%% falls back to the bounded queue) and log.
global_pending_count() ->
    case owner_node() =:= node() of
        true ->
            pending_count_local();
        false ->
            case
                emqx_rpc:call(
                    ?MODULE, owner_node(), ?MODULE, pending_count_local, [], ?SYNC_TIMEOUT_MS
                )
            of
                N when is_integer(N) ->
                    N;
                {badrpc, Reason} ->
                    ?SLOG(error, #{
                        msg => "bcast_global_pending_rpc_failed",
                        reason => Reason
                    }),
                    0
            end
    end.

rebuild_index() ->
    route(0, {rebuild_index}, ?SYNC_TIMEOUT_MS).

reset() ->
    lists:foreach(
        fun(Shard) -> route(Shard, {reset_local}, ?SYNC_TIMEOUT_MS) end,
        lists:seq(0, ?SHARD_COUNT - 1)
    ),
    ok.

route(Shard, Req, Timeout) ->
    Target = shard_owner(Shard),
    case Target =:= node() of
        true ->
            gen_server:call(shard_name(Shard), Req, Timeout);
        false ->
            emqx_rpc:call(?MODULE, Target, ?MODULE, local_handle, [Shard, Req, Timeout], Timeout)
    end.

local_handle(Shard, Req, Timeout) ->
    gen_server:call(shard_name(Shard), Req, Timeout).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

handle_call({append_batch, Entries}, _From, State = #{active := true}) ->
    try
        {State2, Delta} = fold_append(Entries, State),
        quota_update(Delta),
        {reply, ok, State2}
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "bcast_index_owner_op_failed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {reply, {error, append_failed}, State}
    end;
handle_call({remove_batch, Entries}, _From, State = #{active := true}) ->
    try
        {State2, Delta} = lists:foldl(
            fun({PK, DN, Did}, {St, D}) ->
                {St2, D2} = remove_entry(St, {PK, DN}, Did),
                {St2, D + D2}
            end,
            {State, 0},
            Entries
        ),
        quota_update(Delta),
        {reply, ok, State2}
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "bcast_index_owner_op_failed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {reply, {error, {Error, Reason}}, State}
    end;
handle_call({claim, Entries}, _From, State = #{active := true}) ->
    {Results, State2} = lists:mapfoldl(
        fun(E, St) ->
            {R, St2} = claim_one(E, St),
            {{maps:get(clientid, E), R}, St2}
        end,
        State,
        Entries
    ),
    {reply, Results, State2};
handle_call({ack_index, Acks}, _From, State = #{active := true}) ->
    try
        {Results, {State2, Delta}} = lists:mapfoldl(
            fun(Ack, {St, D}) ->
                {R, St2, D2} = ack_one_index(Ack, St),
                {R, {St2, D + D2}}
            end,
            {State, 0},
            Acks
        ),
        quota_update(Delta),
        {reply, Results, State2}
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "bcast_index_owner_op_failed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {reply, {error, {Error, Reason}}, State}
    end;
handle_call({meta_dec_batch, Dids}, _From, State = #{active := true}) ->
    lists:foreach(fun complete_or_count/1, Dids),
    {reply, ok, State};
handle_call({release_claim, PK, DN, Did}, _From, State = #{active := true}) ->
    {reply, ok, release_claim_local(PK, DN, Did, State)};
handle_call({release_client_claims, PK, DN, Tag}, _From, State = #{active := true}) ->
    {reply, ok, release_client_claims_local(PK, DN, Tag, State)};
handle_call({admit, PK, DNs}, _From, State = #{active := true}) ->
    {Reply, State2} = safe_state(fun() -> admit_local(PK, DNs, State) end, State),
    {reply, Reply, State2};
handle_call({release_admit, PK, DNs}, _From, State = #{active := true}) ->
    {Reply, State2} = safe_state(fun() -> {ok, dispatch_release(PK, DNs, State)} end, State),
    {reply, Reply, State2};
handle_call({check_devices, PK, DNs, Max}, _From, State = #{active := true}) ->
    {reply, check_devices_local(PK, DNs, Max, State), State};
handle_call({reserve_devices, PK, DNs}, _From, State = #{active := true}) ->
    State2 = lists:foldl(fun(DN, St) -> reserve_inc(St, {PK, DN}) end, State, DNs),
    {reply, ok, State2};
handle_call({release_devices, PK, DNs}, _From, State = #{active := true}) ->
    {State2, Delta} = lists:foldl(
        fun(DN, {St, D}) ->
            {St2, D2} = reserve_dec(St, {PK, DN}),
            {St2, D + D2}
        end,
        {State, 0},
        DNs
    ),
    quota_update(-Delta),
    {reply, ok, State2};
handle_call({device_deliveries, Key}, _From, State = #{active := true}) ->
    {reply, device_deliveries_local(Key, State), State};
handle_call({device_delivery_entries, Key}, _From, State = #{active := true}) ->
    {reply, device_delivery_entries_local(Key, State), State};
handle_call({pending_count}, _From, State = #{active := true}) ->
    {reply, pending_count_local(), State};
handle_call({pending_count_for, Key}, _From, State = #{active := true}) ->
    {reply, pending_count_for_local(Key, State), State};
handle_call({delete_delivery_rows, Did, MsgId}, _From, State = #{active := true}) ->
    {reply, safe(fun() -> delete_delivery_rows_local(Did, MsgId) end), State};
handle_call({delete_message_rows, ApiId, DeliveryIds}, _From, State = #{active := true}) ->
    {reply, safe(fun() -> delete_message_rows_local(ApiId, DeliveryIds) end), State};
handle_call({cleanup_local}, _From, State = #{active := true}) ->
    State2 = cleanup_orphan_index_local(State),
    State3 = cleanup_stale_reservations(State2),
    {reply, ok, State3};
handle_call({rebuild_index}, _From, State = #{active := true}) ->
    {Reply, State2} = safe_state(fun() -> drive_activation(State) end, State),
    {reply, Reply, State2};
handle_call({activate}, _From, State) ->
    %% No-arg fallback (self-scan): kept for callers that cannot get the
    %% shared projection from the activation leader.
    case activate_partition(scan_sorted_deliveries_projection(), State) of
        {{error, _} = Error, _State1} -> {reply, Error, State};
        {Count, State1} -> {reply, Count, State1#{active => true}}
    end;
handle_call({activate, Proj}, _From, State) ->
    %% The activation leader scans + sorts bcast_msg once and hands
    %% every shard the same sorted projection.
    case activate_partition(Proj, State) of
        {{error, _} = Error, _State1} -> {reply, Error, State};
        {Count, State1} -> {reply, Count, State1#{active => true}}
    end;
handle_call({reset_local}, _From, State = #{shard := 0}) ->
    %% Only the quota owner's shard 0 resets the global counter (the table
    %% lives there); a peer core's shard 0 must not clobber it.
    case is_owner() of
        true -> true = ets:insert(?TAB_QUOTA_ETS, {global, 0});
        false -> ok
    end,
    {reply, ok, reset_state(State)};
handle_call({reset_local}, _From, State) ->
    {reply, ok, reset_state(State)};
%% Dormant (not yet the owner, or takeover in progress): fail calls so
%% callers retry; reads degrade to empty so management stays responsive.
handle_call({admit, _PK, _DNs}, _From, State) ->
    {reply, ok, State};
handle_call({pending_count}, _From, State) ->
    {reply, 0, State};
handle_call({pending_count_for, _Key}, _From, State) ->
    {reply, 0, State};
handle_call({device_deliveries, _Key}, _From, State) ->
    {reply, {ok, []}, State};
handle_call({device_delivery_entries, _Key}, _From, State) ->
    {reply, {ok, []}, State};
handle_call(_Req, _From, State) ->
    {reply, {error, not_active}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(maybe_activate, State = #{active := false, shard := 0}) ->
    Result =
        try maybe_drive_activation(State) of
            {ok, S1} -> {activated, S1};
            _ -> retry
        catch
            _:_ -> retry
        end,
    case Result of
        {activated, S2} ->
            ?SLOG(info, #{
                msg => "bcast_index_owner_activated",
                node => node(),
                shard_count => ?SHARD_COUNT
            }),
            {noreply, S2#{active => true}};
        retry ->
            erlang:send_after(?OWNER_POLL_MS, self(), maybe_activate),
            {noreply, State}
    end;
handle_info(maybe_activate, State) ->
    {noreply, State};
handle_info(maybe_gc, State) ->
    {noreply, maybe_fullsweep(State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Data-path guard: a transient mnesia hiccup (e.g. the legacy-migration
%% test dropping tables under us) must not kill the owner process; the
%% heap index survives and callers retry or the next cleanup repairs.
safe(Fun) ->
    try Fun() of
        Result -> Result
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "bcast_index_owner_op_failed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, {Error, Reason}}
    end.

%% Like safe/1 but the Fun returns {Result, NewState}: state updates
%% that raised mid-way are discarded, so the caller keeps the previous
%% consistent state and the failed operation is retried by the caller
%% (promoter retries the whole batch on failure).
safe_state(Fun, State) ->
    try Fun() of
        {Result, State2} -> {Result, State2}
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "bcast_index_owner_op_failed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {{error, {Error, Reason}}, State}
    end.

%% Conditional fullsweep: once the pending backlog has shrunk well below
%% its peak, force a major GC so the allocator can return the dead heap
%% to the OS. During load/growth the heap is left to minor GCs.
maybe_fullsweep(State) ->
    Pending = maps:size(maps:get(dids, State)),
    Peak = maps:get(peak, State, 0),
    Peak1 = max(Peak, Pending),
    State1 = State#{peak => Peak1},
    ShouldShrink =
        Pending > 0 andalso Pending * 2 < Peak1 andalso Pending < Peak1,
    State2 =
        case ShouldShrink of
            true ->
                case erlang:process_info(self(), total_heap_size) of
                    {total_heap_size, HS} when HS > ?GC_MIN_HEAP_WORDS ->
                        %% {type, major} is the valid fullsweep option for
                        %% garbage_collect/2 (an invalid option is badarg and
                        %% would crash the shard); never let GC take us down.
                        try erlang:garbage_collect(self(), [{type, major}]) of
                            _ -> State1#{peak => Pending}
                        catch
                            _:_ -> State1
                        end;
                    _ ->
                        State1
                end;
            false ->
                State1
        end,
    _ = erlang:send_after(?GC_POLL_MS, self(), maybe_gc),
    State2.

%%--------------------------------------------------------------------
%% Activation: shard 0 drives a coordinated rebuild of every shard
%%--------------------------------------------------------------------

maybe_drive_activation(State) ->
    case is_owner() of
        true ->
            %% (Re)initialize the quota table once we are the quota owner:
            %% a takeover must reset the global counter before the shards
            %% add their rebuilt counts.
            _ = ensure_quota_table(),
            true = ets:insert(?TAB_QUOTA_ETS, {global, 0}),
            drive_activation(State);
        false ->
            not_owner
    end.

%% All shards rebuild their own partition; the counts are summed into the
%% shared global counter. Runs inside the quota owner's shard 0. Each shard
%% activates on its assigned node (local inline/call, or one emqx_rpc hop
%% to the assigned core); the sum is inserted into the local quota table
%% (this node IS the quota owner). Returns {ok, NewState} (the shard-0
%% State carries the rebuilt maps).
drive_activation(State) ->
    MyShard = maps:get(shard, State),
    %% Scan the authoritative bcast_msg table ONCE (projection-only
    %% match spec, no full #bcast_msg{} records) and sort once by
    %% created_at; every shard then activates from the same sorted
    %% projection instead of each shard scanning + sorting the whole table
    %% for itself (4x transient heap + 4x sort at takeover).
    Proj = scan_sorted_deliveries_projection(),
    {Counts, State0} = lists:mapfoldl(
        fun(Shard, St) ->
            case shard_owner(Shard) of
                Node when Node =:= node() ->
                    case Shard =:= MyShard of
                        true ->
                            case activate_partition(Proj, St) of
                                {{error, _} = Error, _St1} -> {Error, St};
                                {Count, St1} -> {Count, St1}
                            end;
                        false ->
                            case
                                try
                                    gen_server:call(
                                        shard_name(Shard), {activate, Proj}, ?SYNC_TIMEOUT_MS
                                    )
                                of
                                    {error, _} = Err1 -> Err1;
                                    Cnt1 when is_integer(Cnt1) -> Cnt1
                                catch
                                    _:_ -> {error, sibling_unavailable}
                                end
                            of
                                {error, _} = Err2 -> {Err2, St};
                                Cnt2 when is_integer(Cnt2) -> {Cnt2, St}
                            end
                    end;
                _Node ->
                    case
                        try
                            emqx_rpc:call(
                                ?MODULE,
                                _Node,
                                ?MODULE,
                                local_handle,
                                [Shard, {activate, Proj}, ?SYNC_TIMEOUT_MS],
                                ?SYNC_TIMEOUT_MS
                            )
                        of
                            {error, _} = Err1 -> Err1;
                            Cnt1 when is_integer(Cnt1) -> Cnt1
                        catch
                            _:_ -> {error, sibling_unavailable}
                        end
                    of
                        {error, _} = Err2 -> {Err2, St};
                        Cnt2 when is_integer(Cnt2) -> {Cnt2, St}
                    end
            end
        end,
        State,
        lists:seq(0, ?SHARD_COUNT - 1)
    ),
    case [Error || {error, _} = Error <- Counts] of
        [] ->
            ets:insert(?TAB_QUOTA_ETS, {global, lists:sum(Counts)}),
            {ok, State0};
        [Error | _] ->
            {Error, State0}
    end.

%% Projection-only scan of bcast_msg ordered by created_at. Returns
%% {DeliveryId, MsgId, ProductKey, TopicTemplate, TargetAckCount, Counter,
%% DeviceNames, CreatedAt} tuples - the fields activation and the legacy
%% meta backfill need - instead of materializing the full 10-field record
%% (whose device_names list alone is ~47KB at bs=1000) in every shard.
scan_sorted_deliveries_projection() ->
    Rows = mnesia:dirty_select(
        ?TAB_MSG_REC,
        [
            {
                #bcast_msg{
                    delivery_id = '$1',
                    msg_id = '$2',
                    product_key = '$3',
                    topic_template = '$4',
                    target_ack_count = '$5',
                    counter = '$6',
                    device_names = '$7',
                    created_at = '$8',
                    _ = '_'
                },
                [],
                [{{'$1', '$2', '$3', '$4', '$5', '$6', '$7', '$8'}}]
            }
        ]
    ),
    %% Sort by created_at (the 8th projection field): the original
    %% projection-sort compared the FIRST field (delivery_id), which
    %% scrambled the per-device FIFO order rebuilt at takeover.
    lists:sort(
        fun({_, _, _, _, _, _, _, Ca}, {_, _, _, _, _, _, _, Cb}) -> Ca =< Cb end,
        Rows
    ).

%% Rebuild (takeover): derive this shard's partition of the heap index
%% from the authoritative mria delivery table (via the shared sorted
%% projection built by drive_activation), preserving per-device FIFO by
%% created_at. All entries are rebuilt as stored (in-flight claims are
%% at-least-once re-delivered, matching the claim lease expiry semantics).
%% Shard 0 additionally backfills bcast_msg_meta rows for deliveries
%% committed by older builds. Returns {Count, NewState}.
activate_partition(Proj, State) ->
    try
        State1 = reset_state(State),
        Shard = maps:get(shard, State),
        case Shard of
            0 -> backfill_meta_from_projection(Proj);
            _ -> ok
        end,
        State2 = lists:foldl(
            fun(
                {DeliveryId, _MsgId, ProductKey, _TopicTemplate, _TargetAckCount, _Counter,
                    DeviceNames, _CreatedAt},
                St
            ) ->
                lists:foldl(
                    fun(DN, St2) ->
                        Key = {ProductKey, DN},
                        case shard_of(Key) of
                            Shard ->
                                {St3, _Delta} = append_entry(St2, Key, DeliveryId),
                                St3;
                            _ ->
                                St2
                        end
                    end,
                    St,
                    DeviceNames
                )
            end,
            State1,
            Proj
        ),
        Count = maps:size(maps:get(dids, State2)),
        ?SLOG(info, #{
            msg => "bcast_index_shard_rebuilt",
            shard => Shard,
            pending => Count
        }),
        {Count, State2}
    catch
        Error:Reason -> {{error, {Error, Reason}}, State}
    end.

%% Deliveries committed by older builds (or rows written before the meta
%% table existed) have no bcast_msg_meta row: the ack/claim hot paths only
%% read the small meta row, so backfill it on owner activation. The legacy
%% bcast_msg.counter counts acks received; the meta counter stores the
%% remaining acks to completion.
%% Backfill missing bcast_msg_meta rows in batched transactions
%% (100 rows/tx) instead of one dirty read + dirty write per delivery.
%% Only runs on takeover/activation and only writes rows that are missing,
%% so normal clusters (meta written in the same tx as bcast_msg) are a
%% no-op scan.
backfill_meta_from_projection(Proj) ->
    %% Read side: one projection scan of the meta table to collect the ids
    %% that already exist (instead of one dirty_read per delivery row).
    %% NO limit here - a bounded ets:select/3 would drop the
    %% continuation and mark every row beyond the budget as missing,
    %% then overwrite live meta rows (resetting the counter of
    %% partially-acked deliveries so they never complete). This runs once
    %% per takeover, so the full projection scan is cheap.
    Existing = ets:select(
        ?TAB_MSG_META,
        [{#bcast_msg_meta{delivery_id = '$1', _ = '_'}, [], ['$1']}]
    ),
    ExistingSet = maps:from_keys(Existing, true),
    Missing = lists:filtermap(
        fun(
            {DeliveryId, MsgId, _ProductKey, TopicTemplate, TargetAckCount, Counter, _DeviceNames,
                _CreatedAt}
        ) ->
            case maps:is_key(DeliveryId, ExistingSet) of
                true ->
                    false;
                false ->
                    {true, {DeliveryId, MsgId, TopicTemplate, max(0, TargetAckCount - Counter)}}
            end
        end,
        Proj
    ),
    lists:foreach(
        fun(Chunk) ->
            case
                mnesia:transaction(
                    fun() ->
                        lists:foreach(
                            fun({DeliveryId, MsgId, TopicTemplate, Remaining}) ->
                                mnesia:write(#bcast_msg_meta{
                                    delivery_id = DeliveryId,
                                    msg_id = MsgId,
                                    topic_template = TopicTemplate,
                                    counter = Remaining
                                })
                            end,
                            Chunk
                        )
                    end,
                    20
                )
            of
                {atomic, _} ->
                    ok;
                {aborted, Reason} ->
                    ?SLOG(warning, #{
                        msg => "bcast_meta_backfill_tx_aborted",
                        reason => Reason,
                        chunk_size => length(Chunk)
                    })
            end
        end,
        chunks(Missing, 100)
    ),
    ok.

reset_state(State) ->
    State#{
        queues => #{},
        dids => #{},
        inflights => #{},
        counts => #{},
        reserves => #{},
        peak => 0,
        orphan_cursor => 0
    }.

%%--------------------------------------------------------------------
%% Routing helpers (public wrappers)
%%--------------------------------------------------------------------

group_entries(Entries) ->
    lists:foldl(
        fun(Entry = {PK, DN, _Did}, Acc) ->
            Shard = shard_of({PK, DN}),
            case lists:keyfind(Shard, 1, Acc) of
                {Shard, List} -> lists:keyreplace(Shard, 1, Acc, {Shard, [Entry | List]});
                false -> [{Shard, [Entry]} | Acc]
            end
        end,
        [],
        Entries
    ).

group_claim_entries(Entries) ->
    lists:foldl(
        fun(Entry, Acc) ->
            Shard = shard_of({maps:get(product_key, Entry), maps:get(clientid, Entry)}),
            case lists:keyfind(Shard, 1, Acc) of
                {Shard, List} -> lists:keyreplace(Shard, 1, Acc, {Shard, [Entry | List]});
                false -> [{Shard, [Entry]} | Acc]
            end
        end,
        [],
        Entries
    ).

%% [{DevShard, DidShard, [Ack]}]
group_acks(Acks) ->
    lists:foldl(
        fun(Ack = {PK, DN, Did}, Acc) ->
            Key = {shard_of({PK, DN}), did_shard_of(Did)},
            case lists:keyfind(Key, 1, Acc) of
                {Key, List} -> lists:keyreplace(Key, 1, Acc, {Key, [Ack | List]});
                false -> [{Key, [Ack]} | Acc]
            end
        end,
        [],
        Acks
    ).

group_devices(PK, DNs) ->
    lists:foldl(
        fun(DN, Acc) ->
            Shard = shard_of({PK, DN}),
            case lists:keyfind(Shard, 1, Acc) of
                {Shard, List} -> lists:keyreplace(Shard, 1, Acc, {Shard, [DN | List]});
                false -> [{Shard, [DN]} | Acc]
            end
        end,
        [],
        DNs
    ).

%%--------------------------------------------------------------------
%% Heap primitives (shard process only, for its own partition)
%%--------------------------------------------------------------------

fold_append(Entries, State) ->
    lists:foldl(
        fun({PK, DN, Did}, {St, D}) ->
            {St2, D2} = append_entry(St, {PK, DN}, Did),
            {St2, D + D2}
        end,
        {State, 0},
        Entries
    ).

%% Append one delivery to the device FIFO. O(1): queue:in at the tail.
%% The dids map is the idempotence guard (re-promotion dedup) and the
%% append timestamp for the fresh/old replication-lag heuristic.
%% Returns {NewState, GlobalDelta}: the caller batches the global counter
%% update (quota_update/1) so remote shards do one RPC per batch, not one
%% per entry. Delta = +1 (real index entry) - reserve slots consumed.
append_entry(State, Key = {PK, DN}, Did) ->
    Key3 = {PK, DN, Did},
    case maps:is_key(Key3, maps:get(dids, State)) of
        true ->
            {State, 0};
        false ->
            Ts = erlang:system_time(millisecond),
            Q = maps:get(Key, maps:get(queues, State), queue:new()),
            State1 = put_in(queues, Key, queue:in(Did, Q), State),
            State2 = put_in(dids, Key3, Ts, State1),
            State3 = incr_in(counts, Key, State2),
            {State4, Decr} = reserve_dec(State3, Key),
            {State4, 1 - Decr}
    end.

%% Lazy removal: the dids/counts/global bookkeeping is updated now; the
%% queue residual is dropped by the next claim pass (the dids guard).
%% When the device drains to zero the whole device entry is dropped.
%% Returns {NewState, GlobalDelta} (batched by the caller).
remove_entry(State, Key = {PK, DN}, Did) ->
    Key3 = {PK, DN, Did},
    case maps:is_key(Key3, maps:get(dids, State)) of
        false ->
            {State, 0};
        true ->
            {remove_did(State, Key, Key3), -1}
    end.

put_in(MapName, K, V, State) ->
    maps:put(MapName, maps:put(K, V, maps:get(MapName, State)), State).

incr_in(MapName, K, State) ->
    M = maps:get(MapName, State),
    maps:put(MapName, maps:update_with(K, fun(N) -> N + 1 end, 1, M), State).

%% Count back to zero removes the key (and the device entry once its
%% queue is empty of live entries).
remove_did(State, Key, Key3) ->
    State1 = decr_in(counts, Key, State),
    State2 = unmark_inflight(State1, Key, Key3),
    State3 = maps:put(dids, maps:remove(Key3, maps:get(dids, State2)), State2),
    maybe_drop_device(State3, Key).

decr_in(MapName, K, State) ->
    M = maps:get(MapName, State),
    case maps:get(K, M) of
        1 -> maps:put(MapName, maps:remove(K, M), State);
        N -> maps:put(MapName, maps:put(K, N - 1, M), State)
    end.

%% A device with zero pending entries can be dropped entirely: its queue
%% residuals are all lazily-deleted entries, safe to discard.
maybe_drop_device(State, Key) ->
    case maps:get(Key, maps:get(counts, State), 0) of
        0 ->
            State#{
                queues => maps:remove(Key, maps:get(queues, State)),
                inflights => maps:remove(Key, maps:get(inflights, State))
            };
        _ ->
            State
    end.

unmark_inflight(State, Key, Key3) ->
    Infl = maps:get(Key, maps:get(inflights, State), #{}),
    case maps:is_key(Key3, Infl) of
        false ->
            State;
        true ->
            Infl2 = maps:remove(Key3, Infl),
            maps:put(inflights, maps:put(Key, Infl2, maps:get(inflights, State)), State)
    end.

save_queue(State, Key, Q) ->
    maps:put(queues, maps:put(Key, Q, maps:get(queues, State)), State).

%% Batch global-counter update. Local when this node owns the quota table,
%% otherwise one emqx_rpc hop (per batch, not per entry).
quota_update(0) ->
    ok;
quota_update(Delta) ->
    case owner_node() =:= node() of
        true ->
            _ = ets:update_counter(?TAB_QUOTA_ETS, global, {2, Delta}),
            ok;
        false ->
            %% The global counter is a single ETS row on the owner node; a
            %% lost update here silently skews the pending quota (it can go
            %% negative when a later -1 lands but its +1 was dropped). Check
            %% the RPC result and log instead of swallowing badrpc.
            case
                emqx_rpc:call(
                    ?MODULE,
                    owner_node(),
                    ?MODULE,
                    quota_update_local,
                    [Delta],
                    ?SYNC_TIMEOUT_MS
                )
            of
                ok ->
                    ok;
                {badrpc, Reason} ->
                    ?SLOG(error, #{
                        msg => "bcast_quota_update_rpc_failed",
                        delta => Delta,
                        reason => Reason
                    })
            end
    end.

%% Runs on the quota owner node (exported for emqx_rpc).
quota_update_local(Delta) ->
    New = ets:update_counter(?TAB_QUOTA_ETS, global, {2, Delta}),
    case New < 0 of
        true ->
            %% Diagnostic: the pending quota must never be negative. A
            %% negative value means an accounting bug (e.g. a lost positive
            %% update via a failed cross-node RPC) - surface it instead of
            %% silently carrying it.
            ?SLOG(error, #{
                msg => "bcast_quota_went_negative",
                new_value => New,
                delta => Delta
            });
        false ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% Claims
%%--------------------------------------------------------------------

%% Claim one entry for a client. Returns {Result, NewState} where Result
%% is {ok, #{delivery_id, product_key, topic_template, payload,
%% claim_tag}} or no_more. The FIFO head is popped; claimable entries are
%% moved to inflights (out of the queue); unclaimable ones (topic
%% mismatch, replication-lag, lazily deleted) are either skipped back to
%% the tail or dropped.
claim_one(#{clientid := DN, product_key := PK} = E, State) ->
    %% External contract (as before the port): {Result, NewState} where
    %% Result is {ok, Map} | no_more. Internally claim_loop threads the
    %% state as a third element, so normalize the shapes here.
    try do_claim_one(E, PK, DN, State) of
        {ok, Result, St} -> {{ok, Result}, St};
        {no_more, St} -> {no_more, St}
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "bcast_index_owner_claim_failed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {{error, {Error, Reason}}, State}
    end.

do_claim_one(E, PK, DN, State) ->
    Topics = maps:get(topics, E, []),
    Tag = maps:get(claim_tag, E, undefined),
    Key = {PK, DN},
    %% Expired in-flight claims become claimable again (at-least-once).
    State1 = release_expired_inflights(Key, State),
    Q = maps:get(Key, maps:get(queues, State1), queue:new()),
    %% Terminate on the starting head element instead of queue:len/1
    %% (O(n) per claim at big queue sizes). One full pass = re-encountering
    %% the head after skipped entries cycled to the tail.
    Head =
        case queue:peek(Q) of
            {value, H} -> H;
            empty -> undefined
        end,
    case claim_loop(State1, Key, Q, Head, Topics, PK, DN, Tag, false, 0) of
        {ok, Result, St, Drops} ->
            flush_claim_drops(St, Drops),
            {ok, Result, St};
        {no_more, St, Drops} ->
            flush_claim_drops(St, Drops),
            {no_more, St}
    end.

%% One batched quota update per claim call instead of one RPC per
%% dropped stale entry (claim_loop can drop many residuals in one pass).
flush_claim_drops(State, 0) ->
    State;
flush_claim_drops(State, Drops) ->
    quota_update(-Drops),
    State.

%% Terminate when we have wrapped around to the head element (BackAtHead)
%% or the queue emptied; undefined head = empty queue.
%% The anchor is only armed when the head entry was CYCLED BACK to the
%% tail. The original version set BackAtHead on the very first pop, so a
%% head that was dropped (residual/stale, never re-queued) left the anchor
%% pointing at an entry that can never reappear: with all remaining entries
%% retrying (topic mismatch), the loop never terminated and wedged the whole
%% shard (every claim/ack for its devices stalled forever). A dropped head
%% re-anchors to the new queue head instead.
claim_loop(State, Key, Q, Head, _Topics, _PK, _DN, _Tag, _BackAtHead, Drops) when
    Head =:= undefined
->
    {no_more, save_queue(State, Key, Q), Drops};
claim_loop(State, Key, Q, Head, Topics, PK, DN, Tag, BackAtHead, Drops) ->
    case queue:out(Q) of
        {empty, _} ->
            {no_more, save_queue(State, Key, Q), Drops};
        {{value, Did}, Q2} ->
            case Did =:= Head andalso BackAtHead of
                true ->
                    %% Wrapped: the head was cycled back once and is now
                    %% popped again - one full pass is done. Put it BACK
                    %% (it was popped just now): saving the popped-empty
                    %% queue silently removed the entry from the FIFO while
                    %% dids/counts/quota still accounted for it - invisible
                    %% to later claims AND to the orphan cleanup
                    %% (t_claim_no_more_cleans_stale_index regression).
                    {no_more, save_queue(State, Key, queue:in(Did, Q2)), Drops};
                false ->
                    claim_loop_step(
                        State, Key, Q2, Head, Topics, PK, DN, Tag, BackAtHead, Drops, Did
                    )
            end
    end.

claim_loop_step(State, Key, Q, Head, Topics, PK, DN, Tag, BackAtHead, Drops, Did) ->
    Key3 = {PK, DN, Did},
    case maps:is_key(Key3, maps:get(dids, State)) of
        false ->
            %% Lazy residual (acked/removed): drop and continue. The dids
            %% entry is already gone (no quota delta needed). Only a dropped
            %% HEAD re-anchors (and resets the wrap anchor); dropping a
            %% non-head entry must keep BackAtHead, otherwise a mixed
            %% residual/topic-mismatch queue degrades to O(n*d) passes.
            claim_loop(
                State,
                Key,
                Q,
                reanchor_head(Did, Head, Q),
                Topics,
                PK,
                DN,
                Tag,
                keep_anchor(Did, Head, BackAtHead),
                Drops
            );
        true ->
            Now = erlang:system_time(millisecond),
            Ts = maps:get(Key3, maps:get(dids, State)),
            case claim_check(State, Key, Did, Key3, Ts, Now, Topics, PK, DN, Tag) of
                {claim, Result, State2} ->
                    {ok, Result, save_queue(State2, Key, Q), Drops};
                {retry, State2} ->
                    %% Fresh-but-missing or topic mismatch: cycle to the
                    %% tail and keep looking. Only here does the anchor
                    %% become meaningful (the head was cycled back).
                    Q3 = queue:in(Did, Q),
                    claim_loop(
                        State2,
                        Key,
                        Q3,
                        Head,
                        Topics,
                        PK,
                        DN,
                        Tag,
                        BackAtHead orelse Did =:= Head,
                        Drops
                    );
                {drop, State2} ->
                    %% Stale entry dropped: the dids entry was removed by
                    %% maybe_drop_stale, count it for the batched quota
                    %% update at the end of this claim call. Same
                    %% re-anchor rule as residuals.
                    claim_loop(
                        State2,
                        Key,
                        Q,
                        reanchor_head(Did, Head, Q),
                        Topics,
                        PK,
                        DN,
                        Tag,
                        keep_anchor(Did, Head, BackAtHead),
                        Drops + 1
                    )
            end
    end.

%% If the dropped entry was the current anchor, anchor at the new head of
%% the remaining queue (or undefined when the queue emptied).
reanchor_head(Did, Head, Q) when Did =:= Head ->
    case queue:peek(Q) of
        {value, NewHead} -> NewHead;
        empty -> undefined
    end;
reanchor_head(_Did, Head, _Q) ->
    Head.

%% The wrap anchor survives drops of non-head entries: only when the head
%% itself is dropped does the anchor become meaningless (and reanchor_head
%% already moved it to the new head, so BackAtHead resets).
keep_anchor(Did, Head, _BackAtHead) when Did =:= Head ->
    false;
keep_anchor(_Did, _Head, BackAtHead) ->
    BackAtHead.

%% Returns {claim, Result, State'} (State' has the inflight mark) |
%% {retry, State} | {drop, State}.
claim_check(State, Key, Did, Key3, Ts, Now, Topics, PK, DN, Tag) ->
    case mnesia:dirty_read(?TAB_MSG_META, Did) of
        [#bcast_msg_meta{msg_id = MsgId, topic_template = Tpl}] ->
            Topic = emqx_bcast_utils:expand_topic(Tpl, PK, DN),
            case topics_match(Topic, Topics) of
                false ->
                    {retry, State};
                {ok, SubQos} ->
                    case mnesia:dirty_read(?TAB_MSG, MsgId) of
                        [#bcast_message{payload = Payload}] ->
                            {claim,
                                #{
                                    delivery_id => Did,
                                    product_key => PK,
                                    topic_template => Tpl,
                                    payload => Payload,
                                    claim_tag => Tag,
                                    sub_qos => SubQos
                                },
                                mark_inflight(State, Key, Key3, Now, Tag)};
                        [] ->
                            maybe_drop_stale(State, Key, Did, Key3, Ts, Now)
                    end
            end;
        [] ->
            maybe_drop_stale(State, Key, Did, Key3, Ts, Now)
    end.

mark_inflight(State, Key, Key3, Ts, Tag) ->
    Infl = maps:get(Key, maps:get(inflights, State), #{}),
    maps:put(
        inflights, maps:put(Key, maps:put(Key3, {Ts, Tag}, Infl), maps:get(inflights, State)), State
    ).

%% A missing delivery/message row is ambiguous: a concurrent promotion on
%% the peer core may still be replicating its transaction (mria lag), so a
%% FRESH entry is skipped - dropping it would lose a committed delivery.
%% An entry older than the replication window with a missing row is
%% genuinely stale (crash-window orphan, management delete) and is dropped
%% so it cannot block the device queue head.
maybe_drop_stale(State, _Key, _Did, _Key3, Ts, Now) when Now - Ts < ?REPLICATION_LAG_MS ->
    {retry, State};
%% Quota accounting is batched by the claim caller (no per-entry
%% RPC to the quota owner from inside the shard).
maybe_drop_stale(State, Key, _Did, Key3, _Ts, _Now) ->
    {drop, remove_did(State, Key, Key3)}.

%% In-flight claims older than the lease TTL return to the queue head
%% (become claimable again).
release_expired_inflights(Key, State) ->
    Infl = maps:get(Key, maps:get(inflights, State), #{}),
    case maps:size(Infl) of
        0 ->
            State;
        _ ->
            Now = erlang:system_time(millisecond),
            {Expired, Kept} = maps:fold(
                fun(Key3, {Ts, Tag}, {Exp, Kp}) ->
                    case Now - Ts >= ?PENDING_TTL_MS of
                        true -> {[Key3 | Exp], Kp};
                        false -> {Exp, maps:put(Key3, {Ts, Tag}, Kp)}
                    end
                end,
                {[], #{}},
                Infl
            ),
            case Expired of
                [] ->
                    State;
                _ ->
                    Q = maps:get(Key, maps:get(queues, State), queue:new()),
                    Q1 = lists:foldl(
                        fun(K3, Qq) ->
                            {_, _, Did} = K3,
                            queue:in_r(Did, Qq)
                        end,
                        Q,
                        Expired
                    ),
                    State1 = save_queue(State, Key, Q1),
                    maps:put(
                        inflights,
                        maps:put(Key, Kept, maps:get(inflights, State1)),
                        State1
                    )
            end
    end.

%% Return the highest matching subscription QoS (not just a boolean)
%% so the claim result can carry it back to prepare_delivery, which no
%% longer needs its own emqx_broker:subscriptions read (3 -> 2 per message).
topics_match(_Topic, []) ->
    false;
topics_match(Topic, [{Filter, Qos} | Rest]) ->
    case emqx_topic:match(Topic, Filter) of
        true ->
            case topics_match(Topic, Rest) of
                {ok, Q2} -> {ok, max(Qos, Q2)};
                false -> {ok, Qos}
            end;
        false ->
            topics_match(Topic, Rest)
    end.

%%--------------------------------------------------------------------
%% Acks: index removal on the device shard, counter on the delivery shard
%%--------------------------------------------------------------------

%% Removes the per-device index entry and adjusts the global counter.
%% The bcast_msg_meta counter decrement runs on the delivery shard via
%% the {meta_dec_batch} call dispatched by the ack_batch wrapper.
%% Returns {counted | not_found, NewState, GlobalDelta} (delta batched by
%% the caller).
ack_one_index({PK, DN, Did}, State) ->
    Key = {PK, DN},
    Key3 = {PK, DN, Did},
    case maps:is_key(Key3, maps:get(dids, State)) of
        false ->
            %% Already acked (or completed): duplicate PUBACKs must not
            %% count twice.
            {not_found, State, 0};
        true ->
            State1 = remove_did(State, Key, Key3),
            %% The qos1_acked metric is counted by pull_pool on the
            %% take_pending match (it owns the dedup of duplicate
            %% PUBACKs); the owner does not count it again.
            {counted, State1, -1}
    end.

%% The delivery counter lives in the small per-request bcast_msg_meta row
%% (not the 47KB bcast_msg row): an ack is one ~200B dirty read + one
%% ~200B dirty write instead of a full 47KB row rewrite. Only the shard
%% that owns the Did mutates it, so a dirty read-modify-write is
%% race-free. Only the completion step (delete the delivery + meta rows
%% and decrement the message's delivery_count) needs a real transaction:
%% the message row is shared with promoter batch transactions.
complete_or_count(Did) ->
    case mnesia:dirty_read(?TAB_MSG_META, Did) of
        [#bcast_msg_meta{counter = Counter} = M] when Counter =< 1 ->
            complete_delivery(Did, M#bcast_msg_meta.msg_id);
        [#bcast_msg_meta{counter = Counter} = M] ->
            mnesia:dirty_write(M#bcast_msg_meta{counter = Counter - 1});
        [] ->
            ok
    end.

complete_delivery(Did, MsgId) ->
    case
        mnesia:transaction(
            fun() ->
                case mnesia:wread({?TAB_MSG_META, Did}) of
                    [#bcast_msg_meta{}] ->
                        mnesia:delete({?TAB_MSG_META, Did}),
                        case mnesia:wread({?TAB_MSG_REC, Did}) of
                            [#bcast_msg{}] ->
                                mnesia:delete({?TAB_MSG_REC, Did}),
                                emqx_bcast_storage:dec_delivery_count_tx(MsgId),
                                ok;
                            [] ->
                                ok
                        end;
                    [] ->
                        ok
                end
            end,
            20
        )
    of
        {atomic, _} ->
            ok;
        {aborted, Reason} ->
            %% The index entry is already gone; a repeated ack cannot
            %% complete it. The delivery row then expires and cleanup
            %% removes it (with its message-count decrement).
            ?SLOG(warning, #{
                msg => "bcast_ack_completion_tx_aborted",
                delivery_id => Did,
                reason => Reason
            }),
            ok
    end.

%%--------------------------------------------------------------------
%% Releases
%%--------------------------------------------------------------------

release_claim_local(PK, DN, Did, State) ->
    Key = {PK, DN},
    Key3 = {PK, DN, Did},
    Infl = maps:get(Key, maps:get(inflights, State), #{}),
    case maps:take(Key3, Infl) of
        error ->
            State;
        {_Old, Infl2} ->
            %% Back to the FIFO head (it was the oldest entry).
            State1 =
                case maps:is_key(Key3, maps:get(dids, State)) of
                    false ->
                        State;
                    true ->
                        Q = maps:get(Key, maps:get(queues, State), queue:new()),
                        save_queue(State, Key, queue:in_r(Did, Q))
                end,
            maps:put(inflights, maps:put(Key, Infl2, maps:get(inflights, State1)), State1)
    end.

release_client_claims_local(PK, DN, Tag, State) ->
    Key = {PK, DN},
    Infl = maps:get(Key, maps:get(inflights, State), #{}),
    {ToRelease, Kept} = maps:fold(
        fun(Key3, {_Ts, EntryTag}, {Rel, Kp}) ->
            case EntryTag =:= Tag of
                true -> {[Key3 | Rel], Kp};
                false -> {Rel, maps:put(Key3, {_Ts, EntryTag}, Kp)}
            end
        end,
        {[], #{}},
        Infl
    ),
    case ToRelease of
        [] ->
            State;
        _ ->
            Q = maps:get(Key, maps:get(queues, State), queue:new()),
            Q1 = lists:foldl(
                fun(K3, Qq) ->
                    {_, _, Did} = K3,
                    queue:in_r(Did, Qq)
                end,
                Q,
                ToRelease
            ),
            State1 = save_queue(State, Key, Q1),
            maps:put(inflights, maps:put(Key, Kept, maps:get(inflights, State1)), State1)
    end.

%%--------------------------------------------------------------------
%% Admission reservations (atomic global reserve + per-shard device check)
%%--------------------------------------------------------------------

%% Serialized admission: shard 0 reserves the global budget atomically
%% (update_counter returns the new value; overshoot rolls back) and each
%% device shard checks + reserves its own devices in its own process.
%% Each accepted device reserves one pending slot; the reservation
%% converts into a real index entry at promotion (append) and is released
%% when the intake rejects the entry (queue full) or the promoter drops
%% it. A reservation whose intake node crashed expires via cleanup
%% (self-healing quota).
admit_local(PK, DNs, State) ->
    GlobalMax = emqx_bcast_config:get(max_pending_deliveries),
    PerDeviceMax = emqx_bcast_config:get(max_pending_deliveries_per_device),
    Len = length(DNs),
    NewGlobal = ets:update_counter(?TAB_QUOTA_ETS, global, {2, Len}),
    case NewGlobal > GlobalMax of
        true ->
            _ = ets:update_counter(?TAB_QUOTA_ETS, global, {2, -Len}),
            {{error, {quota_exceeded, []}}, State};
        false ->
            Over = dispatch_check_devices(PK, DNs, PerDeviceMax, State),
            case Over of
                [] ->
                    {ok, dispatch_reserve(PK, DNs, State)};
                _ ->
                    %% Roll back the global reservation; no per-device
                    %% reservation was made.
                    _ = ets:update_counter(?TAB_QUOTA_ETS, global, {2, -Len}),
                    {{error, {quota_exceeded, Over}}, State}
            end
    end.

check_devices_local(PK, DNs, PerDeviceMax, State) ->
    [
        DN
     || DN <- DNs,
        effective_count_local(State, {PK, DN}) + 1 > PerDeviceMax
    ].

reserve_inc(State, Key) ->
    Res = maps:get(reserves, State),
    Ts = erlang:system_time(millisecond),
    case maps:get(Key, Res, undefined) of
        undefined ->
            maps:put(reserves, maps:put(Key, {1, Ts}, Res), State);
        {Count, _OldTs} ->
            maps:put(reserves, maps:put(Key, {Count + 1, Ts}, Res), State)
    end.

%% One reservation slot is consumed: the reserve row drops and the global
%% counter (which included the reservation) follows. append_entry then
%% adds +1 for the real index entry, so the net global change of an
%% admission -> promotion round trip is zero. Returns {NewState,
%% Decremented} (0/1); the caller batches the global counter change.
reserve_dec(State, Key) ->
    Res = maps:get(reserves, State),
    case maps:get(Key, Res, undefined) of
        undefined ->
            {State, 0};
        {1, _Ts} ->
            {maps:put(reserves, maps:remove(Key, Res), State), 1};
        {Count, Ts} ->
            {maps:put(reserves, maps:put(Key, {Count - 1, Ts}, Res), State), 1}
    end.

reserve_count(State, Key) ->
    case maps:get(Key, maps:get(reserves, State), undefined) of
        undefined -> 0;
        {Count, _} -> Count
    end.

%% The count a quota check must respect: index entries plus outstanding
%% admission reservations for the device.
effective_count_local(State, Key) ->
    pending_count_for_local(Key, State) + reserve_count(State, Key).

%%--------------------------------------------------------------------
%% Quota and reads
%%--------------------------------------------------------------------

exceeds_quota(_Count, infinity) ->
    false;
exceeds_quota(Count, Max) when is_integer(Max) ->
    Count > Max.

device_deliveries_local(Key, State) ->
    {ok, [Did || {Did, _State} <- device_entries(Key, State)]}.

device_delivery_entries_local(Key, State) ->
    {ok, device_entries(Key, State)}.

%% FIFO entries (queue order) then in-flight entries. Queue residuals
%% whose dids entry is gone are filtered out.
device_entries(Key = {PK, DN}, State) ->
    Q = maps:get(Key, maps:get(queues, State), queue:new()),
    Dids = maps:get(dids, State),
    Stored = [
        {Did, stored}
     || Did <- queue:to_list(Q),
        maps:is_key({PK, DN, Did}, Dids)
    ],
    Infl = maps:get(Key, maps:get(inflights, State), #{}),
    Pending = [
        {Did, pending_state(Ts, Tag)}
     || {{_, _, Did}, {Ts, Tag}} <- maps:to_list(Infl)
    ],
    Stored ++ Pending.

pending_state(Ts, undefined) ->
    {pending, Ts};
pending_state(Ts, ClaimTag) when is_integer(ClaimTag) ->
    {pending, Ts, ClaimTag}.

%% The quota ETS row lives on the owner node and is created at
%% activation; guard the read so a non-owner node or a pre-activation call
%% returns 0 instead of raising badarg on a missing table.
pending_count_local() ->
    case ets:info(?TAB_QUOTA_ETS) of
        undefined ->
            0;
        _ ->
            case ets:lookup(?TAB_QUOTA_ETS, global) of
                [{global, N}] -> N;
                [] -> 0
            end
    end.

pending_count_for_local(Key, State) ->
    maps:get(Key, maps:get(counts, State), 0).

%%--------------------------------------------------------------------
%% Management deletes and cleanup
%%--------------------------------------------------------------------

%% Runs on the delivery shard: rows are shared state, so any process may
%% run the transaction.
%% Lock order is meta -> msg_rec, matching complete_delivery/2 and
%% delete_expired_deliveries_batched (the previous rec-first order widened
%% the mnesia deadlock window with the ack hot path; retries masked it).
delete_delivery_rows_local(Did, MsgId) ->
    case
        mnesia:transaction(
            fun() ->
                case mnesia:wread({?TAB_MSG_META, Did}) of
                    [] ->
                        case mnesia:wread({?TAB_MSG_REC, Did}) of
                            [] ->
                                {error, not_found};
                            [#bcast_msg{}] ->
                                mnesia:delete({?TAB_MSG_REC, Did}),
                                emqx_bcast_storage:dec_delivery_count_tx(MsgId),
                                ok
                        end;
                    [#bcast_msg_meta{}] ->
                        mnesia:delete({?TAB_MSG_META, Did}),
                        case mnesia:wread({?TAB_MSG_REC, Did}) of
                            [#bcast_msg{}] ->
                                mnesia:delete({?TAB_MSG_REC, Did}),
                                emqx_bcast_storage:dec_delivery_count_tx(MsgId),
                                ok;
                            [] ->
                                %% meta present but rec gone (partial-delete
                                %% crash window): idempotent cleanup.
                                ok
                        end
                end
            end,
            20
        )
    of
        {atomic, ok} -> ok;
        {atomic, {error, not_found}} -> {error, not_found};
        {aborted, Reason} -> {error, Reason}
    end.

delete_message_rows_local(ApiId, DeliveryIds) ->
    case
        mnesia:transaction(
            fun() ->
                lists:foreach(
                    fun(Did) ->
                        mnesia:delete({?TAB_MSG_REC, Did}),
                        mnesia:delete({?TAB_MSG_META, Did})
                    end,
                    DeliveryIds
                ),
                case mnesia:read(?TAB_MSG_API_ID, ApiId, write) of
                    [#bcast_message_api_id{msg_id = MsgId}] ->
                        case mnesia:read(?TAB_MSG, MsgId, write) of
                            [#bcast_message{content_hash = Hash}] ->
                                mnesia:delete({?TAB_MSG, MsgId}),
                                mnesia:delete({?TAB_MSG_HASH, Hash}),
                                mnesia:delete({?TAB_MSG_API_ID, ApiId}),
                                ok;
                            [] ->
                                ok
                        end;
                    [] ->
                        ok
                end
            end,
            20
        )
    of
        {atomic, _} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%% Projection-only scan of expired deliveries. Returns tuples
%% {DeliveryId, MsgId, ProductKey, DeviceNames} instead of full records so
%% the caller heap never materializes the ~47KB payload rows. The scan is
%% bounded by Budget via ets:select/3's limit (mnesia:dirty_select on this
%% OTP has no limit option): a large expiry backlog is drained over
%% several cleanup ticks instead of one unbounded pass. bcast_msg is a
%% ram_copies mria table, so its backing ETS table is readable directly
%% (the same trick index_entry_valid/1 uses).
scan_expired_deliveries(Now) ->
    scan_expired_deliveries(Now, ?CLEANUP_BUDGET).

scan_expired_deliveries(Now, Budget) ->
    %% ets:select/3 returns '$end_of_table' when the table is empty (not
    %% a {Matches, Cont} tuple).
    case
        ets:select(
            ?TAB_MSG_REC,
            [
                {
                    #bcast_msg{
                        delivery_id = '$1',
                        msg_id = '$2',
                        product_key = '$3',
                        device_names = '$4',
                        expires_at = '$5',
                        _ = '_'
                    },
                    [{'<', '$5', Now}],
                    [{{'$1', '$2', '$3', '$4'}}]
                }
            ],
            Budget
        )
    of
        '$end_of_table' -> [];
        {Rows, _Continuation} -> Rows
    end.

%% Index removal for expired deliveries routes per shard in parallel
%% (remove_batch/1 groups by device shard internally).
dispatch_expired_index(Expired) ->
    remove_batch(
        [
            {ProductKey, DN, DeliveryId}
         || {DeliveryId, _MsgId, ProductKey, DeviceNames} <- Expired,
            DN <- DeviceNames
        ]
    ).

%% Batched mnesia deletes. One transaction per chunk (100 deliveries)
%% instead of one transaction per expired delivery.
delete_expired_deliveries_batched(Expired) ->
    lists:foreach(
        fun(Chunk) ->
            case
                mnesia:transaction(
                    fun() ->
                        lists:foreach(fun delete_one_expired_delivery_tx/1, Chunk)
                    end,
                    20
                )
            of
                {atomic, _} ->
                    ok;
                {aborted, Reason} ->
                    %% Log the abort (previously swallowed silently);
                    %% the next cleanup tick re-scans the expired rows.
                    ?SLOG(warning, #{
                        msg => "bcast_expired_delete_tx_aborted",
                        reason => Reason,
                        chunk_size => length(Chunk)
                    })
            end
        end,
        chunks(Expired, 100)
    ).

%% Lock order is meta -> msg_rec, matching the ack hot path
%% complete_delivery/2 (which reads/writes msg_meta before bcast_msg).
%% The previous msg_rec-first order was the reverse, widening the mnesia
%% deadlock window between the cleanup tx and concurrent acks (the 20
%% retries masked it, but each retry is wasted work and latency).
delete_one_expired_delivery_tx({DeliveryId, _MsgId, _ProductKey, _DeviceNames}) ->
    case mnesia:wread({?TAB_MSG_META, DeliveryId}) of
        [] ->
            %% Meta gone: either the delivery was acked/completed
            %% concurrently or it never had one; clean up the row
            %% defensively.
            case mnesia:wread({?TAB_MSG_REC, DeliveryId}) of
                [#bcast_msg{msg_id = MsgId}] ->
                    mnesia:delete({?TAB_MSG_REC, DeliveryId}),
                    emqx_bcast_storage:dec_delivery_count_tx(MsgId),
                    ok;
                [] ->
                    ok
            end;
        [#bcast_msg_meta{msg_id = MsgId}] ->
            mnesia:delete({?TAB_MSG_META, DeliveryId}),
            case mnesia:wread({?TAB_MSG_REC, DeliveryId}) of
                [#bcast_msg{}] ->
                    mnesia:delete({?TAB_MSG_REC, DeliveryId}),
                    emqx_bcast_storage:dec_delivery_count_tx(MsgId),
                    ok;
                [] ->
                    ok
            end
    end.

chunks([], _N) ->
    [];
chunks(List, N) ->
    {Head, Tail} = lists:split(min(N, length(List)), List),
    [Head | chunks(Tail, N)].

%% Expired-message cleanup is also bounded by ?CLEANUP_BUDGET
%% (previously an unbounded full scan + per-row dirty_delete on every
%% cleanup tick). Remaining rows are picked up by the next tick.
cleanup_expired_messages_local(Now) ->
    Expired =
        case
            ets:select(
                ?TAB_MSG,
                [
                    {
                        #bcast_message{
                            msg_id = '$1',
                            content_hash = '$2',
                            api_msg_id = '$3',
                            expires_at = '$4',
                            _ = '_'
                        },
                        [{'<', '$4', Now}],
                        [{{'$1', '$2', '$3'}}]
                    }
                ],
                ?CLEANUP_BUDGET
            )
        of
            '$end_of_table' -> [];
            {Rows, _Continuation} -> Rows
        end,
    lists:foreach(
        fun({MsgId, ContentHash, ApiMsgId}) ->
            mnesia:dirty_delete({?TAB_MSG, MsgId}),
            mnesia:dirty_delete({?TAB_MSG_HASH, ContentHash}),
            mnesia:dirty_delete({?TAB_MSG_API_ID, ApiMsgId})
        end,
        Expired
    ).

%% Reservations whose intake node died (queue entry lost with the node)
%% expire here so the quota counter does not leak them forever.
cleanup_stale_reservations(State) ->
    Ts = erlang:system_time(millisecond),
    Res = maps:get(reserves, State),
    {Kept, Expired} = maps:fold(
        fun(Key, {Count, ReserveTs}, {Kp, Exp}) ->
            case Ts - ReserveTs > ?RESERVE_TTL_MS of
                true -> {Kp, [{Key, Count} | Exp]};
                false -> {maps:put(Key, {Count, ReserveTs}, Kp), Exp}
            end
        end,
        {#{}, []},
        Res
    ),
    lists:foreach(
        fun({_Key, Count}) -> quota_update(-Count) end,
        Expired
    ),
    maps:put(reserves, Kept, State).

%% Last-resort repair for index entries whose delivery (or message)
%% record disappeared through a crash window or management delete. States
%% are preserved for valid entries.
%%
%% The scan is BOUNDED: at scale a full pass over the queues costs a
%% queue head pop/push per device; the old ETS full-scan stalled the
%% owner gen_server for minutes at a time. Each cleanup tick inspects at
%% most ?ORPHAN_SCAN_BUDGET device heads via a rotating cursor, so a full
%% coverage pass spreads over many ticks - fine for a last-resort repair
%% (the claim path already drops lazily-deleted and stale entries).
cleanup_orphan_index_local(State) ->
    Keys = maps:keys(maps:get(queues, State)),
    case Keys of
        [] ->
            State;
        _ ->
            N = length(Keys),
            Start = maps:get(orphan_cursor, State, 0) rem N,
            Budget = min(?ORPHAN_SCAN_BUDGET, N),
            {Pre, Post} = lists:split(Start, Keys),
            Rotated = Post ++ Pre,
            Chunk = lists:sublist(Rotated, Budget),
            State1 = lists:foldl(
                fun(Key, St) -> cleanup_orphan_head(Key, St) end,
                State,
                Chunk
            ),
            State1#{orphan_cursor => (Start + Budget) rem N}
    end.

%% Inspect one device's FIFO head: lazily-deleted residuals and stale
%% entries are dropped, valid entries are put back in place.
cleanup_orphan_head(Key = {PK, DN}, State) ->
    Q = maps:get(Key, maps:get(queues, State), queue:new()),
    case queue:out(Q) of
        {empty, _} ->
            State;
        {{value, Did}, Q2} ->
            Key3 = {PK, DN, Did},
            case maps:is_key(Key3, maps:get(dids, State)) of
                false ->
                    %% Lazy residual: drop.
                    save_queue(State, Key, Q2);
                true ->
                    case index_entry_valid(Did) of
                        true ->
                            save_queue(State, Key, queue:in_r(Did, Q2));
                        false ->
                            State1 = remove_did(State, Key, Key3),
                            quota_update(-1),
                            save_queue(State1, Key, Q2)
                    end
            end
    end.

%% Direct ETS reads on the owner's local ram_copies replica: this runs
%% inside the owner gen_server on the drain hot path budget, so bypass the
%% mnesia dirty-queue machinery. Dirty reads see the same committed rows.
%% The meta row is written in the same transaction as bcast_msg, so its
%% presence is an equivalent validity check at ~200B instead of ~47KB.
index_entry_valid(Did) ->
    case ets:lookup(?TAB_MSG_META, Did) of
        [#bcast_msg_meta{msg_id = MsgId}] ->
            case ets:lookup(?TAB_MSG, MsgId) of
                [_] -> true;
                [] -> false
            end;
        [] ->
            false
    end.

%%--------------------------------------------------------------------
%% Per-device dispatch from a coordinator handler (shard 0): the local
%% shard's part runs inline with the current State, siblings are called
%% directly (all shards live on the owner node). Each returns the new
%% State (the local part is the only thing that changes it).
%%--------------------------------------------------------------------

dispatch_check_devices(PK, DNs, Max, State) ->
    MyShard = maps:get(shard, State),
    lists:append([
        case Shard =:= MyShard of
            true -> check_devices_local(PK, Sub, Max, State);
            false -> route(Shard, {check_devices, PK, Sub, Max}, ?SYNC_TIMEOUT_MS)
        end
     || {Shard, Sub} <- group_devices(PK, DNs)
    ]).

dispatch_reserve(PK, DNs, State) ->
    MyShard = maps:get(shard, State),
    lists:foldl(
        fun({Shard, Sub}, St) ->
            case Shard =:= MyShard of
                true ->
                    lists:foldl(fun(DN, St2) -> reserve_inc(St2, {PK, DN}) end, St, Sub);
                false ->
                    _ = route(Shard, {reserve_devices, PK, Sub}, ?SYNC_TIMEOUT_MS),
                    St
            end
        end,
        State,
        group_devices(PK, DNs)
    ).

dispatch_release(PK, DNs, State) ->
    MyShard = maps:get(shard, State),
    lists:foldl(
        fun({Shard, Sub}, St) ->
            case Shard =:= MyShard of
                true ->
                    {St2, Delta} = lists:foldl(
                        fun(DN, {St3, D}) ->
                            {St4, D2} = reserve_dec(St3, {PK, DN}),
                            {St4, D + D2}
                        end,
                        {St, 0},
                        Sub
                    ),
                    quota_update(-Delta),
                    St2;
                false ->
                    _ = route(Shard, {release_devices, PK, Sub}, ?SYNC_TIMEOUT_MS),
                    St
            end
        end,
        State,
        group_devices(PK, DNs)
    ).
