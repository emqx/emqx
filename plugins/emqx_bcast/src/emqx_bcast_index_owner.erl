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
%% Routing:
%%   * per-device index state + per-delivery ack counter -> shard_of({PK, DN})
%%     (the ack counter is a 3-tuple mnesia table, decremented by lock-free
%%     dirty writes alongside the per-device acked markers, marker first)
%%   * management delete of delivery rows -> did_shard_of(DeliveryId)
%% The global pending counter lives in one shared ETS row
%% (bcast_quota_ets) updated with atomic update_counter from every shard.

-behaviour(gen_server).

-export([start_link/1]).
-export([shard_count/0, owner_node/0, is_owner/0, shard_of/1, shard_owner/1]).
-export([
    append_batch/1,
    remove_batch/1,
    claim/1,
    claim/2,
    ack_batch/1,
    release_claim/3,
    release_client_claims/3,
    release_claims_async/1,
    release_client_claims_async/1,
    release_client_claims_sync/1,
    check_quota/2,
    admit/2,
    release_admit/2,
    reserve_global_local/2,
    release_global_local/1,
    quota_update_local/1,
    quota_update_remote/2,
    device_deliveries/1,
    device_delivery_entries/1,
    pending_count/0,
    pending_count_for/1,
    pending_count_local/0,
    delete_delivery/1,
    delete_message/1,
    cleanup_expired/0,
    cleanup_completed_deliveries/0,
    delete_expired_messages/2,
    create_sync/2,
    create_delivery/6,
    rebuild_index/0,
    reset/0,
    gauge_sample/0
]).
-export([local_handle/3, local_cast/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

%% Fast activation poll: the first claim can arrive within ~50ms of app
%% start (CT topologies), so the leader must activate promptly. Polling a
%% non-owner node every 50ms is a trivial is_owner() check.
-define(OWNER_POLL_MS, 50).
%% A dormant non-leader shard whose partition is assigned to this node asks
%% the activation leader for a targeted rebuild at this cadence (the leader
%% only drives on its own restart / owner transitions and does not track
%% sibling shard liveness).
-define(ACTIVATE_POLL_MS, 500).
%% An activation drive that could not finish keeps being retried at
%% ?OWNER_POLL_MS while it is this young: at boot the sibling shards a drive
%% cannot reach are the ones the supervisor has not started yet (that is why
%% activation runs from inside the loop rather than from init/1), and they
%% appear within milliseconds. A partition still missing after this window does
%% not come back on its own - a peer running a build without the status call,
%% or one whose plugin is uninstalled - so the retry drops to the slow poll
%% instead of probing every shard at 20Hz for the whole upgrade window.
-define(ACTIVATION_FAST_MS, 1000).
%% Probing a sibling is a state read: a healthy shard answers in microseconds, so
%% it gets its own budget instead of the 5s the activation call needs (that call
%% carries the whole projection and rebuilds a partition).
-define(PROBE_TIMEOUT_MS, 1000).
%% A peer whose probe timed out is left alone for this long: waiting on it again
%% costs another probe timeout, and the peer asks the leader for its own
%% activation as soon as it is up, which clears the entry immediately. Only a
%% timeout is held back - a peer that fails fast is still starting up.
-define(PEER_BACKOFF_MS, 5000).
%% Hot-path leg timeouts: 30s made one stalled shard hold every caller of
%% the pmap fan-out (claim workers, promoter) for half a minute, which
%% turned a single stuck partition into a cluster-wide drain stall. 5s is
%% an order of magnitude above the expected sub-ms call latency while
%% keeping failure recovery prompt.
-define(SYNC_TIMEOUT_MS, 5000).
-define(CLAIM_TIMEOUT_MS, 5000).

%% How many parallel index-owner processes the index is sharded into.
%% Shards are placed round-robin across the RUNNING core nodes (each core
%% runs all SHARD_COUNT processes, only its assigned subset activates), so
%% the index work fans out over all cores instead of one owner node.
%%
%% Sizing: 48 shards / 4 cores = 12 active shards per core. Measured at
%% 16 shards (4 active per core) the active index shards hit the
%% single-process reduction wall (~2.5M red/s) at ~100k delivery/s, so 12
%% active shards per core (3x the shards) leave per-shard headroom to
%% sustain ~300k delivery/s before the same wall. The claim/ack/append
%% fan-out (one pmap leg per touched shard) grows with the shard count,
%% but the per-leg device count shrinks proportionally, so the total spawn
%% cost stays negligible. 48 also divides evenly by 2/3/4 so 2-/3-/4-core
%% topologies each get an equal number of active shards per core.
-define(SHARD_COUNT, 48).

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
%% Per-call bound for the management cascade delete: one message can be
%% shared by an unbounded number of deliveries (content de-duplication), so
%% the sweep is chunked and driven from the calling process.
-define(MGMT_DELETE_CHUNK, 100).

%% Upper bound on deliveries healed (all target devices acked but the atomic
%% counter never reached zero) by a single rebuild drive. The healed ids ride
%% the activation RPCs as a skip set so later shards do not resurrect them;
%% normally the set is empty (it only captures a crash-window drift), so the
%% cap solely guards a systemic drift from inflating the activation payload.
%% Deliveries beyond the cap are left for a later drive or TTL.
-define(MAX_HEAL_PER_DRIVE, 10000).

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

%% Node-local shadow of the global pending counter: every hot-path
%% quota_update(Delta) (append/remove/ack/release, one per shard batch)
%% bumps this local ETS row only - no cross-node RPC. Shard 0 periodically
%% ships the accumulated delta to the quota owner's authoritative row
%% (quota_update_local), which admission still reads (<=1 sync window lag;
%% quota is a soft admission cap, overshoot by one window is harmless).
-define(TAB_QUOTA_LOCAL, bcast_quota_local).
%% Reconcile cadence for shipping local quota deltas to the owner.
-define(QUOTA_SYNC_MS, 1000).
%% Periodic self-healing recount of the authoritative pending row (owner shard
%% 0 only). A drive recounts from live shard pendings but hot-path increments
%% can race the probes; re-running the recount at this cadence bounds any
%% residual drift so the quota can never stay permanently high or low.
-define(QUOTA_RECOUNT_MS, 60000).

%% Ack-counter decrement batching cadence: counted acks accumulate
%% per-delivery acked device lists in the shard's ack_buf and are applied
%% per tick with two lock-free dirty writes (marker persistence, then
%% counter decrement) instead of one rlog write per ack. The old
%% one-rlog-write-per-ack path saturated mria's rlog and aborted the
%% completion transaction under load, leaking the bcast_msg /
%% bcast_msg_meta / bcast_msg_meta_counter rows.
%% Ack bookkeeping flush cadence. Every flush tick writes one acked-marker row
%% and one counter decrement per delivery that received acks in that tick, and
%% both tables are copied to every core: the per-tick writes are what every
%% peer's mnesia_tm has to apply, so the pressure is proportional to
%% `1 / cadence`. At 50ms with 8k deliveries the two writes per (delivery, tick)
%% reached roughly the acknowledgement rate itself (~70k/s per node), each one
%% replicated to three peers, and the peer queues grew into the hundreds of
%% thousands - which stalls every mnesia user on the node, this plugin's
%% completions and EMQX's own registry included.
%%
%% 500ms keeps the ordering guarantee (marker first, then the decrement) and
%% only moves the crash window and the completion of a delivery up to 500ms
%% later. An acknowledgement that is not yet durable when a node dies is
%% redelivered - at-least-once, never counted twice, and never completing a
%% delivery early. Writing the marker per device instead would trade this away:
%% that shape makes the write count scale with the fanout (one row per device
%% per delivery) instead of with the tick, which is why the row carries the
%% tick's device list to begin with.
-define(ACK_DEC_FLUSH_MS, 500).

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
        %% delivery attempts per logical delivery: #{Key3 => N}
        attempts => #{},
        %% claim holder node per device: #{Key => Node}; set when a claim
        %% succeeds, used for node-down reclaim (the holder's ack can never
        %% arrive once its node is gone)
        holders => #{},
        %% admission reservations: #{Key => {Count, Ts}}
        reserves => #{},
        %% peak pending size for conditional fullsweep
        peak => 0,
        %% bounded orphan-scan cursor
        orphan_cursor => 0,
        %% shard 0 only: cumulative local quota delta already shipped to owner
        quota_last_synced => 0,
        %% shard 0 only: drive epoch of the last baseline sync applied from the
        %% owner; reconcile deltas from an older epoch are rejected after a
        %% recount (see quota_update_remote/2)
        quota_epoch => 0,
        %% shard 0 only: at most one self-healing recount in flight, so a slow
        %% probe round cannot pile up behind the timer
        quota_recount_running => false,
        %% batched ack-counter decrements pending flush: #{Did => [DeviceName]}
        ack_buf => #{},
        %% flush timer ref; undefined when idle (nothing buffered)
        ack_flush_ref => undefined,
        %% deliveries whose marker-based completion aborted its transaction:
        %% #{Did => true}, retried by the periodic cleanup tick
        pending_completions => #{},
        %% leader only: #{Shard => BackoffUntilMs} for the peers whose probe
        %% timed out, so a hung peer cannot re-add its timeout to every round
        %% (see probe_siblings/1)
        bad_peers => #{},
        %% leader only: the drive currently running, as
        %% #{pid, ref, mode, started_at} - or undefined. Held here (not in the
        %% coordinator) so only one drive can run at a time and so a
        %% coordinator that dies can be recognised.
        drive_in_flight => undefined,
        %% leader only: the manual rebuilds waiting for the running drive to
        %% report, so one can answer its caller without running the drive
        %% itself.
        drive_waiters => []
    },
    State1 = start_gc_timer(State),
    %% Every node keeps a local quota shadow and (on shard 0) a reconcile
    %% timer that ships its accumulated delta to the quota owner. The local
    %% table must exist before any hot-path quota_update lands on this node.
    ensure_quota_local_table(),
    case Shard of
        0 ->
            %% Shard 0 is the activation leader (quota owner's shard 0).
            %% Activation is NOT driven from init (drive_activation()
            %% gen_server:calls the sibling shards, which may not be started
            %% yet); an immediate message runs inside the loop right after
            %% init, and the drive tolerates siblings that are still
            %% starting (retry at ?OWNER_POLL_MS). The authoritative quota
            %% table is only (re)initialized once this node actually is the
            %% quota owner; the local shadow exists on every node.
            erlang:send_after(?QUOTA_SYNC_MS, self(), quota_sync),
            erlang:send_after(?QUOTA_RECOUNT_MS, self(), quota_recount),
            self() ! maybe_activate,
            {ok, State1};
        _ ->
            %% Non-leader shards are activated by the leader's drive. After
            %% a crash restart the shard is dormant and no drive is coming
            %% (the leader has no liveness tracking of sibling shard
            %% processes), so poll and ask the leader for a targeted
            %% re-activation of this shard's own partition.
            erlang:send_after(?ACTIVATE_POLL_MS, self(), maybe_activate),
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

%% The authoritative quota row has to exist before shard 0 serves: the reserve
%% and release paths use ets:update_counter/3, which raises on a missing key.
%% A completed drive writes the row from the recount; a drive that stopped on
%% an unreachable sibling never gets there, and the true value is unknown at
%% that point, so start it at zero and let the periodic recount replace it once
%% every partition answers again. insert_new never clobbers a live row.
ensure_quota_row() ->
    ensure_quota_table(),
    ets:insert_new(?TAB_QUOTA_ETS, {global, 0}).

%% Node-local quota shadow table: every shard's hot-path quota_update(Delta)
%% bumps this row (no cross-node RPC). Exists on every core node; shard 0's
%% reconcile timer ships the accumulated delta to the owner authoritative row.
%% The row holds the CUMULATIVE net delta since this node started (never
%% reset by the hot path); shard 0 tracks last_synced in its state and sends
%% only the increment, which is race-free against concurrent shard updates.
ensure_quota_local_table() ->
    try ets:new(?TAB_QUOTA_LOCAL, [named_table, set, public, {write_concurrency, true}]) of
        _ -> ok
    catch
        error:badarg -> ok
    end,
    try ets:insert_new(?TAB_QUOTA_LOCAL, {global, 0}) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

%% Hot-path local delta: pure local update, never leaves the node.
quota_local_update(Delta) when Delta =:= 0 ->
    ok;
quota_local_update(Delta) ->
    try ets:update_counter(?TAB_QUOTA_LOCAL, global, {2, Delta}) of
        _ -> ok
    catch
        error:badarg ->
            %% Table missing (dormant shard before first ensure): create and retry.
            ensure_quota_local_table(),
            _ = ets:update_counter(?TAB_QUOTA_LOCAL, global, {2, Delta}),
            ok
    end.

%% Reconcile (runs in shard 0 only): ship this node's cumulative-local minus
%% last-synced delta to the quota owner's authoritative row. Runs once per
%% ?QUOTA_SYNC_MS per node (4 nodes -> 4 sync RPC/s, negligible vs the former
%% per-batch hot-path RPCs). Returns the new baseline: on RPC failure it
%% returns the OLD baseline so the unsent delta is retried next tick (a lost
%% sync would otherwise skew the owner row low forever).
-spec quota_sync(non_neg_integer(), non_neg_integer()) ->
    {non_neg_integer(), non_neg_integer()}.
quota_sync(LastSynced, Epoch) ->
    case owner_node() =:= node() of
        true ->
            %% Owner writes the authoritative row directly on the hot path
            %% (quota_update/1), so the local shadow is never used here.
            {local_quota_total(), Epoch};
        false ->
            Current = local_quota_total(),
            Delta = Current - LastSynced,
            case Delta of
                0 ->
                    {Current, Epoch};
                _ ->
                    case
                        emqx_rpc:call(
                            ?MODULE,
                            owner_node(),
                            ?MODULE,
                            quota_update_remote,
                            [Delta, Epoch],
                            ?SYNC_TIMEOUT_MS
                        )
                    of
                        ok ->
                            {Current, Epoch};
                        %% The owner recounted the authoritative row after this
                        %% delta was computed, so the delta is already in that
                        %% recount. Adopt the new epoch and drop it instead of
                        %% double-counting it; deltas accumulated after the
                        %% recount are then shipped under the new epoch.
                        {stale, OwnerEpoch} ->
                            {Current, OwnerEpoch};
                        {badrpc, _Reason} ->
                            {LastSynced, Epoch}
                    end
            end
    end.

%% Remote reconcile target (runs on the owner node). Applies the delta only
%% while the sender's epoch matches the owner's current drive epoch. A delta
%% computed before a recount carries the old epoch and is rejected with the
%% owner's epoch so the sender can adopt it and drop the already-counted
%% delta.
-spec quota_update_remote(integer(), term()) -> ok | {stale, term()}.
quota_update_remote(Delta, Epoch) ->
    case quota_epoch() of
        Epoch ->
            quota_update_local(Delta),
            ok;
        OwnerEpoch ->
            {stale, OwnerEpoch}
    end.

%% Current drive epoch of the authoritative quota row (owner-local). A missing
%% row (fresh table, no drive yet) reads as 0, which every default epoch
%% matches.
quota_epoch() ->
    try ets:lookup_element(?TAB_QUOTA_ETS, epoch, 2) of
        N when is_integer(N) -> N;
        _ -> 0
    catch
        _:_ -> 0
    end.

%% Current cumulative local quota delta (this node's shadow row).
local_quota_total() ->
    try ets:lookup_element(?TAB_QUOTA_LOCAL, global, 2) of
        N when is_integer(N) -> N;
        _ -> 0
    catch
        _:_ -> 0
    end.

%% Zero this node's local quota shadow (used by reset and takeover so stale
%% cumulative deltas are never re-shipped to a freshly zeroed owner row).
reset_quota_local() ->
    try ets:insert(?TAB_QUOTA_LOCAL, {global, 0}) of
        _ ->
            ok
    catch
        Error:Reason ->
            ?SLOG(error, #{
                msg => "bcast_quota_local_reset_failed",
                exception => Error,
                reason => Reason
            }),
            ok
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
    %% A failed shard call surfaces in three shapes, all of which must
    %% count as failure: a local gen_server:call timeout/exit becomes
    %% {'EXIT', _} (caught by parallel_map), a remote emqx_rpc timeout or
    %% nodedown becomes {badrpc, _}, and a dormant shard replies
    %% {error, not_active}. Treating {badrpc, _} as success silently lost
    %% committed-but-unindexed deliveries (the promoter counted them wanted
    %% and triggered, but the remote shard never got the entries).
    Failed = fun
        ({error, _}) -> true;
        ({'EXIT', _}) -> true;
        ({badrpc, _}) -> true;
        (_) -> false
    end,
    case lists:any(Failed, Results) of
        true -> {error, append_failed};
        false -> ok
    end.

remove_batch(Entries) ->
    lists:sum([
        Removed
     || {ok, Removed} <-
            parallel_map(
                fun({Shard, Sub}) ->
                    route(Shard, {remove_batch, Sub}, ?SYNC_TIMEOUT_MS)
                end,
                group_entries(Entries)
            )
    ]).

%% Error-aware variant, used by the management cascade delete only. Every leg
%% that removed entries reports how many, including in a round that also had
%% failures: the caller counts each round immediately and retries only the
%% failed shards (removing an entry that is already gone is a no-op). Reporting
%% the failures alone would lose whatever was removed before them and leave the
%% canceled ledger permanently short by exactly that many.
remove_batch_groups([]) ->
    {ok, 0, []};
remove_batch_groups(Groups) ->
    Results = parallel_map(
        fun({Shard, Sub}) ->
            try route(Shard, {remove_batch, Sub}, ?SYNC_TIMEOUT_MS) of
                {ok, Removed} when is_integer(Removed) -> {ok, Removed};
                Other -> {error, Other}
            catch
                Error:Reason -> {error, {Error, Reason}}
            end
        end,
        Groups
    ),
    Removed = lists:sum([N || {ok, N} <- Results]),
    case [{Group, Reason} || {Group, {error, Reason}} <- lists:zip(Groups, Results)] of
        [] -> {ok, Removed, []};
        Failures -> {error, Removed, Failures}
    end.

claim(Entries) ->
    claim(Entries, node()).

claim(Entries, Origin) ->
    %% Per-shard calls run in parallel: the shards are independent, and a
    %% sequential fan-out multiplied the batch latency by the shard count.
    %% Origin is the node whose pull shard holds the client buffer for
    %% these claims; it becomes the claim holder for node-down reclaim.
    %% A failed leg must NOT be swallowed into empty results: the caller
    %% (pull side) distinguishes {ok, _} / no_more / {error, _} and only
    %% releases-by-tag the entries of failed legs, keeping the release
    %% storm proportional to the failure, not to the batch size.
    lists:append(
        parallel_map(
            fun({Shard, Sub}) ->
                %% A leg exception (route timeout / noproc while the shard
                %% restarts) must not escape: an {'EXIT', _} tuple leaking
                %% out of parallel_map used to become the tail of an
                %% improper result list and crash the pull shard's
                %% maps:from_list. Normalize to per-entry errors like the
                %% non-list route reply.
                try route(Shard, {claim, Sub, Origin}, ?CLAIM_TIMEOUT_MS) of
                    Results when is_list(Results) ->
                        Results;
                    _ ->
                        [
                            {maps:get(clientid, E), {error, claim_unavailable}}
                         || E <- Sub
                        ]
                catch
                    _:_ ->
                        [
                            {maps:get(clientid, E), {error, claim_unavailable}}
                         || E <- Sub
                        ]
                end
            end,
            group_claim_entries(Entries)
        )
    ).

ack_batch(Acks) ->
    %% Index removal and the per-delivery meta counter decrement both run
    %% on the device shard. The old second {meta_dec_batch} fan-out to the
    %% delivery shard was a hot-spot at high ack rates; the counter now
    %% lives in a 3-tuple mnesia table and is decremented atomically on
    %% the device shard (see handle_call({ack_index,...})).
    Results0 = lists:append(
        parallel_map(
            fun({DevShard, Sub}) ->
                %% Same normalization as claim legs: a route exit must not
                %% leak an {'EXIT', _} tuple into the appended results.
                try route(DevShard, {ack_index, Sub}, ?SYNC_TIMEOUT_MS) of
                    Results when is_list(Results) ->
                        lists:zip(Sub, Results);
                    _ ->
                        [{Ack, {error, not_active}} || Ack <- Sub]
                catch
                    _:_ ->
                        [{Ack, {error, not_active}} || Ack <- Sub]
                end
            end,
            group_acks_by_dev(Acks)
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
    safe_route(shard_of({PK, DN}), {release_claim, PK, DN, Did}),
    ok.

release_client_claims(PK, DN, Tag) ->
    safe_route(shard_of({PK, DN}), {release_client_claims, PK, DN, Tag}),
    ok.

%% Single-shard sync release with the worker-protection semantics of
%% release_client_claims_sync/1.
safe_route(Shard, Req) ->
    try route(Shard, Req, ?SYNC_TIMEOUT_MS) of
        _ -> ok
    catch
        Error:Reason ->
            ?SLOG(warning, #{
                msg => "bcast_release_leg_failed",
                shard => Shard,
                request => Req,
                exception => Error,
                reason => Reason
            })
    end.

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
            Results = parallel_admit_devices(PK, Groups, PerDeviceMax),
            %% Only a definite over-limit is an error. A leg whose shard is
            %% dormant, unreachable or timed out keeps the degrade-to-accept
            %% behaviour: its result is unknown, so it is neither rejected nor
            %% rolled back (the periodic stale-reservation sweep reclaims any
            %% reservation it may have made).
            Over = lists:append([Ds || {error, {over_limit, Ds}} <- Results]),
            case Over of
                [] ->
                    ok;
                _ ->
                    rollback_confirmed(PK, Groups, Results),
                    release_global(Len),
                    {error, {quota_exceeded, Over}}
            end;
        {error, _} ->
            {error, {quota_exceeded, []}};
        {badrpc, _} ->
            %% Quota owner unreachable (startup/takeover): degrade to
            %% acceptance so the API request is not stuck waiting on the
            %% reserve_global RPC; the bounded intake queue provides the
            %% backpressure instead.
            ok
    end.

release_admit(PK, DNs) ->
    parallel_release_devices(PK, group_devices(PK, DNs)),
    ok.

%% Admission runs inside an API request, so its legs are bounded by the
%% framework's budget for that request (see emqx_bcast_utils:api_budget_ms/0):
%% a leg that outlives it would let the framework kill the callback after the
%% global reservation was taken, leaving that reservation to the 60s
%% stale-reservation sweep. 5s of hot-path budget is too long here.
admit_leg_timeout() ->
    emqx_bcast_utils:api_rpc_timeout_ms().

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
                admit_leg_timeout()
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

%% One atomic check-and-reserve call per shard: an active shard either
%% reserves every device of its subset or reports the over-limit ones, so two
%% concurrent requests can no longer both pass a stale check. A dormant or
%% unavailable shard keeps the degrade-to-accept behaviour (admission pressure
%% defers to the bounded intake queue), matching the old coordinator during
%% owner takeover: it answers {ok, 0} without reserving rather than rejecting.
parallel_admit_devices(PK, Groups, PerDeviceMax) ->
    parallel_map(
        fun({Shard, Sub}) ->
            try route(Shard, {admit_devices, PK, Sub, PerDeviceMax}, admit_leg_timeout()) of
                {badrpc, Reason} ->
                    log_admit_leg_unknown(PK, Shard, badrpc, Reason),
                    {error, unknown};
                Result ->
                    Result
            catch
                %% An unreachable or timed-out shard leaves its share of the
                %% per-device cap unenforced for this request, so the
                %% degradation has to be observable.
                Error:Reason ->
                    log_admit_leg_unknown(PK, Shard, Error, Reason),
                    {error, unknown}
            end
        end,
        Groups
    ).

log_admit_leg_unknown(PK, Shard, Error, Reason) ->
    ?SLOG(error, #{
        msg => "bcast_admit_devices_failed",
        product_key => PK,
        shard => Shard,
        exception => Error,
        reason => Reason
    }).

%% Drop the reserve rows of the legs that confirmed a reservation. Uses the
%% shard-local rollback (no global change) because the caller releases the
%% whole global reservation separately. Only a leg that reports having reserved
%% something is rolled back: a dormant shard answers {ok, 0} without reserving,
%% and a rollback sent to it could land after it activates and decrement a
%% reservation taken by a concurrent request. A failed or timed-out leg is
%% unknown, so it is left to the periodic stale-reservation sweep.
rollback_confirmed(PK, Groups, Results) ->
    parallel_foreach(
        fun({Shard, Sub}) ->
            _ = route(Shard, {rollback_devices, PK, Sub}, ?SYNC_TIMEOUT_MS)
        end,
        [Group || {Group, {ok, N}} <- lists:zip(Groups, Results), N > 0]
    ).

parallel_check_devices(PK, Groups, Max) ->
    lists:append(
        parallel_map(
            fun({Shard, Sub}) ->
                %% A timed-out / restarting shard surfaces as a local
                %% gen_server:call exit (parallel_map's catch turns it into
                %% {'EXIT', _}) or a remote emqx_rpc {badrpc, _}. Both must
                %% degrade to "no over-limit device" instead of leaking a
                %% non-list term into lists:append and crashing admit (the
                %% caller then blocks the full timeout and degrades quota to
                %% accept). Normalize here, mirroring the claim/ack legs.
                try route(Shard, {check_devices, PK, Sub, Max}, ?SYNC_TIMEOUT_MS) of
                    {error, not_active} -> [];
                    {badrpc, _} -> [];
                    Over when is_list(Over) -> Over;
                    _ -> []
                catch
                    Error:Reason ->
                        %% A failed probe must not look like "no device is
                        %% over its cap": log it, the per-device quota is
                        %% unenforced for this request.
                        ?SLOG(error, #{
                            msg => "bcast_check_devices_failed",
                            product_key => PK,
                            shard => Shard,
                            exception => Error,
                            reason => Reason
                        }),
                        []
                end
            end,
            Groups
        )
    ).

%% Per-device reservations/releases keep going through the shard
%% gen_servers (parallel): reserve rows are read-modify-write and must be
%% serialized with append_entry's reserve_dec on the same shard process.
parallel_release_devices(PK, Groups) ->
    parallel_foreach(
        fun({Shard, Sub}) ->
            _ = route(Shard, {release_devices, PK, Sub}, admit_leg_timeout()),
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
            maybe_count_canceled(remove_batch([{PK, DN, Did} || DN <- DNs])),
            route(did_shard_of(Did), {delete_delivery_rows, Did, MsgId}, ?SYNC_TIMEOUT_MS)
    end.

delete_message(ApiId) ->
    case mnesia:dirty_read(?TAB_MSG_API_ID, ApiId) of
        [] ->
            {error, not_found};
        [#bcast_message_api_id{msg_id = MsgId}] ->
            Deliveries = dirty_deliveries_of(MsgId),
            DeliveryIds = [D#bcast_msg.delivery_id || D <- Deliveries],
            ?SLOG(info, #{
                msg => "bcast_message_delete_requested",
                api_msg_id => ApiId,
                msg_id => MsgId,
                deliveries => length(DeliveryIds),
                node => node()
            }),
            %% Cascade delete, chunked and driven from THIS process (the
            %% management request), not as one unbounded transaction inside
            %% shard 0. Content de-duplication means one message can be shared
            %% by an unbounded number of deliveries, and a single transaction
            %% over all of them would hold the shard's mailbox and the hot
            %% meta/counter rows for the whole sweep, then retry the entire
            %% batch 20 times on conflict.
            %%
            %% Each chunk commits its Mnesia rows AND its index/ledger effect
            %% together before the next chunk starts: the index entries of a
            %% chunk are removed and counted as soon as that chunk's rows are
            %% gone. A failure in a later chunk (or in the message-row delete)
            %% therefore leaves the earlier chunks fully accounted for, and a
            %% retry only has to finish the chunks whose rows are still there.
            %%
            %% The message/api rows are deleted LAST so the message stays
            %% readable while its deliveries are reclaimed (the delivering
            %% node reads the payload from bcast_message at send time). The
            %% remaining window - a delivery committed by an in-flight intake
            %% entry while the sweep runs - is closed by the re-enumeration
            %% below, and every entry admitted before the delete is refused at
            %% promotion by the delete-epoch check (see bump_delete_epoch/1).
            case bump_delete_epoch(MsgId) of
                ok ->
                    case delete_delivery_chunks(Deliveries) of
                        ok ->
                            case route(0, {delete_message_meta_rows, ApiId}, ?SYNC_TIMEOUT_MS) of
                                ok ->
                                    delete_message_stragglers(MsgId);
                                {error, _} = Error ->
                                    Error
                            end;
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

%% Bump this content hash's delete epoch on every core BEFORE any row is
%% removed, so every entry admitted earlier is dropped at promotion and the
%% delete cannot be undone. An unreachable core aborts the delete (an entry
%% parked there could otherwise re-create the message after the delete
%% returned success); see emqx_bcast:bump_msg_epoch_everywhere/1 for the
%% accepted trade-off when the fan-out succeeds but a later step fails. A
%% message row that is already gone - a previous attempt got past the row
%% deletes - has nothing left to resurrect, so the bump is skipped and the
%% sweep resumes.
bump_delete_epoch(MsgId) ->
    case mnesia:dirty_read(?TAB_MSG, MsgId) of
        [#bcast_message{content_hash = Hash}] ->
            case emqx_bcast:bump_msg_epoch_everywhere(Hash) of
                ok ->
                    ok;
                {error, Reason} = Error ->
                    ?SLOG(error, #{
                        msg => "bcast_delete_epoch_bump_failed",
                        msg_id => MsgId,
                        reason => Reason
                    }),
                    Error
            end;
        [] ->
            ok
    end.

%% Delete the per-delivery rows in bounded chunks, one shard call per chunk,
%% reconciling that chunk's index entries right after its rows are gone. The
%% operation is resumable (the remaining rows are still discoverable by
%% dirty_deliveries_of/1) and reconciled on a best-effort basis: a chunk whose
%% index removal keeps failing is logged and left to the orphan sweep, so the
%% canceled counter can undercount in that rare case rather than block the
%% delete.
delete_delivery_chunks([]) ->
    ok;
delete_delivery_chunks(Deliveries) ->
    {Chunk, Rest} = lists:split(min(mgmt_delete_chunk(), length(Deliveries)), Deliveries),
    DeliveryIds = [D#bcast_msg.delivery_id || D <- Chunk],
    case delete_rows_idempotent(DeliveryIds) of
        ok ->
            reconcile_chunk_index(Chunk),
            delete_delivery_chunks(Rest);
        {error, _} = Error ->
            Error
    end.

%% Deleting already-deleted rows is a no-op, so an uncertain or failed leg can
%% simply be issued again: a timeout may mean the commit landed and only the
%% reply was lost.
delete_rows_idempotent(DeliveryIds) ->
    case try_delete_rows(DeliveryIds) of
        ok ->
            ok;
        First ->
            ?SLOG(warning, #{
                msg => "bcast_delete_rows_retry",
                chunk_size => length(DeliveryIds),
                result => First
            }),
            try_delete_rows(DeliveryIds)
    end.

try_delete_rows(DeliveryIds) ->
    try route(0, {delete_delivery_rows, DeliveryIds}, ?SYNC_TIMEOUT_MS) of
        ok -> ok;
        {error, _} = Error -> Error
    catch
        Error:Reason -> {error, {Error, Reason}}
    end.

%% Drop this chunk's index entries and count them. Every round counts what it
%% removed, including a round that also failed, and only the shards that failed
%% are re-issued. One retry, then the failure is logged and left to the periodic
%% orphan sweep.
reconcile_chunk_index(Chunk) ->
    reconcile_index_groups(group_entries(delivery_index_entries(Chunk)), 1).

reconcile_index_groups(Groups, Attempt) ->
    case remove_batch_groups(Groups) of
        {ok, Removed, []} ->
            maybe_count_canceled(Removed),
            ok;
        {error, Removed, Failures} ->
            maybe_count_canceled(Removed),
            log_reconcile_failure(Attempt, Failures),
            case Attempt of
                1 -> reconcile_index_groups([Group || {Group, _} <- Failures], 2);
                _ -> ok
            end
    end.

log_reconcile_failure(1, Failures) ->
    ?SLOG(warning, #{
        msg => "bcast_delete_index_remove_retry",
        failures => [{Shard, Reason} || {{Shard, _Sub}, Reason} <- Failures]
    });
log_reconcile_failure(_, Failures) ->
    ?SLOG(error, #{
        msg => "bcast_delete_index_remove_failed",
        failures => [{Shard, Reason} || {{Shard, _Sub}, Reason} <- Failures],
        note => "left to the orphan sweep; canceled may undercount"
    }).

%% Chunk size of the management cascade delete. Overridable so tests can
%% exercise the multi-chunk path without creating hundreds of deliveries.
mgmt_delete_chunk() ->
    application:get_env(emqx_bcast, mgmt_delete_chunk, ?MGMT_DELETE_CHUNK).

dirty_deliveries_of(MsgId) ->
    mnesia:dirty_match_object(?TAB_MSG_REC, #bcast_msg{msg_id = MsgId, _ = '_'}).

delivery_index_entries(Deliveries) ->
    [
        {D#bcast_msg.product_key, DN, D#bcast_msg.delivery_id}
     || D <- Deliveries,
        DN <- D#bcast_msg.device_names
    ].

%% Bounded window close: after the message row is deleted no further delivery
%% for it can commit, so one re-enumeration drains the enumerate/delete gap.
%% Its own failure is propagated: the rows survived, so dropping their index
%% entries here would hide the failure behind a successful delete.
delete_message_stragglers(MsgId) ->
    case dirty_deliveries_of(MsgId) of
        [] ->
            ok;
        Deliveries ->
            %% Same per-chunk accounting as the main sweep: a failure here
            %% leaves the already-deleted chunks reconciled and the rest
            %% discoverable.
            delete_delivery_chunks(Deliveries)
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
    case length(Expired) of
        0 ->
            ok;
        ExpiredCount ->
            ?SLOG(info, #{
                msg => "bcast_expired_deliveries_reclaimed",
                count => ExpiredCount,
                now => Now,
                node => node()
            })
    end,
    %% Messages have no shard state: delete directly in the caller.
    cleanup_expired_messages_local(Now),
    %% Index removal routes per shard in parallel (remove_batch/1).
    dispatch_expired_index(Expired),
    %% Batched mnesia deletes: one transaction per chunk instead of one
    %% per expired delivery.
    delete_expired_deliveries_batched(Expired),
    %% Fallback: reclaim counter rows that reached zero but linger because
    %% the ack path deletes them dirty (node-local). Runs on every core.
    cleanup_completed_deliveries_everywhere(),
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
                case mnesia:wread({?TAB_MSG_META, Did}) of
                    [_] ->
                        {error, already_exists};
                    [] ->
                        %% Lock order meta -> counter -> rec, matching
                        %% complete_delivery/1.
                        mnesia:write(#bcast_msg_meta{
                            delivery_id = Did,
                            msg_id = MsgId,
                            topic_template = Tpl,
                            counter = Target
                        }),
                        mnesia:write(#bcast_msg_meta_counter{
                            delivery_id = Did,
                            counter = Target
                        }),
                        mnesia:write(Delivery)
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
    %% Test/maintenance reset: every dropped pending entry is an
    %% unconfirmed logical delivery, so it is counted into canceled to keep
    %% the delivery ledger identity closed across the reset.
    Results = [
        try
            route(Shard, {reset_local}, ?SYNC_TIMEOUT_MS)
        catch
            _:_ -> {ok, 0}
        end
     || Shard <- lists:seq(0, ?SHARD_COUNT - 1)
    ],
    maybe_count_canceled(lists:sum([Dropped || {ok, Dropped} <- Results])),
    ok.

%% Sum of the queued/inflight gauges over the shards this node owns
%% (index_owner processes exist only on core nodes; each core runs all
%% ?SHARD_COUNT processes but only its shard_owner subset is active).
%% Replicants and dormant shards contribute {0, 0}, so a cluster sum()
%% over the metrics endpoint stays correct. The per-shard calls run
%% concurrently: a sequential fold with a per-shard sync timeout could
%% take ?SHARD_COUNT * timeout worst case and outlive the /metrics API
%% timeout when one shard's mailbox is congested.
-spec gauge_sample() -> {non_neg_integer(), non_neg_integer()}.
gauge_sample() ->
    Shards = [
        Shard
     || Shard <- lists:seq(0, ?SHARD_COUNT - 1),
        shard_owner(Shard) =:= node()
    ],
    %% All requests in flight concurrently; waiting them out in order
    %% bounds the total latency by one timeout instead of one per shard.
    ReqIds = [gen_server:send_request(shard_name(Shard), {sample_local}) || Shard <- Shards],
    lists:foldl(
        fun(ReqId, {QueuedAcc, InflightAcc}) ->
            case gen_server:wait_response(ReqId, ?SYNC_TIMEOUT_MS) of
                {reply, {Queued, Inflight}} ->
                    {QueuedAcc + Queued, InflightAcc + Inflight};
                _ ->
                    {QueuedAcc, InflightAcc}
            end
        end,
        {0, 0},
        ReqIds
    ).

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
        {reply, {ok, -Delta}, State2}
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
handle_call({claim, Entries, Origin}, _From, State = #{active := true}) ->
    {Results, State2} = lists:mapfoldl(
        fun(E, St) ->
            {R, St2} = claim_one(E, St),
            {{maps:get(clientid, E), R}, St2}
        end,
        State,
        Entries
    ),
    State3 = lists:foldl(
        fun
            ({{ClientId, {ok, _}}, E}, St) ->
                Key = {maps:get(product_key, E), ClientId},
                St#{holders => maps:put(Key, Origin, maps:get(holders, St))};
            (_, St) ->
                St
        end,
        State2,
        lists:zip(Results, Entries)
    ),
    {reply, Results, State3};
handle_call({ack_index, Acks}, _From, State = #{active := true}) ->
    try
        {Results, {State2, Delta}} = lists:mapfoldl(
            fun(Ack, {St, D}) ->
                {R, St2, D2} = ack_one_index(Ack, St),
                St3 =
                    case R of
                        {counted, _RemQueued} ->
                            %% Batch the ack in ack_buf; the flush applies
                            %% the marker and the counter decrement as two
                            %% lock-free dirty writes instead of one rlog
                            %% write per ack.
                            buffer_ack_decrement(Ack, St2);
                        not_found ->
                            St2
                    end,
                {R, {St3, D + D2}}
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
handle_call({release_claim, PK, DN, Did}, _From, State = #{active := true}) ->
    {reply, ok, release_claim_local(PK, DN, Did, State)};
handle_call({release_client_claims, PK, DN, Tag}, _From, State = #{active := true}) ->
    {reply, ok, release_client_claims_local(PK, DN, Tag, State)};
handle_call({release_client_claims_batch, Entries}, _From, State = #{active := true}) ->
    %% Batched tag release with a synchronous reply: used by release-then-
    %% restage recovery paths that must not race a new claim round against
    %% a still-inflight release cast.
    State2 = lists:foldl(
        fun({PK, DN, Tag}, St) -> release_client_claims_local(PK, DN, Tag, St) end,
        State,
        Entries
    ),
    {reply, ok, State2};
handle_call({admit, PK, DNs}, _From, State = #{active := true}) ->
    {Reply, State2} = safe_state(fun() -> admit_local(PK, DNs, State) end, State),
    {reply, Reply, State2};
handle_call({release_admit, PK, DNs}, _From, State = #{active := true}) ->
    {Reply, State2} = safe_state(fun() -> {ok, dispatch_release(PK, DNs, State)} end, State),
    {reply, Reply, State2};
handle_call({admit_devices, PK, DNs, Max}, _From, State = #{active := true}) ->
    case check_devices_local(PK, DNs, Max, State) of
        [] ->
            State2 = lists:foldl(fun(DN, St) -> reserve_inc(St, {PK, DN}) end, State, DNs),
            {reply, {ok, length(DNs)}, State2};
        Over ->
            {reply, {error, {over_limit, Over}}, State}
    end;
handle_call({rollback_devices, PK, DNs}, _From, State = #{active := true}) ->
    State2 = lists:foldl(
        fun(DN, St) -> element(1, reserve_dec(St, {PK, DN})) end,
        State,
        DNs
    ),
    {reply, ok, State2};
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
    %% Owner node: the authoritative row is updated directly on the hot path
    %% (quota_update/1 owner branch), so this is exact. Remote nodes that
    %% route here read the reconcile-updated row (<=1s lag, management only).
    {reply, pending_count_local(), State};
handle_call({pending_count_for, Key}, _From, State = #{active := true}) ->
    {reply, pending_count_for_local(Key, State), State};
handle_call({delete_delivery_rows, Did, MsgId}, _From, State = #{active := true}) ->
    {reply, safe(fun() -> delete_delivery_rows_local(Did, MsgId) end), State};
handle_call({delete_delivery_rows, DeliveryIds}, _From, State = #{active := true}) ->
    {reply, safe(fun() -> delete_delivery_rows_local(DeliveryIds) end), State};
handle_call({delete_message_meta_rows, ApiId}, _From, State = #{active := true}) ->
    {reply, safe(fun() -> delete_message_meta_rows_local(ApiId) end), State};
handle_call({cleanup_local}, _From, State = #{active := true}) ->
    State2 = cleanup_orphan_index_local(State),
    State2b = reclaim_down_holders(State2),
    State3 = cleanup_stale_reservations(State2b),
    State4 = retry_pending_completions(State3),
    {reply, ok, State4};
handle_call({rebuild_index}, From, State = #{active := true}) ->
    %% A manual rebuild used to run inside this call, so its caller could
    %% rebuild and then look at the result. Keep that contract - answer when the
    %% drive that covers this request reports - but start (or join) the drive in
    %% the coordinator, so this process is not the one doing the work.
    {_Tag, State1} = start_drive(force, State),
    {noreply, add_drive_waiter(From, State1)};
handle_call({activate}, _From, State) ->
    %% No-arg fallback (self-scan): kept for callers that cannot get the
    %% shared projection from the activation leader.
    {Reply, State1} = run_activation(scan_sorted_deliveries_projection(), State),
    {reply, Reply, State1};
handle_call({activate, Proj}, _From, State) ->
    %% The activation leader scans + sorts bcast_msg once and hands
    %% every shard the same sorted projection.
    {Reply, State1} = run_activation(Proj, State),
    {reply, Reply, State1};
handle_call({activate, Proj, Completed}, _From, State) ->
    %% Drive variant: hand back the ids completed so far so the leader's
    %% later shards skip deliveries already reconciled in this drive.
    {Reply, State1, Completed1} = run_activation(Proj, State, Completed),
    {reply, {activated, Reply, Completed1}, State1};
handle_call({shard_status}, _From, State) ->
    {reply, {maps:get(active, State), total_pending(State), total_reserves(State)}, State};
handle_call({drive_count}, _From, State) ->
    %% The drive coordinator asks this of every partition that is already
    %% serving, so the closing recount counts its pending budget (reservations
    %% included) without loading it.
    {reply, total_pending(State) + total_reserves(State), State};
handle_call({reset_local}, _From, State = #{shard := 0}) ->
    %% Only the quota owner's shard 0 resets the global counter (the table
    %% lives there); a peer core's shard 0 must not clobber it.
    case is_owner() of
        true -> true = ets:insert(?TAB_QUOTA_ETS, {global, 0});
        false -> ok
    end,
    %% Every node resets its own local quota shadow and reconcile baseline,
    %% so a reset (which drops all pending) does not leave stale cumulative
    %% deltas that later reconcile ticks would re-ship to the zeroed owner row.
    reset_quota_local(),
    {reply, {ok, total_pending(State)}, reset_state(State#{quota_last_synced => 0})};
handle_call({reset_local}, _From, State) ->
    {reply, {ok, total_pending(State)}, reset_state(State)};
handle_call({sample_local}, _From, State) ->
    {reply, sample_state(State), State};
%% Dormant (not yet the owner, or takeover in progress): fail calls so
%% callers retry; reads degrade to empty so management stays responsive.
handle_call({admit, _PK, _DNs}, _From, State) ->
    {reply, ok, State};
handle_call({admit_devices, _PK, _DNs, _Max}, _From, State) ->
    %% Dormant shard: accept without reserving, as the old check/reserve pair
    %% did (a dormant shard has nothing to enforce).
    {reply, {ok, 0}, State};
handle_call({rollback_devices, _PK, _DNs}, _From, State) ->
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

handle_cast({release_batch, Releases}, State = #{active := true}) ->
    %% Batched async release (one cast per shard instead of one per
    %% release): idempotent per entry, claim lease is the backstop.
    State2 = lists:foldl(
        fun
            ({claim, PK, DN, Did}, St) ->
                release_claim_local(PK, DN, Did, St);
            ({tag, PK, DN, Tag}, St) ->
                release_client_claims_local(PK, DN, Tag, St);
            %% A peer that has not been upgraded yet still sends the nested
            %% shape - the very shape whose fold clause crashed its own index
            %% shards. Release it here instead of crashing this shard, and
            %% never crash on a shape this build does not know.
            ({claim, {PK, DN, Did}}, St) ->
                release_claim_local(PK, DN, Did, St);
            ({tag, {PK, DN, Tag}}, St) ->
                release_client_claims_local(PK, DN, Tag, St);
            (Other, St) ->
                ?SLOG(warning, #{msg => "bcast_release_batch_unknown_entry", entry => Other}),
                St
        end,
        State,
        Releases
    ),
    {noreply, State2};
handle_cast({activate_shard, Shard}, State = #{active := true, shard := 0}) when
    Shard =/= 0
->
    %% A dormant partition asked for a targeted re-activation of its own slice
    %% (e.g. after a crash restart). Only the activation leader scans and hands
    %% out the projection, and it does that in a drive coordinator - the request
    %% starts a targeted pass that loads this partition and nothing else, so the
    %% scan does not run in this process and the authoritative quota row is not
    %% re-based. Repeats coalesce into the drive already in flight.
    %% The peer is up (or coming up) and asking for its own partition, so it is
    %% no longer a peer to skip: forget the backoff entry and let this drive
    %% probe it again.
    State1 = forget_bad_peer(Shard, State),
    {noreply,
        case is_owner() of
            true -> start_and_keep(targeted, #{target => Shard}, State1);
            false -> State1
        end};
handle_cast({activate_shard, _Shard}, State) ->
    {noreply, State};
handle_cast({quota_baseline_sync, Epoch}, State = #{shard := 0}) ->
    %% Post-drive recount: the leader recomputed the authoritative quota row
    %% from live shard pendings, which already includes every entry whose
    %% delta is still unshipped in this node's local shadow. Advance the
    %% baseline and adopt the drive epoch so the reconcile loop neither
    %% re-ships those deltas nor gets them accepted twice.
    {noreply, State#{quota_last_synced => local_quota_total(), quota_epoch => Epoch}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(maybe_activate, State = #{active := false, shard := 0}) ->
    {noreply, drive_and_apply(ensure, State)};
handle_info({drive_result, Pid, Result}, State = #{shard := 0}) ->
    %% The coordinator reports once and exits. Only the drive this shard is
    %% waiting for is applied, and only while this shard still owns partition 0:
    %% a partition that lost ownership mid-drive must not be re-activated by a
    %% result that was collected before it.
    case drive_flight(Pid, State) of
        {ok, Flight} ->
            State1 = drop_drive_flight(State),
            State2 =
                case is_owner() of
                    true ->
                        apply_drive_result(maps:get(mode, Flight), Flight, Result, State1);
                    false ->
                        ?SLOG(warning, #{
                            msg => "bcast_index_drive_abandoned",
                            node => node(),
                            shard => 0,
                            reason => lost_partition
                        }),
                        State1
                end,
            {noreply, reply_drive_waiters(State2)};
        error ->
            {noreply, State}
    end;
handle_info({'DOWN', Ref, process, Pid, Reason}, State = #{shard := 0}) ->
    %% A coordinator that exited without reporting (it crashed, or it was
    %% killed) is a failed pass. The result arrives before this monitor message
    %% whenever it exists, so a drive still in flight here is one that never
    %% reported.
    case drive_flight(Pid, State) of
        {ok, Flight = #{ref := Ref, mode := Mode}} ->
            State1 =
                apply_drive_result(
                    Mode,
                    Flight,
                    #{error => {exit, Reason, []}},
                    drop_drive_flight(State)
                ),
            {noreply, reply_drive_waiters(State1)};
        _ ->
            {noreply, State}
    end;
handle_info(maybe_activate, State = #{active := false, shard := Shard}) when
    Shard =/= 0
->
    case shard_owner(Shard) =:= node() of
        true ->
            %% This node owns the partition but the shard is dormant
            %% (crash restart): ask the activation leader for a targeted
            %% rebuild of this shard's partition only.
            request_partition_activation(Shard);
        false ->
            ok
    end,
    erlang:send_after(?ACTIVATE_POLL_MS, self(), maybe_activate),
    {noreply, State};
handle_info(maybe_activate, State) ->
    %% An active shard must give up its partition once it is no longer the
    %% owner. Ownership is derived from the live core set (shard_owner/1), so
    %% a core joining or leaving remaps most shards; without this check the
    %% former owner keeps an active shard whose index stops being updated and
    %% is never reclaimed, and when ownership comes back - another core
    %% leaving, or a partition healing - that stale index serves live traffic
    %% again: entries appended while it was not the owner are missing, and
    %% acknowledgements it already applied are counted a second time, which
    %% decrements the remaining-ack count twice and can complete a delivery
    %% while never-acked devices no longer hold an entry.
    Shard = maps:get(shard, State),
    case shard_owner(Shard) =:= node() of
        true ->
            case Shard =:= 0 andalso maps:get(activation_pending, State, false) of
                true ->
                    %% The last drive left activation work this node can still
                    %% do: retry it while this shard owns its own partition.
                    {noreply, drive_and_apply(ensure, State)};
                false ->
                    erlang:send_after(?ACTIVATE_POLL_MS, self(), maybe_activate),
                    {noreply, State}
            end;
        false ->
            Owner = shard_owner(Shard),
            ?SLOG(warning, #{
                msg => "bcast_index_owner_deactivated",
                node => node(),
                shard => Shard,
                owner => Owner
            }),
            %% Dropping the heap index is safe: it is a projection of the
            %% committed rows, and the next activation rebuilds it. Only
            %% uncommitted admission reservations are lost, and the periodic
            %% quota recount reclaims those. Re-arm the poll: the dormant
            %% clauses above re-activate this shard if the node becomes the
            %% owner again.
            erlang:send_after(?ACTIVATE_POLL_MS, self(), maybe_activate),
            {noreply, (drop_activation_mark(reset_state(State)))#{active => false}}
    end;
handle_info(
    quota_sync, State = #{shard := 0, quota_last_synced := Last, quota_epoch := Epoch}
) ->
    %% Ship this node's local quota delta increment to the owner, then re-arm.
    {NewLast, NewEpoch} = quota_sync(Last, Epoch),
    erlang:send_after(?QUOTA_SYNC_MS, self(), quota_sync),
    {noreply, State#{quota_last_synced => NewLast, quota_epoch => NewEpoch}};
handle_info(
    quota_recount, State = #{shard := 0, quota_recount_running := Running}
) ->
    erlang:send_after(?QUOTA_RECOUNT_MS, self(), quota_recount),
    %% Run the recount in a throwaway process: the probes are synchronous
    %% gen_server calls that can queue behind a busy shard's mailbox, and
    %% shard 0 must not stall its own claim/ack partition for them. At most
    %% one recount is in flight so a slow probe round cannot pile up.
    case is_owner() andalso not Running of
        true ->
            Parent = self(),
            _ = spawn(fun() ->
                _ = safe(fun() -> periodic_recount() end),
                Parent ! quota_recount_done
            end),
            {noreply, State#{quota_recount_running => true}};
        false ->
            {noreply, State}
    end;
handle_info(quota_recount_done, State = #{shard := 0}) ->
    {noreply, State#{quota_recount_running => false}};
handle_info(maybe_gc, State) ->
    {noreply, maybe_fullsweep(State)};
handle_info(ack_flush, State) ->
    {noreply, flush_ack_decrements(State#{ack_flush_ref => undefined})};
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

%% Start one activation drive: a short-lived coordinator process does the
%% orchestrating (probe, scan, dispatch), shard 0 does the closing bookkeeping
%% and loads its own slice like any other partition (see start_drive/2 and
%% coordinator_entry/2). Returns the state to carry on with; the drive reports
%% back with a {drive_result, Pid, Result} message.
%%
%% The quota owner's shard 0 keeps polling afterwards, so a drive that could not
%% finish is simply retried by the next poll.
drive_and_apply(Mode, State) ->
    %% The tag (ok / already_running / not_owner) is not interesting to these
    %% callers: the poll comes back anyway, a targeted request only needs the
    %% drive to be running, and a manual rebuild answers ok for all three.
    start_and_keep(Mode, #{}, State).

start_and_keep(Mode, Extra, State) ->
    {_Tag, State1} = start_drive(Mode, Extra, State),
    State1.

%% The drive this shard is waiting for, if the reporting process is the one it
%% recorded. Everything else (a late report from a drive that was superseded, a
%% DOWN for a process that is not the drive) is ignored.
drive_flight(Pid, State) ->
    case maps:get(drive_in_flight, State, undefined) of
        #{pid := Pid} = Flight -> {ok, Flight};
        _ -> error
    end.

drop_drive_flight(State) ->
    State#{drive_in_flight => undefined}.

add_drive_waiter(From, State) ->
    State#{drive_waiters => [From | maps:get(drive_waiters, State, [])]}.

%% Answer the rebuilds that were waiting for this drive. They answer ok: a
%% rebuild that reached every partition it could is a success for the caller,
%% and the partitions it could not reach report themselves.
reply_drive_waiters(State) ->
    lists:foreach(
        fun(From) -> gen_server:reply(From, ok) end,
        maps:get(drive_waiters, State, [])
    ),
    State#{drive_waiters => []}.

%% Start a drive unless one is already running. Held in the shard's state so
%% repeats (the poll, a targeted activation request, a manual rebuild) coalesce
%% into the drive that is already in flight instead of piling up behind it.
start_drive(Mode, State = #{shard := 0}) ->
    start_drive(Mode, #{}, State).

start_drive(Mode, Extra, State = #{shard := 0}) ->
    case maps:get(drive_in_flight, State, undefined) of
        undefined ->
            start_drive_now(Mode, Extra, State);
        #{mode := Running} ->
            ?SLOG(info, #{
                msg => "bcast_index_drive_already_running",
                node => node(),
                mode => Mode,
                running => Running
            }),
            {already_running, State}
    end.

start_drive_now(Mode, Extra, State) ->
    case is_owner() of
        true ->
            %% The quota table dies with shard 0 (its creator): it is recreated
            %% here and filled by the drive's closing recount. It has to be
            %% created by *this* process - an ETS table dies with its owner.
            _ = ensure_quota_table(),
            Parent = self(),
            Snapshot =
                maps:merge(
                    #{
                        mode => Mode,
                        shard => maps:get(shard, State),
                        active => maps:get(active, State),
                        bad_peers => maps:get(bad_peers, State, #{})
                    },
                    Extra
                ),
            {Pid, Ref} = spawn_monitor(fun() -> coordinator_entry(Snapshot, Parent) end),
            {ok, State#{
                drive_in_flight => #{
                    pid => Pid,
                    ref => Ref,
                    mode => Mode,
                    started_at => erlang:system_time(millisecond)
                }
            }};
        false ->
            %% Not the owner (yet): stay on the owner poll so this node can
            %% take the leadership over when the current owner leaves. Nothing
            %% else can wake this shard: a dormant shard 0 ignores the
            %% {activate_shard, ...} casts the partitions send it, and the drive
            %% is the only thing that (re)activates partitions.
            erlang:send_after(?OWNER_POLL_MS, self(), maybe_activate),
            {not_owner, State}
    end.

%% The drive's outcome, applied to shard 0's own state. Only this process
%% touches the quota row, the backoff table and the retry bookkeeping.
apply_drive_result(Mode, Flight, Result, State) ->
    case Result of
        #{targeted := _Shard} ->
            targeted_done(Mode, Result, State);
        #{counts := Counts} ->
            activated(Mode, Counts, State);
        #{missing := Missing} = Incomplete ->
            drive_incomplete(Mode, Flight, Incomplete, Missing, State);
        #{error := Error} ->
            drive_failed(Mode, Error, State)
    end.

%% A partition that asked for its own slice is not a drive: no other partition
%% was touched, nothing is re-counted (the authoritative row never stopped
%% counting those entries - they live in the durable log, only this partition's
%% heap copy was lost), and there is nothing to retry: the partition keeps
%% asking until it is serving.
targeted_done(Mode, Result, State) ->
    ?SLOG(debug, #{
        msg => "bcast_index_targeted_drive_done",
        node => node(),
        mode => Mode,
        shard => maps:get(targeted, Result),
        loaded => maps:get(loaded, Result, false)
    }),
    State.

%% Every partition answered and every partition that needed loading was loaded:
%% close the drive by re-basing the authoritative quota row on the counts the
%% drive collected, and advance each node's reconcile baseline so the deltas
%% already included in that total are not shipped on top of it.
activated(Mode, Counts, State) ->
    Epoch = recount_quota(Counts),
    sync_quota_baselines(Epoch),
    case Mode of
        ensure ->
            ?SLOG(info, #{
                msg => "bcast_index_owner_activated",
                node => node(),
                shard_count => ?SHARD_COUNT
            }),
            %% Keep polling after activation so the ownership check runs for
            %% this shard too.
            erlang:send_after(?ACTIVATE_POLL_MS, self(), maybe_activate);
        force ->
            ok
    end,
    drop_activation_mark(State#{active => true}).

%% Keep the partitions this drive did rebuild - shard 0's own included - and
%% serve them: the alternative, staying dormant until every core answers,
%% withholds this partition's devices for the whole time a peer runs a build
%% without the status call or has its plugin uninstalled. The missing partitions
%% are named so the operator can see which node the drive is waiting for; the
%% unreachable one's own shard asks this leader for a targeted activation once it
%% is up, and the periodic recount re-reads the quota once every partition
%% answers again.
drive_incomplete(Mode, Flight, #{probes := Probes}, Missing, State) ->
    State1 = note_probe_results(State, Probes),
    case Mode of
        force ->
            %% A manual rebuild is one shot: arming the retry here would leave
            %% shard 0 re-driving - and re-probing every unreachable peer with
            %% its call timeout - for as long as that peer stays unreachable.
            log_missing_shards(force, Missing, State1),
            mark_incomplete(force, State1, Missing);
        ensure ->
            %% The quota row has to exist before this shard serves anything:
            %% the reserve and release paths update_counter it unconditionally,
            %% and the closing recount is exactly what did not run here.
            _ = ensure_quota_row(),
            FastUntil =
                case maps:get(activation_fast_until, State1, undefined) of
                    undefined -> erlang:system_time(millisecond) + ?ACTIVATION_FAST_MS;
                    Until -> Until
                end,
            %% Never retry faster than the drive itself took. A peer that is
            %% hung rather than merely old costs one probe timeout per round,
            %% and retrying every 500ms would then keep shard 0 busy most of the
            %% time with re-probing, which is the very thing the retry exists to
            %% avoid for the partitions it does serve.
            RetryMs = activation_retry_ms(FastUntil),
            RetryMs2 = max(RetryMs, erlang:system_time(millisecond) - maps:get(started_at, Flight)),
            State2 = incomplete_state(State1, Probes, Missing),
            ?SLOG(warning, #{
                msg => "bcast_index_activation_incomplete",
                node => node(),
                shard => 0,
                missing => maps:get(activation_missing, State2, []),
                retry_ms => RetryMs2
            }),
            case maps:get(activation_pending, State2, false) of
                true ->
                    erlang:send_after(RetryMs2, self(), maybe_activate),
                    State2#{activation_fast_until => FastUntil};
                false ->
                    %% Nothing here is work this node can do: the partitions
                    %% that did not answer in time ask for their own activation
                    %% once they are up, and the periodic recount re-bases the
                    %% quota row. Retrying would only re-complete a whole drive
                    %% - and re-base that row - at an arbitrary later moment.
                    drop_activation_mark(State2)
            end
    end.

%% The coordinator itself failed (it crashed, or the shard lost the partition
%% while it ran). Treat the pass as failed: the automatic drive comes back on
%% the fast poll, the manual rebuild reports what it could and stops.
drive_failed(ensure, Error, State) ->
    ?SLOG(warning, #{msg => "bcast_index_drive_failed", node => node(), reason => Error}),
    erlang:send_after(?OWNER_POLL_MS, self(), maybe_activate),
    State;
drive_failed(force, Error, State) ->
    ?SLOG(warning, #{msg => "bcast_index_drive_failed", node => node(), reason => Error}),
    mark_incomplete(force, State, []).

%% A drive that could not finish is retried at ?OWNER_POLL_MS until
%% ?ACTIVATION_FAST_MS after it first stopped: what a drive misses at boot is a
%% sibling the supervisor has not started yet, and that resolves in
%% milliseconds. A partition still missing after that does not come back on its
%% own, so the retry drops to the slow poll - the same cadence a dormant
%% non-leader shard uses to ask for its own partition.
activation_retry_ms(FastUntil) ->
    case erlang:system_time(millisecond) < FastUntil of
        true -> ?OWNER_POLL_MS;
        false -> ?ACTIVATE_POLL_MS
    end.

%% Drive bookkeeping belongs to a partition that is being brought up: drop it
%% when the shard gives the partition up (ownership moved) or finishes.
drop_activation_mark(State) ->
    maps:without([activation_missing, activation_pending, activation_fast_until], State).

%% Coordinated (re)activation.
%%
%% The orchestrating runs in a short-lived coordinator process, not inside the
%% quota owner's shard 0: shard 0 has to keep answering the claims and
%% acknowledgements of its own partition, and probing 47 partitions, scanning
%% the delivery table and dispatching the slices is work that has nothing to do
%% with them. The coordinator reads a snapshot of shard 0 (never its state),
%% calls into the partitions - shard 0's own included, which loads its slice like
%% any other - and hands the collected counts back; shard 0 then applies them in
%% apply_drive_result/4.
%%
%% ensure mode (automatic drive after boot / leader restart / owner
%% transition): partitions that are already ACTIVE keep their heap state - a
%% leader restart must not reset healthy partitions (their in-flight claims and
%% buffered ack decrements would be lost cluster-wide). Only dormant partitions
%% (fresh boot, crash restart) are rebuilt from the durable log.
%%
%% A drive that reaches a partition it cannot drive at all stops there: that
%% partition stays dormant until it asks for a targeted activation itself, while
%% the partitions this drive did rebuild - shard 0's own included - stay active.
%% Waiting for the whole cluster to be reachable instead would withhold this
%% leader's own partition for the entire time a peer runs an older build or has
%% its plugin uninstalled.
%%
%% The partitions are probed before the projection is scanned, and the scan only
%% happens when some partition actually has to be rebuilt from it. Retrying a
%% drive that is missing a partition it cannot reach therefore costs the probes
%% and no scan, which is what keeps that retry affordable for the length of an
%% upgrade. The probes run together, with a probe-sized timeout, and a peer that
%% timed out is skipped until its backoff expires, so a peer that stays
%% unreachable costs one wait once instead of one wait per round - manual
%% rebuilds included, which used to drive every partition blindly.
%%
%% force mode (manual rebuild_index): every partition is rebuilt.
%%
%% Rebuilt entries are never counted into the quota accounting per entry.
%% Instead the authoritative quota row is RECOUNTED from live per-partition
%% pendings once every partition is active (the row's ETS table dies with
%% shard 0, so a leader restart always loses it), and every node's quota shadow
%% baseline is then advanced so already-counted local deltas are not shipped
%% again on top of the recount.

%% Entry point of the coordinator process. It reports its outcome to shard 0 and
%% exits: an exit without a report (a crash, a kill) is how shard 0 learns that
%% this pass failed, so the report goes out first and the monitor covers the
%% rest.
coordinator_entry(Snapshot, Parent) ->
    Result =
        try coordinator_drive(Snapshot) of
            R -> R
        catch
            Class:Reason:Stacktrace -> #{error => {Class, Reason, Stacktrace}}
        end,
    Parent ! {drive_result, self(), Result},
    ok.

coordinator_drive(#{mode := targeted} = Snapshot) ->
    targeted_drive(Snapshot);
coordinator_drive(#{mode := Mode, shard := MyShard, bad_peers := BadPeers} = Snapshot) ->
    Probes = probe_partitions(MyShard, BadPeers),
    case {needs_drive(Snapshot, Mode, Probes), unreachable(Probes)} of
        {false, []} ->
            %% Nothing to rebuild: every partition answers and is already
            %% active. The drive still has to close, because the closing recount
            %% is what creates and re-bases the authoritative quota row (it died
            %% with shard 0) - and the probes already carry every count, so it
            %% closes without scanning the authoritative table.
            case own_drive_count(MyShard) of
                SelfCount when is_integer(SelfCount) ->
                    #{probes => Probes, counts => counts_from_probes(MyShard, SelfCount, Probes)};
                _ ->
                    #{probes => Probes, missing => [MyShard]}
            end;
        {false, Missing} ->
            %% Nothing to rebuild from the projection, and some partition cannot
            %% be probed at all: the recount must not run from a partial sum, so
            %% report the missing partitions without scanning the table.
            #{probes => Probes, missing => Missing};
        {true, _} ->
            %% Scan the authoritative bcast_msg table ONCE (projection-only
            %% match spec, no full #bcast_msg{} records) and sort once by
            %% created_at; every partition is then handed its slice of that
            %% sorted scan instead of walking the whole list to keep its 1/48
            %% (that filter is O(partitions x list)).
            Proj = scan_sorted_deliveries_projection(),
            %% Deliveries committed by older builds (or before the meta table
            %% existed) have no bcast_msg_meta row, and the ack/claim hot paths
            %% only read that small row. The repair is a batched, idempotent
            %% write and belongs to the drive, not to any one partition's load.
            _ = backfill_meta_from_projection(Proj),
            Groups = group_projection_by_shard(Proj),
            {Counts, Missing, Completed} = dispatch_partitions(Groups, Mode, Snapshot, Probes),
            ok = maybe_log_heal_cap(Completed),
            case Missing of
                [] -> #{probes => Probes, counts => Counts};
                _ -> #{probes => Probes, missing => Missing}
            end
    end.

%% A partition reported that its heap copy is gone (a crash restart) and asked
%% for its slice. That is not a drive: no other partition is loaded, nothing is
%% counted, and no retry is armed - the partition keeps asking by itself. The
%% scan happens in this process rather than in shard 0, which is the point.
targeted_drive(#{shard := MyShard, target := Target, bad_peers := BadPeers}) ->
    Probes = probe_partitions(MyShard, BadPeers),
    Base = #{probes => Probes, targeted => Target},
    case maps:get(Target, Probes, undefined) of
        {active, _, _} ->
            %% Already serving: the request queued behind the drive that did it.
            Base#{loaded => false};
        Status when Status =:= dormant orelse Status =:= undefined ->
            Groups = group_projection_by_shard(scan_sorted_deliveries_projection()),
            case load_partition(Target, maps:get(Target, Groups, []), #{}) of
                {Count, _Completed} when is_integer(Count) -> Base#{loaded => true};
                {{error, _}, _} -> Base#{loaded => false}
            end;
        {error, _} = Error ->
            Base#{loaded => false, error => Error}
    end.

%% Drive every partition in shard order, threading the set of deliveries the
%% drive has already completed (a later partition skips them instead of
%% resurrecting them). The dispatch stays serial: the slice rides an
%% inter-process message, and sending 48 of them at once would multiply the peak
%% memory by the partition count.
dispatch_partitions(Groups, Mode, Snapshot, Probes) ->
    MyShard = maps:get(shard, Snapshot),
    SelfUp = maps:get(active, Snapshot),
    {CountsRev, MissingRev, Completed} = lists:foldl(
        fun(Shard, {CAcc, MAcc, Done}) ->
            Slice = maps:get(Shard, Groups, []),
            Own = Shard =:= MyShard,
            case dispatch_partition(Shard, Slice, Mode, Own andalso SelfUp, Probes, Done) of
                {Count, Done1} when is_integer(Count) ->
                    {[Count | CAcc], MAcc, Done1};
                {{error, _} = Error, Done1} ->
                    {[Error | CAcc], [Shard | MAcc], Done1}
            end
        end,
        {[], [], #{}},
        lists:seq(0, ?SHARD_COUNT - 1)
    ),
    {lists:reverse(CountsRev), lists:reverse(MissingRev), Completed}.

%% A partition that is already serving is not re-loaded: it reports what it
%% holds for the closing recount. This is decided from shard 0's own state for
%% shard 0 - a probe of a busy partition can time out, and believing that would
%% reset a live partition - and from the probe for the siblings. A manual
%% rebuild is the exception: force means every partition is rebuilt, its own
%% included.
dispatch_partition(Shard, _Slice, ensure, true, _Probes, Completed) ->
    case own_drive_count(Shard) of
        Count when is_integer(Count) -> {Count, Completed};
        _ -> {{error, sibling_unavailable}, Completed}
    end;
dispatch_partition(Shard, Slice, ensure, _SelfUp, Probes, Completed) ->
    case maps:get(Shard, Probes, undefined) of
        undefined ->
            %% Our own partition while the drive started dormant (it was not
            %% probed): it needs loading like any other dormant partition.
            load_partition(Shard, Slice, Completed);
        {active, Pending, Reserves} ->
            %% Reservations are part of this partition's pending budget: the
            %% drive hands the total to the closing recount, so leaving them out
            %% would erase every not-yet-promoted admission reservation.
            {Pending + Reserves, Completed};
        dormant ->
            load_partition(Shard, Slice, Completed);
        {error, _} = Error ->
            {Error, Completed}
    end;
dispatch_partition(Shard, Slice, force, _SelfUp, Probes, Completed) ->
    %% A manual rebuild drives every partition it can reach - that is what force
    %% means - but a partition the probe could not reach is not driven: waiting
    %% on it is exactly the call timeout per unreachable peer this probe exists
    %% to remove. It is reported as missing instead, and the peer's own
    %% activation request rebuilds it once it is up.
    case maps:get(Shard, Probes, undefined) of
        {error, _} = Error -> {Error, Completed};
        _ -> load_partition(Shard, Slice, Completed)
    end.

%% Hand one partition its slice: it loads it in its own process and answers with
%% the count for the closing recount plus the deliveries it completed.
load_partition(Shard, Slice, Completed) ->
    try route(Shard, {activate, Slice, Completed}, ?SYNC_TIMEOUT_MS) of
        {activated, Count, Done1} when is_integer(Count), is_map(Done1) ->
            {Count, Done1};
        {error, _} ->
            {{error, sibling_unavailable}, Completed};
        _ ->
            {{error, sibling_unavailable}, Completed}
    catch
        _:_ -> {{error, sibling_unavailable}, Completed}
    end.

%% What our own partition holds: pending plus reservations, as the closing
%% recount needs it. Read with the sync budget rather than the probe budget,
%% because this is not a liveness question - the partition is this node's, and
%% this node is running the drive.
own_drive_count(Shard) ->
    try route(Shard, {drive_count}, ?SYNC_TIMEOUT_MS) of
        Count when is_integer(Count) -> Count;
        _ -> {error, sibling_unavailable}
    catch
        _:_ -> {error, sibling_unavailable}
    end.

%% One pass over the projection groups it for every partition: a delivery whose
%% devices live in several partitions is handed to each of them, and to no one
%% else. The row keeps its whole device list - the load decides which of them
%% are its own, and it also decides from that list whether every device of the
%% delivery has acknowledged it (a slice-local list would look fully
%% acknowledged the moment its own device was, which finishes a delivery the
%% other partitions still hold messages for). Order is kept (rows in
%% created_at/msg_id order, devices as stored) - the rebuilt per-device FIFO
%% depends on it.
group_projection_by_shard(Proj) ->
    Groups = lists:foldl(fun group_projection_row/2, #{}, Proj),
    maps:map(fun(_Shard, RowsRev) -> lists:reverse(RowsRev) end, Groups).

group_projection_row(
    Row = {_Did, _MsgId, PK, _Tpl, _Target, _Counter, DeviceNames, _CreatedAt}, Acc
) ->
    maps:fold(
        fun(Shard, _Owned, A) ->
            maps:update_with(Shard, fun(Rows) -> [Row | Rows] end, [Row], A)
        end,
        Acc,
        devices_by_shard(PK, DeviceNames)
    ).

devices_by_shard(ProductKey, DeviceNames) ->
    lists:foldl(
        fun(DN, Acc) ->
            maps:update_with(shard_of({ProductKey, DN}), fun(L) -> [DN | L] end, [DN], Acc)
        end,
        #{},
        DeviceNames
    ).

%% Counts of every partition in shard order, taken from the probes (ours comes
%% from our own partition): used to close a drive that had nothing to rebuild.
%% Every probed partition answered active in that case - the dormant and the
%% unreachable ones are what needs_drive/3 and unreachable/1 filter out.
counts_from_probes(MyShard, SelfCount, Probes) ->
    [
        case Shard of
            MyShard ->
                SelfCount;
            _ ->
                {active, Pending, Reserves} = maps:get(Shard, Probes),
                Pending + Reserves
        end
     || Shard <- lists:seq(0, ?SHARD_COUNT - 1)
    ].

%% The automatic drive logs the partitions it could not close in
%% drive_and_apply/1, once it knows whether it will retry. A manual rebuild
%% answers its caller and stops, so without this the operator would have no
%% record of the partitions that one-shot rebuild did not reach - including the
%% ones it skipped because their probe had timed out, which a later rebuild
%% picks up again once they are held back no longer.
log_missing_shards(ensure, _Missing, _State) ->
    ok;
log_missing_shards(force, Missing, State) ->
    ?SLOG(warning, #{
        msg => "bcast_index_rebuild_incomplete",
        node => node(),
        shard => 0,
        missing => Missing,
        held_back => maps:keys(maps:get(bad_peers, State, #{}))
    }),
    ok.

mark_incomplete(force, State, _Missing) ->
    maps:remove(activation_missing, State);
mark_incomplete(ensure, State, Missing) ->
    State#{activation_missing => Missing}.

%% Bookkeeping for a drive that could not close: name the partitions the drive
%% could not reach, and decide whether anything is left that retrying this drive
%% could still fix.
%%
%% A partition that did not answer *in time* is not such work. Waiting for it
%% again costs another full probe timeout per round, and it asks for its own
%% activation once it is up (request_partition_activation/1), so the retry buys
%% nothing there - while it does re-complete a whole drive (and re-base the
%% authoritative quota row) whenever that peer happens to come back, at an
%% arbitrary later moment.
%%
%% A partition that answered "dormant" (still needs loading) or failed fast
%% (not started yet, node down) IS such work: the boot race is exactly a run of
%% fast failures, and it must keep retrying at ?OWNER_POLL_MS.
incomplete_state(State, Probes, Missing) ->
    State#{
        activation_missing => Missing,
        activation_pending => (not maps:get(active, State)) orelse pending_work(Probes)
    }.

pending_work(Probes) ->
    lists:any(
        fun
            ({_Shard, {error, peer_timeout}}) -> false;
            ({_Shard, {error, peer_backoff}}) -> false;
            ({_Shard, {error, _}}) -> true;
            ({_Shard, dormant}) -> true;
            ({_Shard, {active, _, _}}) -> false
        end,
        maps:to_list(Probes)
    ).

%% Probe the other partitions, all at once and with the probe-sized timeout.
%%
%% The probes answer in microseconds while a shard is healthy, so they are only
%% expensive when a peer is hung or runs a build that does not answer them at
%% all - and then they cost ?PROBE_TIMEOUT_MS each. Running them together keeps
%% that cost to about one wait instead of one per peer. A peer whose probe
%% already timed out is skipped until its backoff expires (?PEER_BACKOFF_MS):
%% re-waiting on a peer that is stuck - or on a core whose plugin is
%% uninstalled - every round is what made the length of an upgrade visible in
%% the drive coordinator's pass. It reports peer_backoff rather than a fresh
%% timeout so the round still knows that partition did not answer.
%%
%% The leader's own partition is not probed: it is this node's, it is loaded by
%% the same dispatch as any other partition, and its liveness is a local
%% question (see dispatch_partition/6).
probe_partitions(MyShard, Backoff) ->
    Others = [Shard || Shard <- lists:seq(0, ?SHARD_COUNT - 1), Shard =/= MyShard],
    Now = erlang:system_time(millisecond),
    Waited = [Shard || Shard <- Others, maps:get(Shard, Backoff, 0) > Now],
    maps:from_list(
        [{Shard, {error, peer_backoff}} || Shard <- Waited] ++
            parallel_map(fun(Shard) -> {Shard, probe_shard(Shard)} end, Others -- Waited)
    ).

%% Remember the peers that did not answer the probe in time, and forget the ones
%% that answered or whose backoff has run out.
%%
%% Only a timeout is worth holding back: it is the failure that costs another
%% full wait next round, and the peer clears it by asking for its own activation
%% as soon as it is up. A peer that fails fast - a shard whose supervisor has
%% not started it yet - answers within milliseconds, so shutting it out for
%% seconds would only postpone its partition for no saving at all.
note_probe_results(State, Probes) ->
    Now = erlang:system_time(millisecond),
    Prev = maps:get(bad_peers, State, #{}),
    Kept = maps:filter(
        fun(Shard, Until) -> Until > Now andalso not answered(Shard, Probes) end,
        Prev
    ),
    New = maps:fold(
        fun
            (Shard, {error, peer_timeout}, Acc) -> Acc#{Shard => Now + ?PEER_BACKOFF_MS};
            (_Shard, _Result, Acc) -> Acc
        end,
        Kept,
        Probes
    ),
    State#{bad_peers => New}.

answered(Shard, Probes) ->
    case maps:find(Shard, Probes) of
        {ok, {error, peer_timeout}} -> false;
        {ok, {error, peer_backoff}} -> false;
        {ok, _Answered} -> true;
        error -> false
    end.

%% Drop the backoff entry of a peer that asked for its own activation.
forget_bad_peer(Shard, State) ->
    case maps:find(bad_peers, State) of
        {ok, Bad} when map_size(Bad) > 0 -> State#{bad_peers => maps:remove(Shard, Bad)};
        _ -> State
    end.

%% A drive is worth its projection scan only when some partition has to be
%% rebuilt from it: our own (boot / crash restart / manual rebuild) or a
%% sibling that answers dormant. A sibling that cannot answer is not driven
%% from here either way, so a drive whose only remaining work is such a sibling
%% reports it without scanning. Our own partition is decided from the state the
%% snapshot carries - the probe of a busy partition can time out, and believing
%% that would reset a live partition.
needs_drive(_Snapshot, force, _Probes) ->
    true;
needs_drive(Snapshot, ensure, Probes) ->
    (not maps:get(active, Snapshot)) orelse
        lists:any(fun({_Shard, Status}) -> Status =:= dormant end, maps:to_list(Probes)).

%% Partitions this round could not use: probed and it did not answer, or skipped
%% because its probe had already timed out and is still held back.
unreachable(Probes) ->
    [Shard || {Shard, {error, _}} <- maps:to_list(Probes)].

%% Counts of every partition in shard order, taken from the probes: used to
%% close a drive that had nothing to rebuild. Every probed sibling answered
%% active in that case (the dormant and the unreachable ones are what
%% needs_drive/3 and unreachable/1 filter out), and our own partition reports
%% its live pending plus reservations exactly like an active sibling does.
probe_shard(Shard) ->
    try route(Shard, {shard_status}, ?PROBE_TIMEOUT_MS) of
        {true, Pending, Reserves} when is_integer(Pending), is_integer(Reserves) ->
            {active, Pending, Reserves};
        {false, _, _} ->
            dormant;
        _ ->
            {error, sibling_unavailable}
    catch
        %% "Did not answer in time" is not the same as "answered that it cannot
        %% serve": only the former means waiting again would cost another full
        %% timeout, and only the former may be treated as "not worth retrying
        %% right now" by the retry decision below.
        exit:{timeout, _} -> {error, peer_timeout};
        _:_ -> {error, sibling_unavailable}
    end.

%% Owner shard 0 only, run in a spawned process. Self-healing recount: the
%% same probe + recount + baseline-sync as a drive, on a timer so quota drift
%% caused by increments racing a recount is bounded instead of permanent.
%% Read-only probes and two ETS writes; no mnesia transaction, so it does not
%% touch the ack hot path, and shard 0 is not blocked by the probes.
periodic_recount() ->
    case probe_all_shards() of
        {ok, Counts} ->
            Epoch = recount_quota(Counts),
            sync_quota_baselines(Epoch),
            ok;
        {error, {shard_unavailable, Shard}} ->
            %% A shard that cannot be probed (timeout, restart, not yet
            %% activated) has an unknown pending + reserved count. Recounting
            %% from a partial sum would understate the global cap until the
            %% next successful round, so skip this one and retry next tick.
            ?SLOG(debug, #{msg => "bcast_quota_recount_skipped", shard => Shard}),
            ok
    end.

%% Probe every shard for its pending + reserved device counts. Any shard that
%% is not active makes the whole round unusable: counting it as 0 would
%% understate the global cap.
probe_all_shards() ->
    lists:foldl(
        fun
            (_Shard, {error, _} = Error) ->
                Error;
            (Shard, {ok, Acc}) ->
                case probe_shard(Shard) of
                    {active, Pending, Reserves} -> {ok, [Pending + Reserves | Acc]};
                    _ -> {error, {shard_unavailable, Shard}}
                end
        end,
        {ok, []},
        lists:seq(0, ?SHARD_COUNT - 1)
    ).

%% Set the authoritative quota row to the true global pending: the sum of
%% every shard's live pending (probed for active shards, rebuild counts
%% for freshly activated ones). Hot-path increments racing the probes are
%% overwritten (bounded race once per drive; the quota is a soft cap).
recount_quota(Counts) ->
    Total = lists:sum(Counts),
    ensure_quota_table(),
    true = ets:insert(?TAB_QUOTA_ETS, {global, Total}),
    %% Bump the drive epoch so reconcile deltas computed before this recount
    %% are rejected instead of being added on top of Total (they are already
    %% included in Total). Returns the new epoch for the baseline sync.
    Epoch = quota_epoch() + 1,
    true = ets:insert(?TAB_QUOTA_ETS, {epoch, Epoch}),
    Epoch.

%% The recount already includes every entry whose quota delta is still
%% sitting unshipped in a node's local shadow: advance each node's
%% reconcile baseline to its current shadow total so those deltas are not
%% shipped again on top of the recount.
sync_quota_baselines(Epoch) ->
    lists:foreach(
        fun(Node) ->
            case Node =:= node() of
                true ->
                    gen_server:cast(shard_name(0), {quota_baseline_sync, Epoch});
                false ->
                    emqx_rpc:cast(Node, ?MODULE, local_cast, [0, {quota_baseline_sync, Epoch}])
            end
        end,
        emqx_bcast:core_nodes()
    ).

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
    %% Sort by (created_at, msg_id). created_at has second granularity, so
    %% same-second commits tie: break the tie with the msg_id guid, whose
    %% millisecond prefix keeps the rebuilt per-device FIFO in commit
    %% order (an arbitrary tie order scrambles the FIFO after a takeover).
    lists:sort(
        fun({_, Ma, _, _, _, _, _, Ca}, {_, Mb, _, _, _, _, _, Cb}) ->
            {Ca, Ma} =< {Cb, Mb}
        end,
        Rows
    ).

%% Rebuild: derive this shard's partition of the heap index from the
%% authoritative mria delivery table (via the shared sorted projection
%% built by drive_activation), preserving per-device FIFO by created_at.
%% All entries are rebuilt as stored (in-flight claims are at-least-once
%% re-delivered, matching the claim lease expiry semantics). Quota is not
%% counted per entry here: the leader recounts the authoritative row from
%% live shard pendings after the drive. Returns {Count, NewState}.
run_activation(Proj, State) ->
    case activate_partition(Proj, State, #{}) of
        {{error, _} = Error, _State1, _Completed1} -> {Error, State};
        {Count, State1, _Completed1} -> {Count, State1#{active => true}}
    end.

%% Drive variant: threads the set of deliveries already completed earlier in
%% this drive. Their acked markers were deleted by complete_delivery/1, so a
%% later shard must skip them instead of re-indexing them as pending (which
%% would be claimed, dropped after the replication lag, and counted by the
%% closing quota recount).
run_activation(Proj, State, Completed) ->
    case activate_partition(Proj, State, Completed) of
        {{error, _} = Error, _State1, _Completed1} -> {Error, State, Completed};
        {Count, State1, Completed1} -> {Count, State1#{active => true}, Completed1}
    end.

activate_partition(Proj, State, Completed) ->
    try
        %% Flush buffered ack decrements BEFORE the reset: dropping ack_buf
        %% with the reset would strand the meta counter above zero and leak
        %% the delivery rows until TTL expiry.
        State0 = flush_ack_decrements(State#{ack_flush_ref => undefined}),
        State1 = reset_state(State0),
        Shard = maps:get(shard, State),
        {State2, Completed1} = lists:foldl(
            fun(
                {DeliveryId, _MsgId, ProductKey, _TopicTemplate, _TargetAckCount, _Counter,
                    DeviceNames, _CreatedAt},
                {St, Done}
            ) ->
                case maps:is_key(DeliveryId, Done) of
                    true ->
                        %% Completed earlier in this drive: its acked markers
                        %% are already deleted, so indexing it again here would
                        %% resurrect it as pending (claimed, then dropped after
                        %% the replication lag) and bump the closing quota
                        %% recount. Skip it.
                        {St, Done};
                    false ->
                        MyDNs = [
                            DN
                         || DN <- DeviceNames,
                            shard_of({ProductKey, DN}) =:= Shard
                        ],
                        case MyDNs of
                            [] ->
                                {St, Done};
                            _ ->
                                %% Skip devices whose ack was already counted: the
                                %% marker was persisted by the ack flush. Without the
                                %% filter a rebuild resurrects them as pending, the
                                %% redelivered duplicate ack decrements the completion
                                %% counter a second time, and the delivery completes
                                %% early while never-acked devices keep no index
                                %% entry. Deliveries committed by builds without the
                                %% markers have none; their already-acked devices are
                                %% redelivered once (at-least-once).
                                Acked = acked_device_set(DeliveryId),
                                AllAcked =
                                    lists:all(fun(DN) -> maps:is_key(DN, Acked) end, DeviceNames),
                                case AllAcked of
                                    false ->
                                        %% Not fully acked: rebuild only the
                                        %% un-acked devices.
                                        ToAdd = [
                                            DN
                                         || DN <- MyDNs,
                                            not maps:is_key(DN, Acked)
                                        ],
                                        {
                                            append_delivery_entries(
                                                St, ProductKey, DeliveryId, ToAdd
                                            ),
                                            Done
                                        };
                                    true when map_size(Done) >= ?MAX_HEAL_PER_DRIVE ->
                                        %% All target devices acked but the
                                        %% per-drive heal budget is spent: leave
                                        %% the delivery for a later drive or TTL
                                        %% instead of letting the skip set that
                                        %% rides the activation RPCs grow without
                                        %% bound.
                                        {St, Done};
                                    true ->
                                        %% Every target device has a persisted ack
                                        %% marker, so the delivery is finished even
                                        %% though its remaining-ack counter is still
                                        %% positive: the ack flush writes the marker
                                        %% before it decrements the counter, so a
                                        %% shard that died between the two writes left
                                        %% the counter that many acks too high, and no
                                        %% ack is left to drive it to zero.
                                        %%
                                        %% Reconciling from the markers is race-free
                                        %% because markers only ever grow - once every
                                        %% device has one, every ack is durable.
                                        %% Rewriting the counter to the marker-derived
                                        %% remaining instead would double-count the
                                        %% acks of a flush that is between its two
                                        %% writes right now, and could complete the
                                        %% delivery early. Record the id so every later
                                        %% shard in this drive skips it.
                                        case complete_delivery(DeliveryId) of
                                            ok ->
                                                {St, maps:put(DeliveryId, true, Done)};
                                            {error, _} ->
                                                %% Completion aborted after its
                                                %% retries: leave the delivery out
                                                %% of the index and out of the skip
                                                %% set, and remember the id so the
                                                %% periodic cleanup tick retries the
                                                %% completion. A later shard in this
                                                %% drive retries it too, but that
                                                %% does not cover a delivery whose
                                                %% last - or only - owning shard is
                                                %% this one, and the positive counter
                                                %% is not a usable retry marker
                                                %% (cleanup only scans counters
                                                %% =< 0). Re-indexing the already
                                                %% acked devices here would leave
                                                %% ghost entries for a delivery that
                                                %% another shard completes.
                                                {add_pending_completion(St, DeliveryId), Done}
                                        end
                                end
                        end
                end
            end,
            {State1, Completed},
            Proj
        ),
        Count = maps:size(maps:get(dids, State2)),
        ?SLOG(info, #{
            msg => "bcast_index_shard_rebuilt",
            shard => Shard,
            pending => Count
        }),
        {Count, State2, Completed1}
    catch
        Error:Reason -> {{error, {Error, Reason}}, State, Completed}
    end.

%% Re-index the given devices for DeliveryId: the un-acked ones on a normal
%% rebuild, or every target device when a completion transaction aborted.
append_delivery_entries(St, ProductKey, DeliveryId, DNs) ->
    lists:foldl(
        fun(DN, St2) ->
            {St3, _Delta} = append_entry(St2, {ProductKey, DN}, DeliveryId),
            St3
        end,
        St,
        DNs
    ).

%% Report once per drive when the heal budget is spent. Only a systemic
%% ack-counter drift can reach this cap, and reaching it defers already-acked
%% deliveries to the TTL, so this is an error rather than a warning.
maybe_log_heal_cap(Completed) when map_size(Completed) >= ?MAX_HEAL_PER_DRIVE ->
    ?SLOG(error, #{
        msg => "bcast_rebuild_heal_cap_reached",
        healed => map_size(Completed),
        cap => ?MAX_HEAL_PER_DRIVE
    }),
    ok;
maybe_log_heal_cap(_Completed) ->
    ok.

%% Ask the activation leader (quota owner's shard 0) for a targeted rebuild
%% of this shard's partition. Fire-and-forget: the activation flips this
%% shard's own active flag; if the cast or the activation is lost, the next
%% maybe_activate poll re-asks.
request_partition_activation(Shard) ->
    case owner_node() =:= node() of
        true ->
            gen_server:cast(shard_name(0), {activate_shard, Shard});
        false ->
            emqx_rpc:cast(owner_node(), ?MODULE, local_cast, [0, {activate_shard, Shard}])
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
                                }),
                                mnesia:write(#bcast_msg_meta_counter{
                                    delivery_id = DeliveryId,
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
    %% The atomic ack counter table was introduced after bcast_msg_meta;
    %% backfill it from bcast_msg_meta for rows committed by older builds.
    ExistingMeta = ets:select(
        ?TAB_MSG_META,
        [{#bcast_msg_meta{delivery_id = '$1', counter = '$2', _ = '_'}, [], [{{'$1', '$2'}}]}]
    ),
    %% id -> counter: the value (not just the presence) tells a stale legacy
    %% counter apart from a delivery this build is still acking.
    CntByDid = maps:from_list(
        ets:select(
            ?TAB_MSG_META_CNT,
            [
                {
                    #bcast_msg_meta_counter{delivery_id = '$1', counter = '$2'},
                    [],
                    [{{'$1', '$2'}}]
                }
            ]
        )
    ),
    TargetByDid = maps:from_list([
        {DeliveryId, TargetAckCount}
     || {DeliveryId, _MsgId, _PK, _Tpl, TargetAckCount, _Counter, _DNs, _CreatedAt} <- Proj
    ]),
    MarkedDids = marker_did_set(),
    %% Deliveries whose acks are not attributable to devices: no persisted
    %% marker means the delivery was committed by a build that had no marker
    %% table (0.4.0 and earlier). Its remaining-ack counter was decremented by
    %% acks this build cannot map back to devices, while the rebuild
    %% re-indexes every target device of it (activate_partition/3 has no
    %% marker to skip a device with), so the counter has to be the full
    %% target again. Left at the legacy value, the first duplicate ack from a
    %% re-delivered already-acked device drives the stale counter to zero and
    %% completes the delivery while never-acked devices have already been
    %% dropped from the index: their messages are lost, not merely delayed.
    %%
    %% This only ever raises a counter, which delays a completion at worst and
    %% can never cause one, so the repair stays safe even if it interleaves
    %% with a live ack flush. It is a no-op for deliveries this build
    %% committed that have no acks yet, whose counter already equals the
    %% target, so a takeover with a large never-acked backlog writes nothing.
    Legacy = [
        {Did, Target}
     || {Did, _MsgId, _PK, _Tpl, Target, _Counter, _DNs, _CreatedAt} <- Proj,
        not maps:is_key(Did, MarkedDids),
        maps:get(Did, CntByDid, -1) =/= Target
    ],
    LegacySet = maps:from_keys([Did || {Did, _} <- Legacy], true),
    MissingCnt = [
        {Did, backfill_remaining(Did, Cnt, TargetByDid)}
     || {Did, Cnt} <- ExistingMeta,
        not maps:is_key(Did, CntByDid),
        not maps:is_key(Did, LegacySet)
    ],
    ok = write_meta_counters(MissingCnt),
    write_meta_counters(Legacy).

%% bcast_msg_meta.counter is the remaining-ack count, but since 0.4.0 it is
%% only ever written with the full target and never decremented (the atomic
%% bcast_msg_meta_counter is authoritative). When persisted ack markers exist,
%% derive the remaining from them; keep the conservative minimum so a delivery
%% whose counter row was lost can never be completed early. Callers must not
%% route a delivery without markers here: the rebuild re-indexes every device
%% of such a delivery, so its counter belongs at the full target and is
%% restored by backfill_meta_from_projection/1. The zero-marker clause below
%% is only a defensive fallback.
backfill_remaining(Did, MetaCounter, TargetByDid) ->
    case maps:size(acked_device_set(Did)) of
        0 ->
            max(0, MetaCounter);
        Acked ->
            Target = maps:get(Did, TargetByDid, MetaCounter),
            min(max(0, MetaCounter), max(0, Target - Acked))
    end.

%% Deliveries that own at least one persisted ack marker, as a set. One scan
%% of the marker table instead of a dirty read per delivery: the table is
%% keyed by delivery id, so this walks the deliveries that still have pending
%% acks rather than the whole batch.
marker_did_set() ->
    maps:from_keys(
        ets:select(
            ?TAB_MSG_ACKED,
            [{#bcast_msg_acked{delivery_id = '$1', _ = '_'}, [], ['$1']}]
        ),
        true
    ).

%% Write remaining-ack counter rows in batched transactions (100 rows/tx). An
%% aborted chunk is logged and skipped rather than failing the activation: the
%% next takeover retries the repair.
write_meta_counters(Rows) ->
    lists:foreach(
        fun(Chunk) ->
            case
                mnesia:transaction(
                    fun() ->
                        lists:foreach(
                            fun({DeliveryId, Cnt}) ->
                                mnesia:write(#bcast_msg_meta_counter{
                                    delivery_id = DeliveryId,
                                    counter = Cnt
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
                        msg => "bcast_meta_counter_backfill_tx_aborted",
                        reason => Reason,
                        chunk_size => length(Chunk)
                    })
            end
        end,
        chunks(Rows, 100)
    ).

%% Device-name set of the persisted ack markers of a delivery (one dirty
%% read per delivery, only for deliveries that own at least one device of
%% the shard being rebuilt). The table holds one row per flush tick, each
%% carrying that tick's device names.
acked_device_set(Did) ->
    maps:from_keys(
        lists:append([
            DNs
         || #bcast_msg_acked{device_names = DNs} <- mnesia:dirty_read(?TAB_MSG_ACKED, Did)
        ]),
        true
    ).

reset_state(State) ->
    State#{
        queues => #{},
        dids => #{},
        inflights => #{},
        counts => #{},
        attempts => #{},
        holders => #{},
        reserves => #{},
        peak => 0,
        orphan_cursor => 0,
        ack_buf => #{},
        ack_flush_ref => undefined
    }.

%% Live gauges for this shard's partition: queued = live pending minus
%% in-flight (claimed-not-terminal); in-flight = claimed entries awaiting
%% ack/release/expiry. Iterating counts (one entry per device, not per
%% delivery) keeps a scrape cost proportional to the number of active
%% devices.
sample_state(State) ->
    Counts = maps:get(counts, State),
    Infls = maps:get(inflights, State),
    maps:fold(
        fun(Key, N, {QueuedAcc, InflightAcc}) ->
            Inflight = maps:size(maps:get(Key, Infls, #{})),
            {QueuedAcc + (N - Inflight), InflightAcc + Inflight}
        end,
        {0, 0},
        Counts
    ).

total_pending(State) ->
    maps:fold(fun(_Key, N, Acc) -> Acc + N end, 0, maps:get(counts, State)).

%% Devices reserved by admission but not yet promoted into the index. They
%% already consume the global pending budget (admit reserves them before the
%% request enters the intake queue), so a recount that only sums index
%% pendings would erase every reservation still waiting in an intake queue.
total_reserves(State) ->
    maps:fold(fun(_Key, {N, _Ts}, Acc) -> Acc + N end, 0, maps:get(reserves, State)).

maybe_count_canceled(0) ->
    ok;
maybe_count_canceled(N) ->
    emqx_bcast_metrics:qos1_canceled(N),
    ok.

maybe_count_ttl_expired(0) ->
    ok;
maybe_count_ttl_expired(N) ->
    emqx_bcast_metrics:qos1_ttl_expired(N),
    ok.

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

%% [{DevShard, [Ack]}]
group_acks_by_dev(Acks) ->
    lists:foldl(
        fun(Ack = {PK, DN, _Did}, Acc) ->
            Shard = shard_of({PK, DN}),
            case lists:keyfind(Shard, 1, Acc) of
                {Shard, List} -> lists:keyreplace(Shard, 1, Acc, {Shard, [Ack | List]});
                false -> [{Shard, [Ack]} | Acc]
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
    State4 = maps:put(attempts, maps:remove(Key3, maps:get(attempts, State3)), State3),
    maybe_drop_device(State4, Key).

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
                inflights => maps:remove(Key, maps:get(inflights, State)),
                holders => maps:remove(Key, maps:get(holders, State))
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

%% Hot-path pending-counter update, now PURELY LOCAL (no cross-node RPC):
%% bumps this node's quota shadow row. Every pending entry is created and
%% retired on the same shard, so the sum of per-node cumulative deltas equals
%% the true global pending count; shard 0 periodically ships each node's
%% increment to the owner's authoritative row (see quota_sync/1), which
%% admission still reads with at most one sync-window lag.
quota_update(0) ->
    ok;
quota_update(Delta) ->
    case owner_node() =:= node() of
        true ->
            %% This node owns the authoritative row: update it directly
            %% (local ETS, no RPC). Single-node deployments therefore see
            %% an exact pending count immediately, and the reconcile timer
            %% on this node does not double-count (see quota_sync/1).
            quota_update_local(Delta);
        false ->
            %% Remote node: bump the node-local shadow only; shard 0's
            %% reconcile timer ships the accumulated delta to the owner.
            quota_local_update(Delta)
    end.

%% Runs on the quota owner node (exported for emqx_rpc).
quota_update_local(Delta) ->
    try
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
        end
    catch
        error:badarg ->
            %% The table lives and dies with shard 0 (its creator): during
            %% a leader restart it is briefly gone. Drop the delta - the
            %% drive's closing recount recomputes the row from live shard
            %% pendings.
            ok
    end.
%%--------------------------------------------------------------------
%% Claims
%%--------------------------------------------------------------------

%% Claim up to the per-device window of entries for a client. Returns
%% {Result, NewState} where Result is {ok, [ClaimMap]} | {no_more, Residual}.
%% Residual is how many live entries the device still holds in this shard,
%% so the pull side can tell "drained" (0) from "entries are there but not
%% claimable right now" (subscription cache not visible to the core yet,
%% replication lag, blocked head cycling). The FIFO head is popped;
%% claimable entries are moved to inflights (out of the queue);
%% unclaimable ones (topic mismatch, replication-lag, lazily deleted) are
%% either skipped back to the tail or dropped. The scan is budget-bounded
%% so one device with a long blocked head cannot hold the shard (see
%% claim_scan).
claim_one(#{clientid := DN, product_key := PK} = E, State) ->
    try do_claim_one(E, PK, DN, State) of
        {ok, Result, St} -> {{ok, Result}, St};
        {no_more, Residual, St} -> {{no_more, Residual}, St}
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
    InflightCount = maps:size(maps:get(Key, maps:get(inflights, State1), #{})),
    Capacity = window_size() - InflightCount,
    case Capacity =< 0 of
        true ->
            %% The window already holds a delivery: nothing to hand out and
            %% nothing to re-arm, so the residual is reported as 0.
            {no_more, 0, State1};
        false ->
            Q = maps:get(Key, maps:get(queues, State1), queue:new()),
            case claim_scan(State1, Key, Q, Topics, PK, DN, Tag, Capacity, 0, undefined, []) of
                {ok, Maps, St} ->
                    {ok, lists:reverse(Maps), St};
                {no_more, Residual, St} ->
                    {no_more, Residual, St}
            end
    end.

%% Per-device in-flight window is FIXED at 1 (one outstanding delivery
%% per device at a time, preserving per-device FIFO). The claim capacity
%% and the pull-side stage gate both derive from this constant.
window_size() ->
    1.

%% How many queue pops one claim scan may spend at most. A blocked head
%% (topic mismatch, replication lag, stale rows) is cycled once per pass;
%% the budget guarantees a single device cannot hold its shard for an
%% unbounded scan even with a long queue of unclaimable entries.
-define(CLAIM_SCAN_BUDGET, 512).

%% Scan the per-device FIFO and claim up to Capacity entries. Claimable
%% heads are popped into inflight; unclaimable entries cycle to the tail
%% (at most one full pass, tracked by CycleAnchor). Terminates when the
%% queue empties, the budget is spent, Capacity claims are made, or a full
%% pass found nothing more.
claim_scan(State, Key, Q, _Topics, _PK, _DN, _Tag, Capacity, Budget, _CycleAnchor, Acc) when
    Capacity =< 0; Budget > ?CLAIM_SCAN_BUDGET
->
    finish_claim_scan(State, Key, Q, Acc);
claim_scan(State, Key, Q, Topics, PK, DN, Tag, Capacity, Budget, CycleAnchor, Acc) ->
    case queue:out(Q) of
        {empty, _} ->
            finish_claim_scan(State, Key, Q, Acc);
        {{value, Did}, Q2} ->
            case Did =:= CycleAnchor of
                true ->
                    %% One full pass done: nothing new became claimable.
                    %% Put the anchor back and stop.
                    finish_claim_scan(State, Key, queue:in(Did, Q2), Acc);
                false ->
                    claim_scan_step(
                        State,
                        Key,
                        Q2,
                        Topics,
                        PK,
                        DN,
                        Tag,
                        Capacity,
                        Budget + 1,
                        CycleAnchor,
                        Acc,
                        Did
                    )
            end
    end.

claim_scan_step(State, Key, Q, Topics, PK, DN, Tag, Capacity, Budget, CycleAnchor, Acc, Did) ->
    Key3 = {PK, DN, Did},
    case maps:is_key(Key3, maps:get(dids, State)) of
        false ->
            %% Lazy residual (acked/removed): drop and continue.
            claim_scan(State, Key, Q, Topics, PK, DN, Tag, Capacity, Budget, CycleAnchor, Acc);
        true ->
            Now = erlang:system_time(millisecond),
            Ts = maps:get(Key3, maps:get(dids, State)),
            case claim_check(State, Key, Did, Key3, Ts, Now, Topics, PK, DN, Tag) of
                {claim, Map, State2} ->
                    claim_scan(
                        State2,
                        Key,
                        Q,
                        Topics,
                        PK,
                        DN,
                        Tag,
                        Capacity - 1,
                        Budget,
                        CycleAnchor,
                        [Map | Acc]
                    );
                {retry, State2} ->
                    %% Fresh-but-missing or topic mismatch: cycle to the
                    %% tail. Arm the cycle anchor on the first requeued
                    %% entry so a full pass terminates the scan.
                    Anchor =
                        case CycleAnchor of
                            undefined -> Did;
                            _ -> CycleAnchor
                        end,
                    claim_scan(
                        State2,
                        Key,
                        queue:in(Did, Q),
                        Topics,
                        PK,
                        DN,
                        Tag,
                        Capacity,
                        Budget,
                        Anchor,
                        Acc
                    );
                {drop, State2} ->
                    %% Stale entry dropped: the dids entry was removed by
                    %% maybe_drop_stale; quota_update is a local ETS
                    %% update, so account immediately (the owner RPC that
                    %% once made batching necessary is gone).
                    quota_update(-1),
                    claim_scan(
                        State2, Key, Q, Topics, PK, DN, Tag, Capacity, Budget, CycleAnchor, Acc
                    )
            end
    end.

finish_claim_scan(State, Key, _Q, []) ->
    %% Nothing was claimable in this pass. The device may still hold live
    %% entries - the subscription was not visible to the core yet, the
    %% delivery is still replicating, or a blocked head has to cycle - so
    %% report the residual instead of a bare no_more: the pull side re-arms
    %% the client through the deferred-claim mark when it is non-zero, and
    %% treats 0 as the normal drained state.
    {no_more, maps:get(Key, maps:get(counts, State), 0), State};
finish_claim_scan(State, Key, Q, Acc) ->
    {ok, Acc, save_queue(State, Key, Q)}.

%% Returns {claim, Result, State'} (State' has the inflight mark) |
%% {retry, State} | {drop, State}.
%% The reply carries no payload: the claim only validates that the
%% delivery row and its message exist (and are subscribed) on the core;
%% the payload itself is read by the delivering node from its own local
%% mria copy, so the cross-node claim reply stays small.
claim_check(State, Key, Did, Key3, Ts, Now, Topics, PK, DN, Tag) ->
    case mnesia:dirty_read(?TAB_MSG_META, Did) of
        [#bcast_msg_meta{msg_id = MsgId, topic_template = Tpl}] ->
            Topic = emqx_bcast_utils:expand_topic(Tpl, PK, DN),
            case topics_match(Topic, Topics) of
                false ->
                    {retry, State};
                {ok, SubQos} ->
                    case mnesia:dirty_read(?TAB_MSG, MsgId) of
                        [#bcast_message{}] ->
                            %% Attempt counter: one per claim of this logical
                            %% delivery, surviving release and lease-expiry
                            %% re-queues, so a redelivery (attempt >= 2) is
                            %% distinguishable from a first attempt. A claim
                            %% that never results in a send (session died in
                            %% the claim->send window) also consumes an
                            %% attempt; redelivered therefore counts sends
                            %% whose claim number was >= 2. The map lives in
                            %% shard state only: an index rebuild restarts
                            %% every entry at attempt 1, so a redelivery of a
                            %% rebuilt entry counts as a first attempt
                            %% (metric-only skew, delivery is unaffected).
                            Attempts = maps:get(Key3, maps:get(attempts, State), 0) + 1,
                            State1 = put_in(attempts, Key3, Attempts, State),
                            {claim,
                                #{
                                    delivery_id => Did,
                                    msg_id => MsgId,
                                    product_key => PK,
                                    topic_template => Tpl,
                                    claim_tag => Tag,
                                    sub_qos => SubQos,
                                    attempt => Attempts
                                },
                                mark_inflight(State1, Key, Key3, Now, Tag)};
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
%% Quota accounting is applied by the drop branch of claim_scan_step.
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
            %% The holder node is down: its acks can never arrive, so reclaim
            %% its claims immediately regardless of age. Otherwise keep the
            %% PENDING_TTL expiry as the backstop (holder process crash on a
            %% live node is recovered by the pull restart release-by-tag path,
            %% and this timer covers any residual orphan).
            HolderDown = holder_node_down(Key, State),
            {Expired, Kept} = maps:fold(
                fun(Key3, {Ts, Tag}, {Exp, Kp}) ->
                    case HolderDown orelse Now - Ts >= ?PENDING_TTL_MS of
                        true -> {[{Key3, {Ts, Tag}} | Exp], Kp};
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
                    State1 = requeue_inflight(State, Key, Expired),
                    State2 =
                        case maps:size(Kept) of
                            0 -> clear_holder(State1, Key);
                            _ -> State1
                        end,
                    maps:put(
                        inflights,
                        maps:put(Key, Kept, maps:get(inflights, State2)),
                        State2
                    )
            end
    end.

%% Requeue in-flight entries of a device in claim order (Ts ascending,
%% front first): with several in-flight entries per device an unordered
%% requeue would scramble the per-device FIFO. Input: [{Key3, {Ts, _Tag}}].
requeue_inflight(State, Key, Entries) ->
    save_queue(
        State,
        Key,
        requeued_queue(Key, Entries, maps:get(Key, maps:get(queues, State), queue:new()))
    ).

%% Build the requeued FIFO (in-flight entries re-inserted in claim order,
%% Ts ascending, front first) WITHOUT touching the state: callers that only
%% need the queue value (e.g. reclaim_down_holders) must not store the
%% whole state map as the per-device queue.
requeued_queue(_Key, Entries, Q0) ->
    Sorted = [K3 || {_Ts, K3} <- lists:keysort(1, [{Ts, K3} || {K3, {Ts, _Tag}} <- Entries])],
    lists:foldl(
        fun(K3, Qq) ->
            {_, _, Did} = K3,
            queue:in_r(Did, Qq)
        end,
        Q0,
        Sorted
    ).

%% True when the recorded claim holder node is no longer a running member.
holder_node_down(Key, State) ->
    case maps:get(Key, maps:get(holders, State), undefined) of
        undefined -> false;
        Holder -> not lists:member(Holder, running_nodes())
    end.

clear_holder(State, Key) ->
    maps:put(holders, maps:remove(Key, maps:get(holders, State)), State).

running_nodes() ->
    try emqx:running_nodes() of
        Nodes when is_list(Nodes) -> Nodes;
        _ -> [node()]
    catch
        _:_ -> [node()]
    end.

%% Full-pass node-down reclaim: any device whose claim holder node is no
%% longer a running member gets all its in-flight claims requeued so other
%% nodes can deliver them (their holder's ack can never arrive).
reclaim_down_holders(State) ->
    Infls = maps:get(inflights, State),
    Running = running_nodes(),
    maps:fold(
        fun
            (Key, DeviceInfl, St) when map_size(DeviceInfl) > 0 ->
                case maps:get(Key, maps:get(holders, St), undefined) of
                    Holder when Holder =/= undefined ->
                        case lists:member(Holder, Running) of
                            true ->
                                St;
                            false ->
                                St1 =
                                    maps:put(
                                        queues,
                                        maps:put(
                                            Key,
                                            requeued_queue(
                                                Key,
                                                maps:to_list(DeviceInfl),
                                                maps:get(Key, maps:get(queues, St), queue:new())
                                            ),
                                            maps:get(queues, St)
                                        ),
                                        St
                                    ),
                                St2 =
                                    maps:put(
                                        inflights,
                                        maps:remove(Key, maps:get(inflights, St1)),
                                        St1
                                    ),
                                clear_holder(St2, Key)
                        end;
                    _ ->
                        St
                end;
            (_Key, _DeviceInfl, St) ->
                St
        end,
        State,
        Infls
    ).

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
%% Acks: index removal + atomic meta counter decrement on the device shard
%%--------------------------------------------------------------------

%% Removes the per-device index entry and adjusts the global counter.
%% The bcast_msg_meta_counter decrement is performed right here, on the
%% same device shard, for COUNTED acks only. The result carries whether
%% the device still has queued (non-inflight) entries after the removal,
%% so the pull side can decide to refill the window immediately instead of
%% waiting for the next trigger (empty claims disappear structurally).
%% Returns {{counted, RemQueued} | not_found, State, GlobalDelta}.
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
            %% The qos1_acked metric is counted by pull_shard on the
            %% take_pending match (it owns the dedup of duplicate
            %% PUBACKs); the owner does not count it again. The result
            %% carries whether the device still has queued (non-inflight)
            %% entries so the pull side can refill its window without an
            %% empty claim round.
            {{counted, queued_remaining(State1, Key)}, State1, -1}
    end.

%% Queued (not in-flight) entries left for the device after an ack.
queued_remaining(State, Key) ->
    Count = maps:get(Key, maps:get(counts, State), 0),
    Inflight = maps:size(maps:get(Key, maps:get(inflights, State), #{})),
    Count - Inflight > 0.

%% Counted acks accumulate per-delivery acked device lists in ack_buf and
%% are applied in bulk by flush_ack_decrements/1 with two lock-free dirty
%% writes per delivery: the marker first, then the counter decrement.
%% Duplicate suppression is done in ack_one_index/2 (counting an ack
%% removes its dids entry), so the flush only ever sees first acks. A
%% crash between the two writes can only leave the counter too high: an
%% already-acked device is never resurrected on the next rebuild, and a
%% replayed ack cannot decrement a live delivery twice. Such a delivery is
%% reconciled by the next index rebuild, otherwise by TTL expiry.
%% bcast_msg_meta remains the info row used by claim and management queries.
buffer_ack_decrement({_PK, DN, Did}, State) ->
    Buf0 = maps:get(ack_buf, State, #{}),
    Buf1 = maps:update_with(Did, fun(DNs) -> [DN | DNs] end, [DN], Buf0),
    State1 = State#{ack_buf => Buf1},
    case maps:size(Buf0) of
        0 ->
            %% First buffered entry: arm the flush timer.
            arm_ack_flush(State1);
        _ ->
            State1
    end.

arm_ack_flush(State) ->
    Ref = erlang:send_after(?ACK_DEC_FLUSH_MS, self(), ack_flush),
    State#{ack_flush_ref => Ref}.

flush_ack_decrements(State = #{ack_buf := Buf}) ->
    case maps:size(Buf) of
        0 ->
            State;
        _ ->
            %% Entries that could not be applied stay buffered for the next
            %% tick. Both writes are idempotent, and dropping them would let a
            %% later index rebuild resurrect a device that already acked: its
            %% replayed ack decrements the completion counter a second time and
            %% can complete the delivery while other devices have never acked.
            State#{ack_buf => apply_ack_buffer(Buf)}
    end.

%% Apply every buffered ack decrement with lock-free dirty operations.
%%
%% A transaction here is what made the ack path collapse: the per-delivery
%% counter row is shared by every device shard (and replicated to every
%% core), so a transactional decrement serialized on that one row's write
%% lock. Under fanout that produced continuous lock conflicts, and
%% mnesia_tm restarted the whole chunk with a timer:sleep/1 backoff - all
%% inside the shard process, blocking claims, appends and acks behind it.
%% The dirty counter update is atomic and replicated without locks, which
%% is how the ack path worked before the persistent markers were added.
%%
%% Returns the entries that could not be applied, so the caller can retry them.
apply_ack_buffer(Buf) ->
    maps:fold(
        fun(Did, DNs, Retry) ->
            case apply_ack_decrement(Did, DNs) of
                ok -> Retry;
                {error, _} -> maps:put(Did, DNs, Retry)
            end
        end,
        #{},
        Buf
    ).

%% The flush runs from handle_info(ack_flush, ...), so anything raised here
%% used to take the shard process - and every partition it holds - down with
%% it. Report the failure and hand the entry back to the buffer instead: a
%% transient mnesia failure (or a replica still being created) then costs one
%% retry, not a crashed shard.
apply_ack_decrement(Did, DNs) ->
    try do_apply_ack_decrement(Did, DNs) of
        ok ->
            ok
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(warning, #{
                msg => "bcast_ack_flush_failed_retry",
                delivery_id => Did,
                exception => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, {Class, Reason}}
    end.

do_apply_ack_decrement(Did, DNs) ->
    %% Duplicate suppression already happened in ack_one_index/2: counting an
    %% ack removes its dids entry, so a replayed PUBACK returns not_found and
    %% never reaches ack_buf. A rebuild additionally skips devices whose
    %% marker is persisted (acked_device_set/1), so a replayed ack after a
    %% restart is not_found too. Every DN here is therefore a first ack.
    %%
    %% Markers are persisted as ONE row per delivery per flush tick carrying
    %% this tick's device names, not one row per device: at bs=1000 a
    %% per-device marker would write 1000 replicated rows (and delete 1000
    %% on completion) for every delivery, making the ack cost scale with the
    %% fanout throughput instead of the flush tick count.
    Applied = length(DNs),
    case Applied of
        0 ->
            ok;
        _ ->
            %% Marker FIRST, then the counter. The marker is the durable
            %% record consulted by index rebuilds, so writing it before the
            %% counter means a crash between the two dirty operations can
            %% only leave the counter too high: such a delivery completes
            %% late or is reclaimed by TTL, but an acked device is never
            %% resurrected and a replayed ack can never decrement a live
            %% delivery twice. The reverse order would allow exactly that.
            %% Both writes are dirty ops on mria-replicated tables; mria
            %% applies them to every core in submission order, so a core
            %% crash cannot surface the decrement without the marker. (mria
            %% replicates by hooking mnesia's post_commit event, so a plain
            %% `mnesia:dirty_*` is shipped exactly like `mria:dirty_*` would
            %% be - see `emqx_bcast_storage:transaction/1`.)
            mnesia:dirty_write(#bcast_msg_acked{delivery_id = Did, device_names = DNs}),
            case mnesia:dirty_update_counter(?TAB_MSG_META_CNT, Did, -Applied) of
                New when New =< 0 ->
                    %% Counter reached zero, or the row is already gone (the
                    %% delivery completed or was deleted between the ack and
                    %% this flush): drop the completion rows, and with them
                    %% the marker just written.
                    complete_delivery(Did);
                _ ->
                    ok
            end
    end.

%% Completion runs once per delivery, not once per ack, so deleting the
%% info rows transactionally (the replicas must drop them too) is off the
%% hot path.
complete_delivery(Did) ->
    %% Order matters: delete the transactional rows first and release the
    %% dirty counter row last. If the transaction aborts, the counter row is
    %% still there - sitting at (or below) zero - which is exactly what
    %% cleanup_completed_deliveries/0 scans for, so the periodic sweep
    %% retries the completion instead of leaking the delivery rows. Deleting
    %% the counter first would lose that trail.
    case
        mnesia:transaction(
            fun() ->
                mnesia:delete({?TAB_MSG_ACKED, Did}),
                case mnesia:wread({?TAB_MSG_META, Did}) of
                    [#bcast_msg_meta{}] ->
                        mnesia:delete({?TAB_MSG_META, Did}),
                        case mnesia:wread({?TAB_MSG_REC, Did}) of
                            [#bcast_msg{}] ->
                                mnesia:delete({?TAB_MSG_REC, Did}),
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
            _ = mnesia:dirty_delete({?TAB_MSG_META_CNT, Did}),
            ok;
        {aborted, Reason} ->
            ?SLOG(warning, #{
                msg => "bcast_ack_completion_tx_aborted",
                delivery_id => Did,
                reason => Reason
            }),
            {error, Reason}
    end.

%% Remember a delivery whose marker-based completion aborted, so the periodic
%% cleanup tick retries it. Needed because the leftover positive
%% remaining-ack counter cannot be told apart from a delivery that is still
%% being acked, and scan_completed_deliveries/1 only looks at counters =< 0.
%% Without it a fully acked delivery whose completion aborted on the last (or
%% only) shard owning its devices stays with an empty index until the next
%% rebuild or TTL.
add_pending_completion(State, DeliveryId) ->
    Pending = maps:get(pending_completions, State, #{}),
    State#{pending_completions => maps:put(DeliveryId, true, Pending)}.

%% Retry the aborted completions on this shard. complete_delivery/1 is
%% idempotent and reports ok when the rows are already gone, so an entry is
%% kept only while the transaction keeps aborting.
retry_pending_completions(State) ->
    case maps:get(pending_completions, State, #{}) of
        Pending when map_size(Pending) =:= 0 ->
            State;
        Pending ->
            Kept = maps:fold(
                fun(Did, true, Acc) ->
                    case complete_delivery(Did) of
                        ok -> Acc;
                        {error, _} -> maps:put(Did, true, Acc)
                    end
                end,
                #{},
                Pending
            ),
            State#{pending_completions => Kept}
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
        fun(Key3, {Ts, EntryTag}, {Rel, Kp}) ->
            case EntryTag =:= Tag of
                true -> {[{Ts, Key3} | Rel], Kp};
                false -> {Rel, maps:put(Key3, {Ts, EntryTag}, Kp)}
            end
        end,
        {[], #{}},
        Infl
    ),
    case ToRelease of
        [] ->
            State;
        _ ->
            %% Requeue in claim order (Ts ascending, front first): with
            %% several in-flight entries per device, an unordered requeue
            %% would scramble the per-device FIFO.
            Sorted = [K3 || {_Ts1, K3} <- lists:keysort(1, ToRelease)],
            Q = maps:get(Key, maps:get(queues, State), queue:new()),
            Q1 = lists:foldl(
                fun(K3, Qq) ->
                    {_, _, Did} = K3,
                    queue:in_r(Did, Qq)
                end,
                Q,
                Sorted
            ),
            State1 = save_queue(State, Key, Q1),
            maps:put(inflights, maps:put(Key, Kept, maps:get(inflights, State1)), State1)
    end.

%%--------------------------------------------------------------------
%% Batched release entry points (pull side)
%%--------------------------------------------------------------------

%% Async release fan-out without per-call spawns: group by index shard and
%% emit one gen_server cast (local) or emqx_rpc cast (remote) per shard.
%% Fire-and-forget; the shard handlers are idempotent and the claim lease
%% is the backstop for a lost cast. Releases grouped this way cost one
%% cast per shard per flush, not one spawn per release.
release_claims_async(Claims) ->
    lists:foreach(
        fun({Shard, Sub}) ->
            cast_shard(Shard, {release_batch, [{claim, PK, DN, Did} || {PK, DN, Did} <- Sub]})
        end,
        group_entries([{PK, DN, Did} || {PK, DN, Did} <- Claims])
    ),
    ok.

release_client_claims_async(Tags) ->
    lists:foreach(
        fun({Shard, Sub}) ->
            cast_shard(Shard, {release_batch, [{tag, PK, DN, Tag} || {PK, DN, Tag} <- Sub]})
        end,
        group_entries([{PK, DN, Tag} || {PK, DN, Tag} <- Tags])
    ),
    ok.

%% Synchronous release of client claims by tag (used when the caller needs
%% the release applied before the next claim round - e.g. release-then-
%% restage recovery paths). Routes per shard in parallel with a bounded
%% timeout; a failed leg is left to the claim lease.
release_client_claims_sync(Tags) ->
    lists:foreach(
        fun({Shard, Sub}) ->
            %% The release runs in a pull worker; a route exit (timeout /
            %% shard restarting) must not kill the worker, or the caller's
            %% re-stage signaling is lost. The claim lease is the backstop
            %% for a release that never lands.
            try route(Shard, {release_client_claims_batch, Sub}, ?SYNC_TIMEOUT_MS) of
                _ -> ok
            catch
                Error:Reason ->
                    ?SLOG(warning, #{
                        msg => "bcast_release_sync_leg_failed",
                        shard => Shard,
                        exception => Error,
                        reason => Reason,
                        entries => length(Sub)
                    })
            end
        end,
        group_entries([{PK, DN, Tag} || {PK, DN, Tag} <- Tags])
    ),
    ok.

%% Runs on the shard-owner node: cast straight into the local shard
%% gen_server (exported for emqx_rpc:cast).
-spec local_cast(pos_integer(), term()) -> ok.
local_cast(Shard, Msg) ->
    gen_server:cast(shard_name(Shard), Msg),
    ok.

cast_shard(Shard, Msg) ->
    Target = shard_owner(Shard),
    case Target =:= node() of
        true ->
            gen_server:cast(shard_name(Shard), Msg);
        false ->
            emqx_rpc:cast(Target, ?MODULE, local_cast, [Shard, Msg])
    end,
    ok.
%%--------------------------------------------------------------------
%% Admission reservations (atomic global reserve + per-shard device check)
%%--------------------------------------------------------------------

%% Serialized admission: shard 0 reserves the global budget atomically
%% (update_counter returns the new value; overshoot rolls back) and each
%% device shard checks + reserves its own devices in its own process.
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
%% Lock order is meta -> msg_rec, matching complete_delivery/1 and
%% delete_expired_deliveries_batched (the previous rec-first order widened
%% the mnesia deadlock window with the ack hot path; retries masked it).
delete_delivery_rows_local(Did, _MsgId) ->
    case
        mnesia:transaction(
            fun() ->
                %% The acked-device markers share the delivery's lifetime.
                mnesia:delete({?TAB_MSG_ACKED, Did}),
                case mnesia:wread({?TAB_MSG_META, Did}) of
                    [] ->
                        case mnesia:wread({?TAB_MSG_REC, Did}) of
                            [] ->
                                {error, not_found};
                            [#bcast_msg{}] ->
                                mnesia:delete({?TAB_MSG_REC, Did}),
                                ok
                        end;
                    [#bcast_msg_meta{}] ->
                        mnesia:delete({?TAB_MSG_META, Did}),
                        mnesia:delete({?TAB_MSG_META_CNT, Did}),
                        case mnesia:wread({?TAB_MSG_REC, Did}) of
                            [#bcast_msg{}] ->
                                mnesia:delete({?TAB_MSG_REC, Did}),
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

%% One chunk of per-delivery rows, deleted in a bounded transaction. Lock
%% order acked -> meta -> counter -> rec, matching complete_delivery/1 and
%% every other delete path.
delete_delivery_rows_local(DeliveryIds) ->
    case
        mnesia:transaction(
            fun() ->
                lists:foreach(
                    fun(Did) ->
                        mnesia:delete({?TAB_MSG_ACKED, Did}),
                        mnesia:delete({?TAB_MSG_META, Did}),
                        mnesia:delete({?TAB_MSG_META_CNT, Did}),
                        mnesia:delete({?TAB_MSG_REC, Did})
                    end,
                    DeliveryIds
                )
            end,
            20
        )
    of
        {atomic, _} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%% The message rows, deleted last and in one short transaction: until this
%% commits the message is still readable, so in-flight deliveries keep their
%% payload, and afterwards the promotion-time reuse check refuses to
%% re-create it.
delete_message_meta_rows_local(ApiId) ->
    case
        mnesia:transaction(
            fun() ->
                case mnesia:read(?TAB_MSG_API_ID, ApiId, write) of
                    [#bcast_message_api_id{msg_id = MsgId}] ->
                        case mnesia:read(?TAB_MSG, MsgId, write) of
                            [#bcast_message{content_hash = Hash, created_at = CreatedAt}] ->
                                mnesia:delete({?TAB_MSG, MsgId}),
                                mnesia:delete({?TAB_MSG_HASH, Hash}),
                                mnesia:delete({?TAB_MSG_API_ID, ApiId}),
                                mnesia:delete({?TAB_MSG_REG, MsgId}),
                                mnesia:delete({?TAB_MSG_ORDER, {CreatedAt, MsgId}}),
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
    %% Each removed per-device index entry is a logical delivery whose TTL
    %% expired before confirmation; acked devices were already removed at
    %% ack time, so the removed count equals the remaining unacked count.
    Removed = remove_batch([
        {ProductKey, DN, DeliveryId}
     || {DeliveryId, _MsgId, ProductKey, DeviceNames} <- Expired,
        DN <- DeviceNames
    ]),
    maybe_count_ttl_expired(Removed).

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
%% complete_delivery/1 (which reads/writes msg_meta before bcast_msg).
%% The previous msg_rec-first order was the reverse, widening the mnesia
%% deadlock window between the cleanup tx and concurrent acks (the 20
%% retries masked it, but each retry is wasted work and latency).
delete_one_expired_delivery_tx({DeliveryId, _MsgId, _ProductKey, _DeviceNames}) ->
    mnesia:delete({?TAB_MSG_ACKED, DeliveryId}),
    case mnesia:wread({?TAB_MSG_META, DeliveryId}) of
        [] ->
            %% Meta gone: either the delivery was acked/completed
            %% concurrently or it never had one; clean up the row
            %% defensively.
            case mnesia:wread({?TAB_MSG_REC, DeliveryId}) of
                [#bcast_msg{}] ->
                    mnesia:delete({?TAB_MSG_REC, DeliveryId}),
                    ok;
                [] ->
                    ok
            end;
        [#bcast_msg_meta{}] ->
            %% Off the ack hot path: delete the counter transactionally here
            %% (so the replicas drop it too). complete_delivery/1 runs on the
            %% ack path and deletes it dirty after commit instead; an abort
            %% here leaves the whole delivery for the next cleanup tick.
            mnesia:delete({?TAB_MSG_META, DeliveryId}),
            mnesia:delete({?TAB_MSG_META_CNT, DeliveryId}),
            case mnesia:wread({?TAB_MSG_REC, DeliveryId}) of
                [#bcast_msg{}] ->
                    mnesia:delete({?TAB_MSG_REC, DeliveryId}),
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

%% Fallback for deliveries whose rows leaked after completion. Each core scans
%% its OWN local counter rows that already reached zero and deletes them dirty
%% (matching the dirty decrement/delete on the ack path), then deletes the
%% meta/rec rows transactionally (covers the rare case where complete_delivery's
%% meta/rec transaction aborted after the dirty counter delete).
cleanup_completed_deliveries() ->
    Completed = scan_completed_deliveries(?CLEANUP_BUDGET),
    %% Delete the <=0 counter row (the retry marker) only after the delete
    %% transaction commits. On abort the marker survives, so the next tick
    %% re-discovers the rows instead of leaking them to TTL.
    Deleted = delete_completed_deliveries_batched(Completed),
    case Deleted of
        [] ->
            ok;
        _ ->
            ?SLOG(info, #{
                msg => "bcast_completed_deliveries_reclaimed",
                count => length(Deleted),
                node => node()
            })
    end,
    lists:foreach(
        fun(Did) -> _ = mnesia:dirty_delete({?TAB_MSG_META_CNT, Did}) end,
        Deleted
    ),
    ok.

%% Coordinate the counter-linger sweep across every running core: the cleanup
%% leader runs it locally and asks each sibling core to sweep its own copy.
cleanup_completed_deliveries_everywhere() ->
    case emqx_bcast:core_nodes() of
        [] ->
            cleanup_completed_deliveries();
        Nodes ->
            lists:foreach(
                fun(Node) ->
                    case Node =:= node() of
                        true ->
                            ok = cleanup_completed_deliveries();
                        false ->
                            _ = emqx_rpc:call(
                                ?MODULE,
                                Node,
                                ?MODULE,
                                cleanup_completed_deliveries,
                                [],
                                ?SYNC_TIMEOUT_MS
                            ),
                            ok
                    end
                end,
                Nodes
            ),
            ok
    end.

scan_completed_deliveries(Budget) ->
    %% bcast_msg_meta_counter is a 3-tuple {bcast_msg_meta_counter, Did, N}.
    case
        ets:select(
            ?TAB_MSG_META_CNT,
            [
                {{?TAB_MSG_META_CNT, '$1', '$2'}, [{'=<', '$2', 0}], ['$1']}
            ],
            Budget
        )
    of
        '$end_of_table' -> [];
        {Rows, _Continuation} -> Rows
    end.

%% Returns the Dids whose delete transaction committed; an aborted chunk's
%% counter rows are left in place for the next tick to re-discover.
delete_completed_deliveries_batched(Dids) ->
    lists:append([
        case
            mnesia:transaction(
                fun() -> lists:foreach(fun delete_one_completed_delivery_tx/1, Chunk) end,
                20
            )
        of
            {atomic, _} ->
                Chunk;
            {aborted, Reason} ->
                %% The counter row was NOT deleted for this chunk, so the next
                %% cleanup tick re-scans it.
                ?SLOG(warning, #{
                    msg => "bcast_completed_delete_tx_aborted",
                    reason => Reason,
                    chunk_size => length(Chunk)
                }),
                []
        end
     || Chunk <- chunks(Dids, 100)
    ]).

%% Delete the transactional rows of one completed delivery. The remaining-ack
%% counter row (bcast_msg_meta_counter) is deliberately NOT touched here:
%% cleanup_completed_deliveries/0 dirty-deletes it only after this transaction
%% has committed, so an aborted transaction leaves the <=0 counter row behind
%% as the retry marker for the next cleanup tick.
%% Lock order acked -> meta -> rec, matching complete_delivery/1.
delete_one_completed_delivery_tx(Did) ->
    mnesia:delete({?TAB_MSG_ACKED, Did}),
    case mnesia:wread({?TAB_MSG_META, Did}) of
        [#bcast_msg_meta{}] ->
            mnesia:delete({?TAB_MSG_META, Did}),
            case mnesia:wread({?TAB_MSG_REC, Did}) of
                [#bcast_msg{}] ->
                    mnesia:delete({?TAB_MSG_REC, Did}),
                    ok;
                [] ->
                    ok
            end;
        [] ->
            ok
    end.

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
                            created_at = '$5',
                            _ = '_'
                        },
                        [{'<', '$4', Now}],
                        [{{'$1', '$2', '$3', '$4', '$5'}}]
                    }
                ],
                ?CLEANUP_BUDGET
            )
        of
            '$end_of_table' -> [];
            {Rows, _Continuation} -> Rows
        end,
    %% Log the TTL sweep explicitly, with the expiry it acted on: without
    %% this a payload row disappearing because it expired looks identical to
    %% a row disappearing for any other reason.
    {Deleted, Refreshed} = delete_expired_messages(Expired, Now),
    lists:foreach(
        fun({MsgId, _ContentHash, ApiMsgId, ExpiresAt, _CreatedAt}) ->
            ?SLOG(debug, #{
                msg => "bcast_message_expired",
                msg_id => MsgId,
                api_msg_id => ApiMsgId,
                expires_at => ExpiresAt,
                now => Now
            })
        end,
        Deleted
    ),
    case Expired of
        [] ->
            ok;
        _ ->
            ?SLOG(info, #{
                msg => "bcast_messages_expired",
                count => length(Expired),
                deleted => length(Deleted),
                %% Rows a concurrent create/refresh extended between the scan
                %% and the delete: left alone on purpose (see below).
                refreshed => Refreshed,
                now => Now,
                sample => lists:sublist(Deleted, 5),
                node => node()
            })
    end.

%% Delete the rows a scan collected, but only while each one is still the same
%% expired row. The scan is only a snapshot: a concurrent create or re-send of
%% the same content refreshes the row in place (RegisterMessage, a BatchPub
%% that resolves to the same hash), and deleting a refreshed payload together
%% with its hash and API-id rows would leave the delivery behind that refresh
%% without a message body.
%%
%% The deletes stay lock-free on purpose. This runs on the cleanup tick of the
%% leader core, and a transaction here would take write locks on the message
%% and hash rows that promotion needs first, then hold them while its commit
%% queues behind whatever mnesia_tm is already carrying - the same
%% lock-then-wait shape that collapsed broadcast throughput before (see
%% docs/2026-09-11-perf-rootcause-ack-flush.md). Instead every delete is an
%% equality delete of a record read a moment earlier: a row that changed in
%% between simply does not match and stays, and the derived rows carry this
%% message's own ids, so a refresh that wrote new ones cannot be hit either.
%% Returns the deleted rows, so the caller can log them, and the number left
%% to the refresh.
-spec delete_expired_messages(
    [{binary(), binary(), binary(), integer(), integer()}], integer()
) -> {[{binary(), binary(), binary(), integer(), integer()}], non_neg_integer()}.
delete_expired_messages(Expired, Now) ->
    {DeletedRev, Refreshed} = lists:foldl(
        fun(Row, {Deleted, Left}) ->
            case delete_expired_message(Row, Now) of
                true -> {[Row | Deleted], Left};
                false -> {Deleted, Left + 1}
            end
        end,
        {[], 0},
        Expired
    ),
    {lists:reverse(DeletedRev), Refreshed}.

delete_expired_message({MsgId, Hash, ApiMsgId, ExpiresAt, CreatedAt}, Now) ->
    %% The exact expiry the scan saw: a refresh that extended the row in the
    %% meantime changed it, and the equality delete below then matches nothing.
    case mnesia:dirty_read(?TAB_MSG, MsgId) of
        [#bcast_message{expires_at = ExpiresAt} = Row] when ExpiresAt =< Now ->
            %% Derived rows first, each identified by this message's own keys,
            %% then the message row last - the order the management cascade
            %% delete uses, so a rebuild never sees an order or hash row whose
            %% message is already gone.
            _ = mnesia:dirty_delete_object(#bcast_message_hash{hash = Hash, msg_id = MsgId}),
            _ = mnesia:dirty_delete_object(#bcast_message_api_id{
                api_msg_id = ApiMsgId, msg_id = MsgId
            }),
            _ = mnesia:dirty_delete_object(#bcast_message_reg{msg_id = MsgId}),
            _ = mnesia:dirty_delete_object(#bcast_message_order{key = {CreatedAt, MsgId}}),
            _ = mnesia:dirty_delete_object(Row),
            true;
        _ ->
            false
    end.

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
            Now = erlang:system_time(millisecond),
            State1 = lists:foldl(
                fun(Key, St) -> cleanup_orphan_head(Key, Now, St) end,
                State,
                Chunk
            ),
            State1#{orphan_cursor => (Start + Budget) rem N}
    end.

%% Inspect one device's FIFO head: lazily-deleted residuals and stale
%% entries are dropped, valid entries are put back in place.
%%
%% A missing local delivery/message row is ambiguous while the entry is
%% fresh: a concurrent promotion on the peer core may still be replicating its
%% transaction (mria lag). The claim path already refuses to drop such an entry
%% (see maybe_drop_stale/6); this last-resort repair has to apply the same rule,
%% because dropping a committed delivery here takes the message out of the
%% index for good - the device then hears nothing about it until the message
%% TTL expires, and nothing logs the loss.
cleanup_orphan_head(Key = {PK, DN}, Now, State) ->
    Dids = maps:get(dids, State),
    Q = maps:get(Key, maps:get(queues, State), queue:new()),
    case queue:out(Q) of
        {empty, _} ->
            State;
        {{value, Did}, Q2} ->
            Key3 = {PK, DN, Did},
            case maps:find(Key3, Dids) of
                error ->
                    %% Lazy residual: drop.
                    save_queue(State, Key, Q2);
                {ok, Ts} ->
                    case index_entry_valid(Did) of
                        true ->
                            save_queue(State, Key, queue:in_r(Did, Q2));
                        false when Now - Ts < ?REPLICATION_LAG_MS ->
                            %% Fresh entry, invisible row: keep it at the head
                            %% and let a later pass re-check it.
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
