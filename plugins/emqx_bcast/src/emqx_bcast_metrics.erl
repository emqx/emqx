%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_metrics).

-export([init/0]).
-export([qos0_in/0, qos0_targeted/1, qos0_delivery_count/1]).
-export([
    qos1_in/0,
    qos1_wanted/1,
    qos1_delivered/0,
    qos1_redelivered/0,
    qos1_acked/0,
    qos1_auto_acked/0,
    qos1_ttl_expired/1,
    qos1_canceled/1,
    intake_enqueued/0,
    intake_rejected/0,
    qos1_promote_error/0
]).
-export([broadcast_in/0, broadcast_error/0]).
-export([register_in/0, register_refresh/0, register_error/0]).
-export([collect/0, reset/0, check_guarded/0, reset_guarded/0, reset_cluster/0]).

-include("emqx_bcast.hrl").

-define(NS, <<"bcast">>).

%% Business-only metric surface. EMQX's own Prometheus endpoint already
%% exposes node-level system gauges (CPU, memory, connections,
%% messages.delivered); this registry carries only bcast-plugin business
%% counters and gauges.
%%
%% QoS1 delivery ledger (counted in "logical delivery" units, i.e. one
%% (BatchPub request x target device); counters are node-local, aggregate
%% the cluster with sum()).
%%
%% The counters come in three scopes. Each node only ever reports its own
%% registry - nothing is fetched from another node - but the scopes are
%% counted on different nodes, so one node's numbers describe three
%% different sets:
%%
%%   intake scope - counted on the node that ACCEPTED and COMMITTED the API
%%   request (the core that ran the intake queue and the promoter):
%%     in, enqueued, intake_rejected, promote_error, wanted, intake_depth
%%   A replicant forwards the API request to a core for admission, so it
%%   reports 0 for every counter in this scope. wanted counts EVERY device
%%   of each batch this node committed - not only the devices whose clients
%%   are attached here. It is this node's commit (intake) share, NOT its
%%   delivery share.
%%
%%   device scope - counted on the node whose pull shard served the client:
%%     delivered, redelivered, acked, auto_acked
%%   In a core/replicant deployment these live on the replicants holding
%%   the connections.
%%
%%   index scope - counted on the core that owns the device's index shard
%%   or runs the delete/cleanup for it:
%%     queued, inflight, ttl_expired, canceled
%%   Index owner shards are distributed over the CORE nodes
%%   (shard_owner/1 round-robins core_nodes()), so a replicant reports 0
%%   for the shard gauges even while it serves devices.
%%
%%   batch_pub_qos1_wanted    durable ledger base: incremented once per
%%                            logical delivery at the mria commit point
%%                            (promotion), NOT at API acceptance - entries
%%                            dropped before commit never become wanted
%%   batch_pub_qos1_delivered actual PUBLISH sends (includes redeliveries
%%                            and the QoS0-subscription auto path)
%%   batch_pub_qos1_redelivered sends whose claim attempt number >= 2
%%   batch_pub_qos1_acked     PUBACKs matched to a pending delivery
%%
%% acked is counted where the PUBACK is matched, which is up to one marker
%% flush interval (50ms) before that ack becomes durable. A duplicate PUBACK
%% is never counted twice while the index shard survives, because matching an
%% ack removes the device from the index. If the shard dies inside that
%% window the ack is not durable, the rebuild re-indexes the device, and the
%% redelivered duplicate is counted once more: the delivery itself is still
%% at-least-once, and no acked device is resurrected as pending once the
%% marker lands (the marker is written before the counter decrement).
%%   batch_pub_qos1_auto_acked  QoS1 deliveries completed because the
%%                            subscription QoS is 0 (no device PUBACK)
%%   batch_pub_qos1_ttl_expired logical deliveries abandoned because the
%%                            delivery TTL expired before confirmation
%%   batch_pub_qos1_canceled  logical deliveries removed by management
%%                            delete / reset before confirmation
%%
%% Ledger identity (eventually consistent): wanted = acked + auto_acked +
%% ttl_expired + canceled + queued + inflight, where queued/inflight are
%% live gauges; equivalently backlog = wanted - (acked + auto_acked +
%% ttl_expired + canceled). Admission layer (request units) is tracked by
%% the batch_pub_qos1_{in,enqueued,intake_rejected,promote_error}
%% counters; quota rejections are not counted (derivable as in - enqueued
%% - intake_rejected within a node lifetime).
%%
%% Because wanted is intake scoped while the terminal counters are delivery
%% scoped, that identity holds for sum() over all nodes - NOT for a single
%% node unless the same node also delivered the devices it committed. Under
%% a load-balanced deployment the two scopes cover comparable shares and
%% the per-node numbers look consistent. If API traffic is pinned to one
%% node, or the API load balancer distributes differently from the MQTT
%% one, that node's wanted can exceed its own delivered/acked by the ratio
%% of the two shares. That is expected and is not a leak.

-spec init() -> ok.
init() ->
    declare_counters(),
    declare_gauges(),
    ok.

mname(Suffix) when is_list(Suffix) -> <<?NS/binary, "_", (list_to_binary(Suffix))/binary>>;
mname(Suffix) when is_binary(Suffix) -> <<?NS/binary, "_", Suffix/binary>>.

declare_counters() ->
    Cs = [
        {"batch_pub_qos0_in", "BatchPub QoS=0 API requests"},
        {"batch_pub_qos0_targeted", "QoS=0 devices targeted"},
        {"qos0_delivery_count", "QoS=0 one-shot deliveries to online clients"},
        {"batch_pub_qos1_in",
            "BatchPub QoS=1 API requests accepted by this node "
            "(intake scope; a replicant forwards to a core and reports 0)"},
        {"batch_pub_qos1_wanted",
            "QoS=1 logical deliveries this node durably committed, counting every device of "
            "each batch it accepted - this node's commit share, not its delivery share "
            "(intake scope; replicants report 0; sum() over nodes is the cluster ledger base)"},
        {"batch_pub_qos1_delivered",
            "QoS=1 PUBLISH sends by this node to its attached clients "
            "(device scope; includes redeliveries)"},
        {"batch_pub_qos1_redelivered",
            "QoS=1 PUBLISH sends of an already-attempted logical delivery "
            "(attempt >= 2; device scope)"},
        {"batch_pub_qos1_acked",
            "QoS=1 PUBACKs matched to a pending delivery (device scope; counted when the ack "
            "is matched, up to the 50ms marker flush before it is durable)"},
        {"batch_pub_qos1_auto_acked",
            "QoS=1 deliveries completed because subscription QoS is 0 (device scope)"},
        {"batch_pub_qos1_ttl_expired",
            "QoS=1 logical deliveries abandoned at TTL expiry without confirmation "
            "(index scope; counted on the core that reclaims them)"},
        {"batch_pub_qos1_canceled",
            "QoS=1 logical deliveries removed by management delete/reset without confirmation "
            "(index scope; counted on the core that runs the removal)"},
        {"batch_pub_qos1_enqueued",
            "QoS=1 requests accepted into this node's intake queue "
            "(intake scope; replicants report 0)"},
        {"batch_pub_qos1_intake_rejected",
            "QoS=1 requests rejected because this node's intake queue is full (intake scope)"},
        {"batch_pub_qos1_promote_error",
            "QoS=1 promotion batch failures on this node, retries exhausted (intake scope)"},
        {"broadcast_pub_in", "PubBroadcast API requests"},
        {"broadcast_pub_error", "PubBroadcast errors"},
        {"register_message_in", "RegisterMessage API requests"},
        {"register_message_refresh", "RegisterMessage TTL refresh"},
        {"register_message_error", "RegisterMessage errors"}
    ],
    [
        prometheus_counter:declare([
            {registry, ?BCAST_REGISTRY},
            {name, mname(N)},
            {help, list_to_binary(H)}
        ])
     || {N, H} <- Cs
    ],
    ok.

declare_gauges() ->
    [
        prometheus_gauge:declare([
            {registry, ?BCAST_REGISTRY},
            {name, mname(N)},
            {help, list_to_binary(H)}
        ])
     || {N, H} <- [
            {"intake_depth",
                "QoS1 intake queue depth on this node: requests awaiting promotion "
                "(intake scope; replicants report 0)"},
            {"batch_pub_qos1_queued",
                "QoS1 committed logical deliveries queued but not yet claimed on this node's "
                "shards (index scope; cores only; sum() over nodes)"},
            {"batch_pub_qos1_inflight",
                "QoS1 claimed logical deliveries not yet terminal on this node's shards "
                "(index scope; cores only; sum() over nodes)"}
        ]
    ],
    ok.

%% Gauges are sampled at collect time from authoritative live state
%% (intake queue depth, per-shard heap sizes) instead of being maintained
%% on the delivery hot path. Shards only run on core nodes; replicants
%% report 0 so a cluster sum() stays correct.
report_business_gauges() ->
    prometheus_gauge:set(?BCAST_REGISTRY, mname("intake_depth"), [], emqx_bcast_intake:depth()),
    {Queued, Inflight} = emqx_bcast_index_owner:gauge_sample(),
    prometheus_gauge:set(?BCAST_REGISTRY, mname("batch_pub_qos1_queued"), [], Queued),
    prometheus_gauge:set(?BCAST_REGISTRY, mname("batch_pub_qos1_inflight"), [], Inflight),
    ok.

%% helpers
c(N) -> prometheus_counter:inc(?BCAST_REGISTRY, mname(N), [], 1).
c(N, V) -> prometheus_counter:inc(?BCAST_REGISTRY, mname(N), [], V).

-spec qos0_in() -> ok.
qos0_in() -> c("batch_pub_qos0_in").
-spec qos0_targeted(non_neg_integer()) -> ok.
qos0_targeted(N) -> c("batch_pub_qos0_targeted", N).
-spec qos0_delivery_count(non_neg_integer()) -> ok.
qos0_delivery_count(N) -> c("qos0_delivery_count", N).

-spec qos1_in() -> ok.
qos1_in() -> c("batch_pub_qos1_in").
-spec qos1_wanted(non_neg_integer()) -> ok.
qos1_wanted(N) -> c("batch_pub_qos1_wanted", N).
-spec qos1_delivered() -> ok.
qos1_delivered() -> c("batch_pub_qos1_delivered").
-spec qos1_redelivered() -> ok.
qos1_redelivered() -> c("batch_pub_qos1_redelivered").
-spec qos1_acked() -> ok.
qos1_acked() -> c("batch_pub_qos1_acked").
-spec qos1_auto_acked() -> ok.
qos1_auto_acked() -> c("batch_pub_qos1_auto_acked").
-spec qos1_ttl_expired(non_neg_integer()) -> ok.
qos1_ttl_expired(N) -> c("batch_pub_qos1_ttl_expired", N).
-spec qos1_canceled(non_neg_integer()) -> ok.
qos1_canceled(N) -> c("batch_pub_qos1_canceled", N).
-spec intake_enqueued() -> ok.
intake_enqueued() -> c("batch_pub_qos1_enqueued").
-spec intake_rejected() -> ok.
intake_rejected() -> c("batch_pub_qos1_intake_rejected").
-spec qos1_promote_error() -> ok.
qos1_promote_error() -> c("batch_pub_qos1_promote_error").

-spec broadcast_in() -> ok.
broadcast_in() -> c("broadcast_pub_in").
-spec broadcast_error() -> ok.
broadcast_error() -> c("broadcast_pub_error").

-spec register_in() -> ok.
register_in() -> c("register_message_in").
-spec register_refresh() -> ok.
register_refresh() -> c("register_message_refresh").
-spec register_error() -> ok.
register_error() -> c("register_message_error").

-spec collect() -> binary().
collect() ->
    report_business_gauges(),
    prometheus_text_format:format(?BCAST_REGISTRY).

%% Reset this node's registry to zero (counters and gauges). The registry
%% is in-memory per node and starts empty on restart; this mirrors that
%% state for maintenance/testing. State (queued/in-flight deliveries) is
%% NOT reset here - callers must guard against resetting while the ledger
%% is non-empty, otherwise post-reset ack/expiry events have no matching
%% wanted base and the ledger identity breaks.
-spec reset() -> ok.
reset() ->
    ok = prometheus_registry:clear(?BCAST_REGISTRY),
    init(),
    ok.

%% Local readiness check: refuse to reset while this node still holds
%% committed but not-yet-terminal logical deliveries (queued or in-flight),
%% because the ledger identity (wanted = terminal outcomes + live) only holds
%% for events observed after a reset.
-spec check_guarded() -> ok | {error, {pending_deliveries, non_neg_integer(), non_neg_integer()}}.
check_guarded() ->
    {Queued, Inflight} = emqx_bcast_index_owner:gauge_sample(),
    case Queued + Inflight of
        0 -> ok;
        _ -> {error, {pending_deliveries, Queued, Inflight}}
    end.

-spec reset_guarded() -> ok | {error, {pending_deliveries, non_neg_integer(), non_neg_integer()}}.
reset_guarded() ->
    case check_guarded() of
        ok -> reset();
        {error, _} = Error -> Error
    end.

%% Cluster-wide guarded reset. The registry is per-node, so a partial reset
%% would leave a permanent gap in cross-node sums. Two phases: first check
%% every running node is idle, then reset them all, so a busy node cannot
%% leave the cluster partially reset. A node that becomes busy in the small
%% window between the check and the reset still refuses (reset_guarded/0
%% re-checks on the node); every node of the second phase is inspected too,
%% so such a node is reported as a failure instead of being silently dropped
%% from an "ok" result. The operation is intended for maintenance windows.
-spec reset_cluster() ->
    {ok, [{node(), ok}]}
    | {error, {pending_deliveries, [{node(), ok | {error, term()}}]}}
    | {error, {partial_reset, [{node(), ok | {error, term()}}]}}.
reset_cluster() ->
    Nodes = lists:usort([node() | emqx:running_nodes()]),
    %% Both phases run the nodes' calls in parallel, each bounded by the
    %% framework's budget for this request (emqx_bcast_utils:api_budget_ms/0):
    %% the endpoint kills the callback at that point, and a reset that went on
    %% touching the cluster while the caller was told nothing is worse than
    %% refusing. Sequentially this cost one per-node timeout per node, per phase.
    Checks = lists:zip(Nodes, parallel_node_map(fun rpc_check_guarded/1, Nodes)),
    case [R || {_, {error, _} = R} <- Checks] of
        [] ->
            Results = lists:zip(Nodes, parallel_node_map(fun rpc_reset_guarded/1, Nodes)),
            case [R || {_, {error, _} = R} <- Results] of
                [] -> {ok, Results};
                _ -> {error, {partial_reset, Results}}
            end;
        _ ->
            {error, {pending_deliveries, Checks}}
    end.

%% Independent per-node calls, collected in node order. Deliberately not the
%% shard's parallel_map/2: this runs in a request process, which does not trap
%% exits, and a dying leg must not take it down.
parallel_node_map(Fun, Nodes) ->
    Parent = self(),
    Refs = [
        begin
            Ref = make_ref(),
            _ = spawn(fun() -> Parent ! {Ref, catch Fun(Node)} end),
            Ref
        end
     || Node <- Nodes
    ],
    [
        receive
            {Ref, Result} -> Result
        end
     || Ref <- Refs
    ].

%% The local node is wrapped exactly like a remote one: a crash in the local
%% check/reset must be reported as a per-node error, not propagate and abort
%% the whole cluster operation half way through.
rpc_check_guarded(Node) when Node =:= node() ->
    try check_guarded() of
        Result -> Result
    catch
        Error:Reason -> {error, {Error, Reason}}
    end;
rpc_check_guarded(Node) ->
    try emqx_rpc:call(?MODULE, Node, ?MODULE, check_guarded, [], api_node_timeout()) of
        {badrpc, Reason} -> {error, {badrpc, Reason}};
        Result -> Result
    catch
        Error:Reason -> {error, {Error, Reason}}
    end.

rpc_reset_guarded(Node) when Node =:= node() ->
    try reset_guarded() of
        Result -> Result
    catch
        Error:Reason -> {error, {Error, Reason}}
    end;
rpc_reset_guarded(Node) ->
    try emqx_rpc:call(?MODULE, Node, ?MODULE, reset_guarded, [], api_node_timeout()) of
        {badrpc, Reason} -> {error, {badrpc, Reason}};
        Result -> Result
    catch
        Error:Reason -> {error, {Error, Reason}}
    end.

api_node_timeout() ->
    emqx_bcast_utils:api_rpc_timeout_ms().
