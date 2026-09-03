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
-export([collect/0, reset/0, reset_guarded/0, reset_cluster/0]).

-include("emqx_bcast.hrl").

-define(NS, <<"bcast">>).

%% Business-only metric surface. EMQX's own Prometheus endpoint already
%% exposes node-level system gauges (CPU, memory, connections,
%% messages.delivered); this registry carries only bcast-plugin business
%% counters and gauges.
%%
%% QoS1 delivery ledger (counted in "logical delivery" units, i.e. one
%% (BatchPub request x target device); counters are node-local, aggregate
%% the cluster with sum()):
%%
%%   batch_pub_qos1_wanted    durable ledger base: incremented once per
%%                            logical delivery at the mria commit point
%%                            (promotion), NOT at API acceptance - entries
%%                            dropped before commit never become wanted
%%   batch_pub_qos1_delivered actual PUBLISH sends (includes redeliveries
%%                            and the QoS0-subscription auto path)
%%   batch_pub_qos1_redelivered sends whose claim attempt number >= 2
%%   batch_pub_qos1_acked     PUBACKs matched to a pending delivery
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
        {"batch_pub_qos1_in", "BatchPub QoS=1 API requests"},
        {"batch_pub_qos1_wanted", "QoS=1 logical deliveries durably committed (ledger base)"},
        {"batch_pub_qos1_delivered", "QoS=1 PUBLISH sends to clients (includes redeliveries)"},
        {"batch_pub_qos1_redelivered",
            "QoS=1 PUBLISH sends of an already-attempted logical delivery (attempt >= 2)"},
        {"batch_pub_qos1_acked", "QoS=1 PUBACKs matched to a pending delivery"},
        {"batch_pub_qos1_auto_acked", "QoS=1 deliveries completed because subscription QoS is 0"},
        {"batch_pub_qos1_ttl_expired",
            "QoS=1 logical deliveries abandoned at TTL expiry without confirmation"},
        {"batch_pub_qos1_canceled",
            "QoS=1 logical deliveries removed by management delete/reset without confirmation"},
        {"batch_pub_qos1_enqueued", "QoS=1 requests accepted into the intake queue"},
        {"batch_pub_qos1_intake_rejected",
            "QoS=1 requests rejected because the intake queue is full"},
        {"batch_pub_qos1_promote_error", "QoS=1 promotion batch failures (retries exhausted)"},
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
            {"intake_depth", "QoS1 intake queue depth (requests awaiting promotion, node-local)"},
            {"batch_pub_qos1_queued",
                "QoS1 committed logical deliveries queued but not yet claimed (local shards; sum() over nodes)"},
            {"batch_pub_qos1_inflight",
                "QoS1 claimed logical deliveries not yet terminal (local shards; sum() over nodes)"}
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

%% Guarded local reset: refuse to reset while this node still holds
%% committed but not-yet-terminal logical deliveries (queued or in-flight),
%% because the ledger identity (wanted = terminal outcomes + live) only
%% holds for events observed after a reset.
-spec reset_guarded() -> ok | {error, {pending_deliveries, non_neg_integer(), non_neg_integer()}}.
reset_guarded() ->
    {Queued, Inflight} = emqx_bcast_index_owner:gauge_sample(),
    case Queued + Inflight of
        0 -> reset();
        _ -> {error, {pending_deliveries, Queued, Inflight}}
    end.

%% Cluster-wide guarded reset. The registry is per-node, so a partial reset
%% would leave a permanent gap in cross-node sums. Every running node is
%% reset, each guarded by its own local pending state; a node that reports
%% pending deliveries blocks the reset (returns an error for that node and
%% is left untouched). There is a small check-to-reset race with live
%% traffic on each node; the guard re-checks inside reset_guarded/0 on the
%% node itself and the operation is intended for maintenance windows.
-spec reset_cluster() ->
    {ok, [{node(), ok}]} | {error, [{node(), ok | {error, term()}}]}.
reset_cluster() ->
    Nodes = lists:usort([node() | emqx:running_nodes()]),
    Results = [{Node, rpc_reset_guarded(Node)} || Node <- Nodes],
    case [R || {_, {error, _} = R} <- Results] of
        [] -> {ok, Results};
        _ -> {error, Results}
    end.

rpc_reset_guarded(Node) when Node =:= node() ->
    reset_guarded();
rpc_reset_guarded(Node) ->
    try emqx_rpc:call(?MODULE, Node, ?MODULE, reset_guarded, [], ?BCAST_RPC_CALL_TIMEOUT_MS) of
        {badrpc, Reason} -> {error, {badrpc, Reason}};
        Result -> Result
    catch
        Error:Reason -> {error, {Error, Reason}}
    end.
