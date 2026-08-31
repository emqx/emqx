%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_metrics).

-export([init/0]).
-export([qos0_in/0, qos0_targeted/1, qos0_delivery_count/1]).
-export([
    qos1_in/0,
    qos1_wanted/1,
    qos1_acked/0,
    qos1_auto_acked/0,
    qos1_delivered/0,
    qos1_persist_error/0,
    intake_enqueued/0,
    intake_rejected/0,
    qos1_promoted/1,
    qos1_promote_error/0
]).
-export([broadcast_in/0, broadcast_error/0]).
-export([register_in/0, register_refresh/0, register_error/0]).
-export([collect/0]).

-include("emqx_bcast.hrl").

-define(NS, <<"bcast">>).

-spec init() -> ok.
init() ->
    %% emqx_bcast_app:start ensures prometheus is running before init/0 is
    %% called; the prometheus application owns and creates its internal ETS
    %% tables. Recreating them here coupled the plugin to prometheus internals
    %% and duplicated check-then-act table creation.
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
        {"batch_pub_qos1_wanted", "QoS=1 total wanted acks"},
        {"batch_pub_qos1_delivered", "QoS=1 deliveries to clients"},
        {"batch_pub_qos1_acked", "QoS=1 PUBACKs matched to a pending delivery"},
        {"batch_pub_qos1_auto_acked", "QoS=1 deliveries completed because subscription QoS is 0"},
        {"batch_pub_qos1_persist_error", "QoS=1 persistence failures returned to API callers"},
        {"batch_pub_qos1_enqueued", "QoS=1 requests accepted into the intake queue"},
        {"batch_pub_qos1_intake_rejected",
            "QoS=1 requests rejected because the intake queue is full"},
        {"batch_pub_qos1_promoted", "QoS=1 deliveries promoted into mria"},
        {"batch_pub_qos1_promote_error", "QoS=1 promotion batch failures"},
        {"broadcast_pub_in", "PubBroadcast API requests"},
        {"broadcast_pub_error", "PubBroadcast errors"},
        {"register_message_in", "RegisterMessage API requests"},
        {"register_message_refresh", "RegisterMessage TTL refresh"},
        {"register_message_error", "RegisterMessage errors"},
        {"fanout_delivered", "Broker messages.delivered delta (cluster fanout, node-local view)"}
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
            {"node_cpu_use", "Node BEAM CPU utilization percent (delta-based)"},
            {"node_memory", "Node BEAM total memory bytes"},
            {"connections", "Node-local MQTT connection count (emqx_channel table size)"}
        ]
    ],
    ok.

%% Node-level gauges are reported at collect time (they are deltas/levels,
%% not event counters). The fanout counter is fed the broker
%% messages.delivered delta between scrapes so prometheus rate() works.
report_node_gauges() ->
    NowWall = element(1, erlang:statistics(wall_clock)),
    NowRun = element(1, erlang:statistics(runtime)),
    %% Cluster-aggregated broker fanout (same value on every node; the
    %% dashboard must use max()/avg() over instances, not sum()).
    Fanout = emqx_metrics:val(global, 'messages.delivered'),
    %% State must live across collect calls; each scrape runs in a fresh
    %% API handler process, so process dictionary would reset every time.
    Key = {?MODULE, node_stats},
    Prev =
        case persistent_term:get(Key, undefined) of
            undefined -> {NowWall, NowRun, Fanout};
            P -> P
        end,
    {PrevWall, PrevRun, PrevFanout} = Prev,
    persistent_term:put(Key, {NowWall, NowRun, Fanout}),
    Cpu =
        case NowWall > PrevWall of
            true -> (NowRun - PrevRun) * 100.0 / (NowWall - PrevWall);
            false -> 0.0
        end,
    prometheus_gauge:set(?BCAST_REGISTRY, mname("node_cpu_use"), [], Cpu),
    prometheus_gauge:set(?BCAST_REGISTRY, mname("node_memory"), [], erlang:memory(total)),
    prometheus_gauge:set(
        ?BCAST_REGISTRY,
        mname("connections"),
        [],
        case ets:info(emqx_channel, size) of
            undefined -> 0;
            N -> N
        end
    ),
    case Fanout >= PrevFanout of
        true ->
            prometheus_counter:inc(
                ?BCAST_REGISTRY, mname("fanout_delivered"), [], Fanout - PrevFanout
            );
        false ->
            ok
    end,
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
-spec qos1_acked() -> ok.
qos1_acked() -> c("batch_pub_qos1_acked").
-spec qos1_auto_acked() -> ok.
qos1_auto_acked() -> c("batch_pub_qos1_auto_acked").
-spec qos1_delivered() -> ok.
qos1_delivered() -> c("batch_pub_qos1_delivered").
-spec qos1_persist_error() -> ok.
qos1_persist_error() -> c("batch_pub_qos1_persist_error").
-spec intake_enqueued() -> ok.
intake_enqueued() -> c("batch_pub_qos1_enqueued").
-spec intake_rejected() -> ok.
intake_rejected() -> c("batch_pub_qos1_intake_rejected").
-spec qos1_promoted(non_neg_integer()) -> ok.
qos1_promoted(N) -> c("batch_pub_qos1_promoted", N).
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
    report_node_gauges(),
    prometheus_text_format:format(?BCAST_REGISTRY).
