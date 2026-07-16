%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_metrics).

-export([init/0, ensure/0]).
-export([qos0_in/0, qos0_error/0]).
-export([qos0_targeted/1, qos0_delivered/0, qos0_skipped/0]).
-export([qos1_in/0, qos1_error/0, qos1_succeed/0, qos1_incomplete/0]).
-export([
    qos1_wanted/1,
    qos1_acked/0,
    qos1_delivered_inline/0,
    qos1_stored_offline/0,
    qos1_replayed/0,
    qos1_msg_error/0,
    qos1_msg_incomplete/0
]).
-export([broadcast_in/0, broadcast_error/0, broadcast_succeed/0]).
-export([broadcast_devices_online/1, broadcast_delivery_count/1]).
-export([register_in/0, register_refresh/0, register_error/0]).
-export([pending_set/1]).
-export([collect/0]).

-include("emqx_iot.hrl").

-define(NS, <<"iot_mq">>).

init() ->
    create_tables_if_needed(),
    declare_counters(),
    declare_gauges(),
    ok.

ensure() -> init().

create_tables_if_needed() ->
    case ets:info(prometheus_registry_table) of
        undefined ->
            ets:new(prometheus_registry_table, [
                bag, named_table, public, {read_concurrency, true}
            ]);
        _ ->
            ok
    end,
    case ets:info(prometheus_counter_table) of
        undefined ->
            ets:new(prometheus_counter_table, [
                set, named_table, public, {write_concurrency, true}
            ]);
        _ ->
            ok
    end,
    case ets:info(prometheus_gauge_table) of
        undefined ->
            ets:new(prometheus_gauge_table, [
                set, named_table, public, {write_concurrency, true}
            ]);
        _ ->
            ok
    end.

mname(Suffix) when is_list(Suffix) -> <<?NS/binary, "_", (list_to_binary(Suffix))/binary>>;
mname(Suffix) when is_binary(Suffix) -> <<?NS/binary, "_", Suffix/binary>>.

declare_counters() ->
    Cs = [
        {"batch_pub_qos0_in", "BatchPub QoS=0 API requests"},
        {"batch_pub_qos0_error", "BatchPub QoS=0 API errors"},
        {"batch_pub_qos0_targeted", "QoS=0 devices targeted"},
        {"batch_pub_qos0_delivered", "QoS=0 devices delivered"},
        {"batch_pub_qos0_skipped", "QoS=0 devices skipped"},
        {"batch_pub_qos1_in", "BatchPub QoS=1 API requests"},
        {"batch_pub_qos1_error", "BatchPub QoS=1 API errors"},
        {"batch_pub_qos1_succeed", "BatchPub QoS=1 API success"},
        {"batch_pub_qos1_incomplete", "BatchPub QoS=1 incomplete"},
        {"batch_pub_qos1_delivered_inline", "QoS=1 inline deliveries"},
        {"batch_pub_qos1_stored_offline", "QoS=1 stored for offline"},
        {"batch_pub_qos1_wanted", "QoS=1 total wanted acks"},
        {"batch_pub_qos1_acked", "QoS=1 acks received"},
        {"batch_pub_qos1_replayed", "QoS=1 replayed on reconnect"},
        {"batch_pub_qos1_msg_error", "QoS=1 delivery errors"},
        {"batch_pub_qos1_msg_incomplete", "QoS=1 delivery incomplete"},
        {"broadcast_pub_in", "PubBroadcast API requests"},
        {"broadcast_pub_error", "PubBroadcast errors"},
        {"broadcast_pub_succeed", "PubBroadcast success"},
        {"broadcast_pub_devices_online", "PubBroadcast devices online"},
        {"broadcast_pub_delivery_count", "PubBroadcast deliveries"},
        {"register_message_in", "RegisterMessage API requests"},
        {"register_message_refresh", "RegisterMessage TTL refresh"},
        {"register_message_error", "RegisterMessage errors"}
    ],
    [
        prometheus_counter:declare([
            {registry, ?IOT_MQ_REGISTRY},
            {name, mname(N)},
            {help, list_to_binary(H)}
        ])
     || {N, H} <- Cs
    ],
    ok.

declare_gauges() ->
    prometheus_gauge:declare([
        {registry, ?IOT_MQ_REGISTRY},
        {name, mname("batch_pub_qos1_pending")},
        {help, <<"QoS=1 pending deliveries (water level)">>}
    ]).

%% helpers
c(N) -> prometheus_counter:inc(?IOT_MQ_REGISTRY, mname(N), [], 1).
c(N, V) -> prometheus_counter:inc(?IOT_MQ_REGISTRY, mname(N), [], V).

qos0_in() -> c("batch_pub_qos0_in").
qos0_error() -> c("batch_pub_qos0_error").
qos0_targeted(N) -> c("batch_pub_qos0_targeted", N).
qos0_delivered() -> c("batch_pub_qos0_delivered").
qos0_skipped() -> c("batch_pub_qos0_skipped").

qos1_in() -> c("batch_pub_qos1_in").
qos1_error() -> c("batch_pub_qos1_error").
qos1_succeed() -> c("batch_pub_qos1_succeed").
qos1_incomplete() -> c("batch_pub_qos1_incomplete").
qos1_wanted(N) -> c("batch_pub_qos1_wanted", N).
qos1_acked() -> c("batch_pub_qos1_acked").
qos1_delivered_inline() -> c("batch_pub_qos1_delivered_inline").
qos1_stored_offline() -> c("batch_pub_qos1_stored_offline").
qos1_replayed() -> c("batch_pub_qos1_replayed").
qos1_msg_error() -> c("batch_pub_qos1_msg_error").
qos1_msg_incomplete() -> c("batch_pub_qos1_msg_incomplete").

broadcast_in() -> c("broadcast_pub_in").
broadcast_error() -> c("broadcast_pub_error").
broadcast_succeed() -> c("broadcast_pub_succeed").
broadcast_devices_online(N) -> c("broadcast_pub_devices_online", N).
broadcast_delivery_count(N) -> c("broadcast_pub_delivery_count", N).

register_in() -> c("register_message_in").
register_refresh() -> c("register_message_refresh").
register_error() -> c("register_message_error").

pending_set(N) -> prometheus_gauge:set(?IOT_MQ_REGISTRY, mname("batch_pub_qos1_pending"), [], N).

collect() -> prometheus_text_format:format(?IOT_MQ_REGISTRY).
