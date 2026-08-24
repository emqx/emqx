%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_metrics).

-export([init/0]).
-export([qos0_in/0, qos0_targeted/1, qos0_delivery_count/1]).
-export([qos1_in/0, qos1_wanted/1, qos1_acked/0, qos1_delivered/0]).
-export([broadcast_in/0, broadcast_error/0]).
-export([register_in/0, register_refresh/0, register_error/0]).
-export([collect/0]).

-include("emqx_bcast.hrl").

-define(NS, <<"bcast">>).

init() ->
    create_tables_if_needed(),
    declare_counters(),
    ok.

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
    end.

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
        {"batch_pub_qos1_acked", "QoS=1 acks received"},
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

%% helpers
c(N) -> prometheus_counter:inc(?BCAST_REGISTRY, mname(N), [], 1).
c(N, V) -> prometheus_counter:inc(?BCAST_REGISTRY, mname(N), [], V).

qos0_in() -> c("batch_pub_qos0_in").
qos0_targeted(N) -> c("batch_pub_qos0_targeted", N).
qos0_delivery_count(N) -> c("qos0_delivery_count", N).

qos1_in() -> c("batch_pub_qos1_in").
qos1_wanted(N) -> c("batch_pub_qos1_wanted", N).
qos1_acked() -> c("batch_pub_qos1_acked").
qos1_delivered() -> c("batch_pub_qos1_delivered").

broadcast_in() -> c("broadcast_pub_in").
broadcast_error() -> c("broadcast_pub_error").

register_in() -> c("register_message_in").
register_refresh() -> c("register_message_refresh").
register_error() -> c("register_message_error").

collect() -> prometheus_text_format:format(?BCAST_REGISTRY).
