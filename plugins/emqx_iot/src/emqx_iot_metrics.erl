%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_metrics).

-export([init/0, ensure/0]).

%% QoS=0: API
-export([qos0_in/0, qos0_error/0]).
%% QoS=0: per-device
-export([qos0_targeted/1, qos0_delivered/0, qos0_skipped/0]).

%% QoS=1: API
-export([qos1_in/0, qos1_error/0, qos1_succeed/0, qos1_incomplete/0]).
%% QoS=1: per-device
-export([qos1_wanted/1, qos1_acked/0, qos1_delivered_inline/0,
         qos1_stored_offline/0, qos1_replayed/0, qos1_msg_error/0,
         qos1_msg_incomplete/0]).

%% Broadcast: API
-export([broadcast_in/0, broadcast_error/0, broadcast_succeed/0]).
%% Broadcast: per-device
-export([broadcast_devices_online/1, broadcast_delivery_count/1]).

%% Register: API
-export([register_in/0, register_refresh/0, register_error/0]).

%% Gauge
-export([pending_set/1]).

%% Prometheus export
-export([collect/0]).

-include("emqx_iot.hrl").

-define(P, <<"iot_mq">>).

%% metric names following: <prefix>.<layer>
%%   layer = empty for API-level counters
%%   layer = msg for per-device/message-level counters

-define(api(N), <<?P/binary, ".", N/binary>>).
-define(msg(N), <<?P/binary, ".msg.", N/binary>>).

init() ->
    declare_counters(),
    declare_gauges(),
    ok.

ensure() -> init().

declare_counters() ->
    Cs = [
        %% QoS=0 API
        {?api(<<"batch_pub_qos0.in">>), <<"BatchPub QoS=0 API calls">>},
        {?api(<<"batch_pub_qos0.error">>), <<"BatchPub QoS=0 API errors">>},
        {?api(<<"batch_pub_qos0.succeed">>), <<"BatchPub QoS=0 API success">>},
        %% QoS=0 msg
        {?msg(<<"batch_pub_qos0.targeted">>), <<"QoS=0 devices targeted">>},
        {?msg(<<"batch_pub_qos0.delivered">>), <<"QoS=0 devices delivered">>},
        {?msg(<<"batch_pub_qos0.skipped">>), <<"QoS=0 devices skipped">>},

        %% QoS=1 API
        {?api(<<"batch_pub_qos1.in">>), <<"BatchPub QoS=1 API calls">>},
        {?api(<<"batch_pub_qos1.error">>), <<"BatchPub QoS=1 API errors">>},
        {?api(<<"batch_pub_qos1.succeed">>), <<"BatchPub QoS=1 API success">>},
        {?api(<<"batch_pub_qos1.incomplete">>), <<"BatchPub QoS=1 API incomplete">>},
        %% QoS=1 msg
        {?msg(<<"batch_pub_qos1.wanted">>), <<"QoS=1 total wanted acks">>},
        {?msg(<<"batch_pub_qos1.acked">>), <<"QoS=1 acks received">>},
        {?msg(<<"batch_pub_qos1.delivered_inline">>), <<"QoS=1 inline deliveries">>},
        {?msg(<<"batch_pub_qos1.stored_offline">>), <<"QoS=1 stored for offline replay">>},
        {?msg(<<"batch_pub_qos1.replayed">>), <<"QoS=1 replayed on reconnect">>},
        {?msg(<<"batch_pub_qos1.error">>), <<"QoS=1 delivery errors">>},
        {?msg(<<"batch_pub_qos1.incomplete">>), <<"QoS=1 delivery incomplete">>},

        %% Broadcast API
        {?api(<<"broadcast_pub.in">>), <<"PubBroadcast API calls">>},
        {?api(<<"broadcast_pub.error">>), <<"PubBroadcast API errors">>},
        {?api(<<"broadcast_pub.succeed">>), <<"PubBroadcast API success">>},
        %% Broadcast msg
        {?msg(<<"broadcast_pub.devices_online">>), <<"PubBroadcast devices online">>},
        {?msg(<<"broadcast_pub.delivery_count">>), <<"PubBroadcast delivery count">>},

        %% Register API
        {?api(<<"register_message.in">>), <<"RegisterMessage API calls">>},
        {?api(<<"register_message.refresh">>), <<"RegisterMessage TTL refresh">>},
        {?api(<<"register_message.error">>), <<"RegisterMessage API errors">>}
    ],
    [prometheus_counter:declare([{name, N}, {help, H}]) || {N, H} <- Cs].

declare_gauges() ->
    try
        prometheus_gauge:declare([{name, ?msg(<<"batch_pub_qos1.pending">>)}, {help, <<"QoS=1 pending deliveries (water level)">>}])
    catch _:_ -> ok end.

%% helpers
c(N) -> try prometheus_counter:inc(N) catch _:_ -> ok end.
c(N, V) -> try prometheus_counter:inc(N, V) catch _:_ -> ok end.

%% QoS=0
qos0_in() -> c(?api(<<"batch_pub_qos0.in">>)).
qos0_error() -> c(?api(<<"batch_pub_qos0.error">>)).
qos0_targeted(N) -> c(?msg(<<"batch_pub_qos0.targeted">>), N).
qos0_delivered() -> c(?msg(<<"batch_pub_qos0.delivered">>)).
qos0_skipped() -> c(?msg(<<"batch_pub_qos0.skipped">>)).

%% QoS=1
qos1_in() -> c(?api(<<"batch_pub_qos1.in">>)).
qos1_error() -> c(?api(<<"batch_pub_qos1.error">>)).
qos1_succeed() -> c(?api(<<"batch_pub_qos1.succeed">>)).
qos1_incomplete() -> c(?api(<<"batch_pub_qos1.incomplete">>)).
qos1_wanted(N) -> c(?msg(<<"batch_pub_qos1.wanted">>), N).
qos1_acked() -> c(?msg(<<"batch_pub_qos1.acked">>)).
qos1_delivered_inline() -> c(?msg(<<"batch_pub_qos1.delivered_inline">>)).
qos1_stored_offline() -> c(?msg(<<"batch_pub_qos1.stored_offline">>)).
qos1_replayed() -> c(?msg(<<"batch_pub_qos1.replayed">>)).
qos1_msg_error() -> c(?msg(<<"batch_pub_qos1.error">>)).
qos1_msg_incomplete() -> c(?msg(<<"batch_pub_qos1.incomplete">>)).

%% Broadcast
broadcast_in() -> c(?api(<<"broadcast_pub.in">>)).
broadcast_error() -> c(?api(<<"broadcast_pub.error">>)).
broadcast_succeed() -> c(?api(<<"broadcast_pub.succeed">>)).
broadcast_devices_online(N) -> c(?msg(<<"broadcast_pub.devices_online">>), N).
broadcast_delivery_count(N) -> c(?msg(<<"broadcast_pub.delivery_count">>), N).

%% Register
register_in() -> c(?api(<<"register_message.in">>)).
register_refresh() -> c(?api(<<"register_message.refresh">>)).
register_error() -> c(?api(<<"register_message.error">>)).

%% Gauge
pending_set(N) -> try prometheus_gauge:set(?msg(<<"batch_pub_qos1.pending">>), N) catch _:_ -> ok end.

%% Export
collect() -> try prometheus_text_format:format() catch _:_ -> <<>> end.