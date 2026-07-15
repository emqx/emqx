%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_metrics).

-export([init/0, ensure/0]).

%% counters
-export([inc_batch_pub_qos0_in/0, inc_batch_pub_qos0_targeted/1,
         inc_batch_pub_qos0_delivered/0, inc_batch_pub_qos0_skipped/0,
         inc_batch_pub_qos0_error/0]).

-export([inc_batch_pub_qos1_in/0, inc_batch_pub_qos1_error/0,
         inc_batch_pub_qos1_succeed/0, inc_batch_pub_qos1_incomplete/0,
         inc_qos1_delivered_inline/0, inc_qos1_stored_offline/0,
         inc_msg_wanted/1, inc_msg_acked/0, inc_msg_replayed/0,
         inc_msg_error/0, inc_msg_incomplete/0]).

-export([inc_broadcast_in/0, inc_broadcast_devices_online/1,
         inc_broadcast_delivery_count/1, inc_broadcast_error/0]).

-export([inc_register_message_in/0, inc_register_message_refresh/0,
         inc_register_message_error/0]).

%% backward-compat aliases used by batch_pub.erl
-export([inc_qos0_targeted/1, inc_qos0_delivered/0, inc_qos0_skipped/0]).

%% gauge
-export([inc_pending/1, dec_pending/1, set_pending/1]).

%% prometheus export
-export([collect/0, name/1]).

-include("emqx_iot.hrl").

-define(NS, <<"iot_mq">>).

init() ->
    declare_counters(),
    declare_gauges(),
    ok.

ensure() -> init().

declare_counters() ->
    Cs = [
        {<<"batch_pub_qos0_in">>, <<"BatchPub QoS=0 API requests">>},
        {<<"batch_pub_qos0_targeted">>, <<"QoS=0 devices targeted">>},
        {<<"batch_pub_qos0_delivered">>, <<"QoS=0 devices delivered">>},
        {<<"batch_pub_qos0_skipped">>, <<"QoS=0 devices skipped">>},
        {<<"batch_pub_qos0_error">>, <<"QoS=0 API errors">>},
        {<<"batch_pub_qos1_in">>, <<"BatchPub QoS=1 API requests">>},
        {<<"batch_pub_qos1_error">>, <<"QoS=1 API errors">>},
        {<<"batch_pub_qos1_succeed">>, <<"QoS=1 API success">>},
        {<<"batch_pub_qos1_incomplete">>, <<"QoS=1 incomplete">>},
        {<<"batch_pub_qos1_delivered_inline">>, <<"QoS=1 inline deliveries">>},
        {<<"batch_pub_qos1_stored_offline">>, <<"QoS=1 stored for offline">>},
        {<<"batch_pub_qos1_msg_wanted">>, <<"QoS=1 total wanted acks">>},
        {<<"batch_pub_qos1_msg_acked">>, <<"QoS=1 acks received">>},
        {<<"batch_pub_qos1_msg_replayed">>, <<"QoS=1 replayed on reconnect">>},
        {<<"batch_pub_qos1_msg_error">>, <<"QoS=1 delivery errors">>},
        {<<"batch_pub_qos1_msg_incomplete">>, <<"QoS=1 delivery incomplete">>},
        {<<"broadcast_pub_in">>, <<"PubBroadcast API requests">>},
        {<<"broadcast_pub_devices_online">>, <<"PubBroadcast devices online">>},
        {<<"broadcast_pub_delivery_count">>, <<"PubBroadcast deliveries">>},
        {<<"broadcast_pub_error">>, <<"PubBroadcast errors">>},
        {<<"broadcast_pub_succeed">>, <<"PubBroadcast success">>},
        {<<"register_message_in">>, <<"RegisterMessage API requests">>},
        {<<"register_message_refresh">>, <<"RegisterMessage TTL refresh">>},
        {<<"register_message_error">>, <<"RegisterMessage errors">>}
    ],
    [try prometheus_counter:declare([{name, name(N)}, {help, H}]) catch _:_ -> ok end || {N, H} <- Cs].

declare_gauges() ->
    try prometheus_gauge:declare([{name, name(<<"batch_pub_qos1_msg_pending">>)}, {help, <<"QoS=1 pending deliveries (water level)">>}]) catch _:_ -> ok end.

name(Suffix) -> <<?NS/binary, "_", Suffix/binary>>.

%% counter helpers
c(Name) -> try prometheus_counter:inc(name(Name)) catch _:_ -> ok end.
c(Name, N) -> try prometheus_counter:inc(name(Name), N) catch _:_ -> ok end.

inc_batch_pub_qos0_in() -> c(<<"batch_pub_qos0_in">>).
inc_batch_pub_qos0_targeted(N) -> c(<<"batch_pub_qos0_targeted">>, N).
inc_batch_pub_qos0_delivered() -> c(<<"batch_pub_qos0_delivered">>).
inc_batch_pub_qos0_skipped() -> c(<<"batch_pub_qos0_skipped">>).
inc_batch_pub_qos0_error() -> c(<<"batch_pub_qos0_error">>).

inc_batch_pub_qos1_in() -> c(<<"batch_pub_qos1_in">>).
inc_batch_pub_qos1_error() -> c(<<"batch_pub_qos1_error">>).
inc_batch_pub_qos1_succeed() -> c(<<"batch_pub_qos1_succeed">>).
inc_batch_pub_qos1_incomplete() -> c(<<"batch_pub_qos1_incomplete">>).
inc_qos1_delivered_inline() -> c(<<"batch_pub_qos1_delivered_inline">>).
inc_qos1_stored_offline() -> c(<<"batch_pub_qos1_stored_offline">>).
inc_msg_wanted(N) -> c(<<"batch_pub_qos1_msg_wanted">>, N).
inc_msg_acked() -> c(<<"batch_pub_qos1_msg_acked">>).
inc_msg_replayed() -> c(<<"batch_pub_qos1_msg_replayed">>).
inc_msg_error() -> c(<<"batch_pub_qos1_msg_error">>).
inc_msg_incomplete() -> c(<<"batch_pub_qos1_msg_incomplete">>).

inc_broadcast_in() -> c(<<"broadcast_pub_in">>).
inc_broadcast_devices_online(N) -> c(<<"broadcast_pub_devices_online">>, N).
inc_broadcast_delivery_count(N) -> c(<<"broadcast_pub_delivery_count">>, N).
inc_broadcast_error() -> c(<<"broadcast_pub_error">>).

inc_register_message_in() -> c(<<"register_message_in">>).
inc_register_message_refresh() -> c(<<"register_message_refresh">>).
inc_register_message_error() -> c(<<"register_message_error">>).

%% backward-compat
inc_qos0_targeted(N) -> inc_batch_pub_qos0_targeted(N).
inc_qos0_delivered() -> inc_batch_pub_qos0_delivered().
inc_qos0_skipped() -> inc_batch_pub_qos0_skipped().

%% gauge
inc_pending(N) -> try prometheus_gauge:inc(name(<<"batch_pub_qos1_msg_pending">>), N) catch _:_ -> ok end.
dec_pending(N) -> try prometheus_gauge:dec(name(<<"batch_pub_qos1_msg_pending">>), N) catch _:_ -> ok end.
set_pending(N) -> try prometheus_gauge:set(name(<<"batch_pub_qos1_msg_pending">>), N) catch _:_ -> ok end.

%% export
collect() ->
    try prometheus_text_format:format() catch _:_ -> <<>> end.