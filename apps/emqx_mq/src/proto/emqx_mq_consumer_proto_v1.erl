%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0
]).

-export([
    mq_server_connect/4,
    mq_server_disconnect/3,
    mq_server_ack/5,
    mq_server_ping/3,
    mq_server_stop/2
]).

introduced_in() ->
    "6.0.0".

-spec mq_server_connect(
    node(), emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref(), emqx_types:clientid()
) -> ok.
mq_server_connect(Node, ConsumerRef, SubscriberRef, ClientId) ->
    erpc:cast(Node, emqx_mq_consumer, connect_v1, [ConsumerRef, SubscriberRef, ClientId]).

-spec mq_server_disconnect(node(), emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) ->
    ok.
mq_server_disconnect(Node, ConsumerRef, SubscriberRef) ->
    erpc:cast(Node, emqx_mq_consumer, disconnect_v1, [ConsumerRef, SubscriberRef]).

-spec mq_server_ack(
    node(),
    emqx_mq_types:consumer_ref(),
    emqx_mq_types:subscriber_ref(),
    emqx_mq_types:message_id(),
    emqx_mq_types:ack()
) -> ok.
mq_server_ack(Node, ConsumerRef, SubscriberRef, MessageId, Ack) ->
    erpc:cast(Node, emqx_mq_consumer, ack_v1, [ConsumerRef, SubscriberRef, MessageId, Ack]).

-spec mq_server_ping(node(), emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) -> ok.
mq_server_ping(Node, ConsumerRef, SubscriberRef) ->
    erpc:cast(Node, emqx_mq_consumer, ping_v1, [ConsumerRef, SubscriberRef]).

-spec mq_server_stop(node(), emqx_mq_types:consumer_ref()) -> ok.
mq_server_stop(Node, ConsumerRef) ->
    erpc:call(Node, emqx_mq_consumer, stop_v1, [ConsumerRef]).
