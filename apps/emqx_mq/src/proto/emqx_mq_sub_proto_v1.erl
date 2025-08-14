%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sub_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0
]).

-export([
    mq_sub_connected/3,
    mq_sub_message/3,
    mq_sub_ping/2
]).

introduced_in() ->
    "6.0.0".

-spec mq_sub_connected(node(), emqx_mq_types:subscriber_ref(), emqx_mq_types:consumer_ref()) -> ok.
mq_sub_connected(Node, SubscriberRef, ConsumerRef) ->
    erpc:cast(Node, emqx_mq_sub, connected, [SubscriberRef, ConsumerRef]).

-spec mq_sub_message(node(), emqx_mq_types:subscriber_ref(), emqx_types:message()) -> ok.
mq_sub_message(Node, SubscriberRef, Message) ->
    erpc:cast(Node, emqx_mq_sub, message, [SubscriberRef, Message]).

-spec mq_sub_ping(node(), emqx_mq_types:subscriber_ref()) -> ok.
mq_sub_ping(Node, SubscriberRef) ->
    erpc:cast(Node, emqx_mq_sub, ping, [SubscriberRef]).
