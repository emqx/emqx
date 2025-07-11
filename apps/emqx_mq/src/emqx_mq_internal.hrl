%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MQ_INTERNAL_HRL).
-define(EMQX_MQ_INTERNAL_HRL, true).

-define(VIA_GPROC(Id), {via, gproc, {n, l, Id}}).

-define(MQ_HEADER_MESSAGE_ID, mq_msg_id).
-define(MQ_HEADER_SUBSCRIBER_ID, mq_sub_id).

-define(MQ_ACK, 0).
-define(MQ_NACK, 1).

-define(MQ_MESSAGE(MESSAGE), {mq_message, MESSAGE}).
-define(MQ_PING_SUBSCRIBER(SUBSCRIBER_ID), {mq_ping, SUBSCRIBER_ID}).
-define(MQ_TIMEOUT(SUBSCRIBER_ID, MESSAGE), {mq_timeout, SUBSCRIBER_ID, MESSAGE}).

%% TODO
%% configurable

%% 10 seconds
-define(DEFAULT_SUBSCRIBER_TIMEOUT, 10000).
%% 10 seconds
-define(DEFAULT_CONSUMER_TIMEOUT, 10000).
%% 5 seconds
-define(DEFAULT_PING_INTERVAL, 5000).

-endif.
