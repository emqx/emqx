%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MQ_INTERNAL_HRL).
-define(EMQX_MQ_INTERNAL_HRL, true).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(tp_debug(KIND, EVENT), ?tp_ignore_side_effects_in_prod(KIND, EVENT)).
% -define(tp_debug(KIND, EVENT), ?tp(warning, KIND, EVENT)).

-define(MQ_HEADER_MESSAGE_ID, mq_msg_id).
-define(MQ_HEADER_SUBSCRIBER_ID, mq_sub_id).

-define(MQ_ACK, 0).
-define(MQ_NACK, 1).
-define(MQ_REJECTED, 2).

-record(info_to_mq_sub, {
    subscriber_ref :: emqx_mq_types:subscriber_ref(),
    info :: term()
}).

-record(info_mq_inspect, {receiver :: reference(), topic_filter :: emqx_mq_types:mq_topic()}).

-record(mq_sub_connected, {
    consumer_ref :: emqx_mq_types:consumer_ref()
}).
-record(mq_sub_message, {
    consumer_ref :: emqx_mq_types:consumer_ref(),
    message :: term()
}).
-record(mq_sub_ping, {}).

-record(info_to_mq_server, {
    message :: term()
}).
-record(mq_server_connect, {
    subscriber_ref :: emqx_mq_types:subscriber_ref(),
    client_id :: emqx_types:clientid()
}).
-record(mq_server_disconnect, {
    subscriber_ref :: emqx_mq_types:subscriber_ref()
}).
-record(mq_server_ack, {
    subscriber_ref :: emqx_mq_types:subscriber_ref(),
    message_id :: emqx_mq_types:message_id(),
    ack :: emqx_mq_types:ack()
}).
-record(mq_server_ping, {
    subscriber_ref :: emqx_mq_types:subscriber_ref()
}).

-define(MQ_KEY_USER_PROPERTY, <<"mq-key">>).

-define(MQ_CONSUMER_DB, mq_consumer).
-define(MQ_MESSAGE_LASTVALUE_DB, mq_message_lastvalue).
-define(MQ_MESSAGE_REGULAR_DB, mq_message_regular).

-record(claim, {
    consumer_ref :: emqx_mq_types:consumer_ref(),
    last_seen_timestamp :: non_neg_integer()
}).

-record(tombstone, {
    last_seen_timestamp :: non_neg_integer()
}).

-define(MQ_GC_REGULAR, gc_regular).
-define(MQ_GC_LASTVALUE, gc_lastvalue).

-endif.
