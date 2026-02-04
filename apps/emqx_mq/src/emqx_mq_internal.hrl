%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MQ_INTERNAL_HRL).
-define(EMQX_MQ_INTERNAL_HRL, true).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").

-define(tp_debug(KIND, EVENT), ?tp_ignore_side_effects_in_prod(KIND, EVENT)).
% -define(tp_debug(KIND, EVENT), ?tp(warning, KIND, EVENT)).

%% Queue name for pre-6.1.1 queues.
-define(DEFAULT_MQ_NAME, <<"default">>).

-define(MQ_HEADER_MESSAGE_ID, mq_msg_id).
-define(MQ_HEADER_SUBSCRIBER_ID, mq_sub_id).

-define(MQ_ACK, 0).
-define(MQ_NACK, 1).
-define(MQ_REJECTED, 2).

-record(info_to_mq_sub, {
    subscriber_ref :: emqx_mq_types:subscriber_ref(),
    info :: term()
}).

-record(info_mq_inspect, {
    receiver :: reference(),
    topic_filter :: emqx_mq_types:mq_topic() | undefined,
    name :: emqx_mq_types:mq_name()
}).

-record(mq_sub_connected, {
    consumer_ref :: emqx_mq_types:consumer_ref()
}).
-record(mq_sub_messages, {
    consumer_ref :: emqx_mq_types:consumer_ref(),
    messages :: [emqx_types:message()]
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

-define(MQ_GC_REGULAR, gc_regular).
-define(MQ_GC_LASTVALUE, gc_lastvalue).

-define(DEFAULT_MQ_LIMITS,
    (#{
        max_shard_message_count => infinity,
        max_shard_message_bytes => infinity
    })
).

-define(QUOTA_INDEX_TS, 1).
-define(DEFAULT_QUOTA_BUFFER_MAX_SIZE, 100).
-define(DEFAULT_QUOTA_BUFFER_FLUSH_INTERVAL, 1000).

-define(MQ_QUOTA_BUFFER, mq_message_quota_buffer).
-define(DEFAULT_QUOTA_BUFFER_POOL_SIZE, 10).

-define(LEGACY_QUEUE_NAME(TF), <<"/", (TF)/binary>>).

-endif.
