%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_types).

-moduledoc """
The module contains basic types for the Message Queue application.
""".

-include("emqx_mq_internal.hrl").

-type subscriber_ref() :: reference().
-type message_id() :: {emqx_ds:slab(), non_neg_integer()}.
-type monotonic_timestamp_ms() :: integer().
-type interval_ms() :: pos_integer().
-type channel_pid() :: pid().
-type consumer_ref() :: pid().
-type consumer_data() :: #{
    progress := emqx_mq_consumer_streams:progress()
}.
-type ack() :: ?MQ_ACK | ?MQ_NACK | ?MQ_REJECTED.
-type mq_topic() :: binary().
%% Name of the MQ, used to identify MQs by users.
%% Names are unique in a moment of time.
-type mq_name() :: binary().
%% Unique identifier of the MQ, used internally by the MQ server.
%% If a queue is deleted and then another one is created with the same name,
%% they are different queues with different identifiers.
-type mqid() :: binary().
-type consumer_sup_id() :: mq_topic().

-type dispatch_strategy() :: random | least_inflight | round_robin.

-type limits() :: #{
    max_shard_message_count := infinity | pos_integer(),
    max_shard_message_bytes := infinity | pos_integer()
}.

-type mq() :: #{
    id => mqid(),
    name := mq_name(),
    topic_filter := mq_topic(),
    is_lastvalue := boolean(),
    consumer_max_inactive := interval_ms(),
    ping_interval := interval_ms(),
    redispatch_interval := interval_ms(),
    dispatch_strategy := dispatch_strategy(),
    limits := limits(),
    local_max_inflight := non_neg_integer(),
    busy_session_retry_interval := interval_ms(),
    stream_max_buffer_size := non_neg_integer(),
    stream_max_unacked := non_neg_integer(),
    consumer_persistence_interval := interval_ms(),
    data_retention_period := interval_ms(),
    %% For lastvalue MQ.
    key_expression => emqx_variform:compiled(),
    _ => _
}.

%% Minimal data necessary to write data into the MQ.
-type mq_handle() :: #{
    id := mqid(),
    name := mq_name(),
    topic_filter := mq_topic(),
    is_lastvalue := boolean(),
    key_expression => emqx_variform:compiled() | undefined,
    limits := limits()
}.

-export_type([
    subscriber_ref/0,
    message_id/0,
    monotonic_timestamp_ms/0,
    channel_pid/0,
    consumer_ref/0,
    consumer_data/0,
    consumer_sup_id/0,
    ack/0,
    mq_topic/0,
    mq_name/0,
    mqid/0,
    mq/0,
    mq_handle/0,
    dispatch_strategy/0
]).
