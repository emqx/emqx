%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-type ack() :: ?MQ_ACK | ?MQ_NACK.
-type mq_topic() :: binary().

-type dispatch_variform_expr() :: binary().

-type dispatch_strategy() :: random | least_inflight | {hash, dispatch_variform_expr()}.

-type mq() :: #{
    topic_filter := mq_topic(),
    is_compacted := boolean(),
    consumer_max_inactive_ms := interval_ms(),
    ping_interval_ms := interval_ms(),
    redispatch_interval_ms := interval_ms(),
    dispatch_strategy := dispatch_strategy(),
    local_max_inflight := non_neg_integer()
}.
-export_type([
    subscriber_ref/0,
    message_id/0,
    monotonic_timestamp_ms/0,
    channel_pid/0,
    consumer_ref/0,
    consumer_data/0,
    ack/0,
    mq_topic/0,
    mq/0
]).
