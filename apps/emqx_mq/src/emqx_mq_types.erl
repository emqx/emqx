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
-type channel_pid() :: pid().
-type consumer_ref() :: pid().
-type consumer_data() :: #{}.
-type ack() :: ?MQ_ACK | ?MQ_NACK.
-type mq_topic() :: binary().
-type mq() :: #{
    topic_filter := mq_topic(),
    is_compacted := boolean()
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
