%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_types).

-moduledoc """
The module contains basic types for the Message Queue application.
""".

-include("emqx_mq_internal.hrl").
-elvis([{elvis_style, atom_naming_convention, disable}]).
-include("../gen_src/MQMessage.hrl").

-type subscriber_ref() :: reference().
-type message_id() :: {emqx_ds:slab(), non_neg_integer()}.
-type monotonic_timestamp_ms() :: integer().
-type channel_pid() :: pid().
-type consumer_ref() :: pid().
-type consumer_data() :: #{
    progress := emqx_mq_consumer_streams:progress()
}.
-type ack() :: ?MQ_ACK | ?MQ_NACK.
-type mq_topic() :: binary().
-type mq() :: #{
    topic_filter := mq_topic(),
    is_compacted := boolean(),
    consumer_max_inactive_ms := pos_integer(),
    consumer_ping_interval_ms := pos_integer()
}.

-type mq_message() :: #'MQMessage'{}.

-export_type([
    subscriber_ref/0,
    message_id/0,
    monotonic_timestamp_ms/0,
    channel_pid/0,
    consumer_ref/0,
    consumer_data/0,
    ack/0,
    mq_topic/0,
    mq/0,
    mq_message/0
]).
