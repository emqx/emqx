%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_types).

-moduledoc """
The module contains basic types for the Message Queue application.
""".

-include("emqx_mq_internal.hrl").

-type subscriber_id() :: reference().
-type message_id() :: integer().
-type monotonic_timestamp_ms() :: integer().
-type channel_pid() :: pid().
-type consumer_pid() :: pid().
-type ack() :: ?MQ_ACK | ?MQ_NACK.

-export_type([
    subscriber_id/0,
    message_id/0,
    monotonic_timestamp_ms/0,
    channel_pid/0,
    consumer_pid/0,
    ack/0
]).
