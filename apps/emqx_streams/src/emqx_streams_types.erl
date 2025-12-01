%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_types).

-moduledoc """
The module contains basic types for the Streams application.
""".

-include("emqx_streams_internal.hrl").

-type stream_topic() :: binary().
-type stream_id() :: binary().
-type interval_ms() :: pos_integer().

-type limits() :: #{
    max_shard_message_count := infinity | pos_integer(),
    max_shard_message_bytes := infinity | pos_integer()
}.

-type partition() :: emqx_ds:shard().
-type message_ts() :: emqx_ds:time().

-type stream() :: #{
    id := stream_id(),
    topic_filter := stream_topic(),
    is_lastvalue := boolean(),
    key_expression := emqx_variform:compiled(),
    limits := limits(),
    data_retention_period := interval_ms(),
    read_max_unacked := non_neg_integer()
}.

-export_type([
    stream_topic/0,
    stream_id/0,
    stream/0,
    limits/0,
    partition/0,
    message_ts/0,
    interval_ms/0
]).
