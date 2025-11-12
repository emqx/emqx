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

-type limits() :: #{
    max_shard_message_count := infinity | pos_integer(),
    max_shard_message_bytes := infinity | pos_integer()
}.

-type partition() :: emqx_ds:shard().
-type offset() :: {emqx_ds:generation(), emqx_ds:time()} | earliest | latest.
-type stream() :: stream_handle().

%% Minimal data necessary to write data into the MQ.
-type stream_handle() :: #{
    id := stream_id(),
    topic_filter := stream_topic(),
    is_lastvalue := boolean(),
    key_expression => emqx_variform:compiled() | undefined,
    limits := limits()
}.

-export_type([
    stream_topic/0,
    stream_id/0,
    stream_handle/0,
    stream/0,
    limits/0,
    partition/0,
    offset/0
]).
