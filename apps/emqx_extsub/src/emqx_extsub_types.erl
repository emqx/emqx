%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_types).

-moduledoc """
The module contains basic types for the External Subscription application.
""".

-include("emqx_extsub_internal.hrl").

-type handler_ref() :: reference().
-type monotonic_timestamp_ms() :: integer().
-type interval_ms() :: pos_integer().
-type channel_pid() :: pid().
-type topic_filter() :: emqx_types:topic().

-type handler_options() :: #{
    buffer_size => pos_integer(),
    _ => _
}.

-type ack() :: emqx_maybe:t(emqx_types:reason_code()).
-type message_id() :: term().

-export_type([
    message_id/0,
    handler_ref/0,
    monotonic_timestamp_ms/0,
    interval_ms/0,
    channel_pid/0,
    topic_filter/0,
    handler_options/0,
    ack/0
]).
