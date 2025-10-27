%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_types).

-moduledoc """
The module contains basic types for the Message Queue application.
""".

-include("emqx_extsub_internal.hrl").

-type subscriber_ref() :: reference().
-type monotonic_timestamp_ms() :: integer().
-type interval_ms() :: pos_integer().
-type channel_pid() :: pid().
-type topic_filter() :: emqx_types:topic().

-type options() :: #{
    local_max_inflight := non_neg_integer(),
    busy_session_retry_interval := interval_ms(),
    _ => _
}.

-type ack() :: emqx_maybe:t(emqx_types:reason_code()).
-type message_id() :: term().

-export_type([
    message_id/0,
    subscriber_ref/0,
    monotonic_timestamp_ms/0,
    interval_ms/0,
    channel_pid/0,
    topic_filter/0,
    options/0,
    ack/0
]).
