%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_prop).

-moduledoc """
The module contains accessor functions for the Streams/Stream handles.
""".

-export([
    id/1,
    is_limited/1,
    is_lastvalue/1,
    is_append_only/1,
    topic_filter/1,
    quota_index_opts/1,
    max_unacked/1,
    data_retention_period/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec is_limited(emqx_streams_types:stream() | emqx_streams_types:stream_handle()) -> boolean().
is_limited(
    #{limits := #{max_shard_message_count := infinity, max_shard_message_bytes := infinity}} =
        _Stream
) ->
    false;
is_limited(_Stream) ->
    true.

-spec is_lastvalue(emqx_streams_types:stream() | emqx_streams_types:stream_handle()) -> boolean().
is_lastvalue(#{is_lastvalue := IsLastvalue} = _Stream) ->
    IsLastvalue.

-spec topic_filter(emqx_streams_types:stream() | emqx_streams_types:stream_handle()) ->
    emqx_types:topic().
topic_filter(#{topic_filter := TopicFilter} = _Stream) ->
    TopicFilter.

-spec id(emqx_streams_types:stream_handle() | emqx_streams_types:stream()) ->
    emqx_streams_types:stream_id().
id(#{id := ID} = _Stream) ->
    ID.

-spec is_append_only(emqx_streams_types:stream() | emqx_streams_types:stream_handle()) -> boolean().
is_append_only(Stream) ->
    (not is_limited(Stream)) andalso (not is_lastvalue(Stream)).

-spec quota_index_opts(emqx_streams_types:stream() | emqx_streams_types:stream_handle()) ->
    %% move quota out of streams's
    %% emqx_mq_message_quota_index:opts().
    term().
quota_index_opts(Stream) ->
    maps:from_list(
        limit_to_index_opt(bytes, Stream) ++
            limit_to_index_opt(count, Stream)
    ).

-spec max_unacked(emqx_streams_types:stream()) ->
    non_neg_integer().
max_unacked(_Stream) ->
    %% TODO: Implement max unacked
    100.

-spec data_retention_period(emqx_streams_types:stream()) ->
    non_neg_integer().
data_retention_period(_Stream) ->
    %% TODO: Implement data retention period

    %% 7 days
    7 * 24 * 60 * 60 * 1000.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

limit_to_index_opt(_Name, infinity) ->
    [];
limit_to_index_opt(Name, Limit) ->
    [{Name, #{max => Limit, threshold => quota_threshold(Limit)}}].

quota_threshold(Limit) ->
    ThresholdPercentage = threshold_percentage(),
    max(1, ThresholdPercentage * Limit div 100).

threshold_percentage() ->
    %% TODO
    %% emqx:get_config([streams, quota, threshold_percentage]).
    10.
