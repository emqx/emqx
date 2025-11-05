%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_prop).

-moduledoc """
The module contains accessor functions for the MQs/MQ handles.
""".

-export([
    id/1,
    is_limited/1,
    is_lastvalue/1,
    is_append_only/1,
    topic_filter/1,
    quota_index_opts/1
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
topic_filter(#{topic_filter := TopicFilter} = _MQ) ->
    TopicFilter.

-spec id(emqx_streams_types:stream_handle() | emqx_streams_types:stream()) ->
    emqx_streams_types:streamid().
id(#{id := ID} = _StreamHandle) ->
    ID.

-spec is_append_only(emqx_streams_types:stream() | emqx_streams_types:stream_handle()) -> boolean().
is_append_only(Stream) ->
    (not is_limited(Stream)) andalso (not is_lastvalue(Stream)).

-spec quota_index_opts(emqx_streams_types:stream() | emqx_streams_types:stream_handle()) ->
    %% move quota out of mq's
    %% emqx_mq_message_quota_index:opts().
    term().
quota_index_opts(Stream) ->
    maps:from_list(
        limit_to_index_opt(bytes, Stream) ++
            limit_to_index_opt(count, Stream)
    ).

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
