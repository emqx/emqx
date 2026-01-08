%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_prop).

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

-spec is_limited(emqx_mq_types:mq() | emqx_mq_types:mq_handle()) -> boolean().
is_limited(
    #{limits := #{max_shard_message_count := infinity, max_shard_message_bytes := infinity}} = _MQ
) ->
    false;
is_limited(_MQ) ->
    true.

-spec is_lastvalue(emqx_mq_types:mq() | emqx_mq_types:mq_handle()) -> boolean().
is_lastvalue(#{is_lastvalue := IsLastvalue} = _MQ) ->
    IsLastvalue.

-spec topic_filter(emqx_mq_types:mq() | emqx_mq_types:mq_handle()) -> emqx_types:topic().
topic_filter(#{topic_filter := TopicFilter} = _MQ) ->
    TopicFilter.

-spec id(emqx_mq_types:mq_handle() | emqx_mq_types:mq()) -> emqx_mq_types:mqid().
id(#{id := ID} = _MQHandle) ->
    ID.

-spec is_append_only(emqx_mq_types:mq() | emqx_mq_types:mq_handle()) -> boolean().
is_append_only(MQ) ->
    (not is_limited(MQ)) andalso (not is_lastvalue(MQ)).

-spec quota_index_opts(emqx_mq_types:mq() | emqx_mq_types:mq_handle()) ->
    emqx_mq_message_quota_index:opts().
quota_index_opts(#{
    limits := #{
        max_shard_message_count := MaxShardMessageCount,
        max_shard_message_bytes := MaxShardMessageBytes
    }
}) ->
    maps:from_list(
        limit_to_index_opt(bytes, MaxShardMessageBytes) ++
            limit_to_index_opt(count, MaxShardMessageCount)
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

limit_to_index_opt(_Name, infinity) ->
    [];
limit_to_index_opt(Name, Limit) ->
    [{Name, #{max => Limit, threshold => quota_threshold(Limit)}}].

quota_threshold(Limit) ->
    ThresholdPercentage = emqx:get_config([mq, quota, threshold_percentage]),
    max(1, ThresholdPercentage * Limit div 100).
