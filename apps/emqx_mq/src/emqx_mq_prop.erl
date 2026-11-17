%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_prop).

-moduledoc """
The module contains accessor functions for the MQs/MQ handles.
""".

-include("emqx_mq_internal.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    id/1,
    name/1,
    is_limited/1,
    is_lastvalue/1,
    is_append_only/1,
    topic_filter/1,
    quota_index_opts/1,
    delivery_start_time/1,
    deliver_cutoff_time/1,
    retention_start_time/1,
    default_name/1,
    default_name_from_topic/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec id(emqx_mq_types:mq_handle() | emqx_mq_types:mq()) -> emqx_mq_types:mqid().
id(#{id := ID} = _MQHandle) ->
    ID.

-spec name(emqx_mq_types:mq() | emqx_mq_types:mq_handle()) -> emqx_mq_types:mq_name().
name(#{name := Name} = _MQ) ->
    Name.

-spec default_name(emqx_mq_types:mq()) -> emqx_mq_types:mq_name().
default_name(#{name := Name} = _MQ) ->
    Name;
default_name(#{topic_filter := TopicFilter} = _MQ) ->
    default_name_from_topic(TopicFilter).

-spec default_name_from_topic(emqx_types:topic()) -> emqx_mq_types:mq_name().
default_name_from_topic(TopicFilter) ->
    ?LEGACY_QUEUE_NAME(TopicFilter).

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

-spec is_append_only(emqx_mq_types:mq() | emqx_mq_types:mq_handle()) -> boolean().
is_append_only(MQ) ->
    (not is_limited(MQ)) andalso (not is_lastvalue(MQ)).

-spec quota_index_opts(emqx_mq_types:mq() | emqx_mq_types:mq_handle()) ->
    emqx_mq_quota_index:opts().
quota_index_opts(#{
    limits := #{
        max_shard_message_count := MaxShardMessageCount,
        max_shard_message_bytes := MaxShardMessageBytes
    }
}) ->
    #{
        bytes => MaxShardMessageBytes,
        count => MaxShardMessageCount,
        threshold_percentage => emqx:get_config([mq, quota, threshold_percentage])
    }.

-doc "Return the DS time from which messages are delivered: the later of retention start and delivery cutoff".
-spec delivery_start_time(emqx_mq_types:mq()) -> emqx_ds:time().
delivery_start_time(MQ) ->
    max(retention_start_time(MQ), deliver_cutoff_time(MQ)).

-doc "Return the DS time before which messages are expired by retention, or 0".
-spec retention_start_time(emqx_mq_types:mq()) -> emqx_ds:time().
retention_start_time(#{data_retention_period := RetentionMs}) ->
    RetentionStartMs = max(0, erlang:system_time(millisecond) - RetentionMs),
    erlang:convert_time_unit(RetentionStartMs, millisecond, microsecond).

-doc "Return the delivery cutoff as a DS time. Messages stored before it are not delivered. 0 when unset".
-spec deliver_cutoff_time(emqx_mq_types:mq()) -> emqx_ds:time().
deliver_cutoff_time(#{deliver_cutoff_date := CutoffMs}) ->
    erlang:convert_time_unit(CutoffMs, millisecond, microsecond);
deliver_cutoff_time(_MQ) ->
    0.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

-ifdef(TEST).

%% The delivery start time is the later of the retention start and the optional delivery cutoff.
delivery_start_time_test() ->
    NowMs = erlang:system_time(millisecond),
    RetentionMs = 60_000,
    lists:foreach(
        fun(#{settings := Settings, earliest := Earliest}) ->
            StartUs = delivery_start_time(Settings),
            ?assert(StartUs >= Earliest * 1000),
            Latest = max(Earliest, erlang:system_time(millisecond) - RetentionMs),
            ?assert(StartUs =< Latest * 1000)
        end,
        [
            #{settings => #{data_retention_period => RetentionMs}, earliest => NowMs - RetentionMs},
            #{
                settings => #{
                    data_retention_period => RetentionMs, deliver_cutoff_date => NowMs - 120_000
                },
                earliest => NowMs - RetentionMs
            },
            #{
                settings => #{data_retention_period => RetentionMs, deliver_cutoff_date => NowMs},
                earliest => NowMs
            }
        ]
    ).

%% The delivery start time is 0 when the retention period is longer than the time since the epoch.
delivery_start_time_long_retention_test() ->
    ?assertEqual(
        0,
        delivery_start_time(#{data_retention_period => erlang:system_time(millisecond) + 60_000})
    ).

-endif.
