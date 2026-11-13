%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_progress_reporter).

-moduledoc """
Per-shard state that reports consumption progress of a reject queue shard to the cleanup pool.
The cleanup pool deletes consumed records and drops their index buckets, so queues with
the on_full=reject policy accept more messages.

The state keeps the latest consumed boundary and the totals of records consumed since the
last report. It sends the boundary when either total reaches half of the quota
bucket threshold, and at once when the totals are unknown. The consumer also flushes
periodically. A flush repeats the latest boundary, so a restarted cleanup worker recovers it.
The cleanup worker ignores a boundary it has already processed. The state holds no index state.
""".

-include("../emqx_mq_internal.hrl").

-export([new/2, handle_progress/2, flush/1]).

-define(EMPTY_TOTALS, #{count => 0, bytes => 0}).

-type limit() :: pos_integer() | infinity.

-type state() :: #{
    mq := emqx_mq_types:mq(),
    shard := emqx_ds:shard(),
    %% Exclusive DS timestamp boundary of all consumed records
    latest := non_neg_integer(),
    %% Totals of records consumed after the last report
    unsent := emqx_mq_consumer_stream_buffer:totals(),
    threshold := #{count := limit(), bytes := limit()}
}.

%% Queues that do not reject publications need no progress reporter.
-type t() :: state() | undefined.

-export_type([t/0]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Create the progress reporter of a shard for reject queues. Other queues need none.".
-spec new(emqx_mq_types:mq(), emqx_ds:shard()) -> t().
new(MQ, Shard) ->
    case emqx_mq_prop:on_full(MQ) of
        evict ->
            undefined;
        reject ->
            #{count := Count, bytes := Bytes} = emqx_mq_quota_index:bucket_thresholds(
                emqx_mq_prop:quota_index_opts(MQ)
            ),
            #{
                mq => MQ,
                shard => Shard,
                latest => 0,
                unsent => ?EMPTY_TOTALS,
                threshold => #{count => half(Count), bytes => half(Bytes)}
            }
    end.

-doc "Record stream buffer progress and report it when the consumed totals reach the threshold.".
-spec handle_progress(t(), emqx_mq_consumer_stream_buffer:acked_progress()) -> t().
handle_progress(undefined, _Progress) ->
    undefined;
handle_progress(State, undefined) ->
    State;
handle_progress(
    #{shard := _Shard, latest := Latest, unsent := Unsent0} = State0,
    #progress{boundary = Boundary, totals = Totals}
) ->
    ?tp_debug(mq_consumer_consumed, #{shard => _Shard, boundary => Boundary, totals => Totals}),
    case Totals of
        %% A buffer rebuilt by restoration has no totals, so report it at once
        undefined ->
            notify(State0#{latest := max(Latest, Boundary)});
        #{} ->
            State = State0#{
                latest := max(Latest, Boundary), unsent := add_totals(Unsent0, Totals)
            },
            case is_threshold_reached(State) of
                true -> notify(State);
                false -> State
            end
    end.

-doc "Send the latest consumed boundary, also when it was sent before.".
-spec flush(t()) -> t().
flush(undefined) ->
    undefined;
flush(State) ->
    notify(State).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

%% A missing cleanup pool drops the report. The next flush repeats it.
notify(#{mq := MQ, shard := Shard, latest := Latest} = State) when Latest > 0 ->
    _ = emqx_mq_cleanup_worker:report_consumed(MQ, Shard, Latest),
    State#{unsent := ?EMPTY_TOTALS};
notify(State) ->
    State.

is_threshold_reached(#{
    unsent := #{count := Count, bytes := Bytes},
    threshold := #{count := MaxCount, bytes := MaxBytes}
}) ->
    is_limit_reached(Count, MaxCount) orelse is_limit_reached(Bytes, MaxBytes).

is_limit_reached(_Value, infinity) -> false;
is_limit_reached(Value, Max) -> Value >= Max.

add_totals(#{count := Count0, bytes := Bytes0}, #{count := Count1, bytes := Bytes1}) ->
    #{count => Count0 + Count1, bytes => Bytes0 + Bytes1}.

half(infinity) -> infinity;
half(Threshold) -> max(1, Threshold div 2).
