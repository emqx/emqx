%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_progress_reporter).

-moduledoc """
Per-shard state that reports consumption progress of a reject queue shard to the MQ cleaner.
Cleaner is a separate process that drops consumed buckets from the queue shard to allow
accept more messages into queues with on_full=reject policy.

The state keeps the latest consumed boundary and the totals of records consumed since the
last report. It sends the boundary to the cleaner when either total reaches half of the quota
bucket threshold, and at once when the totals are unknown. The consumer also flushes unsent
progress periodically, so a quiet queue reports its last consumption. The state holds no index
state. The cleaner decides whether storage work is needed.
""".

-include("../emqx_mq_internal.hrl").

-export([new/3, handle_progress/2, attach/2, flush/1]).

-define(EMPTY_TOTALS, #{count => 0, bytes => 0}).

-type limit() :: pos_integer() | infinity.

-type state() :: #{
    shard := emqx_ds:shard(),
    cleaner := pid() | undefined,
    %% Exclusive DS timestamp boundary of all consumed records
    latest := non_neg_integer(),
    %% Boundary last sent to the cleaner
    sent := non_neg_integer(),
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
-spec new(emqx_mq_types:mq(), emqx_ds:shard(), pid() | undefined) -> t().
new(MQ, Shard, Cleaner) ->
    case emqx_mq_prop:on_full(MQ) of
        evict ->
            undefined;
        reject ->
            #{count := Count, bytes := Bytes} = emqx_mq_quota_index:bucket_thresholds(
                emqx_mq_prop:quota_index_opts(MQ)
            ),
            #{
                shard => Shard,
                cleaner => Cleaner,
                latest => 0,
                sent => 0,
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

-doc "Attach a new cleaner and send it the latest consumed boundary.".
-spec attach(t(), pid()) -> t().
attach(undefined, _Cleaner) ->
    undefined;
attach(State, Cleaner) ->
    notify(State#{cleaner := Cleaner, sent := 0}).

-doc "Send consumption progress that has not been reported yet.".
-spec flush(t()) -> t().
flush(undefined) ->
    undefined;
flush(State) ->
    notify(State).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

notify(#{shard := Shard, cleaner := Cleaner, latest := Latest, sent := Sent} = State) when
    is_pid(Cleaner), Latest > Sent
->
    ok = emqx_mq_cleanup_worker:consumed(Cleaner, Shard, Latest),
    State#{sent := Latest, unsent := ?EMPTY_TOTALS};
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
