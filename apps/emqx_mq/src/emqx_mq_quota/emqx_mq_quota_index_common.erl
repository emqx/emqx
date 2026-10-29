%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_common).

-moduledoc """
Track byte and message counts in common timestamp buckets.

## Bucket layout and accuracy

The index is a list of `StorageIndexBucket` records, ordered newest first.
Each record stores a lower timestamp boundary, a byte count, and a message count.
The next newer boundary is the exclusive upper boundary of the bucket.
Both counters describe the same time range, even when only one limit is configured.
Timestamps use microseconds.

Each configured limit has a threshold of `max(1, Limit * Percentage div 100)`.
The default percentage is ten.
Smaller thresholds produce more buckets and finer garbage collection boundaries.
Unconfigured limits do not constrain bucket creation, merging, or trimming.

## Insertion

Locate the bucket whose time range contains the message timestamp.
Add the byte and message deltas to that bucket together.

When an insertion targets the leading bucket and would reach either threshold,
prepend an empty bucket with boundary `MaxTsUs + 1`.
`MaxTsUs` includes the current update timestamp and previously created boundaries.
The current insertion still belongs to the previous bucket.
The new boundary ensures that previously indexed messages retain their bucket assignment.
When the index is empty, create the first bucket at zero for reject queues and at `MaxTsUs` otherwise.
Apply the same threshold check to the first insertion.

Out-of-order insertions update their existing bucket without splitting it.
Ignore updates older than the oldest retained boundary.

## Deletion

Use the original message timestamp to locate its bucket.
Subtract its byte size and message count from that bucket.
Deletion does not create buckets.
Ignore deletions whose buckets have already been trimmed.

## Compaction

Compact after each update.
Keep the leading bucket separate and exclude it from the trimming total.
Merge adjacent older buckets when their combined values fit both configured thresholds.
Keep the older boundary when merging.
Accumulate both counters from newer to older buckets.
Once either accumulated counter reaches its limit, discard the remaining older buckets.
For reject queues, retain older buckets regardless of capacity.

## Garbage collection deadline

Scan all buckets from newest to oldest, including the leading bucket.
Return the first boundary where either accumulated counter reaches its limit.
Data strictly before that boundary can be deleted.
Return zero when neither limit is reached.
The boundary bucket is retained in full because the index has no timestamps within it.

## Accuracy limits

Thresholds control bucket creation but do not impose a hard bucket-size limit.
A large insertion can exceed a threshold.
Clock differences and buffered updates can add data to older buckets after a boundary is created.
These updates can increase the data retained beyond the configured limit.
A node crash can lose buffered updates and make the index undercount stored data.
""".

-include_lib("../../gen_src/MessageQueue.hrl").
-include("emqx_mq_quota.hrl").

-elvis([{elvis_style, atom_naming_convention, #{regex => "^[a-z]\\w*$"}}]).

-export([apply_update/4, deadline/2, truncate_before/2, reset/1, thresholds/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Create a zero-count bucket at the purge boundary.".
-spec reset(emqx_mq_quota_index:timestamp_us()) -> [#'StorageIndexBucket'{}].
reset(TsUs) ->
    [#'StorageIndexBucket'{tsUs = TsUs, bytes = 0, count = 0}].

-spec apply_update(
    [#'StorageIndexBucket'{}],
    emqx_mq_quota_index:timestamp_us(),
    emqx_mq_quota_index:update(),
    emqx_mq_quota_index:opts()
) -> {emqx_mq_quota_index:timestamp_us(), [#'StorageIndexBucket'{}]}.
apply_update(
    Records0,
    MaxTsUs0,
    ?QUOTA_INDEX_UPDATE(TsUs, ByteUpdate, CountUpdate),
    Opts
) ->
    MaxTsUs1 = max(MaxTsUs0, TsUs),
    Threshold = thresholds(Opts),
    Delta = {ByteUpdate, CountUpdate},
    {MaxTsUs, Records1} = maybe_advance(Records0, MaxTsUs1, TsUs, Delta, Threshold, Opts),
    Records2 = update_record(Records1, TsUs, Delta),
    Records = compact_records(Records2, Threshold, trimming_limits(Opts)),
    {MaxTsUs, Records}.

-spec deadline([#'StorageIndexBucket'{}], emqx_mq_quota_index:opts()) -> non_neg_integer().
deadline(Records, Opts) ->
    deadline_from_records(Records, limits(Opts), {0, 0}).

-doc """
Keep buckets starting at or after the nearest boundary strictly before BeforeUs.
See "Deletion boundaries" in `emqx_mq_message_db`.
""".
-spec truncate_before([#'StorageIndexBucket'{}], emqx_mq_quota_index:timestamp_us()) ->
    {emqx_mq_quota_index:timestamp_us(), [#'StorageIndexBucket'{}]}.
truncate_before(Records, BeforeUs) ->
    CutoffUs = cutoff_us(Records, BeforeUs),
    {CutoffUs, [R || #'StorageIndexBucket'{tsUs = TsUs} = R <- Records, TsUs >= CutoffUs]}.

-doc "Return the bucket creation thresholds as `{Bytes, Count}`.".
-spec thresholds(emqx_mq_quota_index:opts()) ->
    {pos_integer() | infinity, pos_integer() | infinity}.
thresholds(#{threshold_percentage := Percentage} = Opts) ->
    {threshold(limit(bytes, Opts), Percentage), threshold(limit(count, Opts), Percentage)}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

cutoff_us([], _BeforeUs) ->
    0;
cutoff_us([#'StorageIndexBucket'{tsUs = TsUs} | _], BeforeUs) when TsUs < BeforeUs ->
    TsUs;
cutoff_us([_ | Records], BeforeUs) ->
    cutoff_us(Records, BeforeUs).

deadline_from_records([], _Max, _Acc) ->
    0;
deadline_from_records([#'StorageIndexBucket'{tsUs = TsUs} = Record | Records], Max, Acc0) ->
    Acc = add_values(Acc0, values(Record)),
    case reaches_limit(Acc, Max) of
        true -> TsUs;
        false -> deadline_from_records(Records, Max, Acc)
    end.

maybe_advance(Records, MaxTsUs, _TsUs, {Bytes, Count}, _Threshold, _Opts) when
    Bytes =< 0, Count =< 0
->
    {MaxTsUs, Records};
maybe_advance([], MaxTsUs, AddTsUs, Delta, Threshold, Opts) ->
    Start =
        case Opts of
            %% GC deletes reject queue data below the oldest boundary.
            %% See "Deletion boundaries" in `emqx_mq_message_db`.
            #{on_full := reject} -> 0;
            #{} -> MaxTsUs
        end,
    maybe_advance(reset(Start), MaxTsUs, AddTsUs, Delta, Threshold, Opts);
maybe_advance(
    [#'StorageIndexBucket'{tsUs = TsUs} = Record | _] = Records,
    MaxTsUs,
    AddTsUs,
    Delta,
    Threshold,
    _Opts
) when AddTsUs >= TsUs ->
    case reaches_limit(add_values(values(Record), Delta), Threshold) of
        true ->
            %% The next bucket starts after every timestamp already seen.
            NextTsUs = MaxTsUs + 1,
            {NextTsUs, [#'StorageIndexBucket'{tsUs = NextTsUs, bytes = 0, count = 0} | Records]};
        false ->
            {MaxTsUs, Records}
    end;
maybe_advance(Records, MaxTsUs, _AddTsUs, _AddValue, _Threshold, _Opts) ->
    {MaxTsUs, Records}.

update_record([], _TsUs, _Delta) ->
    [];
update_record(
    [#'StorageIndexBucket'{tsUs = TsUs} = Record | Records], UpdateTsUs, Delta
) when UpdateTsUs < TsUs ->
    [Record | update_record(Records, UpdateTsUs, Delta)];
update_record([Record | Records], _TsUs, Delta) ->
    [set_values(Record, add_values(values(Record), Delta)) | Records].

%% Keep the leading bucket separate and exclude it from the trimming total.
compact_records([], _Threshold, _Max) ->
    [];
compact_records([HeadRecord | Records], Threshold, Max) ->
    [HeadRecord | do_compact_records(Records, Threshold, Max, {0, 0})].

do_compact_records(Records, Threshold, Max, Acc) ->
    case reaches_limit(Acc, Max) of
        true -> [];
        false -> merge_records(Records, Threshold, Max, Acc)
    end.

merge_records([Rec1, Rec2 | Records], Threshold, Max, Acc) ->
    Combined = add_values(values(Rec1), values(Rec2)),
    case fits_threshold(Combined, Threshold) of
        true ->
            do_compact_records([set_values(Rec2, Combined) | Records], Threshold, Max, Acc);
        false ->
            [
                Rec1
                | do_compact_records(
                    [Rec2 | Records], Threshold, Max, add_values(Acc, values(Rec1))
                )
            ]
    end;
merge_records(Records, _Threshold, _Max, _Acc) ->
    Records.

values(#'StorageIndexBucket'{bytes = Bytes, count = Count}) ->
    {Bytes, Count}.

set_values(Record, {Bytes, Count}) ->
    Record#'StorageIndexBucket'{bytes = Bytes, count = Count}.

add_values({Bytes1, Count1}, {Bytes2, Count2}) ->
    {Bytes1 + Bytes2, Count1 + Count2}.

reaches_limit({Bytes, Count}, {MaxBytes, MaxCount}) ->
    Bytes >= MaxBytes orelse Count >= MaxCount.

fits_threshold({Bytes, Count}, {MaxBytes, MaxCount}) ->
    Bytes =< MaxBytes andalso Count =< MaxCount.

trimming_limits(#{on_full := reject}) ->
    {infinity, infinity};
trimming_limits(Opts) ->
    limits(Opts).

limits(Opts) ->
    {limit(bytes, Opts), limit(count, Opts)}.

limit(Kind, Opts) ->
    case Opts of
        #{Kind := Max} -> Max;
        #{} -> infinity
    end.

threshold(infinity, _Percentage) ->
    infinity;
threshold(Max, Percentage) ->
    max(1, Percentage * Max div 100).
