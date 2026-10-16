%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_legacy).

-moduledoc """
Track one counter in timestamp buckets.

## Bucket layout and accuracy

The index is a list of `StorageIndexRecord` records, ordered newest first.
Each record stores a lower timestamp boundary and an accumulated counter value.
The next newer boundary is the exclusive upper boundary of the bucket.
The caller supplies either byte deltas or message-count deltas.
Timestamps use microseconds.

The bucket threshold is `max(1, Limit * Percentage div 100)`.
The default percentage is ten.
Smaller thresholds produce more buckets and finer retention boundaries.

## Insertion

Locate the bucket whose time range contains the message timestamp.
Add the positive delta to that bucket.

When an insertion targets the leading bucket and would reach the threshold,
prepend an empty bucket with boundary `MaxTsUs + 1`.
`MaxTsUs` includes the current update timestamp and previously created boundaries.
The current insertion still belongs to the previous bucket.
The new boundary ensures that previously indexed messages retain their bucket assignment.
When the index is empty, create the first bucket at `MaxTsUs`.

Out-of-order insertions update their existing bucket without splitting it.
Ignore updates older than the oldest retained boundary.

## Deletion

Use the original message timestamp to locate its bucket.
Subtract the absolute value of the negative delta from that bucket.
Deletion does not create buckets.
Ignore deletions whose buckets have already been trimmed.
Zero deltas only update the maximum timestamp.

## Compaction

Compact after each nonzero update.
Keep the leading bucket separate and exclude it from the trimming total.
Merge adjacent older buckets when their combined value is at most the threshold.
Keep the older boundary when merging.
Accumulate counter values from newer to older buckets.
Once the accumulated value reaches the limit, discard the remaining older buckets.
Keep the boundary bucket in full because the index has no timestamps within it.

## Garbage collection deadline

Scan all buckets from newest to oldest, including the leading bucket.
Return the first boundary where the accumulated value reaches the limit.
Data strictly before that boundary can be deleted.
Return zero when the limit is not reached.

## Accuracy limits

The threshold controls bucket creation but does not impose a hard bucket-size limit.
A large insertion can exceed the threshold.
Clock differences and buffered updates can add data to older buckets after a boundary is created.
These updates can increase the data retained beyond the configured limit.
A node crash can lose buffered updates and make the index undercount stored data.
""".

-include_lib("../../gen_src/MessageQueue.hrl").

-elvis([{elvis_style, atom_naming_convention, #{regex => "^[a-z]\\w*$"}}]).

-export([apply_update/4, deadline/2, cutoff/2, reset/1]).

-type opts() :: #{max := pos_integer(), threshold_percentage := pos_integer()}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Create a zero-value bucket at the purge boundary.".
-spec reset(emqx_mq_quota_index:timestamp_us()) -> [#'StorageIndexRecord'{}].
reset(TsUs) ->
    [#'StorageIndexRecord'{tsUs = TsUs, value = 0}].

-spec apply_update(
    [#'StorageIndexRecord'{}],
    emqx_mq_quota_index:timestamp_us(),
    {emqx_mq_quota_index:timestamp_us(), integer()},
    opts()
) -> {emqx_mq_quota_index:timestamp_us(), [#'StorageIndexRecord'{}]}.
apply_update(Records, MaxTsUs, {TsUs, 0}, _Opts) ->
    {max(MaxTsUs, TsUs), Records};
apply_update(Records0, MaxTsUs0, {TsUs, Value}, #{max := Max, threshold_percentage := Percentage}) ->
    Threshold = max(1, Percentage * Max div 100),
    MaxTsUs1 = max(MaxTsUs0, TsUs),
    {MaxTsUs, Records1} =
        case Value > 0 of
            true ->
                {NextMaxTsUs, Advanced} = maybe_advance(Records0, MaxTsUs1, TsUs, Value, Threshold),
                {NextMaxTsUs, insert_record(Advanced, TsUs, Value)};
            false ->
                {MaxTsUs1, delete_record(Records0, TsUs, -Value)}
        end,
    {MaxTsUs, compact_records(Records1, Threshold, Max, 0)}.

-spec deadline([#'StorageIndexRecord'{}], #{max := pos_integer()}) -> non_neg_integer().
deadline(Records, #{max := Max}) ->
    deadline_from_records(Records, Max, 0).

-doc "Keep buckets starting at or after the cutoff supplied by the caller.".
-spec cutoff([#'StorageIndexRecord'{}], emqx_mq_quota_index:timestamp_us()) ->
    [#'StorageIndexRecord'{}].
cutoff(Records, CutoffUs) ->
    [R || #'StorageIndexRecord'{tsUs = TsUs} = R <- Records, TsUs >= CutoffUs].

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

deadline_from_records([], _Max, _Acc) ->
    0;
deadline_from_records([#'StorageIndexRecord'{value = Value, tsUs = TsUs} | _], Max, Acc) when
    Acc + Value >= Max
->
    TsUs;
deadline_from_records([#'StorageIndexRecord'{value = Value} | Records], Max, Acc) ->
    deadline_from_records(Records, Max, Acc + Value).

maybe_advance(
    [#'StorageIndexRecord'{value = Value, tsUs = TsUs} | _] = Records,
    MaxTsUs,
    AddTsUs,
    AddValue,
    Threshold
) when Value + AddValue >= Threshold andalso AddTsUs >= TsUs ->
    {MaxTsUs + 1, [#'StorageIndexRecord'{tsUs = MaxTsUs + 1, value = 0} | Records]};
maybe_advance([], MaxTsUs, _AddTsUs, _AddValue, _Threshold) ->
    {MaxTsUs, [#'StorageIndexRecord'{tsUs = MaxTsUs, value = 0}]};
maybe_advance(Records, MaxTsUs, _AddTsUs, _AddValue, _Threshold) ->
    {MaxTsUs, Records}.

insert_record([], _AddTsUs, _AddValue) ->
    [];
insert_record(
    [#'StorageIndexRecord'{tsUs = TsUs} = Record | Records], AddTsUs, AddValue
) when AddTsUs < TsUs ->
    [Record | insert_record(Records, AddTsUs, AddValue)];
insert_record(
    [#'StorageIndexRecord'{value = Value} = Record | Records], _AddTsUs, AddValue
) ->
    [Record#'StorageIndexRecord'{value = Value + AddValue} | Records].

delete_record([], _DelTsUs, _DelValue) ->
    [];
delete_record(
    [#'StorageIndexRecord'{tsUs = TsUs} = Record | Records], DelTsUs, DelValue
) when DelTsUs < TsUs ->
    [Record | delete_record(Records, DelTsUs, DelValue)];
delete_record(
    [#'StorageIndexRecord'{value = Value} = Record | Records], _DelTsUs, DelValue
) ->
    [Record#'StorageIndexRecord'{value = Value - DelValue} | Records].

%% Keep the leading bucket separate and exclude it from the trimming total.
compact_records([], _Threshold, _Max, _Acc) ->
    [];
compact_records([HeadRecord | Records], Threshold, Max, Acc) ->
    [HeadRecord | do_compact_records(Records, Threshold, Max, Acc)].

do_compact_records(_Records, _Threshold, Max, Acc) when Acc >= Max ->
    [];
do_compact_records(
    [#'StorageIndexRecord'{value = Value1}, #'StorageIndexRecord'{value = Value2} = Rec2 | Records],
    Threshold,
    Max,
    Acc
) when Value1 + Value2 =< Threshold ->
    do_compact_records(
        [Rec2#'StorageIndexRecord'{value = Value1 + Value2} | Records], Threshold, Max, Acc
    );
do_compact_records([#'StorageIndexRecord'{value = Value} = Rec | Records], Threshold, Max, Acc) ->
    [Rec | do_compact_records(Records, Threshold, Max, Acc + Value)];
do_compact_records([], _Threshold, _Max, _Acc) ->
    [].
