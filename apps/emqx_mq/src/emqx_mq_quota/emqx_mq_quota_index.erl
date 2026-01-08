%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index).

-moduledoc """
This module provides a space-efficient data structure for tracking message queue resource usage
(message count and byte size) over time.
Its primary purpose is to calculate a safe "deadline" timestamp for garbage collection,
ensuring that the total size or count of messages newer than the deadline
does not exceed a configured maximum quota.

### Algorithm

The module maintains a time-series index for both message counts and byte sizes.
Each index is a list of timestamped buckets (`#'StorageIndexRecord'`),
sorted in descending order of timestamps (newest first).
Each bucket stores an accumulated value for a specific time range.

The maximum bucket size is called `threshold`. The bucket size is the accuracy of the index,
i.e. when we calculate the deadline, we may leave up to `threshold` data exceeding the max values.

The minimum bucket size (except the first and the last ones) is `threshold / 2`.
So the index size is below `2 + (max/threshold)`.

To support both acceptable accuracy and space efficiency, the threshold is by default configured as
`Max / 10`, thus keeping the index size below ~22 items and leaving at most 10% redundant data after
deletion.

#### Insertion

The "insertion" of a message in the index is accounting for message size in the index, i.e.
increasing the value of the corresponding bucket by the message size and by 1 for the message count.

* If the message's timestamp is greater than the start of the leading bucket, we insert it into the leading bucket.
  If the leading bucket is full, we create a new bucket at the head of the list.
  We take the maximum seen timestamp value + 1 as the timestamp for the new bucket start.
  We do this to make sure that none of the existing insertions should have been done in the new bucket
  (the index may be updated from different nodes with time shifts).
* If the message's timestamp is less than the start of the leading bucket, we insert it into the corresponding
  older bucket. We do not handle bucket overflows in this case.

#### Deletion

Deletion of a message is the reverse operation of insertion,
i.e. decreasing the value of the corresponding bucket by the message size and by 1 for the message count.

Because of the insertion logic, we always know exactly from which bucket to delete the message.

#### Compaction

After each operation, we "compact" the index.
* We squash consecutive buckets that are in common smaller than the threshold.
* We remove tail(the oldest) buckets that are responsible for the older data that exceeds the limits.

### Possible issues

When using the index, we ignore the following situations
that may make redundant data in the queue exceed the configured thresholds for some time.

* When we create a new bucket, we may still insert messages into the old previous bucket for some time because of
    ** time synchronization issues;
    ** buffering.
* When we lose some index updates because of an unexpected node crash.
""".

%% ASN1-generated struct field names are in camelCase.
-elvis([{elvis_style, atom_naming_convention, #{regex => "^[a-z]\\w*$"}}]).

-include_lib("../../gen_src/MessageQueue.hrl").
-include("emqx_mq_quota.hrl").

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    decode/2,
    new/2,
    encode/1,
    apply_update/2,
    apply_updates/2,
    inspect/1,
    deadline/1
]).

-record(index, {
    opts :: opts(),
    index :: #'StorageIndex'{}
}).

-type t() :: #index{}.
-type timestamp_us() :: non_neg_integer().
-type byte_update() :: integer().
-type count_update() :: integer().
-type update() :: ?QUOTA_INDEX_UPDATE(timestamp_us(), byte_update(), count_update()).
-type opts() :: #{
    bytes => pos_integer() | infinity,
    count => pos_integer() | infinity,
    threshold_percentage => pos_integer()
}.

-export_type([
    t/0,
    opts/0,
    update/0,
    timestamp_us/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Decode the index from a binary.

We need to pass the options because for the index only buckets are stored.
The limits are queues' attributes, which allows to change them without re-creating the index.
""".
-spec decode(opts(), binary()) -> t().
decode(Opts, Bin) ->
    #index{opts = Opts, index = 'MessageQueue':decode('StorageIndex', Bin)}.

-doc """
Create a new index.
""".
-spec new(opts(), timestamp_us()) -> t().
new(Opts, TsUs) ->
    #index{
        opts = Opts,
        index = #'StorageIndex'{
            maxTsUs = TsUs,
            count = [],
            bytes = []
        }
    }.

-doc """
Encode the index into a binary.
""".
-spec encode(t()) -> binary().
encode(#index{index = Index}) ->
    'MessageQueue':encode('StorageIndex', Index).

-doc """
Apply a list of updates to the index.
""".
-spec apply_updates(t(), list(update())) -> t().
apply_updates(Index, Updates) ->
    lists:foldl(fun(Update, IndexAcc) -> apply_update(IndexAcc, Update) end, Index, Updates).

-doc """
Apply an update to the index.
""".
-spec apply_update(t(), update()) -> t().
apply_update(
    #index{opts = Opts} = Index0, ?QUOTA_INDEX_UPDATE(TsUs, _ByteUpdate, _CountUpdate) = Update
) ->
    Index = update_max_ts(Index0, TsUs),
    lists:foldl(
        fun(Kind, IndexAcc) ->
            apply_update(Kind, IndexAcc, TsUs, update_vaulue(Kind, Update))
        end,
        Index,
        %% update indices only for the configured kinds of limits: [bytes|count]
        configured_kinds(Opts)
    ).

-doc """
Calculates the deadline for the index, i.e. a safe timestamp before which all data can be deleted.

In other words, either count or byte size of the messages after the deadline
is greater than the corresponding max value.

Returns 0 if there is no data in the index / limits are not reached.
""".
-spec deadline(t()) -> non_neg_integer().
deadline(#index{opts = Opts} = Index) ->
    Deadlines = lists:map(
        fun(Kind) ->
            deadline(Kind, Index)
        end,
        configured_kinds(Opts)
    ),
    case Deadlines of
        [] ->
            0;
        _ ->
            lists:max(Deadlines)
    end.

-doc """
A more readable representation of the index.

For buckets, we also provide human-readable representation of their time ranges.
""".
-spec inspect(t()) -> map().
inspect(#index{
    index = #'StorageIndex'{maxTsUs = MaxTsUs, bytes = Bytes, count = Count}, opts = Opts
}) ->
    #{
        bytes => #{
            records => format_records(MaxTsUs, Bytes),
            total => total(Bytes),
            size => length(Bytes)
        },
        count => #{
            records => format_records(MaxTsUs, Count),
            total => total(Count),
            size => length(Count)
        },
        max_ts => MaxTsUs,
        opts => Opts
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

apply_update(_Kind, Index, _TsUs, 0) ->
    Index;
apply_update(Kind, Index, TsUs, Value) when Value > 0 ->
    insert(Kind, Index, TsUs, Value);
apply_update(Kind, Index, TsUs, Value) when Value < 0 ->
    delete(Kind, Index, TsUs, -Value).

insert(Kind, Index0, AddTsUs, AddValue) ->
    Records0 = index_records(Kind, Index0),
    Threshold = threshold(Kind, Index0),
    MaxTsUs0 = max_ts(Index0),
    {MaxTsUs, Records1} = maybe_advance(Records0, MaxTsUs0, AddTsUs, AddValue, Threshold),
    Records2 = insert_record(Records1, AddTsUs, AddValue),
    Max = max_total(Kind, Index0),
    Records = compact_records(Records2, Threshold, Max, 0),
    Index = set_index_records(Kind, Index0, Records),
    update_max_ts(Index, MaxTsUs).

delete(Kind, Index, TsUs, Value) ->
    Records0 = index_records(Kind, Index),
    Threshold = threshold(Kind, Index),
    Max = max_total(Kind, Index),
    Records1 = delete_record(Records0, TsUs, Value),
    Records = compact_records(Records1, Threshold, Max, 0),
    set_index_records(Kind, Index, Records).

deadline(bytes, #index{opts = #{bytes := Max}, index = #'StorageIndex'{bytes = Bytes}}) ->
    deadline_from_records(Bytes, Max, 0);
deadline(count, #index{opts = #{count := Max}, index = #'StorageIndex'{count = Count}}) ->
    deadline_from_records(Count, Max, 0).

deadline_from_records([], _Max, _Acc) ->
    %% Not enough data to calculate the deadline.
    0;
deadline_from_records([#'StorageIndexRecord'{value = Value, tsUs = TsUs} | _Records], Max, Acc) when
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
) when
    %% NOTE
    %% Add new index bucket only if
    %% * we write to the first bucket and it got overflown.
    %% * or the index is empty.
    %%
    %% We use MaxTsUs + 1 for the new bucket because we want to be able
    %% to uniquely identify buckets by the timestamps of messages.
    Value + AddValue >= Threshold andalso
        AddTsUs >= TsUs
->
    {MaxTsUs + 1, [#'StorageIndexRecord'{tsUs = MaxTsUs + 1, value = 0} | Records]};
maybe_advance([], MaxTsUs, _AddTsUs, _AddValue, _Threshold) ->
    {MaxTsUs, [#'StorageIndexRecord'{tsUs = MaxTsUs, value = 0}]};
maybe_advance(Records, MaxTsUs, _AddTsUs, _AddValue, _Threshold) ->
    {MaxTsUs, Records}.

insert_record([], _AddTsUs, _AddValue) ->
    %% Should not happen, but not a fatal error.
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

%% NOTE
%% Do not compact the first record.
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
) when
    Value1 + Value2 =< Threshold
->
    do_compact_records(
        [Rec2#'StorageIndexRecord'{value = Value1 + Value2} | Records], Threshold, Max, Acc
    );
do_compact_records([#'StorageIndexRecord'{value = Value} = Rec | Records], Threshold, Max, Acc) ->
    [Rec | do_compact_records(Records, Threshold, Max, Acc + Value)];
do_compact_records([], _Threshold, _Max, _Acc) ->
    [].

max_total(Kind, #index{opts = Opts}) ->
    maps:get(Kind, Opts).

threshold(Kind, #index{opts = #{threshold_percentage := ThresholdPercentage} = Opts}) ->
    Max = maps:get(Kind, Opts),
    max(1, ThresholdPercentage * Max div 100).

update_max_ts(#index{index = #'StorageIndex'{maxTsUs = MaxTsUs} = StorageIndex} = Index, Ts) ->
    Index#index{index = StorageIndex#'StorageIndex'{maxTsUs = max(MaxTsUs, Ts)}}.

max_ts(#index{index = #'StorageIndex'{maxTsUs = MaxTsUs}}) ->
    MaxTsUs.

index_records(bytes, #index{index = #'StorageIndex'{bytes = Records}}) ->
    Records;
index_records(count, #index{index = #'StorageIndex'{count = Records}}) ->
    Records.

set_index_records(bytes, #index{index = StorageIndex} = Index, Records) ->
    Index#index{index = StorageIndex#'StorageIndex'{bytes = Records}};
set_index_records(count, #index{index = StorageIndex} = Index, Records) ->
    Index#index{index = StorageIndex#'StorageIndex'{count = Records}}.

update_vaulue(bytes, ?QUOTA_INDEX_UPDATE(_, ByteUpdate, _)) ->
    ByteUpdate;
update_vaulue(count, ?QUOTA_INDEX_UPDATE(_, _, CountUpdate)) ->
    CountUpdate.

format_records(MaxTsUs, [#'StorageIndexRecord'{tsUs = TsUs, value = Value} | _] = Records) ->
    [{TsUs, Value, format_us_diff(TsUs, MaxTsUs)} | format_records_next(Records)];
format_records(_MaxTsUs, []) ->
    [].

format_records_next([
    #'StorageIndexRecord'{tsUs = TsUsTo},
    #'StorageIndexRecord'{tsUs = TsUsFrom, value = ValueFrom} = From
    | Rest
]) ->
    [{TsUsFrom, ValueFrom, format_us_diff(TsUsFrom, TsUsTo)} | format_records_next([From | Rest])];
format_records_next(_) ->
    [].

format_us_diff(FromUs, ToUs) ->
    DiffUs = ToUs - FromUs,
    Hours = DiffUs div 3_600_000_000,
    RemUs1 = DiffUs rem 3_600_000_000,
    Minutes = RemUs1 div 60_000_000,
    RemUs2 = RemUs1 rem 60_000_000,
    Seconds = RemUs2 div 1_000_000,
    RemUs3 = RemUs2 rem 1_000_000,
    Milliseconds = RemUs3 div 1000,
    Units = [
        {Hours, <<"h">>},
        {Minutes, <<"m">>},
        {Seconds, <<"s">>},
        {Milliseconds, <<"ms">>}
    ],
    NonLeadingZeros = skip_leading_zeros(Units),
    Parts = [<<(integer_to_binary(Val))/binary, Unit/binary>> || {Val, Unit} <- NonLeadingZeros],
    iolist_to_binary(lists:join(<<" ">>, Parts)).

skip_leading_zeros([{0, _} | Rest]) when length(Rest) >= 1 ->
    skip_leading_zeros(Rest);
skip_leading_zeros(List) ->
    List.

configured_kinds(Opts) ->
    lists:filtermap(
        fun
            (
                {_, infinity}
            ) ->
                false;
            ({bytes, _}) ->
                {true, bytes};
            ({count, _}) ->
                {true, count};
            (_) ->
                false
        end,
        maps:to_list(Opts)
    ).

total(Records) ->
    lists:sum(lists:map(fun(#'StorageIndexRecord'{value = Value}) -> Value end, Records)).

-ifdef(TEST).

consistency_test_() ->
    Opts = [{numtests, 1000}, {to_file, user}],
    {timeout, 60, [
        fun() -> ?assert(proper:quickcheck(t_consistency(), Opts)) end
    ]}.

t_consistency() ->
    ?FORALL(
        {Updates, MaxBytes, MaxCount, ThresholdPercentage},
        {p_updates(), p_max_bytes(), p_max_count(), p_threshold_percentage()},
        t_consistency(Updates, MaxBytes, MaxCount, ThresholdPercentage)
    ).

t_consistency(
    {DurationMs, KeyCount, Updates}, MaxBytes, MaxCount, ThresholdPercentage
) ->
    DB0 = #{},
    Index0 = new(
        #{
            bytes => MaxBytes,
            count => MaxCount,
            threshold_percentage => ThresholdPercentage
        },
        0
    ),
    {DB1, Index1} = lists:foldl(
        fun({Key, Update}, {DBAcc, IndexAcc}) ->
            update_key({DBAcc, IndexAcc}, {Key, Update})
        end,
        {DB0, Index0},
        sort_by_ts(Updates)
    ),
    {_DB, Index} = lists:foldl(
        fun(Key, {DBAcc, IndexAcc}) ->
            update_key({DBAcc, IndexAcc}, {Key, ?QUOTA_INDEX_UPDATE(to_us(DurationMs), 0, 1)})
        end,
        {DB1, Index1},
        lists:seq(1, KeyCount)
    ),
    #{
        bytes := #{
            total := BytesTotal,
            size := _BytesIndexSize
        },
        count := #{
            total := CountTotal,
            size := _CountIndexSize
        }
    } = inspect(Index),
    0 =:= BytesTotal andalso
        KeyCount =:= CountTotal.

update_key({DB, Index}, {Key, Update}) ->
    DelUpdate =
        case DB of
            #{Key := ?QUOTA_INDEX_UPDATE(PrevTsUs, PrevBytes, PrevCount)} ->
                [?QUOTA_INDEX_UPDATE(PrevTsUs, -PrevBytes, -PrevCount)];
            _ ->
                []
        end,
    InsertUpdate = [Update],
    {
        DB#{Key => Update},
        apply_updates(Index, DelUpdate ++ InsertUpdate)
    }.

-define(MAX_MAX_COUNT, 10000).
-define(MAX_MAX_BYTES, (?MAX_MAX_COUNT * 1024)).

p_max_count() ->
    ?SUCHTHAT(Max, proper_types:pos_integer(), Max =< ?MAX_MAX_COUNT).

p_max_bytes() ->
    ?SUCHTHAT(Max, proper_types:pos_integer(), Max =< ?MAX_MAX_BYTES).

p_threshold_percentage() ->
    proper_types:oneof(lists:seq(1, 110)).

p_updates() ->
    ?LET(
        {DurationMs, KeyCount},
        {p_duration_ms(), p_key_count()},
        {DurationMs, KeyCount, proper_types:list(p_update(DurationMs, KeyCount))}
    ).

p_update(DurationMs, KeyCount) ->
    ?LET(
        {Ms, Key, RecordSize},
        {p_ms(DurationMs), p_key(KeyCount), p_record_size()},
        {Key, ?QUOTA_INDEX_UPDATE(to_us(Ms), RecordSize, 1)}
    ).

p_ms(DurationMs) ->
    proper_types:oneof(lists:seq(0, DurationMs)).

p_key(KeyCount) ->
    proper_types:oneof(lists:seq(1, KeyCount)).

p_duration_ms() ->
    ?SUCHTHAT(DurationMs, proper_types:pos_integer(), DurationMs =< 1000).

p_key_count() ->
    ?SUCHTHAT(KeyCount, proper_types:pos_integer(), KeyCount =< 1000).

p_record_size() ->
    ?SUCHTHAT(RecordSize, proper_types:pos_integer(), RecordSize =< 2 * ?MAX_MAX_COUNT).

to_us(Ms) ->
    Ms * 1000.

sort_by_ts(Updates) ->
    lists:sort(
        fun({_, ?QUOTA_INDEX_UPDATE(Ts1, _, _)}, {_, ?QUOTA_INDEX_UPDATE(Ts2, _, _)}) ->
            Ts1 < Ts2
        end,
        Updates
    ).

-endif.
