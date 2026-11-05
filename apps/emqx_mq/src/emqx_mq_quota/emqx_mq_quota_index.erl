%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index).

-moduledoc """
Encode, decode, and migrate queue quota indices.
Dispatch bucket updates to `emqx_mq_quota_index_common` and `emqx_mq_quota_index_legacy`.
During a rolling upgrade, update both representations independently.
When all nodes support common buckets, remove the legacy indices before updating.
Use legacy deadlines while legacy indices are present.
Use the common deadline after removing the legacy indices.
Initialize missing common buckets from legacy buckets at or after the legacy deadline.
""".

%% ASN1-generated struct field names are in camelCase.
-elvis([{elvis_style, atom_naming_convention, #{regex => "^[a-z]\\w*$"}}]).

-include_lib("../../gen_src/MessageQueue.hrl").
-include("emqx_mq_quota.hrl").

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-export([t_consistency/1]).
-endif.

-export([
    decode/2,
    new/2,
    encode/1,
    apply_update/2,
    apply_updates/2,
    drop_legacy/1,
    truncate_before/2,
    reset/2,
    inspect/1,
    deadline/1,
    occupancy/1,
    next_boundary/1,
    oldest_boundary/1,
    bucket_thresholds/1,
    truncate/2,
    supports_common_index/0
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
    on_full => evict | reject,
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

-spec supports_common_index() -> true.
supports_common_index() ->
    true.

-spec occupancy(t()) -> #{bytes := integer(), count := integer()}.
occupancy(#index{index = #'StorageIndex'{common = Records}}) ->
    #{
        bytes => lists:sum([Bytes || #'StorageIndexBucket'{bytes = Bytes} <- Records]),
        count => lists:sum([Count || #'StorageIndexBucket'{count = Count} <- Records])
    }.

-spec next_boundary(t()) -> timestamp_us() | undefined.
next_boundary(#index{index = #'StorageIndex'{common = Records}}) ->
    case lists:reverse(Records) of
        [_, #'StorageIndexBucket'{tsUs = TsUs} | _] -> TsUs;
        _ -> undefined
    end.

-doc """
Return the start of the oldest common bucket, or 0 without common buckets.
Periodic GC deletes reject queue data below it. See "Deletion boundaries" in `emqx_mq_message_db`.
""".
-spec oldest_boundary(t()) -> timestamp_us().
oldest_boundary(#index{index = #'StorageIndex'{common = []}}) ->
    0;
oldest_boundary(#index{index = #'StorageIndex'{common = Records}}) ->
    #'StorageIndexBucket'{tsUs = TsUs} = lists:last(Records),
    TsUs.

-doc "Return the count and byte values at which a common bucket closes.".
-spec bucket_thresholds(opts()) ->
    #{bytes := pos_integer() | infinity, count := pos_integer() | infinity}.
bucket_thresholds(Opts) ->
    {Bytes, Count} = emqx_mq_quota_index_common:thresholds(Opts),
    #{bytes => Bytes, count => Count}.

-doc """
Retire whole buckets at an exclusive consumed boundary and retain the newest bucket.
Return the retained oldest boundary, which does not exceed `BoundaryUs`, or 0 when no bucket
retired. See "Deletion boundaries" in `emqx_mq_message_db`.
""".
-spec truncate(t(), timestamp_us()) -> {timestamp_us(), t()}.
truncate(Index, BoundaryUs) ->
    case next_boundary(Index) of
        Next when is_integer(Next), Next =< BoundaryUs -> truncate_before(Index, BoundaryUs + 1);
        _ -> {0, Index}
    end.

-doc """
Decode the index from a binary.

We need to pass the options because for the index only buckets are stored.
The limits are queues' attributes, which allows to change them without re-creating the index.
""".
-spec decode(opts(), binary()) -> t().
decode(Opts, Bin) ->
    StorageIndex = 'MessageQueue':decode('StorageIndex', Bin),
    #index{opts = Opts, index = ensure_common_index(StorageIndex, Opts)}.

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
            bytes = [],
            common = []
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

-spec drop_legacy(t()) -> t().
drop_legacy(#index{index = StorageIndex} = Index) ->
    Index#index{index = StorageIndex#'StorageIndex'{bytes = asn1_NOVALUE, count = asn1_NOVALUE}}.

-doc """
Select the nearest common bucket boundary strictly before BeforeUs.
Remove common and legacy buckets starting before that boundary.
Return zero and retain all buckets when no earlier boundary exists.
See "Deletion boundaries" in `emqx_mq_message_db`.
""".
-spec truncate_before(t(), timestamp_us()) -> {timestamp_us(), t()}.
truncate_before(#index{index = StorageIndex0} = Index, BeforeUs) ->
    #'StorageIndex'{common = Common0, bytes = Bytes, count = Count} = StorageIndex0,
    {CutoffUs, Common} = emqx_mq_quota_index_common:truncate_before(Common0, BeforeUs),
    StorageIndex = StorageIndex0#'StorageIndex'{
        common = Common,
        bytes = truncate_legacy_before(Bytes, CutoffUs),
        count = truncate_legacy_before(Count, CutoffUs)
    },
    {CutoffUs, Index#index{index = StorageIndex}}.

-doc """
Reset counters to zero at the purge boundary.
Keep removed legacy indices absent.
Ignore subsequent updates older than the new boundary.
""".
-spec reset(t(), timestamp_us()) -> t().
reset(#index{index = StorageIndex0} = Index, TsUs) ->
    #'StorageIndex'{bytes = Bytes, count = Count} = StorageIndex0,
    StorageIndex = StorageIndex0#'StorageIndex'{
        maxTsUs = TsUs,
        common = emqx_mq_quota_index_common:reset(TsUs),
        bytes = reset_legacy(Bytes, TsUs),
        count = reset_legacy(Count, TsUs)
    },
    Index#index{index = StorageIndex}.

-doc """
Apply an update to the index.
""".
-spec apply_update(t(), update()) -> t().
apply_update(
    #index{
        opts = Opts,
        index = #'StorageIndex'{maxTsUs = MaxTsUs0, common = Common0} = StorageIndex0
    } = Index,
    Update
) ->
    {CommonMaxTsUs, Common} = emqx_mq_quota_index_common:apply_update(
        Common0, MaxTsUs0, Update, Opts
    ),
    StorageIndex = apply_legacy_update(StorageIndex0, Update, Opts),
    Index#index{
        index = StorageIndex#'StorageIndex'{
            maxTsUs = max(CommonMaxTsUs, StorageIndex#'StorageIndex'.maxTsUs), common = Common
        }
    }.

-doc """
Calculates the deadline for the index, i.e. a safe timestamp before which all data can be deleted.

In other words, either count or byte size of the messages after the deadline
is greater than the corresponding max value.

Returns 0 if there is no data in the index / limits are not reached.
""".
-spec deadline(t()) -> non_neg_integer().
deadline(#index{opts = #{on_full := reject}}) ->
    0;
deadline(#index{
    opts = Opts,
    index = #'StorageIndex'{bytes = asn1_NOVALUE, count = asn1_NOVALUE, common = Records}
}) ->
    emqx_mq_quota_index_common:deadline(Records, Opts);
deadline(#index{opts = Opts, index = StorageIndex}) ->
    legacy_deadline(StorageIndex, Opts).

-doc """
A more readable representation of the index.

For buckets, we also provide human-readable representation of their time ranges.
Missing legacy indices are represented as `undefined`.
""".
-spec inspect(t()) -> map().
inspect(#index{
    index = #'StorageIndex'{maxTsUs = MaxTsUs, common = Common, bytes = Bytes, count = Count},
    opts = Opts
}) ->
    #{
        common => inspect_common(MaxTsUs, Common),
        bytes => inspect_legacy(MaxTsUs, Bytes),
        count => inspect_legacy(MaxTsUs, Count),
        max_ts => MaxTsUs,
        opts => Opts
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

reset_legacy(asn1_NOVALUE, _TsUs) ->
    asn1_NOVALUE;
reset_legacy(_Records, TsUs) ->
    emqx_mq_quota_index_legacy:reset(TsUs).

truncate_legacy_before(asn1_NOVALUE, _CutoffUs) ->
    asn1_NOVALUE;
truncate_legacy_before(Records, CutoffUs) ->
    emqx_mq_quota_index_legacy:truncate_before(Records, CutoffUs).

ensure_common_index(#'StorageIndex'{common = asn1_NOVALUE} = Index, Opts) ->
    #'StorageIndex'{maxTsUs = MaxTsUs, bytes = Bytes0, count = Count0} = Index,
    Deadline = legacy_deadline(Index, Opts),
    Bytes = legacy_records(truncate_legacy_before(Bytes0, Deadline)),
    Count = legacy_records(truncate_legacy_before(Count0, Deadline)),
    TsUs = lists:min([MaxTsUs | [Ts || #'StorageIndexRecord'{tsUs = Ts} <- Bytes ++ Count]]),
    Index#'StorageIndex'{
        common = [#'StorageIndexBucket'{tsUs = TsUs, bytes = total(Bytes), count = total(Count)}]
    };
ensure_common_index(Index, _Opts) ->
    Index.

legacy_deadline(_Index, #{on_full := reject}) ->
    0;
legacy_deadline(Index, Opts) ->
    lists:foldl(
        fun(Kind, Deadline) ->
            #{Kind := Max} = Opts,
            Records = legacy_records(index_records(Kind, Index)),
            max(Deadline, emqx_mq_quota_index_legacy:deadline(Records, #{max => Max}))
        end,
        0,
        configured_kinds(Opts)
    ).

legacy_records(asn1_NOVALUE) ->
    [];
legacy_records(Records) ->
    Records.

apply_legacy_update(
    #'StorageIndex'{bytes = asn1_NOVALUE, count = asn1_NOVALUE} = Index, _Update, _Opts
) ->
    Index;
apply_legacy_update(
    Index, ?QUOTA_INDEX_UPDATE(TsUs, _, _) = Update, #{threshold_percentage := Percentage} = Opts
) ->
    lists:foldl(
        fun(Kind, #'StorageIndex'{maxTsUs = MaxTsUs0} = Acc) ->
            #{Kind := Max} = Opts,
            LegacyOpts = maps:merge(maps:with([on_full], Opts), #{
                max => Max, threshold_percentage => Percentage
            }),
            {MaxTsUs, Records} = emqx_mq_quota_index_legacy:apply_update(
                index_records(Kind, Acc), MaxTsUs0, {TsUs, update_value(Kind, Update)}, LegacyOpts
            ),
            set_index_records(Kind, Acc#'StorageIndex'{maxTsUs = MaxTsUs}, Records)
        end,
        Index,
        configured_kinds(Opts)
    ).

index_records(bytes, #'StorageIndex'{bytes = Records}) ->
    Records;
index_records(count, #'StorageIndex'{count = Records}) ->
    Records.

set_index_records(bytes, Index, Records) ->
    Index#'StorageIndex'{bytes = Records};
set_index_records(count, Index, Records) ->
    Index#'StorageIndex'{count = Records}.

update_value(bytes, ?QUOTA_INDEX_UPDATE(_, ByteUpdate, _)) ->
    ByteUpdate;
update_value(count, ?QUOTA_INDEX_UPDATE(_, _, CountUpdate)) ->
    CountUpdate.

configured_kinds(Opts) ->
    lists:filtermap(
        fun
            ({_, infinity}) -> false;
            ({bytes, _}) -> {true, bytes};
            ({count, _}) -> {true, count};
            (_) -> false
        end,
        maps:to_list(Opts)
    ).

inspect_common(MaxTsUs, Records) ->
    Values = [
        {Ts, #{bytes => Bytes, count => Count}}
     || #'StorageIndexBucket'{tsUs = Ts, bytes = Bytes, count = Count} <- Records
    ],
    #{
        records => format_records(MaxTsUs, Values),
        total => #{
            bytes => lists:sum([Bytes || {_, #{bytes := Bytes}} <- Values]),
            count => lists:sum([Count || {_, #{count := Count}} <- Values])
        },
        size => length(Records)
    }.

inspect_legacy(_MaxTsUs, asn1_NOVALUE) ->
    undefined;
inspect_legacy(MaxTsUs, Records) ->
    Values = [{Ts, Value} || #'StorageIndexRecord'{tsUs = Ts, value = Value} <- Records],
    #{records => format_records(MaxTsUs, Values), total => total(Records), size => length(Records)}.

format_records(MaxTsUs, [{TsUs, Value} | _] = Records) ->
    [{TsUs, Value, format_us_diff(TsUs, MaxTsUs)} | format_records_next(Records)];
format_records(_MaxTsUs, []) ->
    [].

format_records_next([{TsUsTo, _}, {TsUsFrom, ValueFrom} = From | Rest]) ->
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

total(Records) ->
    lists:sum(lists:map(fun(#'StorageIndexRecord'{value = Value}) -> Value end, Records)).

-ifdef(TEST).

t_consistency(LegacyUpdates) ->
    ?FORALL(
        {Updates, MaxBytes, MaxCount, ThresholdPercentage},
        {p_updates(), p_max_bytes(), p_max_count(), p_threshold_percentage()},
        t_consistency(Updates, MaxBytes, MaxCount, ThresholdPercentage, LegacyUpdates)
    ).

t_consistency(
    {DurationMs, KeyCount, Updates}, MaxBytes, MaxCount, ThresholdPercentage, LegacyUpdates
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
    Index1 =
        case LegacyUpdates of
            true -> Index0;
            false -> drop_legacy(Index0)
        end,
    {DB1, Index2} = lists:foldl(
        fun({Key, Update}, {DBAcc, IndexAcc}) ->
            update_key({DBAcc, IndexAcc}, {Key, Update})
        end,
        {DB0, Index1},
        sort_by_ts(Updates)
    ),
    {_DB2, Index3} = lists:foldl(
        fun(Key, {DBAcc, IndexAcc}) ->
            update_key({DBAcc, IndexAcc}, {Key, ?QUOTA_INDEX_UPDATE(to_us(DurationMs), 0, 1)})
        end,
        {DB1, Index2},
        lists:seq(1, KeyCount)
    ),
    #{
        common := #{total := #{bytes := BytesTotal, count := CountTotal}},
        bytes := LegacyBytes,
        count := LegacyCount
    } = inspect(Index3),
    LegacyCorrect =
        case LegacyUpdates of
            true ->
                #{total := LegacyBytesTotal} = LegacyBytes,
                #{total := LegacyCountTotal} = LegacyCount,
                LegacyBytesTotal =:= 0 andalso LegacyCountTotal =:= KeyCount;
            false ->
                LegacyBytes =:= undefined andalso LegacyCount =:= undefined
        end,
    0 =:= BytesTotal andalso
        KeyCount =:= CountTotal andalso
        LegacyCorrect.

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
