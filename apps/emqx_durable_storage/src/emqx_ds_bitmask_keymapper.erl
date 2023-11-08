%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_bitmask_keymapper).

%%================================================================================
%% @doc This module is used to map N-dimensional coordinates to a
%% 1-dimensional space.
%%
%% Example:
%%
%% Let us assume that `T' is a topic and `t' is time. These are the two
%% dimensions used to index messages. They can be viewed as
%% "coordinates" of an MQTT message in a 2D space.
%%
%% Oftentimes, when wildcard subscription is used, keys must be
%% scanned in both dimensions simultaneously.
%%
%% Rocksdb allows to iterate over sorted keys very fast. This means we
%% need to map our two-dimentional keys to a single index that is
%% sorted in a way that helps to iterate over both time and topic
%% without having to do a lot of random seeks.
%%
%% == Mapping of 2D keys to rocksdb keys ==
%%
%% We use "zigzag" pattern to store messages, where rocksdb key is
%% composed like like this:
%%
%%              |ttttt|TTTTTTTTT|tttt|
%%                 ^       ^      ^
%%                 |       |      |
%%         +-------+       |      +---------+
%%         |               |                |
%% most significant    topic hash   least significant
%% bits of timestamp                bits of timestamp
%% (a.k.a epoch)                    (a.k.a time offset)
%%
%% Topic hash is level-aware: each topic level is hashed separately
%% and the resulting hashes are bitwise-concatentated. This allows us
%% to map topics to fixed-length bitstrings while keeping some degree
%% of information about the hierarchy.
%%
%% Next important concept is what we call "epoch". Duration of the
%% epoch is determined by maximum time offset. Epoch is calculated by
%% shifting bits of the timestamp right.
%%
%% The resulting index is a space-filling curve that looks like
%% this in the topic-time 2D space:
%%
%% T ^ ---->------   |---->------   |---->------
%%   |       --/     /      --/     /      --/
%%   |   -<-/       |   -<-/       |   -<-/
%%   | -/           | -/           | -/
%%   | ---->------  | ---->------  | ---->------
%%   |       --/    /       --/    /       --/
%%   |   ---/      |    ---/      |    ---/
%%   | -/          ^  -/          ^  -/
%%   | ---->------ |  ---->------ |  ---->------
%%   |       --/   /        --/   /        --/
%%   |   -<-/     |     -<-/     |     -<-/
%%   | -/         |   -/         |   -/
%%   | ---->------|   ---->------|   ---------->
%%   |
%%  -+------------+-----------------------------> t
%%        epoch
%%
%% This structure allows to quickly seek to a the first message that
%% was recorded in a certain epoch in a certain topic or a
%% group of topics matching filter like `foo/bar/#`.
%%
%% Due to its structure, for each pair of rocksdb keys K1 and K2, such
%% that K1 > K2 and topic(K1) = topic(K2), timestamp(K1) >
%% timestamp(K2).
%% That is, replay doesn't reorder messages published in each
%% individual topic.
%%
%% This property doesn't hold between different topics, but it's not deemed
%% a problem right now.
%%
%%================================================================================

%% API:
-export([
    make_keymapper/1,
    vector_to_key/2,
    bin_vector_to_key/2,
    key_to_vector/2,
    bin_key_to_vector/2,
    key_to_bitstring/2,
    bitstring_to_key/2,
    make_filter/2,
    ratchet/2,
    bin_increment/2,
    bin_checkmask/2,
    bitsize/1
]).

-export_type([vector/0, key/0, dimension/0, offset/0, bitsize/0, bitsource/0, keymapper/0]).

-compile(
    {inline, [
        ones/1,
        extract/2,
        extract_inv/2
    ]}
).

-elvis([{elvis_style, no_if_expression, disable}]).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type scalar() :: integer().

-type vector() :: [scalar()].

%% N-th coordinate of a vector:
-type dimension() :: pos_integer().

-type offset() :: non_neg_integer().

-type bitsize() :: pos_integer().

%% The resulting 1D key:
-type key() :: non_neg_integer().

-type bitsource() ::
    %% Consume `_Size` bits from timestamp starting at `_Offset`th
    %% bit from N-th element of the input vector:
    {dimension(), offset(), bitsize()}.

-record(scan_action, {
    src_bitmask :: integer(),
    src_offset :: offset(),
    dst_offset :: offset()
}).

-type scan_action() :: #scan_action{}.

-type scanner() :: [[scan_action()]].

-record(keymapper, {
    schema :: [bitsource()],
    scanner :: scanner(),
    size :: non_neg_integer(),
    dim_sizeof :: [non_neg_integer()]
}).

-opaque keymapper() :: #keymapper{}.

-type scalar_range() ::
    any | {'=', scalar() | infinity} | {'>=', scalar()} | {scalar(), '..', scalar()}.

-include("emqx_ds_bitmask.hrl").

-type filter() :: #filter{}.

%%================================================================================
%% API functions
%%================================================================================

%% @doc Create a keymapper object that stores the "schema" of the
%% transformation from a list of bitsources.
%%
%% Note: Dimension is 1-based.
%%
%% Note: order of bitsources is important. First element of the list
%% is mapped to the _least_ significant bits of the key, and the last
%% element becomes most significant bits.
-spec make_keymapper([bitsource()]) -> keymapper().
make_keymapper(Bitsources) ->
    Arr0 = array:new([{fixed, false}, {default, {0, []}}]),
    {Size, Arr} = fold_bitsources(
        fun(DestOffset, {Dim0, Offset, Size}, Acc) ->
            Dim = Dim0 - 1,
            Action = #scan_action{
                src_bitmask = ones(Size), src_offset = Offset, dst_offset = DestOffset
            },
            {DimSizeof, Actions} = array:get(Dim, Acc),
            array:set(Dim, {DimSizeof + Size, [Action | Actions]}, Acc)
        end,
        Arr0,
        Bitsources
    ),
    {DimSizeof, Scanner} = lists:unzip(array:to_list(Arr)),
    #keymapper{
        schema = Bitsources,
        scanner = Scanner,
        size = Size,
        dim_sizeof = DimSizeof
    }.

-spec bitsize(keymapper()) -> pos_integer().
bitsize(#keymapper{size = Size}) ->
    Size.

%% @doc Map N-dimensional vector to a scalar key.
%%
%% Note: this function is not injective.
-spec vector_to_key(keymapper(), vector()) -> key().
vector_to_key(#keymapper{scanner = []}, []) ->
    0;
vector_to_key(#keymapper{scanner = [Actions | Scanner]}, [Coord | Vector]) ->
    do_vector_to_key(Actions, Scanner, Coord, Vector, 0).

%% @doc Same as `vector_to_key', but it works with binaries, and outputs a binary.
-spec bin_vector_to_key(keymapper(), [binary()]) -> binary().
bin_vector_to_key(Keymapper = #keymapper{dim_sizeof = DimSizeof, size = Size}, Binaries) ->
    Vec = lists:zipwith(
        fun(Bin, SizeOf) ->
            <<Int:SizeOf>> = Bin,
            Int
        end,
        Binaries,
        DimSizeof
    ),
    Key = vector_to_key(Keymapper, Vec),
    <<Key:Size>>.

%% @doc Map key to a vector.
%%
%% Note: `vector_to_key(key_to_vector(K)) = K' but
%% `key_to_vector(vector_to_key(V)) = V' is not guaranteed.
-spec key_to_vector(keymapper(), key()) -> vector().
key_to_vector(#keymapper{scanner = Scanner}, Key) ->
    lists:map(
        fun(Actions) ->
            lists:foldl(
                fun(Action, Acc) ->
                    Acc bor extract_inv(Key, Action)
                end,
                0,
                Actions
            )
        end,
        Scanner
    ).

%% @doc Same as `key_to_vector', but it works with binaries.
-spec bin_key_to_vector(keymapper(), binary()) -> [binary()].
bin_key_to_vector(Keymapper = #keymapper{dim_sizeof = DimSizeof, size = Size}, BinKey) ->
    <<Key:Size>> = BinKey,
    Vector = key_to_vector(Keymapper, Key),
    lists:zipwith(
        fun(Elem, SizeOf) ->
            <<Elem:SizeOf>>
        end,
        Vector,
        DimSizeof
    ).

%% @doc Transform a bitstring to a key
-spec bitstring_to_key(keymapper(), bitstring()) -> key().
bitstring_to_key(#keymapper{size = Size}, Bin) ->
    case Bin of
        <<Key:Size>> ->
            Key;
        _ ->
            error({invalid_key, Bin, Size})
    end.

%% @doc Transform key to a fixed-size bistring
-spec key_to_bitstring(keymapper(), key()) -> bitstring().
key_to_bitstring(#keymapper{size = Size}, Key) ->
    <<Key:Size>>.

%% @doc Create a filter object that facilitates range scans.
-spec make_filter(keymapper(), [scalar_range()]) -> filter().
make_filter(
    KeyMapper = #keymapper{schema = Schema, dim_sizeof = DimSizeof, size = TotalSize}, Filter0
) ->
    NDim = length(DimSizeof),
    %% Transform "symbolic" constraints to ranges:
    Filter1 = constraints_to_ranges(KeyMapper, Filter0),
    {Bitmask, Bitfilter} = make_bitfilter(KeyMapper, Filter1),
    %% Calculate maximum source offset as per bitsource specification:
    MaxOffset = lists:foldl(
        fun({Dim, Offset, _Size}, Acc) ->
            maps:update_with(
                Dim, fun(OldVal) -> max(OldVal, Offset) end, maps:merge(#{Dim => 0}, Acc)
            )
        end,
        #{},
        Schema
    ),
    %% Adjust minimum and maximum values for each interval like this:
    %%
    %% Min: 110100|101011 -> 110100|00000
    %% Max: 110101|001011 -> 110101|11111
    %%            ^
    %%            |
    %%       max offset
    %%
    %% This is needed so when we increment the vector, we always scan
    %% the full range of least significant bits.
    Filter2 = lists:zipwith(
        fun
            ({Val, Val}, _Dim) ->
                {Val, Val};
            ({Min0, Max0}, Dim) ->
                Offset = maps:get(Dim, MaxOffset, 0),
                %% Set least significant bits of Min to 0:
                Min = (Min0 bsr Offset) bsl Offset,
                %% Set least significant bits of Max to 1:
                Max = Max0 bor ones(Offset),
                {Min, Max}
        end,
        Filter1,
        lists:seq(1, NDim)
    ),
    %% Project the vector into "bitsource coordinate system":
    {_, Filter} = fold_bitsources(
        fun(DstOffset, {Dim, SrcOffset, Size}, Acc) ->
            {Min0, Max0} = lists:nth(Dim, Filter2),
            Min = (Min0 bsr SrcOffset) band ones(Size),
            Max = (Max0 bsr SrcOffset) band ones(Size),
            Action = #filter_scan_action{
                offset = DstOffset,
                size = Size,
                min = Min,
                max = Max
            },
            [Action | Acc]
        end,
        [],
        Schema
    ),
    Ranges = array:from_list(lists:reverse(Filter)),
    %% Compute estimated upper and lower bounds of a _continous_
    %% interval where all keys lie:
    case Filter of
        [] ->
            RangeMin = 0,
            RangeMax = 0;
        [#filter_scan_action{offset = MSBOffset, min = MSBMin, max = MSBMax} | _] ->
            RangeMin = MSBMin bsl MSBOffset,
            RangeMax = MSBMax bsl MSBOffset bor ones(MSBOffset)
    end,
    %% Final value
    #filter{
        size = TotalSize,
        bitmask = Bitmask,
        bitfilter = Bitfilter,
        bitsource_ranges = Ranges,
        range_min = RangeMin,
        range_max = RangeMax
    }.

%% @doc Given a filter `F' and key `K0', return the smallest key `K'
%% that satisfies the following conditions:
%%
%% 1. `K >= K0'
%%
%% 2. `K' satisfies filter `F'.
%%
%% If these conditions cannot be satisfied, return `overflow'.
%%
%% Corollary: `K' may be equal to `K0'.
-spec ratchet(filter(), key()) -> key() | overflow.
ratchet(#filter{bitsource_ranges = Ranges, range_max = Max}, Key) when Key =< Max ->
    %% This function works in two steps: first, it finds the position
    %% of bitsource ("pivot point") corresponding to the part of the
    %% key that should be incremented (or set to the _minimum_ value
    %% of the range, in case the respective part of the original key
    %% is less than the minimum). It also returns "increment": value
    %% that should be added to the part of the key at the pivot point.
    %% Increment can be 0 or 1.
    %%
    %% Then it transforms the key using the following operation:
    %%
    %% 1. Parts of the key that are less than the pivot point are
    %% reset to their minimum values.
    %%
    %% 2. `Increment' is added to the part of the key at the pivot
    %% point.
    %%
    %% 3. The rest of key stays the same
    NDim = array:size(Ranges),
    case ratchet_scan(Ranges, NDim, Key, 0, {_Pivot0 = -1, _Increment0 = 0}, _Carry = 0) of
        overflow ->
            overflow;
        {Pivot, Increment} ->
            ratchet_do(Ranges, Key, NDim - 1, Pivot, Increment)
    end;
ratchet(_, _) ->
    overflow.

%% @doc Given a binary representing a key and a filter, return the
%% next key matching the filter, or `overflow' if such key doesn't
%% exist.
-spec bin_increment(filter(), binary()) -> binary() | overflow.
bin_increment(Filter = #filter{size = Size}, <<>>) ->
    Key = ratchet(Filter, 0),
    <<Key:Size>>;
bin_increment(
    Filter = #filter{size = Size, bitmask = Bitmask, bitfilter = Bitfilter, range_max = RangeMax},
    KeyBin
) ->
    %% The key may contain random suffix, skip it:
    <<Key0:Size, _/binary>> = KeyBin,
    Key1 = Key0 + 1,
    if
        Key1 band Bitmask =:= Bitfilter, Key1 =< RangeMax ->
            <<Key1:Size>>;
        true ->
            case ratchet(Filter, Key1) of
                overflow ->
                    overflow;
                Key ->
                    <<Key:Size>>
            end
    end.

%% @doc Given a filter and a binary representation of a key, return
%% `false' if the key _doesn't_ match the fitler. This function
%% returning `true' is necessary, but not sufficient condition that
%% the key satisfies the filter.
-spec bin_checkmask(filter(), binary()) -> boolean().
bin_checkmask(#filter{size = Size, bitmask = Bitmask, bitfilter = Bitfilter}, Key) ->
    case Key of
        <<Int:Size>> ->
            Int band Bitmask =:= Bitfilter;
        _ ->
            false
    end.

%%================================================================================
%% Internal functions
%%================================================================================

%% Note: this function operates in bitsource basis, scanning it from 0
%% to NDim (i.e. from the least significant bits to the most
%% significant bits)
ratchet_scan(_Ranges, NDim, _Key, NDim, Pivot, 0) ->
    %% We've reached the end:
    Pivot;
ratchet_scan(_Ranges, NDim, _Key, NDim, _Pivot, 1) ->
    %% We've reached the end, but key is still not large enough:
    overflow;
ratchet_scan(Ranges, NDim, Key, I, Pivot0, Carry) ->
    #filter_scan_action{offset = Offset, size = Size, min = Min, max = Max} = array:get(I, Ranges),
    %% Extract I-th element of the vector from the original key:
    Elem = ((Key bsr Offset) band ones(Size)) + Carry,
    if
        Elem < Min ->
            %% I-th coordinate is less than the specified minimum.
            %%
            %% We reset this coordinate to the minimum value. It means
            %% we incremented this bitposition, the less significant
            %% bits have to be reset to their respective minimum
            %% values:
            Pivot = {I + 1, 0},
            ratchet_scan(Ranges, NDim, Key, I + 1, Pivot, 0);
        Elem > Max ->
            %% I-th coordinate is larger than the specified
            %% minimum. We can only fix this problem by incrementing
            %% the next coordinate (i.e. more significant bits).
            %%
            %% We reset this coordinate to the minimum value, and
            %% increment the next coordinate (by setting `Carry' to
            %% 1).
            Pivot = {I + 1, 1},
            ratchet_scan(Ranges, NDim, Key, I + 1, Pivot, 1);
        true ->
            %% Coordinate is within range:
            ratchet_scan(Ranges, NDim, Key, I + 1, Pivot0, 0)
    end.

%% Note: this function operates in bitsource basis, scanning it from
%% NDim to 0. It applies the transformation specified by
%% `ratchet_scan'.
ratchet_do(_Ranges, _Key, I, _Pivot, _Increment) when I < 0 ->
    0;
ratchet_do(Ranges, Key, I, Pivot, Increment) ->
    #filter_scan_action{offset = Offset, size = Size, min = Min} = array:get(I, Ranges),
    Mask = ones(Offset + Size) bxor ones(Offset),
    Elem =
        if
            I > Pivot ->
                Mask band Key;
            I =:= Pivot ->
                (Mask band Key) + (Increment bsl Offset);
            true ->
                Min bsl Offset
        end,
    %% erlang:display(
    %%     {ratchet_do, I, integer_to_list(Key, 16), integer_to_list(Mask, 2),
    %%         integer_to_list(Elem, 16)}
    %% ),
    Elem bor ratchet_do(Ranges, Key, I - 1, Pivot, Increment).

-spec make_bitfilter(keymapper(), [{non_neg_integer(), non_neg_integer()}]) ->
    {non_neg_integer(), non_neg_integer()}.
make_bitfilter(Keymapper = #keymapper{dim_sizeof = DimSizeof}, Ranges) ->
    L = lists:zipwith(
        fun
            ({N, N}, Bits) ->
                %% For strict equality we can employ bitmask:
                {ones(Bits), N};
            (_, _) ->
                {0, 0}
        end,
        Ranges,
        DimSizeof
    ),
    {Bitmask, Bitfilter} = lists:unzip(L),
    {vector_to_key(Keymapper, Bitmask), vector_to_key(Keymapper, Bitfilter)}.

%% Transform constraints into a list of closed intervals that the
%% vector elements should lie in.
constraints_to_ranges(#keymapper{dim_sizeof = DimSizeof}, Filter) ->
    lists:zipwith(
        fun(Constraint, Bitsize) ->
            Max = ones(Bitsize),
            case Constraint of
                any ->
                    {0, Max};
                {'=', infinity} ->
                    {Max, Max};
                {'=', Val} when Val =< Max ->
                    {Val, Val};
                {'>=', Val} when Val =< Max ->
                    {Val, Max};
                {A, '..', B} when A =< Max, B =< Max ->
                    {A, B}
            end
        end,
        Filter,
        DimSizeof
    ).

-spec fold_bitsources(fun((_DstOffset :: non_neg_integer(), bitsource(), Acc) -> Acc), Acc, [
    bitsource()
]) -> {bitsize(), Acc}.
fold_bitsources(Fun, InitAcc, Bitsources) ->
    lists:foldl(
        fun(Bitsource = {_Dim, _Offset, Size}, {DstOffset, Acc0}) ->
            Acc = Fun(DstOffset, Bitsource, Acc0),
            {DstOffset + Size, Acc}
        end,
        {0, InitAcc},
        Bitsources
    ).

do_vector_to_key([], [], _Coord, [], Acc) ->
    Acc;
do_vector_to_key([], [NewActions | Scanner], _Coord, [NewCoord | Vector], Acc) ->
    do_vector_to_key(NewActions, Scanner, NewCoord, Vector, Acc);
do_vector_to_key([Action | Actions], Scanner, Coord, Vector, Acc0) ->
    Acc = Acc0 bor extract(Coord, Action),
    do_vector_to_key(Actions, Scanner, Coord, Vector, Acc).

-spec extract(_Source :: scalar(), scan_action()) -> integer().
extract(Src, #scan_action{src_bitmask = SrcBitmask, src_offset = SrcOffset, dst_offset = DstOffset}) ->
    ((Src bsr SrcOffset) band SrcBitmask) bsl DstOffset.

%% extract^-1
-spec extract_inv(_Dest :: scalar(), scan_action()) -> integer().
extract_inv(Dest, #scan_action{
    src_bitmask = SrcBitmask, src_offset = SrcOffset, dst_offset = DestOffset
}) ->
    ((Dest bsr DestOffset) band SrcBitmask) bsl SrcOffset.

ones(Bits) ->
    1 bsl Bits - 1.

%%================================================================================
%% Unit tests
%%================================================================================

-ifdef(TEST).

make_keymapper0_test() ->
    Schema = [],
    ?assertEqual(
        #keymapper{
            schema = Schema,
            scanner = [],
            size = 0,
            dim_sizeof = []
        },
        make_keymapper(Schema)
    ).

make_keymapper1_test() ->
    Schema = [{1, 0, 3}, {2, 0, 5}],
    ?assertEqual(
        #keymapper{
            schema = Schema,
            scanner = [
                [#scan_action{src_bitmask = 2#111, src_offset = 0, dst_offset = 0}],
                [#scan_action{src_bitmask = 2#11111, src_offset = 0, dst_offset = 3}]
            ],
            size = 8,
            dim_sizeof = [3, 5]
        },
        make_keymapper(Schema)
    ).

make_keymapper2_test() ->
    Schema = [{1, 0, 3}, {2, 0, 5}, {1, 3, 5}],
    ?assertEqual(
        #keymapper{
            schema = Schema,
            scanner = [
                [
                    #scan_action{src_bitmask = 2#11111, src_offset = 3, dst_offset = 8},
                    #scan_action{src_bitmask = 2#111, src_offset = 0, dst_offset = 0}
                ],
                [#scan_action{src_bitmask = 2#11111, src_offset = 0, dst_offset = 3}]
            ],
            size = 13,
            dim_sizeof = [8, 5]
        },
        make_keymapper(Schema)
    ).

vector_to_key0_test() ->
    Schema = [],
    Vector = [],
    ?assertEqual(0, vec2key(Schema, Vector)).

vector_to_key1_test() ->
    Schema = [{1, 0, 8}],
    ?assertEqual(16#ff, vec2key(Schema, [16#ff])),
    ?assertEqual(16#1a, vec2key(Schema, [16#1a])),
    ?assertEqual(16#ff, vec2key(Schema, [16#aaff])).

%% Test handling of source offset:
vector_to_key2_test() ->
    Schema = [{1, 8, 8}],
    ?assertEqual(0, vec2key(Schema, [16#ff])),
    ?assertEqual(16#1a, vec2key(Schema, [16#1aff])),
    ?assertEqual(16#aa, vec2key(Schema, [16#11aaff])).

%% Basic test of 2D vector:
vector_to_key3_test() ->
    Schema = [{1, 0, 8}, {2, 0, 8}],
    ?assertEqual(16#aaff, vec2key(Schema, [16#ff, 16#aa])),
    ?assertEqual(16#2211, vec2key(Schema, [16#aa11, 16#bb22])).

%% Advanced test with 2D vector:
vector_to_key4_test() ->
    Schema = [{1, 0, 8}, {2, 0, 8}, {1, 8, 8}, {2, 16, 8}],
    ?assertEqual(16#bb112211, vec2key(Schema, [16#aa1111, 16#bb2222])).

%% Test with binaries:
vector_to_key_bin_test() ->
    Schema = [{1, 0, 8 * 4}, {2, 0, 8 * 5}, {3, 0, 8 * 5}],
    Keymapper = make_keymapper(lists:reverse(Schema)),
    ?assertMatch(
        <<"wellhelloworld">>, bin_vector_to_key(Keymapper, [<<"well">>, <<"hello">>, <<"world">>])
    ).

key_to_vector0_test() ->
    Schema = [],
    key2vec(Schema, []).

key_to_vector1_test() ->
    Schema = [{1, 0, 8}, {2, 0, 8}],
    key2vec(Schema, [1, 1]),
    key2vec(Schema, [255, 255]),
    key2vec(Schema, [255, 1]),
    key2vec(Schema, [0, 1]),
    key2vec(Schema, [255, 0]).

key_to_vector2_test() ->
    Schema = [{1, 0, 3}, {2, 0, 8}, {1, 3, 5}],
    key2vec(Schema, [1, 1]),
    key2vec(Schema, [255, 255]),
    key2vec(Schema, [255, 1]),
    key2vec(Schema, [0, 1]),
    key2vec(Schema, [255, 0]).

make_bitmask0_test() ->
    Keymapper = make_keymapper([]),
    ?assertMatch({0, 0}, mkbmask(Keymapper, [])).

make_bitmask1_test() ->
    Keymapper = make_keymapper([{1, 0, 8}]),
    ?assertEqual({0, 0}, mkbmask(Keymapper, [any])),
    ?assertEqual({16#ff, 1}, mkbmask(Keymapper, [{'=', 1}])),
    ?assertEqual({16#ff, 255}, mkbmask(Keymapper, [{'=', 255}])),
    ?assertEqual({0, 0}, mkbmask(Keymapper, [{'>=', 0}])),
    ?assertEqual({0, 0}, mkbmask(Keymapper, [{'>=', 1}])),
    ?assertEqual({0, 0}, mkbmask(Keymapper, [{'>=', 16#f}])).

make_bitmask2_test() ->
    Keymapper = make_keymapper([{1, 0, 3}, {2, 0, 4}, {3, 0, 2}]),
    ?assertEqual({2#00_0000_000, 2#00_0000_000}, mkbmask(Keymapper, [any, any, any])),
    ?assertEqual({2#11_0000_000, 2#00_0000_000}, mkbmask(Keymapper, [any, any, {'=', 0}])),
    ?assertEqual({2#00_1111_000, 2#00_0000_000}, mkbmask(Keymapper, [any, {'=', 0}, any])),
    ?assertEqual({2#00_0000_111, 2#00_0000_000}, mkbmask(Keymapper, [{'=', 0}, any, any])).

make_bitmask3_test() ->
    %% Key format of type |TimeOffset|Topic|Epoch|:
    Keymapper = make_keymapper([{1, 0, 8}, {2, 0, 8}, {1, 8, 8}]),
    ?assertEqual({2#00000000_00000000_00000000, 16#00_00_00}, mkbmask(Keymapper, [any, any])),
    ?assertEqual(
        {2#11111111_11111111_11111111, 16#aa_cc_bb},
        mkbmask(Keymapper, [{'=', 16#aabb}, {'=', 16#cc}])
    ),
    ?assertEqual(
        {2#00000000_11111111_00000000, 16#00_bb_00}, mkbmask(Keymapper, [{'>=', 255}, {'=', 16#bb}])
    ).

make_filter_test() ->
    KeyMapper = make_keymapper([]),
    Filter = [],
    ?assertMatch(#filter{size = 0, bitmask = 0, bitfilter = 0}, make_filter(KeyMapper, Filter)).

ratchet1_test() ->
    Bitsources = [{1, 0, 8}],
    M = make_keymapper(Bitsources),
    F = make_filter(M, [any]),
    #filter{bitsource_ranges = Rarr} = F,
    ?assertMatch(
        [
            #filter_scan_action{
                offset = 0,
                size = 8,
                min = 0,
                max = 16#ff
            }
        ],
        array:to_list(Rarr)
    ),
    ?assertEqual(0, ratchet(F, 0)),
    ?assertEqual(16#fa, ratchet(F, 16#fa)),
    ?assertEqual(16#ff, ratchet(F, 16#ff)),
    ?assertEqual(overflow, ratchet(F, 16#100)).

%% erlfmt-ignore
ratchet2_test() ->
    Bitsources = [{1, 0, 8},  %% Static topic index
                  {2, 8, 8},  %% Epoch
                  {3, 0, 8},  %% Varying topic hash
                  {2, 0, 8}], %% Timestamp offset
    M = make_keymapper(lists:reverse(Bitsources)),
    F1 = make_filter(M, [{'=', 16#aa}, any, {'=', 16#cc}]),
    ?assertEqual(16#aa00cc00, ratchet(F1, 0)),
    ?assertEqual(16#aa01cc00, ratchet(F1, 16#aa00cd00)),
    ?assertEqual(16#aa01cc11, ratchet(F1, 16#aa01cc11)),
    ?assertEqual(16#aa11cc00, ratchet(F1, 16#aa10cd00)),
    ?assertEqual(16#aa11cc00, ratchet(F1, 16#aa10dc11)),
    ?assertEqual(overflow,    ratchet(F1, 16#ab000000)),
    F2 = make_filter(M, [{'=', 16#aa}, {'>=', 16#dddd}, {'=', 16#cc}]),
    %% TODO: note that it's `16#aaddcc00` instead of
    %% `16#aaddccdd'. That is because currently ratchet function
    %% doesn't take LSBs of an '>=' interval if it has a hole in the
    %% middle (see `make_filter/2'). This only adds extra keys to the
    %% very first interval, so it's not deemed a huge problem.
    ?assertEqual(16#aaddcc00, ratchet(F2, 0)),
    ?assertEqual(16#aa_de_cc_00, ratchet(F2, 16#aa_dd_cd_11)).

%% erlfmt-ignore
ratchet3_test_() ->
    EpochBits = 4,
    Bitsources = [{1, 0, 2},  %% Static topic index
                  {2, EpochBits, 4},  %% Epoch
                  {3, 0, 2},  %% Varying topic hash
                  {2, 0, EpochBits}], %% Timestamp offset
    Keymapper = make_keymapper(lists:reverse(Bitsources)),
    Filter1 = make_filter(Keymapper, [{'=', 2#10}, any, {'=', 2#01}]),
    Filter2 = make_filter(Keymapper, [{'=', 2#01}, any, any]),
    Filter3 = make_filter(Keymapper, [{'=', 2#01}, {'>=', 16#aa}, any]),
    {timeout, 15,
     [?_assert(test_iterate(Filter1, 0)),
      ?_assert(test_iterate(Filter2, 0)),
      %% Not starting from 0 here for simplicity, since the beginning
      %% of a >= interval can't be properly checked with a bitmask:
      ?_assert(test_iterate(Filter3, ratchet(Filter3, 1)))
     ]}.

%% Note: this function iterates through the full range of keys, so its
%% complexity grows _exponentially_ with the total size of the
%% keymapper.
test_iterate(_Filter, overflow) ->
    true;
test_iterate(Filter, Key0) ->
    Key = ratchet(Filter, Key0 + 1),
    ?assert(ratchet_prop(Filter, Key0, Key)),
    test_iterate(Filter, Key).

ratchet_prop(#filter{bitfilter = Bitfilter, bitmask = Bitmask, size = Size}, Key0, Key) ->
    %% Validate basic properties of the generated key. It must be
    %% greater than the old key, and match the bitmask:
    ?assert(Key =:= overflow orelse (Key band Bitmask =:= Bitfilter)),
    ?assert(Key > Key0, {Key, '>=', Key0}),
    IMax = ones(Size),
    %% Iterate through all keys between `Key0 + 1' and `Key' and
    %% validate that none of them match the bitmask. Ultimately, it
    %% means that `ratchet' function doesn't skip over any valid keys:
    CheckGaps = fun
        F(I) when I >= Key; I > IMax ->
            true;
        F(I) ->
            ?assertNot(
                I band Bitmask =:= Bitfilter,
                {found_gap, Key0, I, Key}
            ),
            F(I + 1)
    end,
    CheckGaps(Key0 + 1).

mkbmask(Keymapper, Filter0) ->
    Filter = constraints_to_ranges(Keymapper, Filter0),
    make_bitfilter(Keymapper, Filter).

key2vec(Schema, Vector) ->
    Keymapper = make_keymapper(Schema),
    Key = vector_to_key(Keymapper, Vector),
    ?assertEqual(Vector, key_to_vector(Keymapper, Key)).

vec2key(Schema, Vector) ->
    vector_to_key(make_keymapper(Schema), Vector).

-endif.
