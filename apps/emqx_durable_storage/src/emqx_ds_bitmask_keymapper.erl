%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_ds_bitmask_keymapper).

%%================================================================================
%% @doc This module is used to map an N-dimensional vector to a
%% 1-dimensional scalar.
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
%%
%% topic
%%
%%   ^ ---->------   |---->------   |---->------
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
%%  -+--------------+-------------+-------------> time
%%        epoch         epoch         epoch
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
%% Notes on the terminology:
%%
%% - "Coordinates" of the original message (usually topic and the
%% timestamp, like in the example above) will be referred to as the
%% "vector".
%%
%% - The 1D scalar that these coordinates are transformed to will be
%% referred to as the "scalar".
%%
%% - Fixed-size binary representation of the scalar is called the
%% "key".
%%
%%================================================================================

%% API:
-export([
    make_keymapper/1,
    vector_to_key/2,
    bin_vector_to_key/2,
    key_to_vector/2,
    key_to_coord/3,
    bin_key_to_vector/2,
    bin_key_to_coord/3,
    key_to_bitstring/2,
    bitstring_to_key/2,
    make_filter/2,
    ratchet/2,
    bin_increment/2,
    bin_checkmask/2,
    bitsize/1
]).

-export_type([vector/0, scalar/0, key/0, dimension/0, offset/0, bitsize/0, bitsource/0, keymapper/0]).

-compile(
    {inline, [
        ones/1,
        extract/2,
        extract_inv/2,
        constr_adjust_min/2,
        constr_adjust_max/2
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

-type coord() :: integer().

-type vector() :: [coord()].

%% N-th coordinate of a vector:
-type dimension() :: pos_integer().

-type key() :: binary().

-type offset() :: non_neg_integer().

-type bitsize() :: pos_integer().

-type scalar() :: non_neg_integer().

-type bitsource() ::
    %% Consume `_Size` bits from timestamp starting at `_Offset`th
    %% bit from N-th element of the input vector:
    {dimension(), offset(), bitsize()}.

%% This record is used during transformation of the source vector into
%% a key.
%%
%% Every dimension of the source vector has a list of `scan_action's
%% associated with it, and the key is formed by applying the scan
%% actions to the respective coordinate of the vector using `extract'
%% function.
-record(scan_action, {
    vec_coord_bitmask :: integer(),
    vec_coord_offset :: offset(),
    scalar_offset :: offset()
}).

-type scan_action() :: #scan_action{}.

-type scanner() :: [_CoorScanActions :: [scan_action()]].

-record(keymapper, {
    %% The original schema of the transformation:
    schema :: [bitsource()],
    %% Number of dimensions:
    vec_n_dim :: non_neg_integer(),
    %% List of operations used to map a vector to the scalar
    vec_scanner :: scanner(),
    %% Total size of the resulting key, in bits:
    key_size :: non_neg_integer(),
    %% Bit size of each dimension of the vector:
    vec_coord_size :: [non_neg_integer()],
    %% Maximum offset of the part, for each the vector element:
    vec_max_offset :: [offset()]
}).

-opaque keymapper() :: #keymapper{}.

-type coord_range() ::
    any | {'=', coord() | infinity} | {'>=', coord()} | {coord(), '..', coord()}.

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
%%
%% Warning: currently the algorithm doesn't handle the following
%% situations, and will produce WRONG results WITHOUT warning:
%%
%% - Parts of the vector elements are reordered in the resulting
%% scalar, i.e. its LSBs are mapped to more significant bits in the
%% scalar than its MSBs.
%%
%% - Overlapping bitsources.
-spec make_keymapper([bitsource()]) -> keymapper().
make_keymapper(Bitsources) ->
    Arr0 = array:new([{fixed, false}, {default, {0, []}}]),
    {Size, Arr} = fold_bitsources(
        fun(DestOffset, {Dim0, Offset, Size}, Acc) ->
            Dim = Dim0 - 1,
            Action = #scan_action{
                vec_coord_bitmask = ones(Size),
                vec_coord_offset = Offset,
                scalar_offset = DestOffset
            },
            {DimSizeof, Actions} = array:get(Dim, Acc),
            array:set(Dim, {DimSizeof + Size, [Action | Actions]}, Acc)
        end,
        Arr0,
        Bitsources
    ),
    {DimSizeof, Scanner} = lists:unzip(array:to_list(Arr)),
    NDim = length(Scanner),
    MaxOffsets = vec_max_offset(NDim, Bitsources),
    #keymapper{
        schema = Bitsources,
        vec_n_dim = NDim,
        vec_scanner = Scanner,
        key_size = Size,
        vec_coord_size = DimSizeof,
        vec_max_offset = MaxOffsets
    }.

-spec bitsize(keymapper()) -> pos_integer().
bitsize(#keymapper{key_size = Size}) ->
    Size.

%% @doc Map N-dimensional vector to a scalar key.
%%
%% Note: this function is not injective.
%%
%% TODO: should be renamed to `vector_to_scalar' to make terminology
%% consistent.
-spec vector_to_key(keymapper(), vector()) -> scalar().
vector_to_key(#keymapper{vec_scanner = []}, []) ->
    0;
vector_to_key(#keymapper{vec_scanner = [Actions | Scanner]}, [Coord | Vector]) ->
    do_vector_to_key(Actions, Scanner, Coord, Vector, 0).

%% @doc Same as `vector_to_key', but it works with binaries, and outputs a binary.
-spec bin_vector_to_key(keymapper(), [binary()]) -> binary().
bin_vector_to_key(Keymapper = #keymapper{vec_coord_size = DimSizeof, key_size = Size}, Binaries) ->
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
%%
%% TODO: should be renamed to `scalar_to_vector' to make terminology
%% consistent.
-spec key_to_vector(keymapper(), scalar()) -> vector().
key_to_vector(#keymapper{vec_scanner = Scanner}, Key) ->
    lists:map(
        fun(Actions) ->
            extract_coord(Actions, Key)
        end,
        Scanner
    ).

%% @doc Same as `key_to_vector', but it works with binaries.
%%
%% TODO: should be renamed to `key_to_vector' to make terminology
%% consistent.
-spec bin_key_to_vector(keymapper(), key()) -> [binary()].
bin_key_to_vector(Keymapper = #keymapper{vec_coord_size = DimSizeof, key_size = Size}, BinKey) ->
    <<Key:Size>> = BinKey,
    Vector = key_to_vector(Keymapper, Key),
    lists:zipwith(
        fun(Elem, SizeOf) ->
            <<Elem:SizeOf>>
        end,
        Vector,
        DimSizeof
    ).

-spec key_to_coord(keymapper(), scalar(), dimension()) -> coord().
key_to_coord(#keymapper{vec_scanner = Scanner}, Key, Dim) ->
    Actions = lists:nth(Dim, Scanner),
    extract_coord(Actions, Key).

-spec bin_key_to_coord(keymapper(), key(), dimension()) -> coord().
bin_key_to_coord(Keymapper = #keymapper{key_size = Size}, BinKey, Dim) ->
    <<Key:Size>> = BinKey,
    key_to_coord(Keymapper, Key, Dim).

%% @doc Transform a bitstring to a key
-spec bitstring_to_key(keymapper(), bitstring()) -> scalar().
bitstring_to_key(#keymapper{key_size = Size}, Bin) ->
    case Bin of
        <<Key:Size>> ->
            Key;
        _ ->
            error({invalid_key, Bin, Size})
    end.

%% @doc Transform key to a fixed-size bistring
-spec key_to_bitstring(keymapper(), scalar()) -> bitstring().
key_to_bitstring(#keymapper{key_size = Size}, Key) ->
    <<Key:Size>>.

%% @doc Create a filter object that facilitates range scans.
-spec make_filter(keymapper(), [coord_range()]) -> filter().
make_filter(
    KeyMapper = #keymapper{schema = Schema, key_size = TotalSize},
    Filter0
) ->
    {Intervals, Bitmask, Bitfilter} = transform_constraints(KeyMapper, Filter0),
    %% Project the intervals into the "bitsource coordinate system":
    {_, Filter} = fold_bitsources(
        fun(DstOffset, {Dim, SrcOffset, Size}, Acc) ->
            {Min0, Max0} = element(Dim, Intervals),
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
    Ranges = list_to_tuple(lists:reverse(Filter)),
    %% Compute estimated upper and lower bounds of a _continous_
    %% interval where all keys lie:
    case Filter of
        [] ->
            RangeMin = 0,
            RangeMax = 0;
        [#filter_scan_action{offset = MSBOffset, min = MSBMin, max = MSBMax} | _] ->
            %% Hack: currently this function only considers the first bitsource:
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
-spec ratchet(filter(), scalar()) -> scalar() | overflow.
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
    NDim = tuple_size(Ranges),
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
    #filter_scan_action{offset = Offset, size = Size, min = Min, max = Max} = element(
        I + 1, Ranges
    ),
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
    #filter_scan_action{offset = Offset, size = Size, min = Min} = element(I + 1, Ranges),
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

%% Calculate maximum offset for each dimension of the vector.
%%
%% These offsets are cached because during the creation of the filter
%% we need to adjust the search interval for the presence of holes.
-spec vec_max_offset(non_neg_integer(), [bitsource()]) -> [offset()].
vec_max_offset(NDim, Bitsources) ->
    Arr0 = array:new([{size, NDim}, {default, 0}, {fixed, true}]),
    Arr = lists:foldl(
        fun({Dimension, Offset, _Size}, Acc) ->
            OldVal = array:get(Dimension - 1, Acc),
            array:set(Dimension - 1, max(Offset, OldVal), Acc)
        end,
        Arr0,
        Bitsources
    ),
    array:to_list(Arr).

%% Transform constraints into a list of closed intervals that the
%% vector elements should lie in.
transform_constraints(
    #keymapper{
        vec_scanner = Scanner, vec_coord_size = DimSizeL, vec_max_offset = MaxOffsetL
    },
    FilterL
) ->
    do_transform_constraints(
        Scanner, DimSizeL, MaxOffsetL, FilterL, [], 0, 0
    ).

do_transform_constraints([], [], [], [], RangeAcc, BitmaskAcc, BitfilterAcc) ->
    {
        list_to_tuple(lists:reverse(RangeAcc)),
        BitmaskAcc,
        BitfilterAcc
    };
do_transform_constraints(
    [Actions | Scanner],
    [DimSize | DimSizeL],
    [MaxOffset | MaxOffsetL],
    [Filter | FilterL],
    RangeAcc,
    BitmaskAcc,
    BitfilterAcc
) ->
    %% This function does four things:
    %%
    %% 1. It transforms the list of "symbolic inequations" to a list
    %% of closed intervals for each vector element.
    %%
    %% 2. In addition, this function adjusts minimum and maximum
    %% values for each interval like this:
    %%
    %% Min: 110100|101011 -> 110100|00000
    %% Max: 110101|001011 -> 110101|11111
    %%            ^
    %%            |
    %%       max offset
    %%
    %% This is needed so when we increment the vector, we always scan
    %% the full range of the least significant bits.
    %%
    %% This leads to some out-of-range elements being exposed at the
    %% beginning and the end of the range, so they should be filtered
    %% out during post-processing.
    %%
    %% 3. It calculates the bitmask that can be used together with the
    %% bitfilter (see 4) to quickly filter out keys that don't satisfy
    %% the strict equations, using `Key && Bitmask != Bitfilter' check
    %%
    %% 4. It calculates the bitfilter
    Max = ones(DimSize),
    case Filter of
        any ->
            Range = {0, Max},
            Bitmask = 0,
            Bitfilter = 0;
        {'=', infinity} ->
            Range = {Max, Max},
            Bitmask = Max,
            Bitfilter = Max;
        {'=', Val} when Val =< Max ->
            Range = {Val, Val},
            Bitmask = Max,
            Bitfilter = Val;
        {'>=', Val} when Val =< Max ->
            Range = {constr_adjust_min(MaxOffset, Val), constr_adjust_max(MaxOffset, Max)},
            Bitmask = 0,
            Bitfilter = 0;
        {A, '..', B} when A =< Max, B =< Max ->
            Range = {constr_adjust_min(MaxOffset, A), constr_adjust_max(MaxOffset, B)},
            Bitmask = 0,
            Bitfilter = 0
    end,
    do_transform_constraints(
        Scanner,
        DimSizeL,
        MaxOffsetL,
        FilterL,
        [Range | RangeAcc],
        vec_elem_to_key(Bitmask, Actions, BitmaskAcc),
        vec_elem_to_key(Bitfilter, Actions, BitfilterAcc)
    ).

constr_adjust_min(MaxOffset, Num) ->
    (Num bsr MaxOffset) bsl MaxOffset.

constr_adjust_max(MaxOffset, Num) ->
    Num bor ones(MaxOffset).

-spec vec_elem_to_key(non_neg_integer(), [scan_action()], Acc) -> Acc when
    Acc :: non_neg_integer().
vec_elem_to_key(_Elem, [], Acc) ->
    Acc;
vec_elem_to_key(Elem, [Action | Actions], Acc) ->
    vec_elem_to_key(Elem, Actions, Acc bor extract(Elem, Action)).

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

-spec extract(_Source :: coord(), scan_action()) -> integer().
extract(Src, #scan_action{
    vec_coord_bitmask = SrcBitmask, vec_coord_offset = SrcOffset, scalar_offset = DstOffset
}) ->
    ((Src bsr SrcOffset) band SrcBitmask) bsl DstOffset.

%% extract^-1
-spec extract_inv(_Dest :: coord(), scan_action()) -> integer().
extract_inv(Dest, #scan_action{
    vec_coord_bitmask = SrcBitmask, vec_coord_offset = SrcOffset, scalar_offset = DestOffset
}) ->
    ((Dest bsr DestOffset) band SrcBitmask) bsl SrcOffset.

extract_coord(Actions, Key) ->
    lists:foldl(
        fun(Action, Acc) ->
            Acc bor extract_inv(Key, Action)
        end,
        0,
        Actions
    ).

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
            vec_n_dim = 0,
            vec_scanner = [],
            key_size = 0,
            vec_coord_size = [],
            vec_max_offset = []
        },
        make_keymapper(Schema)
    ).

make_keymapper1_test() ->
    Schema = [{1, 0, 3}, {2, 0, 5}],
    ?assertEqual(
        #keymapper{
            schema = Schema,
            vec_n_dim = 2,
            vec_scanner = [
                [#scan_action{vec_coord_bitmask = 2#111, vec_coord_offset = 0, scalar_offset = 0}],
                [#scan_action{vec_coord_bitmask = 2#11111, vec_coord_offset = 0, scalar_offset = 3}]
            ],
            key_size = 8,
            vec_coord_size = [3, 5],
            vec_max_offset = [0, 0]
        },
        make_keymapper(Schema)
    ).

make_keymapper2_test() ->
    Schema = [{1, 0, 3}, {2, 0, 5}, {1, 3, 5}],
    ?assertEqual(
        #keymapper{
            schema = Schema,
            vec_n_dim = 2,
            vec_scanner = [
                [
                    #scan_action{
                        vec_coord_bitmask = 2#11111, vec_coord_offset = 3, scalar_offset = 8
                    },
                    #scan_action{vec_coord_bitmask = 2#111, vec_coord_offset = 0, scalar_offset = 0}
                ],
                [#scan_action{vec_coord_bitmask = 2#11111, vec_coord_offset = 0, scalar_offset = 3}]
            ],
            key_size = 13,
            vec_coord_size = [8, 5],
            vec_max_offset = [3, 0]
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
    #filter{bitsource_ranges = Ranges} = F,
    ?assertMatch(
        {
            #filter_scan_action{
                offset = 0,
                size = 8,
                min = 0,
                max = 16#ff
            }
        },
        Ranges
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

mkbmask(Keymapper, Filter) ->
    {_Ranges, Bitmask, Bitfilter} = transform_constraints(Keymapper, Filter),
    {Bitmask, Bitfilter}.

key2vec(Schema, Vector) ->
    Keymapper = make_keymapper(Schema),
    Key = vector_to_key(Keymapper, Vector),
    ?assertEqual(Vector, key_to_vector(Keymapper, Key)).

vec2key(Schema, Vector) ->
    vector_to_key(make_keymapper(Schema), Vector).

-endif.
