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
-export([make_keymapper/1, vector_to_key/2, key_to_vector/2, next_range/3]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([vector/0, key/0, dimension/0, offset/0, bitsize/0, bitsource/0, keymapper/0]).

-compile(
    {inline, [
        ones/1,
        extract/2
    ]}
).

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
-type key() :: binary().

-type bitsource() ::
    %% Consume `_Size` bits from timestamp starting at `_Offset`th
    %% bit from N-th element of the input vector:
    {dimension(), offset(), bitsize()}.

-record(scan_action, {
    src_bitmask :: integer(),
    src_offset :: offset(),
    dst_offset :: offset()
}).

-type scanner() :: [[#scan_action{}]].

-record(keymapper, {
    schema :: [bitsource()],
    scanner :: scanner(),
    size :: non_neg_integer(),
    dim_sizeof :: [non_neg_integer()]
}).

-opaque keymapper() :: #keymapper{}.

-type scalar_range() :: any | {'=', scalar()} | {'>=', scalar()}.

%%================================================================================
%% API functions
%%================================================================================

%% @doc
%%
%% Note: Dimension is 1-based.
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

%% @doc Map N-dimensional vector to a scalar key.
%%
%% Note: this function is not injective.
-spec vector_to_key(keymapper(), vector()) -> key().
vector_to_key(#keymapper{scanner = []}, []) ->
    0;
vector_to_key(#keymapper{scanner = [Actions | Scanner]}, [Coord | Vector]) ->
    do_vector_to_key(Actions, Scanner, Coord, Vector, 0).

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

%% @doc Given a keymapper, a filter, and a key, return a triple containing:
%%
%% 1. `NextKey', a key that is greater than the given one, and is
%% within the given range.
%%
%% 2. `Bitmask'
%%
%% 3. `Bitfilter'
%%
%% Bitmask and bitfilter can be used to verify that key any K is in
%% the range using the following inequality:
%%
%% K >= NextKey && (K band Bitmask) =:= Bitfilter.
%%
%% ...or `undefined' if the next key is outside the range.
-spec next_range(keymapper(), [scalar_range()], key()) -> {key(), integer(), integer()} | undefined.
next_range(Keymapper, Filter0, PrevKey) ->
    %% Key -> Vector -> +1 on vector -> Key
    Filter = desugar_filter(Keymapper, Filter0),
    PrevVec = key_to_vector(Keymapper, PrevKey),
    case inc_vector(Filter, PrevVec) of
        overflow ->
            undefined;
        NextVec ->
            NewKey = vector_to_key(Keymapper, NextVec),
            Bitmask = make_bitmask(Keymapper, Filter),
            Bitfilter = NewKey band Bitmask,
            {NewKey, Bitmask, Bitfilter}
    end.

%%================================================================================
%% Internal functions
%%================================================================================

-spec make_bitmask(keymapper(), [{non_neg_integer(), non_neg_integer()}]) -> non_neg_integer().
make_bitmask(Keymapper = #keymapper{dim_sizeof = DimSizeof}, Ranges) ->
    BitmaskVector = lists:map(
        fun
            ({{N, N}, Bits}) ->
                %% For strict equality we can employ bitmask:
                ones(Bits);
            (_) ->
                0
        end,
        lists:zip(Ranges, DimSizeof)
    ),
    vector_to_key(Keymapper, BitmaskVector).

-spec inc_vector([{non_neg_integer(), non_neg_integer()}], vector()) -> vector() | overflow.
inc_vector(Filter, Vec0) ->
    case normalize_vector(Filter, Vec0) of
        {true, Vec} ->
            Vec;
        {false, Vec} ->
            do_inc_vector(Filter, Vec, [])
    end.

do_inc_vector([], [], _Acc) ->
    overflow;
do_inc_vector([{Min, Max} | Intervals], [Elem | Vec], Acc) ->
    case Elem of
        Max ->
            do_inc_vector(Intervals, Vec, [Min | Acc]);
        _ when Elem < Max ->
            lists:reverse(Acc) ++ [Elem + 1 | Vec]
    end.

normalize_vector(Intervals, Vec0) ->
    Vec = lists:map(
        fun
            ({{Min, _Max}, Elem}) when Min > Elem ->
                Min;
            ({{_Min, Max}, Elem}) when Max < Elem ->
                Max;
            ({_, Elem}) ->
                Elem
        end,
        lists:zip(Intervals, Vec0)
    ),
    {Vec > Vec0, Vec}.

%% Transform inequalities into a list of closed intervals that the
%% vector elements should lie in.
desugar_filter(#keymapper{dim_sizeof = DimSizeof}, Filter) ->
    lists:map(
        fun
            ({any, Bitsize}) ->
                {0, ones(Bitsize)};
            ({{'=', Val}, _Bitsize}) ->
                {Val, Val};
            ({{'>=', Val}, Bitsize}) ->
                {Val, ones(Bitsize)}
        end,
        lists:zip(Filter, DimSizeof)
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

%% Specialized version of fold:
do_vector_to_key([], [], _Coord, [], Acc) ->
    Acc;
do_vector_to_key([], [NewActions | Scanner], _Coord, [NewCoord | Vector], Acc) ->
    do_vector_to_key(NewActions, Scanner, NewCoord, Vector, Acc);
do_vector_to_key([Action | Actions], Scanner, Coord, Vector, Acc0) ->
    Acc = Acc0 bor extract(Coord, Action),
    do_vector_to_key(Actions, Scanner, Coord, Vector, Acc).

-spec extract(_Source :: scalar(), #scan_action{}) -> integer().
extract(Src, #scan_action{src_bitmask = SrcBitmask, src_offset = SrcOffset, dst_offset = DstOffset}) ->
    ((Src bsr SrcOffset) band SrcBitmask) bsl DstOffset.

%% extract^-1
-spec extract_inv(_Dest :: scalar(), #scan_action{}) -> integer().
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

%% %% Create a bitmask that is sufficient to cover a given number. E.g.:
%% %%
%% %% 2#1000 -> 2#1111; 2#0 -> 2#0; 2#10101 -> 2#11111
%% bitmask_of(N) ->
%%     %% FIXME: avoid floats
%%     NBits = ceil(math:log2(N + 1)),
%%     ones(NBits).


%% bitmask_of_test() ->
%%     ?assertEqual(2#0, bitmask_of(0)),
%%     ?assertEqual(2#1, bitmask_of(1)),
%%     ?assertEqual(2#11, bitmask_of(2#10)),
%%     ?assertEqual(2#11, bitmask_of(2#11)),
%%     ?assertEqual(2#1111, bitmask_of(2#1000)),
%%     ?assertEqual(2#1111, bitmask_of(2#1111)),
%%     ?assertEqual(ones(128), bitmask_of(ones(128))),
%%     ?assertEqual(ones(256), bitmask_of(ones(256))).

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

inc_vector0_test() ->
    Keymapper = make_keymapper([]),
    ?assertMatch(overflow, incvec(Keymapper, [], [])).

inc_vector1_test() ->
    Keymapper = make_keymapper([{1, 0, 8}]),
    ?assertMatch([3], incvec(Keymapper, [{'=', 3}], [1])),
    ?assertMatch([3], incvec(Keymapper, [{'=', 3}], [2])),
    ?assertMatch(overflow, incvec(Keymapper, [{'=', 3}], [3])),
    ?assertMatch(overflow, incvec(Keymapper, [{'=', 3}], [4])),
    ?assertMatch(overflow, incvec(Keymapper, [{'=', 3}], [255])),
    %% Now with >=:
    ?assertMatch([1], incvec(Keymapper, [{'>=', 0}], [0])),
    ?assertMatch([255], incvec(Keymapper, [{'>=', 0}], [254])),
    ?assertMatch(overflow, incvec(Keymapper, [{'>=', 0}], [255])),

    ?assertMatch([100], incvec(Keymapper, [{'>=', 100}], [0])),
    ?assertMatch([100], incvec(Keymapper, [{'>=', 100}], [99])),
    ?assertMatch([255], incvec(Keymapper, [{'>=', 100}], [254])),
    ?assertMatch(overflow, incvec(Keymapper, [{'>=', 100}], [255])).

inc_vector2_test() ->
    Keymapper = make_keymapper([{1, 0, 8}, {2, 0, 8}, {3, 0, 8}]),
    Filter = [{'>=', 0}, {'=', 100}, {'>=', 30}],
    ?assertMatch([0, 100, 30], incvec(Keymapper, Filter, [0, 0, 0])),
    ?assertMatch([1, 100, 30], incvec(Keymapper, Filter, [0, 100, 30])),
    ?assertMatch([255, 100, 30], incvec(Keymapper, Filter, [254, 100, 30])),
    ?assertMatch([0, 100, 31], incvec(Keymapper, Filter, [255, 100, 30])),
    ?assertMatch([0, 100, 30], incvec(Keymapper, Filter, [0, 100, 29])),
    ?assertMatch(overflow, incvec(Keymapper, Filter, [255, 100, 255])),
    ?assertMatch([255, 100, 255], incvec(Keymapper, Filter, [254, 100, 255])),
    ?assertMatch([0, 100, 255], incvec(Keymapper, Filter, [255, 100, 254])),
    %% Nasty cases (shouldn't happen, hopefully):
    ?assertMatch([1, 100, 30], incvec(Keymapper, Filter, [0, 101, 0])),
    ?assertMatch([1, 100, 33], incvec(Keymapper, Filter, [0, 101, 33])),
    ?assertMatch([0, 100, 255], incvec(Keymapper, Filter, [255, 101, 254])),
    ?assertMatch(overflow, incvec(Keymapper, Filter, [255, 101, 255])).

make_bitmask0_test() ->
    Keymapper = make_keymapper([]),
    ?assertMatch(0, mkbmask(Keymapper, [])).

make_bitmask1_test() ->
    Keymapper = make_keymapper([{1, 0, 8}]),
    ?assertEqual(0, mkbmask(Keymapper, [any])),
    ?assertEqual(16#ff, mkbmask(Keymapper, [{'=', 1}])),
    ?assertEqual(16#ff, mkbmask(Keymapper, [{'=', 255}])),
    ?assertEqual(0, mkbmask(Keymapper, [{'>=', 0}])),
    ?assertEqual(0, mkbmask(Keymapper, [{'>=', 1}])),
    ?assertEqual(0, mkbmask(Keymapper, [{'>=', 16#f}])).

make_bitmask2_test() ->
    Keymapper = make_keymapper([{1, 0, 3}, {2, 0, 4}, {3, 0, 2}]),
    ?assertEqual(2#00_0000_000, mkbmask(Keymapper, [any, any, any])),
    ?assertEqual(2#11_0000_000, mkbmask(Keymapper, [any, any, {'=', 0}])),
    ?assertEqual(2#00_1111_000, mkbmask(Keymapper, [any, {'=', 0}, any])),
    ?assertEqual(2#00_0000_111, mkbmask(Keymapper, [{'=', 0}, any, any])).

make_bitmask3_test() ->
    %% Key format of type |TimeOffset|Topic|Epoch|:
    Keymapper = make_keymapper([{1, 8, 8}, {2, 0, 8}, {1, 0, 8}]),
    ?assertEqual(2#00000000_00000000_00000000, mkbmask(Keymapper, [any, any])),
    ?assertEqual(2#11111111_11111111_11111111, mkbmask(Keymapper, [{'=', 33}, {'=', 22}])),
    ?assertEqual(2#11111111_11111111_11111111, mkbmask(Keymapper, [{'=', 33}, {'=', 22}])),
    ?assertEqual(2#00000000_11111111_00000000, mkbmask(Keymapper, [{'>=', 255}, {'=', 22}])).

next_range0_test() ->
    Keymapper = make_keymapper([]),
    Filter = [],
    PrevKey = 0,
    ?assertMatch(undefined, next_range(Keymapper, Filter, PrevKey)).

next_range1_test() ->
    Keymapper = make_keymapper([{1, 0, 8}, {2, 0, 8}]),
    ?assertMatch(undefined, next_range(Keymapper, [{'=', 0}, {'=', 0}], 0)),
    ?assertMatch({1, 16#ffff, 1}, next_range(Keymapper, [{'=', 1}, {'=', 0}], 0)),
    ?assertMatch({16#100, 16#ffff, 16#100}, next_range(Keymapper, [{'=', 0}, {'=', 1}], 0)),
    %% Now with any:
    ?assertMatch({1, 0, 0}, next_range(Keymapper, [any, any], 0)),
    ?assertMatch({2, 0, 0}, next_range(Keymapper, [any, any], 1)),
    ?assertMatch({16#fffb, 0, 0}, next_range(Keymapper, [any, any], 16#fffa)),
    %% Now with >=:
    ?assertMatch(
        {16#42_30, 16#ff00, 16#42_00}, next_range(Keymapper, [{'>=', 16#30}, {'=', 16#42}], 0)
    ),
    ?assertMatch(
        {16#42_31, 16#ff00, 16#42_00},
        next_range(Keymapper, [{'>=', 16#30}, {'=', 16#42}], 16#42_30)
    ),

    ?assertMatch(
        {16#30_42, 16#00ff, 16#00_42}, next_range(Keymapper, [{'=', 16#42}, {'>=', 16#30}], 0)
    ),
    ?assertMatch(
        {16#31_42, 16#00ff, 16#00_42},
        next_range(Keymapper, [{'=', 16#42}, {'>=', 16#30}], 16#00_43)
    ).

%% Bunch of tests that verifying that next_range doesn't skip over keys:

-define(assertIterComplete(A, B),
    ?assertEqual(A -- [0], B)
).

-define(assertSameSet(A, B),
    ?assertIterComplete(lists:sort(A), lists:sort(B))
).

iterate1_test() ->
    SizeX = 3,
    SizeY = 3,
    Keymapper = make_keymapper([{1, 0, SizeX}, {2, 0, SizeY}]),
    Keys = test_iteration(Keymapper, [any, any]),
    Expected = [
        X bor (Y bsl SizeX)
     || Y <- lists:seq(0, ones(SizeY)), X <- lists:seq(0, ones(SizeX))
    ],
    ?assertIterComplete(Expected, Keys).

iterate2_test() ->
    SizeX = 64,
    SizeY = 3,
    Keymapper = make_keymapper([{1, 0, SizeX}, {2, 0, SizeY}]),
    X = 123456789,
    Keys = test_iteration(Keymapper, [{'=', X}, any]),
    Expected = [
        X bor (Y bsl SizeX)
     || Y <- lists:seq(0, ones(SizeY))
    ],
    ?assertIterComplete(Expected, Keys).

iterate3_test() ->
    SizeX = 3,
    SizeY = 64,
    Y = 42,
    Keymapper = make_keymapper([{1, 0, SizeX}, {2, 0, SizeY}]),
    Keys = test_iteration(Keymapper, [any, {'=', Y}]),
    Expected = [
        X bor (Y bsl SizeX)
     || X <- lists:seq(0, ones(SizeX))
    ],
    ?assertIterComplete(Expected, Keys).

iterate4_test() ->
    SizeX = 8,
    SizeY = 4,
    MinX = 16#fa,
    MinY = 16#a,
    Keymapper = make_keymapper([{1, 0, SizeX}, {2, 0, SizeY}]),
    Keys = test_iteration(Keymapper, [{'>=', MinX}, {'>=', MinY}]),
    Expected = [
        X bor (Y bsl SizeX)
     || Y <- lists:seq(MinY, ones(SizeY)), X <- lists:seq(MinX, ones(SizeX))
    ],
    ?assertIterComplete(Expected, Keys).

iterate1_prop() ->
    Size = 4,
    ?FORALL(
        {SizeX, SizeY},
        {integer(1, Size), integer(1, Size)},
        ?FORALL(
            {SplitX, MinX, MinY},
            {integer(0, SizeX), integer(0, SizeX), integer(0, SizeY)},
            begin
                Keymapper = make_keymapper([
                    {1, 0, SplitX}, {2, 0, SizeY}, {1, SplitX, SizeX - SplitX}
                ]),
                Keys = test_iteration(Keymapper, [{'>=', MinX}, {'>=', MinY}]),
                Expected = [
                    vector_to_key(Keymapper, [X, Y])
                 || X <- lists:seq(MinX, ones(SizeX)),
                    Y <- lists:seq(MinY, ones(SizeY))
                ],
                ?assertSameSet(Expected, Keys),
                true
            end
        )
    ).

iterate5_test() ->
    ?assert(proper:quickcheck(iterate1_prop(), 100)).

iterate2_prop() ->
    Size = 4,
    ?FORALL(
        {SizeX, SizeY},
        {integer(1, Size), integer(1, Size)},
        ?FORALL(
            {SplitX, MinX, MinY},
            {integer(0, SizeX), integer(0, SizeX), integer(0, SizeY)},
            begin
                Keymapper = make_keymapper([
                    {1, SplitX, SizeX - SplitX}, {2, 0, SizeY}, {1, 0, SplitX}
                ]),
                Keys = test_iteration(Keymapper, [{'>=', MinX}, {'>=', MinY}]),
                Expected = [
                    vector_to_key(Keymapper, [X, Y])
                 || X <- lists:seq(MinX, ones(SizeX)),
                    Y <- lists:seq(MinY, ones(SizeY))
                ],
                ?assertSameSet(Expected, Keys),
                true
            end
        )
    ).

iterate6_test() ->
    ?assert(proper:quickcheck(iterate2_prop(), 1000)).

test_iteration(Keymapper, Filter) ->
    test_iteration(Keymapper, Filter, 0).

test_iteration(Keymapper, Filter, PrevKey) ->
    case next_range(Keymapper, Filter, PrevKey) of
        undefined ->
            [];
        {Key, Bitmask, Bitfilter} ->
            ?assert((Key band Bitmask) =:= Bitfilter),
            [Key | test_iteration(Keymapper, Filter, Key)]
    end.

mkbmask(Keymapper, Filter0) ->
    Filter = desugar_filter(Keymapper, Filter0),
    make_bitmask(Keymapper, Filter).

incvec(Keymapper, Filter0, Vector) ->
    Filter = desugar_filter(Keymapper, Filter0),
    inc_vector(Filter, Vector).

key2vec(Schema, Vector) ->
    Keymapper = make_keymapper(Schema),
    Key = vector_to_key(Keymapper, Vector),
    ?assertEqual(Vector, key_to_vector(Keymapper, Key)).

vec2key(Schema, Vector) ->
    vector_to_key(make_keymapper(Schema), Vector).

-endif.
