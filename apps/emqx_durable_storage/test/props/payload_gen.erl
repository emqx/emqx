%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module provides lazy, composable producer streams that
%% can be considered counterparts to Archiver's consumer pipes and
%% therefore can facilitate testing
%%
%% Also it comes with an implementation of binary data stream which is
%% able to produce sufficiently large amounts of plausibly
%% pseudorandom binary payload in a deterministic way. It also
%% contains routines to check binary blobs via sampling
-module(payload_gen).

-define(end_of_stream, []).

-dialyzer(no_improper_lists).

%% Generic stream API:
-export([
    interleave_streams/1,
    retransmits/2,
    next/1,
    consume/2,
    consume/1
]).

%% Binary payload generator API:
-export([
    interleave_chunks/2,
    interleave_chunks/1,

    mb/1,

    generator_fun/2,
    generate_chunks/3,
    generate_chunk/2,
    check_consistency/3,
    check_file_consistency/3,
    get_byte/2
]).

%% List to stream generator API:
-export([list_to_stream/1]).

%% Proper generators:
-export([
    binary_stream_gen/1,
    interleaved_streams_gen/1,
    interleaved_binary_gen/1,
    interleaved_list_gen/1
]).

-export_type([payload/0, binary_payload/0]).

-define(hash_size, 16).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-type payload() :: {Seed :: term(), Size :: integer()}.

-type binary_payload() :: {
    binary(), _ChunkNum :: non_neg_integer(), _ChunkCnt :: non_neg_integer()
}.

%% For performance reasons we treat regular lists as streams, see `next/1'
-type cont(Data) ::
    fun(() -> stream(Data))
    | stream(Data).

-type stream(Data) ::
    maybe_improper_list(Data, cont(Data))
    | ?end_of_stream.

-type tagged_binstream() :: stream({Tag :: term(), Payload :: chunk_state()}).

-record(chunk_state, {
    seed :: term(),
    payload_size :: non_neg_integer(),
    offset :: non_neg_integer(),
    chunk_size :: non_neg_integer()
}).

-type chunk_state() :: #chunk_state{}.

-record(interleave_state, {streams :: [{Tag :: term(), Stream :: term()}]}).

-type interleave_state() :: #interleave_state{}.

%% =============================================================================
%% API functions
%% =============================================================================

%% -----------------------------------------------------------------------------
%% Proper generators
%% -----------------------------------------------------------------------------

%% @doc Proper generator that creates a binary stream
-spec binary_stream_gen(_ChunkSize :: non_neg_integer()) -> proper_types:type().
binary_stream_gen(ChunkSize) when ChunkSize rem ?hash_size =:= 0 ->
    ?LET(
        {Seed, Size},
        {nat(), range(1, 16#100000)},
        generate_chunk({Seed, Size}, ChunkSize)
    ).

%% @equiv interleaved_streams_gen(10, Type)
-spec interleaved_streams_gen(proper_types:type()) -> proper_types:type().
interleaved_streams_gen(Type) ->
    interleaved_streams_gen(10, Type).

%% @doc Proper generator that creates a term of type
%% ```[{_Tag :: binary(), stream()}]''' that is ready to be fed
%% into `interleave_streams/1' function
-spec interleaved_streams_gen(non_neg_integer(), proper_types:type()) ->
    proper_types:type().
interleaved_streams_gen(MaxNStreams, StreamType) ->
    ?LET(
        NStreams,
        range(1, MaxNStreams),
        ?LET(
            Streams,
            vector(NStreams, StreamType),
            begin
                Tags = [<<I/integer>> || I <- lists:seq(1, length(Streams))],
                lists:zip(Tags, Streams)
            end
        )
    ).

-spec interleaved_binary_gen(non_neg_integer()) -> proper_types:type().
interleaved_binary_gen(ChunkSize) ->
    interleaved_streams_gen(binary_stream_gen(ChunkSize)).

-spec interleaved_list_gen(proper_types:type()) -> proper_types:type().
interleaved_list_gen(Type) ->
    interleaved_streams_gen(non_empty(list(Type))).

%% -----------------------------------------------------------------------------
%% Generic streams
%% -----------------------------------------------------------------------------

%% @doc Consume one element from the stream.
-spec next(cont(A)) -> stream(A).
next(Fun) when is_function(Fun, 0) ->
    Fun();
next(L) ->
    L.

%% @doc Take a list of tagged streams and return a stream where
%% elements of the streams are tagged and randomly interleaved.
%%
%% Note: this function is more or less generic and it's compatible
%% with this module's `generate_chunks' function family, as well as
%% `ets:next', lists and what not
%%
%% Consider using simplified versions of this function
-spec interleave_streams([{Tag, stream(Data)}]) -> stream({Tag, Data}).
interleave_streams(Streams) ->
    do_interleave_streams(
        #interleave_state{streams = Streams}
    ).

%% @doc Take an arbitrary stream and add repetitions of the elements
%% TODO: Make retransmissions of arbitrary length
-spec retransmits(stream(Data), float()) -> stream(Data).
retransmits(Stream, Probability) ->
    case Stream of
        [Data | Cont0] ->
            Cont = fun() -> retransmits(next(Cont0), Probability) end,
            case rand:uniform() < Probability of
                true -> [Data, Data | Cont];
                false -> [Data | Cont]
            end;
        ?end_of_stream ->
            ?end_of_stream
    end.

%% @doc Consume all elements of the stream and feed them into a
%% callback (e.g. brod:produce)
-spec consume(
    stream(A),
    fun((A) -> Ret)
) -> [Ret].
consume(Stream, Callback) ->
    case Stream of
        [Data | Cont] -> [Callback(Data) | consume(next(Cont), Callback)];
        ?end_of_stream -> []
    end.

%% @equiv consume(Stream, fun(A) -> A end)
-spec consume(stream(A)) -> [A].
consume(Stream) ->
    consume(Stream, fun(A) -> A end).

%% -----------------------------------------------------------------------------
%% Misc functions
%% -----------------------------------------------------------------------------

%% @doc Return number of bytes in `N' megabytes
-spec mb(integer()) -> integer().
mb(N) ->
    N * 1048576.

%% -----------------------------------------------------------------------------
%% List streams
%% -----------------------------------------------------------------------------
-spec list_to_stream([A]) -> stream(A).
list_to_stream(L) -> L.

%% -----------------------------------------------------------------------------
%% Binary streams
%% -----------------------------------------------------------------------------

%% @doc First argument is a chunk number, the second one is a seed.
%% This implementation is hardly efficient, but it was chosen for
%% clarity reasons
-spec generator_fun(integer(), binary()) -> binary().
generator_fun(N, Seed) ->
    crypto:hash(md5, <<N:32, Seed/binary>>).

%% @doc Get byte at offset `N'
-spec get_byte(integer(), term()) -> byte().
get_byte(N, Seed) ->
    do_get_byte(N, seed_hash(Seed)).

%% @doc Stream of binary chunks. Limitation: both payload size and
%% `ChunkSize' should be dividable by `?hash_size'
-spec generate_chunk(payload(), integer()) -> stream(binary_payload()).
generate_chunk({Seed, Size}, ChunkSize) when
    ChunkSize rem ?hash_size =:= 0
->
    State = #chunk_state{
        seed = Seed,
        payload_size = Size,
        chunk_size = ChunkSize,
        offset = 0
    },
    generate_chunk(State).

%% @doc Take a list of `payload()'s and a callback function, and start
%% producing the payloads in random order. Seed is used as a tag
%% @see interleave_streams/4
-spec interleave_chunks([{payload(), ChunkSize :: non_neg_integer()}]) ->
    tagged_binstream().
interleave_chunks(Streams0) ->
    Streams = [
        {Tag, generate_chunk(Payload, ChunkSize)}
     || {Payload = {Tag, _}, ChunkSize} <- Streams0
    ],
    interleave_streams(Streams).

%% @doc Take a list of `payload()'s and a callback function, and start
%% consuming the payloads in a random order. Seed is used as a
%% tag. All streams use the same chunk size
%% @see interleave_streams/2
-spec interleave_chunks(
    [payload()],
    non_neg_integer()
) -> tagged_binstream().
interleave_chunks(Streams0, ChunkSize) ->
    Streams = [
        {Seed, generate_chunk({Seed, Size}, ChunkSize)}
     || {Seed, Size} <- Streams0
    ],
    interleave_streams(Streams).

%% @doc Generate chunks of data and feed them into
%% `Callback'
-spec generate_chunks(
    payload(),
    integer(),
    fun((binary()) -> A)
) -> [A].
generate_chunks(Payload, ChunkSize, Callback) ->
    consume(generate_chunk(Payload, ChunkSize), Callback).

-spec check_consistency(
    payload(),
    integer(),
    fun((integer()) -> {ok, binary()} | undefined)
) -> ok.
check_consistency({Seed, Size}, SampleSize, Callback) ->
    SeedHash = seed_hash(Seed),
    Random = [rand:uniform(Size) - 1 || _ <- lists:seq(1, SampleSize)],
    %% Always check first and last bytes, and one that should not exist:
    Samples = [0, Size - 1, Size | Random],
    lists:foreach(
        fun
            (N) when N < Size ->
                Expected = do_get_byte(N, SeedHash),
                ?assertEqual(
                    {N, {ok, Expected}},
                    {N, Callback(N)}
                );
            (N) ->
                ?assertMatch(undefined, Callback(N))
        end,
        Samples
    ).

-spec check_file_consistency(
    payload(),
    integer(),
    file:filename()
) -> ok.
check_file_consistency(Payload, SampleSize, FileName) ->
    {ok, FD} = file:open(FileName, [read, raw]),
    try
        Fun = fun(N) ->
            case file:pread(FD, [{N, 1}]) of
                {ok, [[X]]} -> {ok, X};
                {ok, [eof]} -> undefined
            end
        end,
        check_consistency(Payload, SampleSize, Fun)
    after
        file:close(FD)
    end.

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec do_interleave_streams(interleave_state()) -> stream(_Data).
do_interleave_streams(#interleave_state{streams = []}) ->
    ?end_of_stream;
do_interleave_streams(#interleave_state{streams = Streams} = State0) ->
    %% Not the most efficient implementation (lots of avoidable list
    %% traversals), but we don't expect the number of streams to be the
    %% bottleneck
    N = rand:uniform(length(Streams)),
    {Hd, [{Tag, SC} | Tl]} = lists:split(N - 1, Streams),
    case SC of
        [Payload | SC1] ->
            State = State0#interleave_state{streams = Hd ++ [{Tag, next(SC1)} | Tl]},
            Cont = fun() -> do_interleave_streams(State) end,
            [{Tag, Payload} | Cont];
        ?end_of_stream ->
            State = State0#interleave_state{streams = Hd ++ Tl},
            do_interleave_streams(State)
    end.

%% @doc Continue generating chunks
-spec generate_chunk(chunk_state()) -> stream(binary()).
generate_chunk(#chunk_state{offset = Offset, payload_size = Size}) when
    Offset >= Size
->
    ?end_of_stream;
generate_chunk(State0 = #chunk_state{offset = Offset, chunk_size = ChunkSize}) ->
    State = State0#chunk_state{offset = Offset + ChunkSize},
    Payload = generate_chunk(
        State#chunk_state.seed,
        Offset,
        ChunkSize,
        State#chunk_state.payload_size
    ),
    [Payload | fun() -> generate_chunk(State) end].

generate_chunk(Seed, Offset, ChunkSize, Size) ->
    SeedHash = seed_hash(Seed),
    To = min(Offset + ChunkSize, Size) - 1,
    Payload = iolist_to_binary([
        generator_fun(I, SeedHash)
     || I <- lists:seq(Offset div 16, To div 16)
    ]),
    ChunkNum = Offset div ChunkSize + 1,
    ChunkCnt = ceil(Size / ChunkSize),
    {Payload, ChunkNum, ChunkCnt}.

%% @doc Hash any term
-spec seed_hash(term()) -> binary().
seed_hash(Seed) ->
    crypto:hash(md5, term_to_binary(Seed)).

%% @private Get byte at offset `N'
-spec do_get_byte(integer(), binary()) -> byte().
do_get_byte(N, Seed) ->
    Chunk = generator_fun(N div ?hash_size, Seed),
    binary:at(Chunk, N rem ?hash_size).
