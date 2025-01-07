%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Inspired by
%% https://github.com/kafka4beam/kflow/blob/master/src/testbed/payload_gen.erl

-module(emqx_ft_content_gen).

-include_lib("eunit/include/eunit.hrl").

-dialyzer(no_improper_lists).

-export([new/2]).
-export([generate/3]).
-export([next/1]).
-export([consume/1]).
-export([consume/2]).
-export([fold/3]).

-export([hash/2]).
-export([check_file_consistency/3]).

-export_type([cont/1]).
-export_type([stream/1]).
-export_type([binary_payload/0]).

-define(hash_size, 16).

-type payload() :: {Seed :: term(), Size :: integer()}.

-type binary_payload() :: {
    binary(), _ChunkNum :: non_neg_integer(), _Meta :: #{}
}.

-type cont(Data) ::
    fun(() -> stream(Data))
    | stream(Data).

-type stream(Data) ::
    maybe_improper_list(Data, cont(Data))
    | eos.

-record(chunk_state, {
    seed :: term(),
    payload_size :: non_neg_integer(),
    offset :: non_neg_integer(),
    chunk_size :: non_neg_integer()
}).

-type chunk_state() :: #chunk_state{}.

%% -----------------------------------------------------------------------------
%% Generic streams
%% -----------------------------------------------------------------------------

%% @doc Consume one element from the stream.
-spec next(cont(A)) -> stream(A).
next(Fun) when is_function(Fun, 0) ->
    Fun();
next(L) ->
    L.

%% @doc Consume all elements of the stream and feed them into a
%% callback (e.g. brod:produce)
-spec consume(cont(A), fun((A) -> Ret)) -> [Ret].
consume([Data | Cont], Callback) ->
    [Callback(Data) | consume(next(Cont), Callback)];
consume(Cont, Callback) when is_function(Cont, 0) ->
    consume(next(Cont), Callback);
consume(eos, _Callback) ->
    [].

%% @equiv consume(Stream, fun(A) -> A end)
-spec consume(cont(A)) -> [A].
consume(Stream) ->
    consume(Stream, fun(A) -> A end).

-spec fold(fun((A, Acc) -> Acc), Acc, cont(A)) -> Acc.
fold(Fun, Acc, [Data | Cont]) ->
    fold(Fun, Fun(Data, Acc), next(Cont));
fold(Fun, Acc, Cont) when is_function(Cont, 0) ->
    fold(Fun, Acc, next(Cont));
fold(_Fun, Acc, eos) ->
    Acc.

%% -----------------------------------------------------------------------------
%% Binary streams
%% -----------------------------------------------------------------------------

%% @doc Stream of binary chunks.
%% Limitation: `ChunkSize' should be dividable by `?hash_size'
-spec new(payload(), integer()) -> cont(binary_payload()).
new({Seed, Size}, ChunkSize) when ChunkSize rem ?hash_size =:= 0 ->
    fun() ->
        generate_next_chunk(#chunk_state{
            seed = Seed,
            payload_size = Size,
            chunk_size = ChunkSize,
            offset = 0
        })
    end.

%% @doc Generate chunks of data and feed them into
%% `Callback'
-spec generate(payload(), integer(), fun((binary_payload()) -> A)) -> [A].
generate(Payload, ChunkSize, Callback) ->
    consume(new(Payload, ChunkSize), Callback).

-spec hash(cont(binary_payload()), crypto:hash_state()) -> binary().
hash(Stream, HashCtxIn) ->
    crypto:hash_final(
        fold(
            fun({Chunk, _, _}, HashCtx) ->
                crypto:hash_update(HashCtx, Chunk)
            end,
            HashCtxIn,
            Stream
        )
    ).

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

%% @doc Continue generating chunks
-spec generate_next_chunk(chunk_state()) -> stream(binary()).
generate_next_chunk(#chunk_state{offset = Offset, payload_size = Size}) when Offset >= Size ->
    eos;
generate_next_chunk(State0 = #chunk_state{offset = Offset, chunk_size = ChunkSize}) ->
    State = State0#chunk_state{offset = Offset + ChunkSize},
    Payload = generate_chunk(
        State#chunk_state.seed,
        Offset,
        ChunkSize,
        State#chunk_state.payload_size
    ),
    [Payload | fun() -> generate_next_chunk(State) end].

generate_chunk(Seed, Offset, ChunkSize, Size) ->
    SeedHash = seed_hash(Seed),
    To = min(Offset + ChunkSize, Size) - 1,
    Payload = iolist_to_binary([
        generator_fun(I, SeedHash)
     || I <- lists:seq(Offset div 16, To div 16)
    ]),
    ChunkNum = Offset div ChunkSize + 1,
    Meta = #{
        chunk_size => ChunkSize,
        chunk_count => ceil(Size / ChunkSize)
    },
    Chunk =
        case Offset + ChunkSize of
            NextOffset when NextOffset > Size ->
                binary:part(Payload, 0, Size rem ChunkSize);
            _ ->
                Payload
        end,
    {Chunk, ChunkNum, Meta}.

%% @doc First argument is a chunk number, the second one is a seed.
%% This implementation is hardly efficient, but it was chosen for
%% clarity reasons
-spec generator_fun(integer(), binary()) -> binary().
generator_fun(N, Seed) ->
    crypto:hash(md5, <<N:32, Seed/binary>>).

%% @doc Hash any term
-spec seed_hash(term()) -> binary().
seed_hash(Seed) ->
    crypto:hash(md5, term_to_binary(Seed)).

%% @private Get byte at offset `N'
-spec do_get_byte(integer(), binary()) -> byte().
do_get_byte(N, Seed) ->
    Chunk = generator_fun(N div ?hash_size, Seed),
    binary:at(Chunk, N rem ?hash_size).
