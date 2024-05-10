%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_blob_storage_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

take_chunk(Buffer, BlockSize) ->
    emqx_bridge_azure_blob_storage_connector:take_chunk(Buffer, BlockSize).

%%------------------------------------------------------------------------------
%% Generators
%%------------------------------------------------------------------------------

iodata_gen() ->
    frequency([
        {10, []},
        {10, binary()},
        {10, list(oneof([binary(), byte()]))},
        {1, ?SIZED(Size, resize(Size div 3, list(iodata_gen())))}
    ]).

buffer_gen() ->
    ?LET(
        DataParts,
        list(iodata_gen()),
        lists:foldl(
            fun(DataPart, Buffer) ->
                [Buffer, DataPart]
            end,
            _InitialBuffer = [],
            DataParts
        )
    ).

%%------------------------------------------------------------------------------
%% Properties
%%------------------------------------------------------------------------------

%% Verifies that we can take several chunks from the buffer, and that data is not lost nor
%% created.
take_chunk_preserves_data_prop() ->
    ?FORALL(
        {Buffer, BlockSize, Steps},
        {buffer_gen(), non_neg_integer(), pos_integer()},
        begin
            {Chunks, FinalBuffer} =
                lists:mapfoldl(
                    fun(_, Acc) ->
                        take_chunk(Acc, BlockSize)
                    end,
                    Buffer,
                    lists:seq(1, Steps)
                ),
            %% Original buffer is preserved
            BufferBin = iolist_to_binary(Buffer),
            ConcatenatedBin = iolist_to_binary([Chunks, FinalBuffer]),
            ?WHENFAIL(
                ct:pal(
                    "block size: ~b\nsteps: ~b\noriginal buffer:\n  ~p\nchunks + final buffer:\n  ~p",
                    [BlockSize, Steps, Buffer, {Chunks, FinalBuffer}]
                ),
                BufferBin =:= ConcatenatedBin
            )
        end
    ).

%% Verifies that the produced chunk has at most the requested size.
take_chunk_size_prop() ->
    ?FORALL(
        {Buffer, BlockSize},
        {buffer_gen(), non_neg_integer()},
        begin
            {Chunk, FinalBuffer} = take_chunk(Buffer, BlockSize),
            ?WHENFAIL(
                ct:pal(
                    "block size: ~b\n\noriginal buffer:\n  ~p\nchunk + final buffer:\n  ~p"
                    "\nchunk size: ~b",
                    [BlockSize, Buffer, {Chunk, FinalBuffer}, iolist_size(Chunk)]
                ),
                iolist_size(Chunk) =< BlockSize
            )
        end
    ).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

take_chunk_test_() ->
    Props = [take_chunk_preserves_data_prop(), take_chunk_size_prop()],
    Opts = [{numtests, 1000}, {to_file, user}, {max_size, 100}],
    {timeout, 300, [?_assert(proper:quickcheck(Prop, Opts)) || Prop <- Props]}.
