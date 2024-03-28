-module(bitswizzle_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("bitswizzle.hrl").

init_matrix_test_() ->
    %% Wrong argument types:
    [
        ?_assertError(ematrix_size, bitswizzle:make_sparse_matrix(1, [])),
        ?_assertError(badarg, bitswizzle:make_sparse_matrix(64, [{1, 1}, 1])),
        %% Index too high/low:
        ?_assertError(badarg, bitswizzle:make_sparse_matrix(64, [{-1, 1}])),
        ?_assertError(ecoord, bitswizzle:make_sparse_matrix(64, [{0, 64}])),
        %% Size of the row is too large:
        ?_assertError(
            badarg,
            bitswizzle:make_sparse_matrix(128, [{1 bsl 64, 0}])
        ),
        %% Success:
        ?_assertMatch(R when is_reference(R), bitswizzle:make_sparse_matrix(0, [])),
        ?_assertMatch(R when is_reference(R), bitswizzle:make_sparse_matrix(64, [{0, 0}, {63, 63}]))
    ].

dump_sparse_matrix_empty_test() ->
    Dump = bitswizzle:dump_sparse_matrix(bitswizzle:make_sparse_matrix(0, [])),
    ?assertMatch(
        #sparse_matrix{
            n_dim = 0,
            cells = []
        },
        Dump
    ).

dump_sparse_identity_test() ->
    Dump = bitswizzle:dump_sparse_matrix(bitswizzle:make_sparse_matrix(128, identity_matrix(128))),
    #sparse_matrix{
        n_dim = 128,
        cells = Cells
    } = Dump,
    [
        #smatrix_cell{row = 1, column = 1, size = 1, diags = Diags11},
        #smatrix_cell{row = 0, column = 0, size = 1, diags = Diags00}
    ] = Cells,
    ?assertMatch([#smatrix_diag{offset = 0, mask = 16#ffffffffffffffff}], Diags11),
    ?assertMatch([#smatrix_diag{offset = 0, mask = 16#ffffffffffffffff}], Diags00).

mv_mul_fail_test_() ->
    %% Wrong matrix type:
    [
        ?_assertError(badarg, bitswizzle:sm_v_multiply(1, <<>>)),
        %% Wrong size:
        ?_assertError(
            evector_size, bitswizzle:sm_v_multiply(bitswizzle:make_sparse_matrix(0, []), <<1>>)
        ),
        ?_assertError(
            evector_size, bitswizzle:sm_v_multiply(bitswizzle:make_sparse_matrix(64, []), <<1>>)
        )
    ].

%% Verify mapping of coordinates to a z-order curve, a.k.a Lebesgue
%% curve, a.k.a Morton space-filling curve
lebesgue_mul_test() ->
    %% Note: 0xa is 1010 in binary, and 0x5 is 0101. Hence, if the
    %% first coordinate is 0xff... and the second is 0x00.., the
    %% result is 0x5555... , and if the coordinates are swapped it's
    %% 0xaaaa...
    Mtx64 = bitswizzle:make_sparse_matrix(64, lebesgue(64)),
    ?assertMatch(
        <<16#aaaaaaaaaaaaaaaa:64>>,
        bitswizzle:sm_v_multiply(Mtx64, <<16#00000000:32, 16#ffffffff:32>>)
    ),
    ?assertMatch(
        <<16#5555555555555555:64>>,
        bitswizzle:sm_v_multiply(Mtx64, <<16#ffffffff:32, 16#00000000:32>>)
    ).

%% Same as the previous test, but the matrix has 2 cells.
lebesgue_2cell_mul_test() ->
    Mtx128 = bitswizzle:make_sparse_matrix(128, lebesgue(128)),
    ?assertMatch(
        <<16#aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:128>>,
        bitswizzle:sm_v_multiply(Mtx128, <<0:64, 16#ffffffffffffffff:64>>)
    ),
    ?assertMatch(
        <<16#55555555555555555555555555555555:128>>,
        bitswizzle:sm_v_multiply(Mtx128, <<16#ffffffffffffffff:64, 0:64>>)
    ).

proper_test_() ->
    Props = [single_bit_map_prop()],
    Opts = [{numtests, 1000}, {to_file, user}],
    {timeout, 30, [?_assert(proper:quickcheck(Prop, Opts)) || Prop <- Props]}.

%% Very simple way to understand the matrix transformation is the
%% following: matrix element m_{i j} maps i-th bit of the input vector
%% to j-th bit of the output vector.
%%
%% This test verifies this property by creating a matrix containing a
%% single element m_{i j} and the input vector where only i-th bit is
%% set. Then it verifies that the output is a number where only j-th
%% bit is set.
single_bit_map_prop() ->
    ?FORALL(
        {Size, FromPos, ToPos},
        {oneof([64, 64 * 2, 64 * 3]), range(0, 63), range(0, 63)},
        begin
            Mtx = bitswizzle:make_sparse_matrix(Size, [{ToPos, FromPos}]),
            N0 = 1 bsl FromPos,
            <<N:Size/little-unsigned>> = bitswizzle:sm_v_multiply(Mtx, <<N0:Size/little-unsigned>>),
            Expected = 1 bsl ToPos,
            ?WHENFAIL(
                begin
                    io:format(user, "Mapping bit from position ~p to ~p~n", [FromPos, ToPos]),
                    io:format(user, "Original number ~.16b, expected ~.16b~n", [N0, Expected]),
                    io:format(user, "Matrix: ~p~n", [bitswizzle:dump_sparse_matrix(Mtx)])
                end,
                Expected == N
            )
        end
    ).

%%%% Test helper funtions:

identity_matrix(Size) ->
    [{I, I} || I <- lists:seq(0, Size - 1)].

lebesgue(Size) ->
    Half = Size div 2,
    [{Col * 2, Col} || Col <- lists:seq(0, Half - 1)] ++
        [{(Col - Half) * 2 + 1, Col} || Col <- lists:seq(Half, Size - 1)].

transpose(L) ->
    [{Col, Row} || {Row, Col} <- L].

random_orthogonal_matrix(Size) ->
    L = lists:seq(0, Size - 1),
    Rows = shuffle(L),
    lists:zip(Rows, L).

shuffle(L0) ->
    L1 = lists:sort([{rand:uniform(), I} || I <- L0]),
    {_, L} = lists:unzip(L1),
    L.

bin_to_bitlist(Binary) ->
    Size = size(Binary) * 8,
    Fun = fun
        F(I) when I >= Size ->
            [];
        F(I) ->
            <<_:I, A:1, _/bitstring>> = Binary,
            [A | F(I + 1)]
    end,
    Fun(0).
