%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements an encoding schema for bit vectors that
%% aims to fulfill the following criteria:
%%
%% 1. Cost of sending encoded bitmask over the wire is low for both
%% very dense and very sparse masks, e.g. both 11111111 and 000010000,
%% that correspond to masks common in the catchup and the realtime
%% cases respectively, can be encoded efficiently.
%%
%% 2. Encoding and decoding can be done incrementally, and encoding of
%% 0 is a NOP: there's no need to run any code to encode _absence_ of
%% data.
%%
%% 3. Encoding and decoding produce minimum of garbage in the heap and
%% can be implemented efficiently in Erlang VM.
%%
%% We use a variation of run length encoding. Bit vector is encoded as
%% a list of lengths of continuous intervals of 0s and 1s.
%%
%% Once decoder reaches end of the current interval, it flips its
%% state (state' := !state) and continues to produce elements equal to
%% its state until the next flip. Initial decoder state is `false'.
%%
%% For example, encoding of 1111111 is [0], and encoding of 00001000
%% is [4, 1].
-module(emqx_ds_dispatch_mask).

%% API:
-export([dec_make/1, dec_pop/1, filter/2, filter_and_size/2]).
-export([enc_make/0, enc_push_true/2, enc_finalize/2, encode/1]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([dec/0, enc/0, encoding/0]).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% Decoder state:
%% erlfmt-ignore
-record(dec_s,
    {
        %% Current state of the decoder; whether to produce `true's or
        %% `false's:
        istrue = false :: boolean(),
        %% Countdown to the next state flip. Flip happens when the
        %% counter reaches 0. If there are no more flips, this counter
        %% is set to a negative value, so it never reaches 0 again:
        countdown :: integer(),
        %% List of state flips that should occur after the current
        %% countdown reaches 0:
        flips :: [non_neg_integer()]
    }
).

-opaque dec() :: #dec_s{}.

%% Encoder state:
%% erlfmt-ignore
-record(enc_s,
    {
        %% Position of the last true element in the mask so far:
        last_true_pos :: non_neg_integer(),
        %% Number of adjacent true elements so far:
        n_trues :: non_neg_integer(),
        %% List of encoder state flips, reversed:
        flips :: [non_neg_integer()]
    }
).

-opaque enc() :: #enc_s{} | enc_s_empty.

-type encoding() :: [non_neg_integer()].

%%================================================================================
%% API functions
%%================================================================================

%% @doc Initialize the decoder:
-spec dec_make(encoding()) -> dec().
dec_make([]) ->
    #dec_s{countdown = -1, flips = []};
dec_make([Countdown | Flips]) ->
    #dec_s{countdown = Countdown, flips = Flips}.

%% @doc Consume one element from the decoder
-spec dec_pop(dec()) -> {boolean(), dec()}.
dec_pop(#dec_s{countdown = 0, istrue = IsTrue, flips = []}) ->
    dec_pop(#dec_s{
        istrue = not IsTrue,
        countdown = -1,
        flips = []
    });
dec_pop(#dec_s{countdown = 0, istrue = IsTrue, flips = [Countdown | Flips]}) ->
    dec_pop(#dec_s{
        istrue = not IsTrue,
        countdown = Countdown,
        flips = Flips
    });
dec_pop(Dec = #dec_s{istrue = Result, countdown = Countdown}) ->
    {Result, Dec#dec_s{countdown = Countdown - 1}}.

%% @doc Make an encoder
-spec enc_make() -> enc().
enc_make() ->
    enc_s_empty.

%% @doc Add `true' at position `Index' to the encoding of the bit
%% vector. Indices are 0-based. Note: subsequent calls of this
%% function should be done for increasing values of `Index'.
-spec enc_push_true(non_neg_integer(), enc()) -> enc().
enc_push_true(Index, enc_s_empty) ->
    %% This is the first `true' in the dispatch mask. Here we add a
    %% flip from 0 and set up encoder for a new continous 1-interval:
    #enc_s{
        last_true_pos = Index,
        n_trues = 1,
        flips = [Index]
    };
enc_push_true(Index, Enc = #enc_s{last_true_pos = LastTrue, n_trues = N}) when
    Index =:= LastTrue + 1
->
    %% This 1 is adjacent to the previous one. Extend the current
    %% 1-interval:
    Enc#enc_s{
        last_true_pos = Index,
        n_trues = N + 1
    };
enc_push_true(Index, #enc_s{last_true_pos = LastTrue, flips = Flips, n_trues = N}) when
    Index > LastTrue
->
    %% There has been a gap in adjacent 1s.
    %%
    %% 1. Push length of the last continous 1-interval to the flip
    %% list
    %%
    %% 2. Calculate length of the 0-interval and push it to the flip
    %% list. It's equal to (`Index' - 1) - `LastTrue'. Note: we
    %% subtract 1 because `Index' itself points at a 1, so it should
    %% not be included in the run length of the 0-interval.
    %%
    %% 3. Open a new 1-interval
    #enc_s{
        last_true_pos = Index,
        n_trues = 1,
        flips = [(Index - 1) - LastTrue, N | Flips]
    }.

%% @doc Finish encoding of the vector. First argument is size of the
%% vector, second is the encoder.
-spec enc_finalize(non_neg_integer(), enc()) -> encoding().
enc_finalize(_, enc_s_empty) ->
    [];
enc_finalize(Size, #enc_s{last_true_pos = LastTrue, flips = Flips}) when
    LastTrue =:= Size - 1
->
    %% Last element of the vector was a 1:
    lists:reverse(Flips);
enc_finalize(Size, #enc_s{last_true_pos = LastTrue, flips = Flips, n_trues = N}) when
    LastTrue < Size - 1
->
    %% Last element(s) of the vector were 0s, add one last flip from
    %% the 1 state to 0 state:
    lists:reverse([N | Flips]).

%% @doc Simplified version of the algorithm that compresses a list of
%% booleans. Inefficient compared to
%% `enc_make'/`enc_push_true'/`enc_finalize'.
-spec encode([boolean()]) -> encoding().
encode(L) ->
    do_encode(0, L, enc_make()).

%% @doc Filter elements of a list according to the dispatch mask.
-spec filter([A], encoding()) -> [A].
filter(L0, Encoding) ->
    {_Size, L} = filter_and_size(L0, Encoding),
    L.

%% @doc Filter elements of a list according to the dispatch mask,
%% return filtered list and its length:
-spec filter_and_size([A], encoding()) -> {non_neg_integer(), [A]}.
filter_and_size(L, Encoding) ->
    do_filter(L, dec_make(Encoding), [], 0).

%%================================================================================
%% Internal functions
%%================================================================================

-spec do_encode(non_neg_integer(), [boolean()], enc()) -> encoding().
do_encode(Size, [], Acc) ->
    enc_finalize(Size, Acc);
do_encode(Idx, [false | Rest], Acc) ->
    do_encode(Idx + 1, Rest, Acc);
do_encode(Idx, [true | Rest], Acc0) ->
    Acc = enc_push_true(Idx, Acc0),
    do_encode(Idx + 1, Rest, Acc).

-spec do_filter([A], dec(), [A], non_neg_integer()) -> {non_neg_integer(), [A]}.
do_filter([], _, Acc, Size) ->
    {Size, lists:reverse(Acc)};
do_filter([Elem | Rest], Dec0, Acc, Size) ->
    {Keep, Dec} = dec_pop(Dec0),
    case Keep of
        true ->
            do_filter(Rest, Dec, [Elem | Acc], Size + 1);
        false ->
            do_filter(Rest, Dec, Acc, Size)
    end.

%%================================================================================
%% Test
%%================================================================================

-ifdef(TEST).

filter__test() ->
    Mask = encode([]),
    ?assertMatch(
        [],
        filter([], Mask),
        Mask
    ),
    ?assertMatch(
        {0, []},
        filter_and_size([], Mask),
        Mask
    ).

filter_tf_test() ->
    Mask = encode([true, false]),
    ?assertMatch(
        [1],
        filter([1, 2], Mask)
    ),
    ?assertMatch(
        {1, [1]},
        filter_and_size([1, 2], Mask)
    ).

filter_ft_test() ->
    Mask = encode([false, true]),
    ?assertMatch(
        {1, [2]},
        filter_and_size([1, 2], Mask)
    ).

filter_ff_test() ->
    Mask = encode([false, false]),
    ?assertMatch(
        {0, []},
        filter_and_size([1, 2], Mask)
    ).

filter_tt_test() ->
    Mask = encode([true, true]),
    ?assertMatch(
        {2, [1, 2]},
        filter_and_size([1, 2], Mask)
    ).

encode_test() ->
    %% Empty mask or mask that consists only of 0s:
    ?assertMatch(
        [],
        encode([])
    ),
    ?assertMatch(
        [],
        encode([false, false, false])
    ),
    %% Mask that consists only of 1s:
    ?assertMatch(
        [0],
        encode([true])
    ),
    ?assertMatch(
        [0],
        encode([true, true, true, true, true, true])
    ),
    %% Mixed:
    ?assertMatch(
        [1],
        encode([false, true, true, true])
    ),
    ?assertMatch(
        [2],
        encode([false, false, true, true, true])
    ),
    ?assertMatch(
        [0, 1],
        encode([true, false, false])
    ),
    ?assertMatch(
        [3, 1, 2],
        encode([false, false, false, true, false, false, true])
    ),
    ?assertMatch(
        [0, 1, 1],
        encode([true, false, true])
    ),
    ?assertMatch(
        [1, 1, 1],
        encode([false, true, false, true])
    ),
    ?assertMatch(
        [1, 3],
        encode([false, true, true, true, false, false, false])
    ).

%% This property verifies that for each bit vector (a random list of
%% booleans), encoding and decoding return the original vector.
encode_decode_prop() ->
    ?FORALL(
        L,
        list(boolean()),
        begin
            Size = length(L),
            Encoding = encode(L),
            Decoded = test_decode(dec_make(Encoding), Size),
            ?assertEqual(
                L,
                Decoded,
                #{
                    encoding => Encoding
                }
            ),
            true
        end
    ).

%% eunit glue code:
encode_decode_prop_test_() ->
    Opts = [{numtests, 1000}, {to_file, user}],
    {timeout, 30, [?_assert(proper:quickcheck(encode_decode_prop(), Opts))]}.

%% Helper functions:

test_decode(_, 0) ->
    [];
test_decode(Dec0, Size) ->
    {Result, Dec} = dec_pop(Dec0),
    [Result | test_decode(Dec, Size - 1)].

-endif.
