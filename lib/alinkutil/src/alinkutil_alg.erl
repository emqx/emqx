%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022
%%% @doc
%%%
%%% @end
%%% Created : 25. 6月 2022 下午1:27
%%%-------------------------------------------------------------------
-module(alinkutil_alg).
-author("yqfclid").

%% API
-export([
    md5/1,
    shuffle/1,
    shuffle/2,
    strong_random/1
]).

%%%===================================================================
%%% API
%%%===================================================================
-spec(md5(S :: binary() | list()) -> binary()).
md5(S) ->
    SStr = alinkutil_type:to_list(S),
    Md5Bin = erlang:md5(SStr),
    Md5List = alinkutil_type:to_list(Md5Bin),
    Md5Str = lists:flatten(list_to_hex(Md5List)),
    alinkutil_type:to_binary(Md5Str).

-spec(shuffle(T :: list()) -> list()).
shuffle(L) ->
    shuffle(list_to_tuple(L), length(L)).

-spec(shuffle(T :: list(), Len :: integer()) -> list()).
shuffle(T, 0) ->
    tuple_to_list(T);
shuffle(T, Len) ->
    Rand = rand:uniform(Len),
    A = element(Len, T),
    B = element(Rand, T),
    T1 = setelement(Len, T, B),
    T2 = setelement(Rand, T1, A),
    shuffle(T2, Len - 1).

-spec(strong_random(Max :: integer()) -> integer()).
strong_random(Max) ->
    <<Random:32>> = crypto:strong_rand_bytes(4),
    Random rem Max.
%%%===================================================================
%%% Internal functions
%%%===================================================================
list_to_hex(L) ->
    lists:map(fun int_to_hex/1, L).

int_to_hex(N) when N < 256 ->
    [hex(N div 16), hex(N rem 16)].

hex(N) when N < 10 ->
    $0 + N;
hex(N) when N >=10, N < 16 ->
    $a + (N - 10).