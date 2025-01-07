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

-module(emqx_utils_stream_tests).

-include_lib("eunit/include/eunit.hrl").

empty_test() ->
    S = emqx_utils_stream:empty(),
    ?assertEqual([], emqx_utils_stream:next(S)).

empty_consume_test() ->
    S = emqx_utils_stream:empty(),
    ?assertEqual([], emqx_utils_stream:consume(S)).

chain_empties_test() ->
    S = emqx_utils_stream:chain(
        emqx_utils_stream:empty(),
        emqx_utils_stream:empty()
    ),
    ?assertEqual([], emqx_utils_stream:next(S)).

chain_list_test() ->
    S = emqx_utils_stream:chain(
        emqx_utils_stream:list([1, 2, 3]),
        emqx_utils_stream:list([4, 5, 6])
    ),
    ?assertEqual(
        [1, 2, 3, 4, 5, 6],
        emqx_utils_stream:consume(S)
    ).

chain_take_test() ->
    S = emqx_utils_stream:chain(
        emqx_utils_stream:list([1, 2, 3]),
        emqx_utils_stream:list([4, 5, 6, 7, 8])
    ),
    ?assertMatch(
        {[1, 2, 3, 4, 5], _SRest},
        emqx_utils_stream:consume(5, S)
    ),
    {_, SRest} = emqx_utils_stream:consume(5, S),
    ?assertEqual(
        [6, 7, 8],
        emqx_utils_stream:consume(5, SRest)
    ).

chain_list_map_test() ->
    S = emqx_utils_stream:map(
        fun integer_to_list/1,
        emqx_utils_stream:chain(
            emqx_utils_stream:list([1, 2, 3]),
            emqx_utils_stream:chain(
                emqx_utils_stream:empty(),
                emqx_utils_stream:list([4, 5, 6])
            )
        )
    ),
    ?assertEqual(
        ["1", "2", "3", "4", "5", "6"],
        emqx_utils_stream:consume(S)
    ).

filter_test() ->
    S = emqx_utils_stream:filter(
        fun(N) -> N rem 2 =:= 0 end,
        emqx_utils_stream:chain(
            emqx_utils_stream:list([1, 2, 3]),
            emqx_utils_stream:chain(
                emqx_utils_stream:empty(),
                emqx_utils_stream:list([4, 5, 6])
            )
        )
    ),
    ?assertEqual(
        [2, 4, 6],
        emqx_utils_stream:consume(S)
    ).

drop_test() ->
    S = emqx_utils_stream:drop(2, emqx_utils_stream:list([1, 2, 3, 4, 5])),
    ?assertEqual(
        [3, 4, 5],
        emqx_utils_stream:consume(S)
    ).

foreach_test() ->
    Self = self(),
    ok = emqx_utils_stream:foreach(
        fun(N) -> erlang:send(Self, N) end,
        emqx_utils_stream:chain(
            emqx_utils_stream:list([1, 2, 3]),
            emqx_utils_stream:chain(
                emqx_utils_stream:empty(),
                emqx_utils_stream:list([4, 5, 6])
            )
        )
    ),
    ?assertEqual(
        [1, 2, 3, 4, 5, 6],
        emqx_utils_stream:consume(emqx_utils_stream:mqueue(100))
    ).

fold_test() ->
    S = emqx_utils_stream:drop(2, emqx_utils_stream:list([1, 2, 3, 4, 5])),
    ?assertEqual(
        3 * 4 * 5,
        emqx_utils_stream:fold(fun(X, P) -> P * X end, 1, S)
    ).

fold_n_test() ->
    S = emqx_utils_stream:repeat(
        emqx_utils_stream:map(
            fun(X) -> X * 2 end,
            emqx_utils_stream:list([1, 2, 3])
        )
    ),
    ?assertMatch(
        {2 + 4 + 6 + 2 + 4 + 6 + 2, _SRest},
        emqx_utils_stream:fold(fun(X, Sum) -> Sum + X end, 0, _N = 7, S)
    ).

chainmap_test() ->
    S = emqx_utils_stream:chainmap(
        fun(N) ->
            case N rem 2 of
                1 ->
                    emqx_utils_stream:chain(
                        emqx_utils_stream:chain(emqx_utils_stream:list([N]), []),
                        emqx_utils_stream:list([N + 1])
                    );
                0 ->
                    emqx_utils_stream:empty()
            end
        end,
        emqx_utils_stream:chain(
            emqx_utils_stream:list([1, 2, 3]),
            emqx_utils_stream:chain(
                emqx_utils_stream:empty(),
                emqx_utils_stream:list([4, 5, 6])
            )
        )
    ),
    ?assertEqual(
        [1, 2, 3, 4, 5, 6],
        emqx_utils_stream:consume(S)
    ).

transpose_test() ->
    S = emqx_utils_stream:transpose([
        emqx_utils_stream:list([1, 2, 3]),
        emqx_utils_stream:list([4, 5, 6, 7])
    ]),
    ?assertEqual(
        [[1, 4], [2, 5], [3, 6]],
        emqx_utils_stream:consume(S)
    ).

transpose_none_test() ->
    ?assertEqual(
        [],
        emqx_utils_stream:consume(emqx_utils_stream:transpose([]))
    ).

transpose_one_test() ->
    S = emqx_utils_stream:transpose([emqx_utils_stream:list([1, 2, 3])]),
    ?assertEqual(
        [[1], [2], [3]],
        emqx_utils_stream:consume(S)
    ).

transpose_many_test() ->
    S = emqx_utils_stream:transpose([
        emqx_utils_stream:list([1, 2, 3]),
        emqx_utils_stream:list([4, 5, 6, 7]),
        emqx_utils_stream:list([8, 9])
    ]),
    ?assertEqual(
        [[1, 4, 8], [2, 5, 9]],
        emqx_utils_stream:consume(S)
    ).

transpose_many_empty_test() ->
    S = emqx_utils_stream:transpose([
        emqx_utils_stream:list([1, 2, 3]),
        emqx_utils_stream:list([4, 5, 6, 7]),
        emqx_utils_stream:empty()
    ]),
    ?assertEqual(
        [],
        emqx_utils_stream:consume(S)
    ).

repeat_test() ->
    S = emqx_utils_stream:repeat(emqx_utils_stream:list([1, 2, 3])),
    ?assertMatch(
        {[1, 2, 3, 1, 2, 3, 1, 2], _},
        emqx_utils_stream:consume(8, S)
    ),
    {_, SRest} = emqx_utils_stream:consume(8, S),
    ?assertMatch(
        {[3, 1, 2, 3, 1, 2, 3, 1], _},
        emqx_utils_stream:consume(8, SRest)
    ).

repeat_empty_test() ->
    S = emqx_utils_stream:repeat(emqx_utils_stream:list([])),
    ?assertEqual(
        [],
        emqx_utils_stream:consume(8, S)
    ).

transpose_repeat_test() ->
    S = emqx_utils_stream:transpose([
        emqx_utils_stream:repeat(emqx_utils_stream:list([1, 2])),
        emqx_utils_stream:list([4, 5, 6, 7, 8])
    ]),
    ?assertEqual(
        [[1, 4], [2, 5], [1, 6], [2, 7], [1, 8]],
        emqx_utils_stream:consume(S)
    ).

mqueue_test() ->
    _ = erlang:send_after(1, self(), 1),
    _ = erlang:send_after(100, self(), 2),
    _ = erlang:send_after(20, self(), 42),
    ?assertEqual(
        [1, 42, 2],
        emqx_utils_stream:consume(emqx_utils_stream:mqueue(400))
    ).

interleave_test() ->
    S1 = emqx_utils_stream:list([1, 2, 3]),
    S2 = emqx_utils_stream:list([a, b, c, d]),
    ?assertEqual(
        [1, 2, a, b, 3, c, d],
        emqx_utils_stream:consume(emqx_utils_stream:interleave([{2, S1}, {2, S2}], true))
    ).

interleave_stop_test() ->
    S1 = emqx_utils_stream:const(1),
    S2 = emqx_utils_stream:list([a, b, c, d]),
    ?assertEqual(
        [1, 1, a, b, 1, 1, c, d, 1, 1],
        emqx_utils_stream:consume(emqx_utils_stream:interleave([{2, S1}, {2, S2}], false))
    ).

ets_test() ->
    T = ets:new(tab, [ordered_set]),
    Objects = [{N, N} || N <- lists:seq(1, 10)],
    ets:insert(T, Objects),
    S = emqx_utils_stream:ets(
        fun
            (undefined) -> ets:match_object(T, '_', 4);
            (Cont) -> ets:match_object(Cont)
        end
    ),
    ?assertEqual(
        Objects,
        emqx_utils_stream:consume(S)
    ).

csv_test() ->
    Data1 = <<"h1,h2,h3\r\nvv1,vv2,vv3\r\nvv4,vv5,vv6">>,
    ?assertEqual(
        [
            #{<<"h1">> => <<"vv1">>, <<"h2">> => <<"vv2">>, <<"h3">> => <<"vv3">>},
            #{<<"h1">> => <<"vv4">>, <<"h2">> => <<"vv5">>, <<"h3">> => <<"vv6">>}
        ],
        emqx_utils_stream:consume(emqx_utils_stream:csv(Data1))
    ),

    Data2 = <<"h1, h2, h3\nvv1, vv2, vv3\nvv4,vv5,vv6\n">>,
    ?assertEqual(
        [
            #{<<"h1">> => <<"vv1">>, <<"h2">> => <<"vv2">>, <<"h3">> => <<"vv3">>},
            #{<<"h1">> => <<"vv4">>, <<"h2">> => <<"vv5">>, <<"h3">> => <<"vv6">>}
        ],
        emqx_utils_stream:consume(emqx_utils_stream:csv(Data2))
    ),

    ?assertEqual(
        [],
        emqx_utils_stream:consume(emqx_utils_stream:csv(<<"">>))
    ),

    BadData = <<"h1,h2,h3\r\nv1,v2,v3\r\nv4,v5">>,
    ?assertException(
        error,
        bad_format,
        emqx_utils_stream:consume(emqx_utils_stream:csv(BadData))
    ).
