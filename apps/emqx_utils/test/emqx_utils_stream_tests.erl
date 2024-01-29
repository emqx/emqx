%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

mqueue_test() ->
    _ = erlang:send_after(1, self(), 1),
    _ = erlang:send_after(100, self(), 2),
    _ = erlang:send_after(20, self(), 42),
    ?assertEqual(
        [1, 42, 2],
        emqx_utils_stream:consume(emqx_utils_stream:mqueue(400))
    ).

csv_test() ->
    Data = <<"h1,h2,h3\r\nv1,v2,v3\r\nv4,v5,v6">>,
    ?assertEqual(
        [
            #{<<"h1">> => <<"v1">>, <<"h2">> => <<"v2">>, <<"h3">> => <<"v3">>},
            #{<<"h1">> => <<"v4">>, <<"h2">> => <<"v5">>, <<"h3">> => <<"v6">>}
        ],
        emqx_utils_stream:consume(emqx_utils_stream:csv(Data))
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
