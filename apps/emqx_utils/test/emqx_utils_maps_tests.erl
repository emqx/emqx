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

-module(emqx_utils_maps_tests).
-include_lib("eunit/include/eunit.hrl").

-import(emqx_utils_maps, [indent/3, unindent/2]).

best_effort_recursive_sum_test_() ->
    DummyLogger = fun(_) -> ok end,
    [
        ?_assertEqual(
            #{foo => 3},
            emqx_utils_maps:best_effort_recursive_sum(#{foo => 1}, #{foo => 2}, DummyLogger)
        ),
        ?_assertEqual(
            #{foo => 3, bar => 6.0},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => 1, bar => 2.0}, #{foo => 2, bar => 4.0}, DummyLogger
            )
        ),
        ?_assertEqual(
            #{foo => 1, bar => 2},
            emqx_utils_maps:best_effort_recursive_sum(#{foo => 1}, #{bar => 2}, DummyLogger)
        ),
        ?_assertEqual(
            #{foo => #{bar => 42}},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => #{bar => 2}}, #{foo => #{bar => 40}}, DummyLogger
            )
        ),
        fun() ->
            Self = self(),
            Logger = fun(What) -> Self ! {log, What} end,
            ?assertEqual(
                #{foo => 1, bar => 2},
                emqx_utils_maps:best_effort_recursive_sum(
                    #{foo => 1, bar => 2}, #{bar => bar}, Logger
                )
            ),
            receive
                {log, Log} ->
                    ?assertEqual(#{failed_to_merge => bar, bad_value => bar}, Log)
            after 1000 -> error(timeout)
            end
        end,
        ?_assertEqual(
            #{},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => foo}, #{foo => bar}, DummyLogger
            )
        ),
        ?_assertEqual(
            #{foo => 1},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => 1}, #{foo => bar}, DummyLogger
            )
        ),
        ?_assertEqual(
            #{foo => 1},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => bar}, #{foo => 1}, DummyLogger
            )
        ),
        ?_assertEqual(
            #{foo => #{bar => 1}},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => #{bar => 1}}, #{foo => 1}, DummyLogger
            )
        ),
        ?_assertEqual(
            #{foo => #{bar => 1}},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => 1}, #{foo => #{bar => 1}}, DummyLogger
            )
        ),
        ?_assertEqual(
            #{foo => #{bar => 1}},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => 1, bar => ignored}, #{foo => #{bar => 1}}, DummyLogger
            )
        ),
        ?_assertEqual(
            #{foo => #{bar => 2}, bar => #{foo => 1}},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => 1, bar => #{foo => 1}}, #{foo => #{bar => 2}, bar => 2}, DummyLogger
            )
        ),
        ?_assertEqual(
            #{foo => #{bar => 2}, bar => #{foo => 1}},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => #{bar => 2}, bar => 2}, #{foo => 1, bar => #{foo => 1}}, DummyLogger
            )
        ),
        ?_assertEqual(
            #{foo => #{bar => #{}}},
            emqx_utils_maps:best_effort_recursive_sum(
                #{foo => #{bar => #{foo => []}}}, #{foo => 1}, DummyLogger
            )
        )
    ].

key_comparer_test() ->
    Comp = emqx_utils_maps:key_comparer(foo),
    ?assertEqual(
        [
            #{},
            #{baz => 42},
            #{foo => 1},
            #{foo => 42},
            #{foo => bar, baz => 42}
        ],
        lists:sort(Comp, [
            #{foo => 42},
            #{baz => 42},
            #{foo => bar, baz => 42},
            #{foo => 1},
            #{}
        ])
    ).

map_indent_unindent_test_() ->
    M = #{a => 1, b => 2},
    [
        ?_assertEqual(
            #{a => 1, c => #{b => 2}},
            indent(c, [b], M)
        ),
        ?_assertEqual(
            M,
            unindent(c, indent(c, [b], M))
        ),
        ?_assertEqual(
            #{a => 1, b => #{b => 2}},
            indent(b, [b], M)
        ),
        ?_assertEqual(
            M,
            unindent(b, #{a => 1, b => #{b => 2}})
        ),
        ?_assertEqual(
            #{a => 2},
            unindent(b, #{a => 1, b => #{a => 2}})
        ),
        ?_assertEqual(
            #{c => #{a => 1, b => 2}},
            indent(c, [a, b], M)
        ),
        ?_assertEqual(
            #{a => 1, b => 2, c => #{}},
            indent(c, [], M)
        ),
        ?_assertEqual(
            #{a => 1, b => 2, c => #{}},
            indent(c, [d, e, f], M)
        ),
        ?_assertEqual(
            #{a => 1, b => 2},
            unindent(c, M)
        ),
        ?_assertEqual(
            #{a => #{c => 3, d => 4}},
            unindent(b, #{a => #{c => 3}, b => #{a => #{d => 4}}})
        )
    ].
