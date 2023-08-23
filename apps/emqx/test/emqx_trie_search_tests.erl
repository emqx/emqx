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

-module(emqx_trie_search_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

escape_test() ->
    ?assert(
        proper:quickcheck(
            ?FORALL(Words, topic_filter(), test_esc_unesc(Words)),
            [{max_size, 100}, {numtests, 1000}]
        )
    ).

lex_order_test() ->
    ?assert(
        proper:quickcheck(
            ?FORALL(Filters, non_empty(filters()), test_lex_order(Filters)),
            [{max_size, 100}, {numtests, 100}]
        )
    ).

test_lex_order(Filters0) ->
    Seq = lists:seq(1, length(Filters0)),
    Filters = lists:zip(Filters0, Seq),
    FiltersEsc0 = lists:sort(escape_filters(Filters)),
    FiltersEsc = [{emqx_trie_search:unescape(F), S} || {F, S} <- FiltersEsc0],
    FiltersTokens0 = lists:sort(split(Filters)),
    FiltersTokens = [{join(F), S} || {F, S} <- FiltersTokens0],
    ?assertEqual(FiltersTokens, FiltersEsc),
    true.

test_esc_unesc(Filter) ->
    Escaped = escape(Filter),
    ?assertEqual(Filter, emqx_trie_search:unescape(Escaped)),
    true.

join(Words) ->
    iolist_to_binary(lists:join("/", lists:map(fun rt/1, Words))).

escape(Filter) ->
    emqx_trie_search:join(emqx_trie_search:words(Filter)).

escape_filters([]) ->
    [];
escape_filters([{F, Seq} | Filters]) ->
    [{escape(F), Seq} | escape_filters(Filters)].

split([]) ->
    [];
split([{F, Seq} | Filters]) ->
    [{lists:map(fun tr/1, emqx_topic:tokens(F)), Seq} | split(Filters)].

tr(<<"#">>) -> '#';
tr(<<"+">>) -> '+';
tr(Word) -> Word.

rt('#') -> <<"#">>;
rt('+') -> <<"+">>;
rt(Word) -> Word.

filters() ->
    list(topic_filter()).

topic_filter() ->
    ?LET(
        W,
        non_empty(
            list(
                frequency([
                    {5, topic_level()},
                    {2, <<"+">>},
                    {1, <<"#">>}
                ])
            )
        ),
        iolist_to_binary(lists:join("/", W))
    ).

topic_level() ->
    ?LET(L, list(byte()), iolist_to_binary(L)).
