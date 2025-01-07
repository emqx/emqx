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

-module(emqx_topic_index_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(emqx_proper_types, [scaled/2]).

all() ->
    [
        {group, ets},
        {group, gb_tree}
    ].

groups() ->
    All = emqx_common_test_helpers:all(?MODULE),
    [
        {ets, All},
        {gb_tree, All}
    ].

init_per_group(ets, Config) ->
    [{index_module, emqx_topic_index} | Config];
init_per_group(gb_tree, Config) ->
    [{index_module, emqx_topic_gbt_pterm} | Config].

end_per_group(_Group, _Config) ->
    ok.

get_module(Config) ->
    proplists:get_value(index_module, Config).

t_insert(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    true = M:insert(<<"sensor/1/metric/2">>, t_insert_1, <<>>, Tab),
    true = M:insert(<<"sensor/+/#">>, t_insert_2, <<>>, Tab),
    true = M:insert(<<"sensor/#">>, t_insert_3, <<>>, Tab),
    ?assertEqual(<<"sensor/#">>, topic(match(M, <<"sensor">>, Tab))),
    ?assertEqual(t_insert_3, id(match(M, <<"sensor">>, Tab))).

t_insert_filter(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    Topic = <<"sensor/+/metric//#">>,
    true = M:insert(Topic, 1, <<>>, Tab),
    true = M:insert(emqx_trie_search:filter(Topic), 2, <<>>, Tab),
    ?assertEqual(
        [Topic, Topic],
        [topic(X) || X <- matches(M, <<"sensor/1/metric//2">>, Tab)]
    ).

t_match(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    true = M:insert(<<"sensor/1/metric/2">>, t_match_1, <<>>, Tab),
    true = M:insert(<<"sensor/+/#">>, t_match_2, <<>>, Tab),
    true = M:insert(<<"sensor/#">>, t_match_3, <<>>, Tab),
    ?assertMatch(
        [<<"sensor/#">>, <<"sensor/+/#">>],
        [topic(X) || X <- matches(M, <<"sensor/1">>, Tab)]
    ).

t_match2(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    true = M:insert(<<"#">>, t_match2_1, <<>>, Tab),
    true = M:insert(<<"+/#">>, t_match2_2, <<>>, Tab),
    true = M:insert(<<"+/+/#">>, t_match2_3, <<>>, Tab),
    ?assertEqual(
        [<<"#">>, <<"+/#">>, <<"+/+/#">>],
        [topic(X) || X <- matches(M, <<"a/b/c">>, Tab)]
    ),
    ?assertEqual(
        false,
        M:match(<<"$SYS/broker/zenmq">>, Tab)
    ),
    ?assertEqual(
        [],
        matches(M, <<"$SYS/broker/zenmq">>, Tab)
    ).

t_match3(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    Records = [
        {<<"d/#">>, t_match3_1},
        {<<"a/b/+">>, t_match3_2},
        {<<"a/#">>, t_match3_3},
        {<<"#">>, t_match3_4},
        {<<"$SYS/#">>, t_match3_sys}
    ],
    lists:foreach(
        fun({Topic, ID}) -> M:insert(Topic, ID, <<>>, Tab) end,
        Records
    ),
    Matched = matches(M, <<"a/b/c">>, Tab),
    case length(Matched) of
        3 -> ok;
        _ -> error({unexpected, Matched})
    end,
    ?assertEqual(
        t_match3_sys,
        id(match(M, <<"$SYS/a/b/c">>, Tab))
    ).

t_match4(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    Records = [{<<"/#">>, t_match4_1}, {<<"/+">>, t_match4_2}, {<<"/+/a/b/c">>, t_match4_3}],
    lists:foreach(
        fun({Topic, ID}) -> M:insert(Topic, ID, <<>>, Tab) end,
        Records
    ),
    ?assertEqual(
        [<<"/#">>, <<"/+">>],
        [topic(X) || X <- matches(M, <<"/">>, Tab)]
    ),
    ?assertEqual(
        [<<"/#">>, <<"/+/a/b/c">>],
        [topic(X) || X <- matches(M, <<"/0/a/b/c">>, Tab)]
    ).

t_match5(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    T = <<"a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z">>,
    Records = [
        {<<"#">>, t_match5_1},
        {<<T/binary, "/#">>, t_match5_2},
        {<<T/binary, "/+">>, t_match5_3}
    ],
    lists:foreach(
        fun({Topic, ID}) -> M:insert(Topic, ID, <<>>, Tab) end,
        Records
    ),
    ?assertEqual(
        [<<"#">>, <<T/binary, "/#">>],
        [topic(X) || X <- matches(M, T, Tab)]
    ),
    ?assertEqual(
        [<<"#">>, <<T/binary, "/#">>, <<T/binary, "/+">>],
        [topic(X) || X <- matches(M, <<T/binary, "/1">>, Tab)]
    ).

t_match6(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    T = <<"a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z">>,
    W = <<"+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/#">>,
    M:insert(W, ID = t_match6, <<>>, Tab),
    ?assertEqual(ID, id(match(M, T, Tab))).

t_match7(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    T = <<"a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z">>,
    W = <<"a/+/c/+/e/+/g/+/i/+/k/+/m/+/o/+/q/+/s/+/u/+/w/+/y/+/#">>,
    M:insert(W, t_match7, <<>>, Tab),
    ?assertEqual(W, topic(match(M, T, Tab))).

t_match8(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    Filters = [<<"+">>, <<"dev/global/sensor">>, <<"dev/+/sensor/#">>],
    IDs = [1, 2, 3],
    Keys = [{F, ID} || F <- Filters, ID <- IDs],
    lists:foreach(
        fun({F, ID}) ->
            M:insert(F, ID, <<>>, Tab)
        end,
        Keys
    ),
    Topic = <<"dev/global/sensor">>,
    Matches = lists:sort(matches(M, Topic, Tab)),
    ?assertEqual(
        [
            <<"dev/+/sensor/#">>,
            <<"dev/+/sensor/#">>,
            <<"dev/+/sensor/#">>,
            <<"dev/global/sensor">>,
            <<"dev/global/sensor">>,
            <<"dev/global/sensor">>
        ],
        [emqx_topic_index:get_topic(Match) || Match <- Matches]
    ).

t_match_fast_forward(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    M:insert(<<"a/b/1/2/3/4/5/6/7/8/9/#">>, id1, <<>>, Tab),
    M:insert(<<"z/y/x/+/+">>, id2, <<>>, Tab),
    M:insert(<<"a/b/c/+">>, id3, <<>>, Tab),
    ?assertEqual(id1, id(match(M, <<"a/b/1/2/3/4/5/6/7/8/9/0">>, Tab))),
    ?assertEqual([id1], [id(X) || X <- matches(M, <<"a/b/1/2/3/4/5/6/7/8/9/0">>, Tab)]).

t_match_unique(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    M:insert(<<"a/b/c">>, t_match_id1, <<>>, Tab),
    M:insert(<<"a/b/+">>, t_match_id1, <<>>, Tab),
    M:insert(<<"a/b/c/+">>, t_match_id2, <<>>, Tab),
    ?assertEqual(
        [t_match_id1, t_match_id1],
        [id(X) || X <- matches(M, <<"a/b/c">>, Tab, [])]
    ),
    ?assertEqual(
        [t_match_id1],
        [id(X) || X <- matches(M, <<"a/b/c">>, Tab, [unique])]
    ).

t_match_wildcard_edge_cases(Config) ->
    M = get_module(Config),
    CommonTopics = [
        <<"a/b">>,
        <<"a/b/#">>,
        <<"a/b/#">>,
        <<"a/b/c">>,
        <<"a/b/+">>,
        <<"a/b/d">>,
        <<"a/+/+">>,
        <<"a/+/#">>
    ],
    Datasets =
        [
            %% Topics, TopicName, Results
            {CommonTopics, <<"a/b/c">>, [2, 3, 4, 5, 7, 8]},
            {CommonTopics, <<"a/b">>, [1, 2, 3, 8]},
            {[<<"+/b/c">>, <<"/">>], <<"a/b/c">>, [1]},
            {[<<"#">>, <<"/">>], <<"a">>, [1]},
            {[<<"/">>, <<"+">>], <<"a">>, [2]}
        ],
    F = fun({Topics, TopicName, Expected}) ->
        Tab = M:new(),
        _ = [M:insert(T, N, <<>>, Tab) || {N, T} <- lists:enumerate(Topics)],
        ?assertEqual(
            lists:last(Expected),
            id(M:match(TopicName, Tab)),
            #{"Base topics" => Topics, "Topic name" => TopicName}
        ),
        ?assertEqual(
            Expected,
            [id(X) || X <- matches(M, TopicName, Tab, [unique])],
            #{"Base topics" => Topics, "Topic name" => TopicName}
        )
    end,
    lists:foreach(F, Datasets).

t_prop_edgecase(Config) ->
    M = get_module(Config),
    Tab = M:new(),
    Topic = <<"01/01">>,
    Filters = [
        {1, <<>>},
        {2, <<"+/01">>},
        {3, <<>>},
        {4, <<"+/+/01">>}
    ],
    _ = [M:insert(F, N, <<>>, Tab) || {N, F} <- Filters],
    ?assertMatch([2], [id(X) || X <- matches(M, Topic, Tab, [unique])]).

t_prop_matches(Config) ->
    M = get_module(Config),
    ?assert(
        proper:quickcheck(
            topic_matches_prop(M),
            [{max_size, 100}, {numtests, 100}]
        )
    ),
    Statistics = [{C, account(C)} || C <- [filters, topics, matches, maxhits]],
    ct:pal("Statistics: ~p", [maps:from_list(Statistics)]).

topic_matches_prop(M) ->
    ?FORALL(
        % Generate a longer list of topics and a shorter list of topic filter patterns.
        #{
            topics := TTopics,
            patterns := Pats
        },
        emqx_proper_types:fixedmap(#{
            % NOTE
            % Beware adding non-empty contraint, proper will have a hard time with `topic_t/1`
            % for some reason.
            topics => scaled(4, list(topic_t([1, 2, 3, 4]))),
            patterns => list(topic_filter_pattern_t())
        }),
        begin
            Tab = M:new(),
            Topics = [emqx_topic:join(T) || T <- TTopics],
            % Produce topic filters from generated topics and patterns.
            % Number of filters is equal to the number of patterns, most of the time.
            Filters = lists:enumerate(mk_filters(Pats, TTopics)),
            _ = [M:insert(F, N, <<>>, Tab) || {N, F} <- Filters],
            % Gather some basic statistics
            _ = account(filters, length(Filters)),
            _ = account(topics, NTopics = length(Topics)),
            _ = account(maxhits, NTopics * NTopics),
            % Verify that matching each topic against index returns the same results as
            % matching it against the list of filters one by one.
            lists:all(
                fun(Topic) ->
                    Ids1 = [id(X) || X <- matches(M, Topic, Tab, [unique])],
                    Ids2 = lists:filtermap(
                        fun({N, F}) ->
                            case emqx_topic:match(Topic, F) of
                                true -> {true, N};
                                false -> false
                            end
                        end,
                        Filters
                    ),
                    % Account a number of matches to compute hitrate later
                    _ = account(matches, length(Ids1)),
                    case (Ids2 -- Ids1) ++ (Ids2 -- Ids1) of
                        [] ->
                            true;
                        [_ | _] = _Differences ->
                            ct:pal(
                                "Topic name: ~p~n"
                                "Index results: ~p~n"
                                "Topic match results: ~p~n"
                                "Filters: ~p~n",
                                [Topic, Ids1, Ids2, Filters]
                            ),
                            false
                    end
                end,
                Topics
            )
        end
    ).

mk_filters([Pat | PRest], [Topic | TRest]) ->
    [emqx_topic:join(mk_topic_filter(Pat, Topic)) | mk_filters(PRest, TRest)];
mk_filters(_, _) ->
    [].

account(Counter, N) ->
    put({?MODULE, Counter}, account(Counter) + N).

account(Counter) ->
    emqx_maybe:define(get({?MODULE, Counter}), 0).

%%

match(M, T, Tab) ->
    M:match(T, Tab).

matches(M, T, Tab) ->
    lists:sort(M:matches(T, Tab, [])).

matches(M, T, Tab, Opts) ->
    M:matches(T, Tab, Opts).

id(Match) ->
    emqx_trie_search:get_id(Match).

topic(Match) ->
    emqx_trie_search:get_topic(Match).

%%

topic_t(EntropyWeights) ->
    EWLast = lists:last(EntropyWeights),
    ?LET(L, scaled(1 / 4, list(EWLast)), begin
        EWs = lists:sublist(EntropyWeights ++ L, length(L)),
        ?SIZED(S, [oneof([topic_level_t(S * EW), topic_level_fixed_t()]) || EW <- EWs])
    end).

topic_level_t(Entropy) ->
    S = floor(1 + math:log2(Entropy) / 4),
    ?LET(I, range(1, Entropy), iolist_to_binary(io_lib:format("~*.16.0B", [S, I]))).

topic_level_fixed_t() ->
    oneof([
        <<"foo">>,
        <<"bar">>,
        <<"baz">>,
        <<"xyzzy">>
    ]).

topic_filter_pattern_t() ->
    list(topic_level_pattern_t()).

topic_level_pattern_t() ->
    frequency([
        {5, level},
        {2, '+'},
        {1, '#'}
    ]).

mk_topic_filter([], _) ->
    [];
mk_topic_filter(_, []) ->
    [];
mk_topic_filter(['#' | _], _) ->
    ['#'];
mk_topic_filter(['+' | Rest], [_ | Levels]) ->
    ['+' | mk_topic_filter(Rest, Levels)];
mk_topic_filter([level | Rest], [L | Levels]) ->
    [L | mk_topic_filter(Rest, Levels)].
