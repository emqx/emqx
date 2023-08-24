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

%% @doc Topic index for matching topics to topic filters.
%%
%% Works on top of a ordered collection data set, such as ETS ordered_set table.
%% Keys are tuples constructed from parsed topic filters and record IDs,
%% wrapped in a tuple to order them strictly greater than unit tuple (`{}`).
%% Existing table may be used if existing keys will not collide with index keys.
%%
%% Designed to effectively answer questions like:
%% 1. Does any topic filter match given topic?
%% 2. Which records are associated with topic filters matching given topic?
%% 3. Which topic filters match given topic?
%% 4. Which record IDs are associated with topic filters matching given topic?
%%
%% Trie-search algorithm:
%%
%% Given a 3-level topic (e.g. a/b/c), if we leave out '#' for now,
%% all possible subscriptions of a/b/c can be enumerated as below:
%%
%% a/b/c
%% a/b/+
%% a/+/c <--- subscribed
%% a/+/+
%% +/b/c <--- subscribed
%% +/b/+
%% +/+/c
%% +/+/+ <--- start searching upward from here
%%
%% Let's name this search space "Space1".
%% If we brute-force it, the scope would be 8 (2^3).
%% Meaning this has O(2^N) complexity (N being the level of topics).
%%
%% This clearly isn't going to work.
%% Should we then try to enumerate all subscribers instead?
%% If there are also other subscriptions, e.g. "+/x/y" and "+/b/0"
%%
%% a/+/c <--- match of a/b/c
%% +/x/n
%% ...
%% +/x/2
%% +/x/1
%% +/b/c <--- match of a/b/c
%% +/b/1
%% +/b/0
%%
%% Let's name it "Space2".
%%
%% This has O(M * L) complexity (M being the total number of subscriptions,
%% and L being the number of topic levels).
%% This is usually a lot smaller than "Space1", but still not very effective
%% if the collection size is e.g. 1 million.
%%
%% To make it more effective, we'll need to combine the two algorithms:
%% Use the ordered subscription topics' prefixes as starting points to make
%% guesses about whether or not the next word can be a '+', and skip-over
%% to the next possible match.
%%
%% NOTE: A prerequisite of the ordered collection is, it should be able
%% to find the *immediate-next* topic/filter with a given prefix.
%%
%% In the above example, we start from "+/b/0". When comparing "+/b/0"
%% with "a/b/c", we know the matching prefix is "+/b", meaning we can
%% start guessing if the next word is '+' or 'c':
%%   * It can't be '+' because '+' < '0'
%%   * It might be 'c' because 'c' > '0'
%%
%% So, we try to jump to the next topic which has a prefix of "+/b/c"
%% (this effectively means skipping over "+/b/1").
%%
%% After "+/b/c" is found to be a matching filter, we move up:
%%   * The next possible match is "a/+/+" according to Space1
%%   * The next subscription is "+/x/1" according to Space2
%%
%% "a/+/+" is lexicographically greater than "+/x/+", so let's jump to
%% the immediate-next of 'a/+/+', which is "a/+/c", allowing us to skip
%% over all the ones starting with "+/x".
%%
%% If we take '#' into consideration, it's only one extra comparison to see
%% if a filter ends with '#'.
%%
%% In summary, the complexity of this algorithm is O(N * L)
%% N being the number of total matches, and L being the level of the topic.

-module(emqx_trie_search).

-export([make_key/2]).
-export([match/2, matches/3, get_id/1, get_topic/1]).
-export_type([key/1, word/0, nextf/0, opts/0]).

-define(END, '$end_of_table').

-type word() :: binary() | '+' | '#'.
-type base_key() :: {binary() | [word()], {}}.
-type key(ID) :: {binary() | [word()], {ID}}.
-type nextf() :: fun((key(_) | base_key()) -> ?END | key(_)).
-type opts() :: [unique | return_first].

%% Holds the constant values of each search.
-record(ctx, {
    %% A function which can quickly find the immediate-next record of the given prefix
    nextf :: nextf(),
    %% The initial words of a topic
    words0 :: [word()],
    %% Return as soon as there is one match found
    return_first :: boolean()
}).

%% @doc Make a search-key for the given topic.
-spec make_key(emqx_types:topic(), ID) -> key(ID).
make_key(Topic, ID) when is_binary(Topic) ->
    Words = words(Topic),
    case emqx_topic:wildcard(Words) of
        true ->
            %% it's a wildcard
            {Words, {ID}};
        false ->
            %% Not a wildcard. We do not split the topic
            %% because they can be found with direct lookups.
            %% it is also more compact in memory.
            {Topic, {ID}}
    end.

%% @doc Extract record ID from the match.
-spec get_id(key(ID)) -> ID.
get_id({_Filter, {ID}}) ->
    ID.

%% @doc Extract topic (or topic filter) from the match.
-spec get_topic(key(_ID)) -> emqx_types:topic().
get_topic({Filter, _ID}) when is_list(Filter) ->
    emqx_topic:join(Filter);
get_topic({Topic, _ID}) ->
    Topic.

%% Make the base-key which can be used to locate the desired search target.
base(Prefix) ->
    {Prefix, {}}.

%% Move the search target to the key next to the given Base.
move_up(#ctx{nextf = NextF}, Base) ->
    NextF(Base).

%% Add the given key to the accumulation.
add(#ctx{return_first = true}, _Acc, Key) ->
    throw({return_first, Key});
add(_C, Acc, Key) ->
    match_add(Key, Acc).

%% @doc Match given topic against the index and return the first match, or `false` if
%% no match is found.
-spec match(emqx_types:topic(), nextf()) -> false | key(_).
match(Topic, NextF) ->
    try search(Topic, NextF, [return_first]) of
        [] ->
            false
    catch
        throw:{return_first, Res} ->
            Res
    end.

%% @doc Match given topic against the index and return _all_ matches.
%% If `unique` option is given, return only unique matches by record ID.
-spec matches(emqx_types:topic(), nextf(), opts()) -> [key(_)].
matches(Topic, NextF, Opts) ->
    search(Topic, NextF, Opts).

%% @doc Entrypoint of the search for a given topic.
search(Topic, NextF, Opts) ->
    Words = words(Topic),
    Context = #ctx{
        nextf = NextF,
        words0 = Words,
        return_first = proplists:get_bool(return_first, Opts)
    },
    Acc0 =
        case proplists:get_bool(unique, Opts) of
            true ->
                #{};
            false ->
                []
        end,
    Base =
        case hd(Words) of
            <<$$, _/binary>> ->
                %% skip all filters starts with # or +
                base([hd(Words)]);
            _ ->
                base([])
        end,
    {MaybeEnd, Acc1} = search_new(Context, Base, Acc0),
    Acc = match_topics(Context, Topic, MaybeEnd, Acc1),
    case is_map(Acc) of
        true ->
            maps:values(Acc);
        false ->
            Acc
    end.

%% The recursive entrypoint of the trie-search algorithm.
%% Always start from the initial prefix and words.
search_new(#ctx{words0 = Words} = C, NewBase, Acc) ->
    case move_up(C, NewBase) of
        ?END ->
            {?END, Acc};
        {Filter, _} = T ->
            search_plus(C, Words, Filter, [], T, Acc)
    end.

%% Try to use '+' as the next word in the prefix.
search_plus(C, [W, X | Words], [W, X | Filter], RPrefix, T, Acc) ->
    %% Directly append the current word to the matching prefix (RPrefix).
    %% Micro optimization: try not to call the next clause because
    %% it is not a continuation.
    search_plus(C, [X | Words], [X | Filter], [W | RPrefix], T, Acc);
search_plus(C, [W | Words], ['+' | _] = Filter, RPrefix, T, Acc) ->
    case search_up(C, '+', Words, Filter, RPrefix, T, Acc) of
        {T, Acc1} ->
            search_up(C, W, Words, Filter, RPrefix, T, Acc1);
        TargetMoved ->
            TargetMoved
    end;
search_plus(C, [W | Words], Filter, RPrefix, T, Acc) ->
    %% not a plus
    search_up(C, W, Words, Filter, RPrefix, T, Acc).

%% Search to the bigger end of ordered collection of topics and topic-filters.
search_up(C, Word, Words, Filter, RPrefix, T, Acc) ->
    case compare(Word, Filter, Words) of
        {match, full} ->
            search_new(C, T, add(C, Acc, T));
        {match, prefix} ->
            search_new(C, T, Acc);
        lower ->
            {T, Acc};
        higher ->
            NewBase = base(lists:reverse([Word | RPrefix])),
            search_new(C, NewBase, Acc);
        shorter ->
            search_plus(C, Words, tl(Filter), [Word | RPrefix], T, Acc)
    end.

%% Compare prefix word then the next words in suffix against the search-target
%% topic or topic-filter.
compare(_, NotFilter, _) when is_binary(NotFilter) ->
    lower;
compare(H, [H | Filter], Words) ->
    compare(Filter, Words);
compare(_, ['#'], _Words) ->
    {match, full};
compare(H1, [H2 | _T2], _Words) when H1 < H2 ->
    lower;
compare(_H, [_ | _], _Words) ->
    higher.

%% Now compare the filter suffix and the topic suffix.
compare([], []) ->
    {match, full};
compare([], _Words) ->
    {match, prefix};
compare(['#'], _Words) ->
    {match, full};
compare([_ | _], []) ->
    lower;
compare([_ | _], _Words) ->
    %% cannot know if it's a match, lower, or higher,
    %% must search with a longer prefix.
    shorter.

match_add(K = {_Filter, ID}, Acc = #{}) ->
    % NOTE: ensuring uniqueness by record ID
    Acc#{ID => K};
match_add(K, Acc) ->
    [K | Acc].

-spec words(emqx_types:topic()) -> [word()].
words(Topic) when is_binary(Topic) ->
    % NOTE
    % This is almost identical to `emqx_topic:words/1`, but it doesn't convert empty
    % tokens to ''. This is needed to keep ordering of words consistent with what
    % `match_filter/3` expects.
    [word(W) || W <- emqx_topic:tokens(Topic)].

-spec word(binary()) -> word().
word(<<"+">>) -> '+';
word(<<"#">>) -> '#';
word(Bin) -> Bin.

%% match non-wildcard topics
match_topics(#ctx{nextf = NextF} = C, Topic, {Topic, _} = Key, Acc) ->
    %% found a topic match
    match_topics(C, Topic, NextF(Key), add(C, Acc, Key));
match_topics(#ctx{nextf = NextF} = C, Topic, {F, _}, Acc) when F < Topic ->
    %% the last key is a filter, try jump to the topic
    match_topics(C, Topic, NextF(base(Topic)), Acc);
match_topics(_C, _Topic, _Key, Acc) ->
    %% gone pass the topic
    Acc.
