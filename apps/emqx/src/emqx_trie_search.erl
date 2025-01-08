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

-export([make_key/2, make_pat/2, filter/1]).
-export([match/2, matches/3, get_id/1, get_topic/1, matches_filter/3]).
-export_type([key/1, word/0, words/0, nextf/0, opts/0]).

-define(END, '$end_of_table').

-type word() :: binary() | '+' | '#'.
-type words() :: [word()].
-type base_key() :: {binary() | [word()], {}}.
-type key(ID) :: {binary() | [word()], {ID}}.
-type nextf() :: fun((key(_) | base_key()) -> ?END | key(_)).
-type opts() :: [unique | return_first].

%% @doc Make a search-key for the given topic.
-spec make_key(emqx_types:topic() | words(), ID) -> key(ID).
make_key(Topic, ID) when is_binary(Topic) ->
    case filter(Topic) of
        Words when is_list(Words) ->
            %% it's a wildcard
            {Words, {ID}};
        false ->
            %% Not a wildcard. We do not split the topic
            %% because they can be found with direct lookups.
            %% it is also more compact in memory.
            {Topic, {ID}}
    end;
make_key(Words, ID) when is_list(Words) ->
    {Words, {ID}}.

-spec make_pat(emqx_types:topic() | words() | '_', _ID | '_') -> _Pat.
make_pat(Pattern = '_', ID) ->
    {Pattern, {ID}};
make_pat(Topic, ID) ->
    make_key(Topic, ID).

%% @doc Parse a topic filter into a list of words. Returns `false` if it's not a filter.
-spec filter(emqx_types:topic()) -> words() | false.
filter(Topic) ->
    Words = filter_words(Topic),
    emqx_topic:wildcard(Words) andalso Words.

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

-compile({inline, [base/1, move_up/2, match_add/2, compare/3]}).

%% Make the base-key which can be used to locate the desired search target.
base(Prefix) ->
    {Prefix, {}}.

base_init([W = <<"$", _/bytes>> | _]) ->
    base([W]);
base_init(_) ->
    base([]).

%% Move the search target to the key next to the given Base.
move_up(NextF, Base) ->
    NextF(Base).

%% @doc Match given topic against the index and return the first match, or `false` if
%% no match is found.
-spec match(emqx_types:topic(), nextf()) -> false | key(_).
match(Topic, NextF) ->
    try search(Topic, NextF, [return_first]) of
        _ -> false
    catch
        throw:{first, Res} ->
            Res
    end.

%% @doc Match given topic against the index and return _all_ matches.
%% If `unique` option is given, return only unique matches by record ID.
-spec matches(emqx_types:topic() | [word()], nextf(), opts()) -> [key(_)].
matches(Topic, NextF, Opts) ->
    search(Topic, NextF, Opts).

%% @doc Match given topic filter against the index and return _all_ matches.
-spec matches_filter(emqx_types:topic() | [word()], nextf(), opts()) -> [key(_)].
matches_filter(TopicFilter, NextF, Opts) ->
    search(TopicFilter, NextF, [topic_filter | Opts]).

%% @doc Entrypoint of the search for a given topic.
search(Topic, NextF, Opts) ->
    %% A private opt
    IsFilter = proplists:get_bool(topic_filter, Opts),
    Words =
        case IsFilter of
            true -> filter_words(Topic);
            false -> topic_words(Topic)
        end,
    Base = base_init(Words),
    ORetFirst = proplists:get_bool(return_first, Opts),
    OUnique = proplists:get_bool(unique, Opts),
    Acc0 =
        case ORetFirst of
            true ->
                first;
            false when OUnique ->
                #{};
            false ->
                []
        end,
    Matches =
        case search_new(Words, Base, NextF, Acc0) of
            {Cursor, Acc} when not IsFilter ->
                match_topics(Topic, Cursor, NextF, Acc);
            {_Cursor, Acc} ->
                Acc;
            Acc ->
                Acc
        end,
    case is_map(Matches) of
        true ->
            maps:values(Matches);
        false ->
            Matches
    end.

%% The recursive entrypoint of the trie-search algorithm.
%% Always start from the initial prefix and words.
search_new(Words0, NewBase, NextF, Acc) ->
    case move_up(NextF, NewBase) of
        ?END ->
            Acc;
        Cursor ->
            search_up(Words0, Cursor, NextF, Acc)
    end.

%% Search to the bigger end of ordered collection of topics and topic-filters.
search_up(Words, {Filter, _} = Cursor, NextF, Acc) ->
    case compare(Filter, Words, 0) of
        match_full ->
            search_new(Words, Cursor, NextF, match_add(Cursor, Acc));
        match_prefix ->
            search_new(Words, Cursor, NextF, Acc);
        lower ->
            {Cursor, Acc};
        {Pos, SeekWord} ->
            % NOTE
            % This is a seek instruction. It means we need to take `Pos` words
            % from the current topic filter and attach `SeekWord` to the end of it.
            NewBase = base(seek(Pos, SeekWord, Filter)),
            search_new(Words, NewBase, NextF, Acc)
    end.

seek(_Pos = 0, SeekWord, _FilterTail) ->
    [SeekWord];
seek(Pos, SeekWord, [FilterWord | Rest]) ->
    [FilterWord | seek(Pos - 1, SeekWord, Rest)].

compare(NotFilter, _, _) when is_binary(NotFilter) ->
    lower;
compare([], [], _) ->
    % NOTE
    %  Topic: a/b/c/d
    % Filter: a/+/+/d
    % We matched the topic to a topic filter exactly (possibly with pluses).
    % We include it in the result set, and now need to try next entry in the table.
    % Closest possible next entries that we must not miss:
    % * a/+/+/d (same topic but a different ID)
    % * a/+/+/d/# (also a match)
    match_full;
compare([], _Words, _) ->
    % NOTE
    %  Topic: a/b/c/d
    % Filter: a/+/c
    % We found out that a topic filter is a prefix of the topic (possibly with pluses).
    % We discard it, and now need to try next entry in the table.
    % Closest possible next entries that we must not miss:
    % * a/+/c/# (which is a match)
    % * a/+/c/+ (also a match)
    match_prefix;
compare(['#'], _Words, _) ->
    % NOTE
    %  Topic: a/b/c/d
    % Filter: a/+/+/d/# or just a/#
    % We matched the topic to a topic filter with wildcard (possibly with pluses).
    % We include it in the result set, and now need to try next entry in the table.
    % Closest possible next entries that we must not miss:
    % * a/+/+/d/# (same topic but a different ID)
    match_full;
%% Filter search %%
compare(_Filter, ['#'], _) ->
    match_full;
compare([_ | TF], ['+' | TW], Pos) ->
    case compare(TF, TW, Pos + 1) of
        lower ->
            lower;
        Other ->
            Other
    end;
%% Filter search end %%
compare(['+' | TF], [HW | TW], Pos) ->
    case compare(TF, TW, Pos + 1) of
        lower ->
            % NOTE
            %  Topic: a/b/c/d
            % Filter: a/+/+/e/1 or a/b/+/d/1
            % The topic is lower than a topic filter. But we're at the `+` position,
            % so we emit a backtrack point to seek to:
            % Seek: {2, c}
            % We skip over part of search space, and seek to the next possible match:
            % Next: a/+/c
            {Pos, HW};
        Other ->
            % NOTE
            % It's either already a backtrack point, emitted from the last `+`
            % position or just a seek / match. In both cases we just pass it
            % through.
            Other
    end;
compare([HW | TF], [HW | TW], Pos) ->
    % NOTE
    % Skip over the same word in both topic and filter, keeping the last backtrack point.
    compare(TF, TW, Pos + 1);
compare([HF | _], [HW | _], _) when HF > HW ->
    % NOTE
    %  Topic: a/b/c/d
    % Filter: a/b/c/e/1 or a/b/+/e
    % The topic is lower than a topic filter. In the first case there's nowhere to
    % backtrack to, we're out of the search space. In the second case there's a `+`
    % on 3rd level, we'll seek up from there.
    lower;
compare([_ | _], [], _) ->
    % NOTE
    %  Topic: a/b/c/d
    % Filter: a/b/c/d/1 or a/+/c/d/1
    % The topic is lower than a topic filter (since it's shorter). In the first case
    % there's nowhere to backtrack to, we're out of the search space. In the second case
    % there's a `+` on 2nd level, we'll seek up from there.
    lower;
compare([_ | _], [HW | _], Pos) ->
    % NOTE
    %  Topic: a/b/c/d
    % Filter: a/+/+/0/1/2
    % Topic is higher than the filter, we need to skip over to the next possible filter.
    % Seek: {3, d}
    % Next: a/+/+/d
    {Pos, HW}.

match_add(K = {_Filter, ID}, Acc = #{}) ->
    % NOTE: ensuring uniqueness by record ID
    Acc#{ID => K};
match_add(K, Acc) when is_list(Acc) ->
    [K | Acc];
match_add(K, first) ->
    throw({first, K}).

-spec filter_words(emqx_types:topic() | [word()]) -> [word()].
filter_words(Words) when is_list(Words) ->
    Words;
filter_words(Topic) when is_binary(Topic) ->
    % NOTE
    % This is almost identical to `emqx_topic:words/1`, but it doesn't convert empty
    % tokens to ''. This is needed to keep ordering of words consistent with what
    % `match_filter/3` expects.
    [word(W, filter) || W <- emqx_topic:tokens(Topic)].

-spec topic_words(emqx_types:topic()) -> [binary()].
topic_words(Words) when is_list(Words) ->
    Words;
topic_words(Topic) when is_binary(Topic) ->
    [word(W, topic) || W <- emqx_topic:tokens(Topic)].

word(<<"+">>, topic) -> error(badarg);
word(<<"#">>, topic) -> error(badarg);
word(<<"+">>, filter) -> '+';
word(<<"#">>, filter) -> '#';
word(Bin, _) -> Bin.

%% match non-wildcard topics
match_topics(Topic, {Topic, _} = Key, NextF, Acc) ->
    %% found a topic match
    match_topics(Topic, NextF(Key), NextF, match_add(Key, Acc));
match_topics(Topic, {F, _}, NextF, Acc) when F < Topic ->
    %% the last key is a filter, try jump to the topic
    match_topics(Topic, NextF(base(Topic)), NextF, Acc);
match_topics(_Topic, _Key, _NextF, Acc) ->
    %% gone pass the topic
    Acc.
