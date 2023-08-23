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
%% Works on top of ETS ordered_set table. Keys are tuples constructed from
%% parsed topic filters and record IDs, wrapped in a tuple to order them
%% strictly greater than unit tuple (`{}`). Existing table may be used if
%% existing keys will not collide with index keys.
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
%% test
-export([words/1, join/1, unescape/1]).

-export_type([key/1, word/0, nextf/0, opts/0]).

-type topic() :: emqx_types:topic().
-type word() :: binary().
-type base_key() :: {topic(), {}}.
-type key(ID) :: {topic(), {ID}}.
-type nextf() :: fun((key(_) | base_key()) -> '$end_of_table' | key(_)).
-type opts() :: [unique | return_first].

%% Holds the constant values of each search.
-record(ctx, {
    %% A function which can quickly find the immediate-next record of the given prefix
    nextf :: nextf(),
    %% The initial prefix to start searching from
    %% if the input topic starts with a dollar-word, it's the first word like <<"$SYS">>
    %% otherwise it's a <<"">>
    prefix0 :: word(),
    %% The initial words of a topic
    words0 :: [word()],
    %% Return as soon as there is one match found
    return_first :: boolean()
}).

%% Holds the variable parts of each search.
-record(acc, {
    %% The current searching target topic/filter
    target,
    cont = [],
    %% Search result accumulation
    matches = []
}).

%% Escapes UTF-8 encoded topic string to ensure the same lexicographic order
%% as split words.
%%
%% <<0>> for <<"/">>
%% <<1>> for <<"#">>
%% <<2>> for <<"+">>
%% <<3>> for <<"">>
%% <<9, N>> for <<N>> when N < 10
%%
%% NOTE: Although 0~9 are very much unlikely to be used in topics,
%% but they are valid UTF-8 characters so we have to escape them.
-define(SLASH, 0).
-define(HASH, 1).
-define(PLUS, 2).
-define(EMPTY, 3).
-define(ESC, 9).

%% @doc Make a search-key for the given topic.
-spec make_key(topic(), ID) -> key(ID).
make_key(Topic, ID) when is_binary(Topic) ->
    Words = words(Topic),
    {join(Words), {ID}}.

%% @doc Extract record ID from the match.
-spec get_id(key(ID)) -> ID.
get_id({_Filter, {ID}}) ->
    ID.

%% @doc Extract topic (or topic filter) from the match.
-spec get_topic(key(_ID)) -> topic().
get_topic({Filter, _ID}) ->
    unescape(Filter).

%% Make the base-key which can be used to locate the desired search target.
base(Prefix) ->
    {Prefix, {}}.

%% Move the search target to the key next to the given Base.
move_up(#ctx{nextf = NextF}, #acc{} = Acc, Base) ->
    Acc#acc{target = NextF(Base), cont = []}.

%% The current target key is a match, add it to the accumulation.
add(C, #acc{target = Key} = Acc) ->
    add(C, Acc, Key).

%% Add the given key to the accumulation.
add(#ctx{return_first = true}, _Acc, Key) ->
    throw({return_first, Key});
add(_C, #acc{matches = Matches} = Acc, Key) ->
    Acc#acc{matches = match_add(Key, Matches)}.

%% @doc Match given topic against the index and return the first match, or `false` if
%% no match is found.
-spec match(topic(), nextf()) -> false | key(_).
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
-spec matches(topic(), nextf(), opts()) -> [key(_)].
matches(Topic, NextF, Opts) ->
    search(Topic, NextF, Opts).

%% @doc Entrypoint of the search for a given topic.
search(Topic, NextF, Opts) ->
    {Words, Prefix} = match_init(Topic),
    Context = #ctx{
        nextf = NextF,
        prefix0 = Prefix,
        words0 = Words,
        return_first = proplists:get_bool(return_first, Opts)
    },
    Matches0 =
        case proplists:get_bool(unique, Opts) of
            true ->
                #{};
            false ->
                []
        end,
    Acc = search_new(Context, base(Prefix), #acc{matches = Matches0}),
    #acc{matches = Matches} = Acc,
    case is_map(Matches) of
        true ->
            maps:values(Matches);
        false ->
            Matches
    end.

%% The recursive entrypoint of the trie-search algorithm.
%% Always start from the initial prefix and words.
search_new(#ctx{prefix0 = Prefix, words0 = Words0} = C, NewBase, Acc0) ->
    case move_up(C, Acc0, NewBase) of
        #acc{target = '$end_of_table'} = Acc ->
            Acc;
        #acc{target = {Filter, _}} = Acc when Prefix =:= <<>> ->
            %% This is not a '$' topic, start from '+'
            search_plus(C, Words0, Filter, [], Acc);
        #acc{target = {Filter, _}} = Acc ->
            %% Start from the '$' word
            search_up(C, Prefix, Words0, Filter, [], Acc)
    end.

%% Search to the bigger end of ordered collection of topics and topic-filters.
search_up(C, Word, Words, Filter, RPrefix, #acc{target = Base, cont = Cont} = Acc) ->
    case compare(Word, Filter, Words) of
        {match, full} ->
            search_new(C, Base, add(C, Acc));
        {match, prefix} ->
            search_new(C, Base, Acc);
        lower ->
            case Cont of
                [] ->
                    Acc;
                [F | More] ->
                    F(Acc#acc{cont = More})
            end;
        higher ->
            BaseFilter = join(lists:reverse([Word | RPrefix])),
            NewBase = base(BaseFilter),
            search_new(C, NewBase, Acc);
        shorter ->
            <<Word:(size(Word))/binary, ?SLASH, Filter1/binary>> = Filter,
            search_plus(C, Words, Filter1, [Word | RPrefix], Acc)
    end.

%% Try to use '+' as the next word in the prefix.
search_plus(C, [W | Words], <<?PLUS, _/binary>> = Filter, RPrefix,
            #acc{cont = Cont} = Acc) ->
    F = fun(AccIn) -> search_up(C, W, Words, Filter, RPrefix, AccIn) end,
    search_up(C, <<?PLUS>>, Words, Filter, RPrefix, Acc#acc{cont = [F | Cont]});
search_plus(C, [W | Words], Filter, RPrefix, Acc) ->
    search_up(C, W, Words, Filter, RPrefix, Acc).

%% Compare prefix word then the next words in suffix against the search-target
%% topic or topic-filter.
compare(_Word, <<?HASH>>, _Words) ->
    {match, full};
compare(Word, Word, []) ->
    {match, full};
compare(Word, Word, _) ->
    {match, prefix};
compare(Word, Filter, Words) ->
    case Filter of
        <<Word:(size(Word))/binary, ?SLASH, Filter1/binary>> ->
            compare(Filter1, Words);
        _ when Word < Filter ->
            lower;
        _ ->
            higher
    end.

%% Now compare the filter suffix and the topic suffix.
compare(<<?HASH>>, _Words) ->
    {match, full};
compare(<<_, _/binary>>, []) ->
    lower;
compare(<<_, _/binary>>, _Words) ->
    %% cannot know if it's a match, lower, or higher,
    %% must search with a longer prefix.
    shorter.

match_add(K = {_Filter, ID}, Acc = #{}) ->
    % NOTE: ensuring uniqueness by record ID
    Acc#{ID => K};
match_add(K, Acc) ->
    [K | Acc].

match_init(Topic) ->
    case words(Topic) of
        [W = <<"$", _/bytes>> | Rest] ->
            % NOTE
            % This will effectively skip attempts to match special topics to `#` or `+/...`.
            {Rest, W};
        Words ->
            {Words, <<>>}
    end.

%% Split the given topic or topic-filter and return the escaped words list.
-spec words(topic()) -> [word()].
words(Topic) when is_binary(Topic) ->
    lists:map(fun esc/1, emqx_topic:tokens(Topic)).

%% Use \0 to join the escaped-words.
join([H | EscapedWords]) ->
    join_loop(EscapedWords, H).

join_loop([], Filter) ->
    Filter;
join_loop([NextWord | Words], Filter) ->
    join_loop(Words, <<Filter/binary, ?SLASH, NextWord/binary>>).

esc(<<"#">>) ->
    <<?HASH>>;
esc(<<"+">>) ->
    <<?PLUS>>;
esc(<<>>) ->
    <<?EMPTY>>;
esc(Word) ->
    %% check before calling esc_w because 99.999% of the time
    %% it makes a copy of Word.
    case (<<<<C>> || <<C>> <= Word, C < 10>>) of
        <<>> ->
            Word;
        _ ->
            esc_w(Word, <<>>)
    end.

esc_w(<<C, Rest/binary>>, Acc) when C < 10 ->
    esc_w(Rest, <<Acc/binary, 9, C>>);
esc_w(<<C, Rest/binary>>, Acc) ->
    esc_w(Rest, <<Acc/binary, C>>);
esc_w(<<>>, Result) ->
    Result.

%% Convert escaped topic or topic-filter back to the original format.
unescape(Filter) ->
    unesc(Filter, <<>>).

unesc(<<>>, Filter) ->
    Filter;
unesc(<<?SLASH, Rest/binary>>, Filter) ->
    unesc(Rest, <<Filter/binary, $/>>);
unesc(<<?HASH, Rest/binary>>, Filter) ->
    unesc(Rest, <<Filter/binary, $#>>);
unesc(<<?PLUS, Rest/binary>>, Filter) ->
    unesc(Rest, <<Filter/binary, $+>>);
unesc(<<?EMPTY, Rest/binary>>, Filter) ->
    unesc(Rest, Filter);
unesc(<<?ESC, C, Rest/binary>>, Filter) ->
    true = (C < 10),
    unesc(Rest, <<Filter/binary, C>>);
unesc(<<C, Rest/binary>>, Filter) ->
    unesc(Rest, <<Filter/binary, C>>).
