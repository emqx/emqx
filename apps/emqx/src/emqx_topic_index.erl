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

-module(emqx_topic_index).

-export([new/0]).
-export([insert/4]).
-export([delete/3]).
-export([match/2]).
-export([matches/3]).

-export([get_id/1]).
-export([get_topic/1]).
-export([get_record/2]).

-type word() :: binary() | '+' | '#'.
-type key(ID) :: {[word()], {ID}}.
-type match(ID) :: key(ID).

%% @doc Create a new ETS table suitable for topic index.
%% Usable mostly for testing purposes.
-spec new() -> ets:table().
new() ->
    ets:new(?MODULE, [public, ordered_set, {read_concurrency, true}]).

%% @doc Insert a new entry into the index that associates given topic filter to given
%% record ID, and attaches arbitrary record to the entry. This allows users to choose
%% between regular and "materialized" indexes, for example.
-spec insert(emqx_types:topic(), _ID, _Record, ets:table()) -> true.
insert(Filter, ID, Record, Tab) ->
    ets:insert(Tab, {{words(Filter), {ID}}, Record}).

%% @doc Delete an entry from the index that associates given topic filter to given
%% record ID. Deleting non-existing entry is not an error.
-spec delete(emqx_types:topic(), _ID, ets:table()) -> true.
delete(Filter, ID, Tab) ->
    ets:delete(Tab, {words(Filter), {ID}}).

%% @doc Match given topic against the index and return the first match, or `false` if
%% no match is found.
-spec match(emqx_types:topic(), ets:table()) -> match(_ID) | false.
match(Topic, Tab) ->
    {Words, RPrefix} = match_init(Topic),
    match(Words, RPrefix, Tab).

match(Words, RPrefix, Tab) ->
    Prefix = lists:reverse(RPrefix),
    match(ets:next(Tab, {Prefix, {}}), Prefix, Words, RPrefix, Tab).

match(K, Prefix, Words, RPrefix, Tab) ->
    case match_next(Prefix, K, Words) of
        true ->
            K;
        skip ->
            match(ets:next(Tab, K), Prefix, Words, RPrefix, Tab);
        stop ->
            false;
        Matched ->
            match_rest(Matched, Words, RPrefix, Tab)
    end.

match_rest([W1 | [W2 | _] = SLast], [W1 | [W2 | _] = Rest], RPrefix, Tab) ->
    % NOTE
    % Fast-forward through identical words in the topic and the last key suffixes.
    % This should save us a few redundant `ets:next` calls at the cost of slightly
    % more complex match patterns.
    match_rest(SLast, Rest, [W1 | RPrefix], Tab);
match_rest(SLast, [W | Rest], RPrefix, Tab) when is_list(SLast) ->
    match(Rest, [W | RPrefix], Tab);
match_rest(plus, [W | Rest], RPrefix, Tab) ->
    % NOTE
    % There's '+' in the key suffix, meaning we should consider 2 alternatives:
    % 1. Match the rest of the topic as if there was '+' in the current position.
    % 2. Skip this key and try to match the topic as it is.
    case match(Rest, ['+' | RPrefix], Tab) of
        Match = {_, _} ->
            Match;
        false ->
            match(Rest, [W | RPrefix], Tab)
    end;
match_rest(_, [], _RPrefix, _Tab) ->
    false.

%% @doc Match given topic against the index and return _all_ matches.
%% If `unique` option is given, return only unique matches by record ID.
-spec matches(emqx_types:topic(), ets:table(), _Opts :: [unique]) -> [match(_ID)].
matches(Topic, Tab, Opts) ->
    {Words, RPrefix} = match_init(Topic),
    AccIn =
        case Opts of
            [unique | _] -> #{};
            [] -> []
        end,
    Matches = matches(Words, RPrefix, AccIn, Tab),
    case Matches of
        #{} -> maps:values(Matches);
        _ -> Matches
    end.

matches(Words, RPrefix, Acc, Tab) ->
    Prefix = lists:reverse(RPrefix),
    matches(ets:next(Tab, {Prefix, {}}), Prefix, Words, RPrefix, Acc, Tab).

matches(Words, RPrefix, K = {Filter, _}, Acc, Tab) ->
    Prefix = lists:reverse(RPrefix),
    case Prefix > Filter of
        true ->
            % NOTE: Prefix already greater than the last key seen, need to `ets:next/2`.
            matches(ets:next(Tab, {Prefix, {}}), Prefix, Words, RPrefix, Acc, Tab);
        false ->
            % NOTE: Prefix is still less than or equal to the last key seen, reuse it.
            matches(K, Prefix, Words, RPrefix, Acc, Tab)
    end.

matches(K, Prefix, Words, RPrefix, Acc, Tab) ->
    case match_next(Prefix, K, Words) of
        true ->
            matches(ets:next(Tab, K), Prefix, Words, RPrefix, match_add(K, Acc), Tab);
        skip ->
            matches(ets:next(Tab, K), Prefix, Words, RPrefix, Acc, Tab);
        stop ->
            Acc;
        Matched ->
            % NOTE: Prserve next key on the stack to save on `ets:next/2` calls.
            matches_rest(Matched, Words, RPrefix, K, Acc, Tab)
    end.

matches_rest([W1 | [W2 | _] = SLast], [W1 | [W2 | _] = Rest], RPrefix, K, Acc, Tab) ->
    % NOTE
    % Fast-forward through identical words in the topic and the last key suffixes.
    % This should save us a few redundant `ets:next` calls at the cost of slightly
    % more complex match patterns.
    matches_rest(SLast, Rest, [W1 | RPrefix], K, Acc, Tab);
matches_rest(SLast, [W | Rest], RPrefix, K, Acc, Tab) when is_list(SLast) ->
    matches(Rest, [W | RPrefix], K, Acc, Tab);
matches_rest(plus, [W | Rest], RPrefix, K, Acc, Tab) ->
    % NOTE
    % There's '+' in the key suffix, meaning we should accumulate all matches from
    % each of 2 branches:
    % 1. Match the rest of the topic as if there was '+' in the current position.
    % 2. Skip this key and try to match the topic as it is.
    NAcc = matches(Rest, ['+' | RPrefix], K, Acc, Tab),
    matches(Rest, [W | RPrefix], K, NAcc, Tab);
matches_rest(_, [], _RPrefix, _K, Acc, _Tab) ->
    Acc.

match_add(K = {_Filter, ID}, Acc = #{}) ->
    % NOTE: ensuring uniqueness by record ID
    Acc#{ID => K};
match_add(K, Acc) ->
    [K | Acc].

match_next(Prefix, {Filter, _ID}, Suffix) ->
    match_filter(Prefix, Filter, Suffix);
match_next(_, '$end_of_table', _) ->
    stop.

match_filter([], [], []) ->
    % NOTE: we matched the topic exactly
    true;
match_filter([], [], _Suffix) ->
    % NOTE: we matched the prefix, but there may be more matches next
    skip;
match_filter([], ['#'], _Suffix) ->
    % NOTE: naturally, '#' < '+', so this is already optimal for `match/2`
    true;
match_filter([], ['+' | _], _Suffix) ->
    plus;
match_filter([], [_H | _] = Rest, _Suffix) ->
    Rest;
match_filter([H | T1], [H | T2], Suffix) ->
    match_filter(T1, T2, Suffix);
match_filter([H1 | _], [H2 | _], _Suffix) when H2 > H1 ->
    % NOTE: we're strictly past the prefix, no need to continue
    stop.

match_init(Topic) ->
    case words(Topic) of
        [W = <<"$", _/bytes>> | Rest] ->
            % NOTE
            % This will effectively skip attempts to match special topics to `#` or `+/...`.
            {Rest, [W]};
        Words ->
            {Words, []}
    end.

%% @doc Extract record ID from the match.
-spec get_id(match(ID)) -> ID.
get_id({_Filter, {ID}}) ->
    ID.

%% @doc Extract topic (or topic filter) from the match.
-spec get_topic(match(_ID)) -> emqx_types:topic().
get_topic({Filter, _ID}) ->
    emqx_topic:join(Filter).

%% @doc Fetch the record associated with the match.
%% NOTE: Only really useful for ETS tables where the record ID is the first element.
-spec get_record(match(_ID), ets:table()) -> _Record.
get_record(K, Tab) ->
    ets:lookup_element(Tab, K, 2).

%%

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
