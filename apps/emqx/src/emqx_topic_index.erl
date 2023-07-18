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
%% Works on top of ETS ordered_set table. Keys are parsed topic filters
%% with record ID appended to the end, wrapped in a tuple to disambiguate from
%% topic filter words. Existing table may be used if existing keys will not
%% collide with index keys.
%%
%% Designed to effectively answer questions like:
%% 1. Does any topic filter match given topic?
%% 2. Which records are associated with topic filters matching given topic?
%%
%% Questions like these are _only slightly_ less effective:
%% 1. Which topic filters match given topic?
%% 2. Which record IDs are associated with topic filters matching given topic?

-module(emqx_topic_index).

-export([new/0]).
-export([insert/4]).
-export([delete/3]).
-export([match/2]).
-export([matches/2]).

-export([get_id/1]).
-export([get_topic/1]).
-export([get_record/2]).

-type key(ID) :: [binary() | '+' | '#' | {ID}].
-type match(ID) :: key(ID).

new() ->
    ets:new(?MODULE, [public, ordered_set, {write_concurrency, true}]).

insert(Filter, ID, Record, Tab) ->
    ets:insert(Tab, {emqx_topic:words(Filter) ++ [{ID}], Record}).

delete(Filter, ID, Tab) ->
    ets:delete(Tab, emqx_topic:words(Filter) ++ [{ID}]).

-spec match(emqx_types:topic(), ets:table()) -> match(_ID) | false.
match(Topic, Tab) ->
    {Words, RPrefix} = match_init(Topic),
    match(Words, RPrefix, Tab).

match(Words, RPrefix, Tab) ->
    Prefix = lists:reverse(RPrefix),
    K = ets:next(Tab, Prefix),
    case match_filter(Prefix, K, Words =/= []) of
        true ->
            K;
        stop ->
            false;
        Matched ->
            match_rest(Matched, Words, RPrefix, Tab)
    end.

match_rest(false, [W | Rest], RPrefix, Tab) ->
    match(Rest, [W | RPrefix], Tab);
match_rest(plus, [W | Rest], RPrefix, Tab) ->
    case match(Rest, ['+' | RPrefix], Tab) of
        Match when is_list(Match) ->
            Match;
        false ->
            match(Rest, [W | RPrefix], Tab)
    end;
match_rest(_, [], _RPrefix, _Tab) ->
    false.

-spec matches(emqx_types:topic(), ets:table()) -> [match(_ID)].
matches(Topic, Tab) ->
    {Words, RPrefix} = match_init(Topic),
    matches(Words, RPrefix, Tab).

matches(Words, RPrefix, Tab) ->
    Prefix = lists:reverse(RPrefix),
    matches(ets:next(Tab, Prefix), Prefix, Words, RPrefix, Tab).

matches(K, Prefix, Words, RPrefix, Tab) ->
    case match_filter(Prefix, K, Words =/= []) of
        true ->
            [K | matches(ets:next(Tab, K), Prefix, Words, RPrefix, Tab)];
        stop ->
            [];
        Matched ->
            matches_rest(Matched, Words, RPrefix, Tab)
    end.

matches_rest(false, [W | Rest], RPrefix, Tab) ->
    matches(Rest, [W | RPrefix], Tab);
matches_rest(plus, [W | Rest], RPrefix, Tab) ->
    matches(Rest, ['+' | RPrefix], Tab) ++ matches(Rest, [W | RPrefix], Tab);
matches_rest(_, [], _RPrefix, _Tab) ->
    [].

match_filter([], [{_ID}], _IsPrefix = false) ->
    % NOTE: exact match is `true` only if we match whole topic, not prefix
    true;
match_filter([], ['#', {_ID}], _IsPrefix) ->
    % NOTE: naturally, '#' < '+', so this is already optimal for `match/2`
    true;
match_filter([], ['+' | _], _) ->
    plus;
match_filter([], [_H | _], _) ->
    false;
match_filter([H | T1], [H | T2], IsPrefix) ->
    match_filter(T1, T2, IsPrefix);
match_filter([H1 | _], [H2 | _], _) when H2 > H1 ->
    % NOTE: we're strictly past the prefix, no need to continue
    stop;
match_filter(_, '$end_of_table', _) ->
    stop.

match_init(Topic) ->
    case emqx_topic:words(Topic) of
        [W = <<"$", _/bytes>> | Rest] ->
            % NOTE
            % This will effectively skip attempts to match special topics to `#` or `+/...`.
            {Rest, [W]};
        Words ->
            {Words, []}
    end.

-spec get_id(match(ID)) -> ID.
get_id([{ID}]) ->
    ID;
get_id([_ | Rest]) ->
    get_id(Rest).

-spec get_topic(match(_ID)) -> emqx_types:topic().
get_topic(K) ->
    emqx_topic:join(cut_topic(K)).

cut_topic([{_ID}]) ->
    [];
cut_topic([W | Rest]) ->
    [W | cut_topic(Rest)].

-spec get_record(match(_ID), ets:table()) -> _Record.
get_record(K, Tab) ->
    ets:lookup_element(Tab, K, 2).
