%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_index).

-export([
    foreach_index_key/3,
    to_index_key/2,
    index_score/2,
    select_index/2,
    condition/1,
    condition/2,
    restore_topic/1
]).

-export_type([index/0]).

-type index() :: list(pos_integer()).

%% @doc Index key is a term that can be effectively searched in the index table.
-type index_key() :: {index(), {emqx_topic:words(), emqx_topic:words()}}.

-type match_pattern_part() :: term().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Given words of a concrete topic (`Tokens') and a list of `Indices',
%% constructs index keys for the topic and each of the indices.
%% `Fun' is called with each of these keys.
-spec foreach_index_key(fun((index_key()) -> any()), list(index()), emqx_topic:words()) -> ok.
foreach_index_key(_Fun, [], _Tokens) ->
    ok;
foreach_index_key(Fun, [Index | Indices], Tokens) ->
    Key = to_index_key(Index, Tokens),
    _ = Fun(Key),
    foreach_index_key(Fun, Indices, Tokens).

%% @doc Given a concrete topic and an index
%% returns the corresponding index key.
%%
%% In an index key words from indexed and unindexed positions are split.
%%
%% E.g given `[2, 3]' index and `[<<"a">>, <<"b">>, <<"c">>, <<"d">>]' topic,
%% returns `{[2, 3], {[<<"b">>, <<"c">>], [<<"a">>, <<"d">>]}}' term.
%%
%% @see foreach_index_key/3
-spec to_index_key(index(), emqx_topic:words()) -> index_key().
to_index_key(Index, Tokens) ->
    {Index, split_index_tokens(Index, Tokens, 1, [], [])}.

%% @doc Given an index and a wildcard topic
%% returns the length of the constant prefix of the
%% according index key.
%%
%% E.g. for `[2,3]' index and <code>['+', <<"b">>, '+', <<"d">>]</code> wildcard topic
%% the score is `1', because the according index key pattern is
%% <code>{[<<"b">>, '_'], ['_', <<"d">>]}</code>.
%%
%% @see foreach_index_key/3
%% @see to_index_key/2
-spec index_score(index(), emqx_topic:words()) -> non_neg_integer().
index_score(Index, Tokens) ->
    index_score(Index, Tokens, 1, 0).

%% @doc Given a list of indices and a wildcard topic
%% returns index with the best score.
%%
%% Returns `undefined' if there are no indices with score `> 0'.
%%
%% @see index_score/2
-spec select_index(emqx:words(), list(index())) -> index() | undefined.
select_index(Tokens, Indices) ->
    select_index(Tokens, Indices, 0, undefined).

%% @doc For an index and a wildcard topic
%% returns a matchspec pattern for the corresponding index key.
%%
%% E.g. for `[2, 3]' index and <code>['+', <<"b">>, '+', <<"d">>]</code> wildcard topic
%% returns <code>{[2, 3], {[<<"b">>, '_'], ['_', <<"d">>]}}</code> pattern.
-spec condition(index(), emqx_topic:words()) -> match_pattern_part().
condition(Index, Tokens) ->
    {Index, condition(Index, Tokens, 1, [], [])}.

%% @doc Returns a matchspec pattern for a wildcard topic.
%%
%% E.g. for <code>['+', <<"b">>, '+', <<"d">>, '#']</code> wildcard topic
%% returns <code>['_', <<"b">>, '_', <<"d">> | '_']</code> pattern.
-spec condition(emqx_topic:words()) -> match_pattern_part().
condition(Tokens) ->
    Tokens1 = [
        case W =:= '+' of
            true -> '_';
            _ -> W
        end
     || W <- Tokens
    ],
    case length(Tokens1) > 0 andalso lists:last(Tokens1) =:= '#' of
        false -> Tokens1;
        _ -> (Tokens1 -- ['#']) ++ '_'
    end.

%% @doc Restores concrete topic from its index key representation.
%%
%% E.g given `{[2, 3], {[<<"b">>, <<"c">>], [<<"a">>, <<"d">>]}}' index key
%% returns `[<<"a">>, <<"b">>, <<"c">>, <<"d">>]' topic.
-spec restore_topic(index_key()) -> emqx_topic:words().
restore_topic({Index, {IndexTokens, OtherTokens}}) ->
    restore_topic(Index, IndexTokens, OtherTokens, 1, []).

%%--------------------------------------------------------------------
%% Private
%%--------------------------------------------------------------------

split_index_tokens([NIndex | OtherIndex], [Token | Tokens], N, IndexTokens, OtherTokens) when
    NIndex == N
->
    split_index_tokens(OtherIndex, Tokens, N + 1, [Token | IndexTokens], OtherTokens);
split_index_tokens([_NIndex | _] = Index, [Token | Tokens], N, IndexTokens, OtherTokens) ->
    split_index_tokens(Index, Tokens, N + 1, IndexTokens, [Token | OtherTokens]);
split_index_tokens([], Tokens, _N, IndexTokens, OtherTokens) ->
    {lists:reverse(IndexTokens), lists:reverse(OtherTokens) ++ Tokens};
split_index_tokens(_Index, [], _N, IndexTokens, OtherTokens) ->
    {lists:reverse(IndexTokens), lists:reverse(OtherTokens)}.

index_score([N | _Index], [Ph | _Tokens], N, Score) when
    Ph =:= '+'; Ph =:= '#'
->
    Score;
index_score([N | Index], [_Word | Tokens], N, Score) ->
    index_score(Index, Tokens, N + 1, Score + 1);
index_score(Index, [_Word | Tokens], N, Score) ->
    index_score(Index, Tokens, N + 1, Score);
index_score([], _Tokens, _N, Score) ->
    Score;
index_score(_Index, [], _N, Score) ->
    Score.

select_index(_Tokens, [], _MaxScore, SelectedIndex) ->
    SelectedIndex;
select_index(Tokens, [Index | Indices], MaxScore, SelectedIndex) ->
    Score = index_score(Index, Tokens),
    case Score > MaxScore of
        true ->
            select_index(Tokens, Indices, Score, Index);
        false ->
            select_index(Tokens, Indices, MaxScore, SelectedIndex)
    end.

condition([_NIndex | _OtherIndex], ['#' | _OtherTokens], _N, IndexMatch, OtherMatch) ->
    {lists:reverse(IndexMatch) ++ '_', lists:reverse(OtherMatch) ++ '_'};
condition([], ['#' | _OtherTokens], _N, IndexMatch, OtherMatch) ->
    {lists:reverse(IndexMatch), lists:reverse(OtherMatch) ++ '_'};
condition([], Tokens, _N, IndexMatch, OtherMatch) ->
    {lists:reverse(IndexMatch), lists:reverse(OtherMatch) ++ condition(Tokens)};
condition([_NIndex | _OtherIndex], [], _N, IndexMatch, OtherMatch) ->
    {lists:reverse(IndexMatch) ++ '_', lists:reverse(OtherMatch)};
condition([NIndex | OtherIndex], ['+' | OtherTokens], N, IndexMatch, OtherMatch) when
    NIndex =:= N
->
    condition(OtherIndex, OtherTokens, N + 1, ['_' | IndexMatch], OtherMatch);
condition(Index, ['+' | OtherTokens], N, IndexMatch, OtherMatch) ->
    condition(Index, OtherTokens, N + 1, IndexMatch, ['_' | OtherMatch]);
condition([NIndex | OtherIndex], [Token | OtherTokens], N, IndexMatch, OtherMatch) when
    NIndex =:= N, is_binary(Token) orelse Token =:= ''
->
    condition(OtherIndex, OtherTokens, N + 1, [Token | IndexMatch], OtherMatch);
condition(Index, [Token | OtherTokens], N, IndexMatch, OtherMatch) when
    is_binary(Token) orelse Token =:= ''
->
    condition(Index, OtherTokens, N + 1, IndexMatch, [Token | OtherMatch]).

restore_topic(_Index, [], OtherTokens, _N, Tokens) ->
    lists:reverse(Tokens) ++ OtherTokens;
restore_topic([NIndex | OtherIndex], [IndexToken | OtherIndexTokens], OtherTokens, N, Tokens) when
    NIndex =:= N
->
    restore_topic(OtherIndex, OtherIndexTokens, OtherTokens, N + 1, [IndexToken | Tokens]);
restore_topic(OtherIndex, IndexTokens, [Token | OtherTokens], N, Tokens) ->
    restore_topic(OtherIndex, IndexTokens, OtherTokens, N + 1, [Token | Tokens]).
