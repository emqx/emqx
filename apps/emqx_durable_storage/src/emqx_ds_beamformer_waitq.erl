%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This helper module matches stream events to waiting poll
%% requests in the beamformer.
-module(emqx_ds_beamformer_waitq).

%% API:
-export([new/0, insert/5, delete/4, matches/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% API functions
%%================================================================================

new() ->
    ets:new(?MODULE, [ordered_set, private]).

insert(Stream, Filter, ID, Record, Tab) ->
    Key = make_key(Stream, Filter, ID),
    true = ets:insert(Tab, {Key, Record}).

delete(Stream, Filter, ID, Tab) ->
    ets:delete(Tab, make_key(Stream, Filter, ID)).

matches(Stream, Topic, Tab) ->
    Ids = emqx_trie_search:matches(Topic, make_nextf(Stream, Tab), []),
    [Val || Id <- Ids, {_, Val} <- ets:lookup(Tab, {Stream, Id})].

%%================================================================================
%% Internal functions
%%================================================================================

make_key(Stream, TopicFilter, ID) ->
    {Stream, emqx_trie_search:make_key(TopicFilter, ID)}.

make_nextf(Stream, Tab) ->
    fun(Key0) ->
        case ets:next(Tab, {Stream, Key0}) of
            '$end_of_table' -> '$end_of_table';
            {Stream, Key} -> Key;
            {_OtherStream, _Key} -> '$end_of_table'
        end
    end.

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

topic_match_test() ->
    Tab = new(),
    insert(s1, [<<"foo">>, '+'], 1, {val, 1}, Tab),
    insert(s1, [<<"foo">>, <<"bar">>], 2, {val, 2}, Tab),
    insert(s1, [<<"1">>, <<"2">>], 3, {val, 3}, Tab),

    insert(s2, [<<"foo">>, '+'], 4, {val, 4}, Tab),
    insert(s2, [<<"foo">>, <<"bar">>], 5, {val, 5}, Tab),
    insert(s2, [<<"1">>, <<"2">>], 6, {val, 6}, Tab),

    ?assertEqual(
        [{val, 1}],
        lists:sort(matches(s1, [<<"foo">>, <<"2">>], Tab))
    ),
    ?assertEqual(
        [{val, 4}],
        lists:sort(matches(s2, [<<"foo">>, <<"2">>], Tab))
    ),
    ?assertEqual(
        [{val, 1}, {val, 2}],
        lists:sort(matches(s1, [<<"foo">>, <<"bar">>], Tab))
    ),
    ?assertEqual(
        [{val, 4}, {val, 5}],
        lists:sort(matches(s2, [<<"foo">>, <<"bar">>], Tab))
    ),
    ?assertEqual(
        [],
        matches(s3, [<<"foo">>, <<"bar">>], Tab)
    ),
    ?assertEqual(
        [{val, 3}],
        matches(s1, [<<"1">>, <<"2">>], Tab)
    ),
    ?assertEqual(
        [{val, 6}],
        matches(s2, [<<"1">>, <<"2">>], Tab)
    ).

-endif.
