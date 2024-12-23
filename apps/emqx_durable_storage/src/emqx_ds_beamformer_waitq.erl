%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([
    new/0,
    insert/5,
    delete/4,
    del_stream/2,
    matching_keys/3,
    streams_of_rank/2,
    has_candidates/2,
    size/1,
    all_reqs/1
]).

-export_type([t/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-define(rec(STREAM, SEARCH, RANK), {{STREAM, SEARCH}, RANK}).

-type t() :: ets:tid().

%%================================================================================
%% API functions
%%================================================================================

new() ->
    ets:new(?MODULE, [ordered_set, private]).

insert(Stream, Filter, ID, Rank, Tab) ->
    Key = make_key(Stream, Filter, ID),
    true = ets:insert(Tab, {Key, Rank}).

delete(Stream, Filter, ID, Tab) ->
    ets:delete(Tab, make_key(Stream, Filter, ID)).

matching_keys(Stream, Topic, Tab) ->
    [
        emqx_trie_search:get_id(Key)
     || Key <- emqx_trie_search:matches(Topic, make_nextf(Stream, Tab), [])
    ].

%% @doc Return the list of streams that have certain rank.
%%
%% Warning: slow!
streams_of_rank(Rank, Tab) ->
    MS = {?rec('$1', '_', Rank), [], ['$1']},
    streams_of_rank_(ets:select(Tab, [MS], 100), #{}).

%% @doc Quick check if there are any subscribers to the given stream:
has_candidates(Stream, Tab) ->
    case ets:next(Tab, {Stream, 0}) of
        {Stream, _} -> true;
        _ -> false
    end.

size(Tab) ->
    ets:info(Tab, size).

all_reqs(Queue) ->
    MS = {?rec('_', '$1', '_'), [], ['$1']},
    [
        emqx_trie_search:get_id(Key)
     || Key <- ets:select(Queue, [MS])
    ].

%% @doc Delete all records with a given stream:
del_stream(Stream, Tab) ->
    ets:match_delete(Tab, ?rec(Stream, '_', '_')).

%%================================================================================
%% Internal functions
%%================================================================================

streams_of_rank_('$end_of_table', Acc) ->
    maps:keys(Acc);
streams_of_rank_({Streams, Cont}, Acc0) ->
    Acc = maps:merge(Acc0, maps:from_keys(Streams, [])),
    streams_of_rank_(ets:select(Cont), Acc).

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

-ifdef(TEST_FIXME).

topic_match_test() ->
    Tab = new(),
    insert(s1, [<<"foo">>, '+'], 1, {val, 1}, Tab),
    insert(s1, [<<"foo">>, <<"bar">>], 2, {val, 2}, Tab),
    insert(s1, [<<"1">>, <<"2">>], 3, {val, 3}, Tab),

    insert(s2, [<<"foo">>, '+'], 4, {val, 4}, Tab),
    insert(s2, [<<"foo">>, <<"bar">>], 5, {val, 5}, Tab),
    insert(s2, [<<"1">>, <<"2">>], 6, {val, 6}, Tab),

    S3 = [1 | {stream, <<207, 9, 108, 69, 242, 143, 34, 122>>}],
    insert(S3, [<<"foo">>, <<"1">>], 7, {val, 7}, Tab),

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
    ),
    ?assertEqual(
        [{val, 7}],
        matches(S3, [<<"foo">>, <<"1">>], Tab)
    ).

-endif.
