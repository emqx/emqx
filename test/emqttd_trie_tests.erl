%%--------------------------------------------------------------------
%% Copyright (c) 2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
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

-module(emqttd_trie_tests).

-ifdef(TEST).

-include("emqttd.hrl").

-include("emqttd_trie.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(TRIE, emqttd_trie).

trie_test_() ->
    {setup, fun setup/0, fun teardown/1,
     [{foreach, fun() -> ok end, fun(_) -> clear() end,
       [?_test(t_insert()),
        ?_test(t_match()),
        ?_test(t_match2()),
        ?_test(t_match3()),
        ?_test(t_delete()),
        ?_test(t_delete2()),
        ?_test(t_delete3())]
      }]}.

setup() ->
    emqttd_mnesia:ensure_started(),
    ?TRIE:mnesia(boot),
    ?TRIE:mnesia(copy).

teardown(_) ->
    emqttd_mnesia:ensure_stopped(),
    emqttd_mnesia:delete_schema().

t_insert() ->
    TN = #trie_node{node_id = <<"sensor">>,
                    edge_count = 3,
                    topic = <<"sensor">>,
                    flags = undefined},
    ?assertEqual({atomic, [TN]}, mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"sensor/1/metric/2">>),
                ?TRIE:insert(<<"sensor/+/#">>),
                ?TRIE:insert(<<"sensor/#">>),
                ?TRIE:insert(<<"sensor">>),
                ?TRIE:insert(<<"sensor">>),
                ?TRIE:lookup(<<"sensor">>)
            end)).

t_match() ->
    Machted = [<<"sensor/+/#">>, <<"sensor/#">>],
    ?assertEqual({atomic, Machted}, mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"sensor/1/metric/2">>),
                ?TRIE:insert(<<"sensor/+/#">>),
                ?TRIE:insert(<<"sensor/#">>),
                ?TRIE:match(<<"sensor/1">>)
            end)).

t_match2() ->
    Matched = {[<<"+/+/#">>, <<"+/#">>, <<"#">>], []},
    ?assertEqual({atomic, Matched}, mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"#">>),
                ?TRIE:insert(<<"+/#">>),
                ?TRIE:insert(<<"+/+/#">>),
                {?TRIE:match(<<"a/b/c">>),
                 ?TRIE:match(<<"$SYS/broker/zenmq">>)}
            end)).

t_match3() ->
    Topics = [<<"d/#">>, <<"a/b/c">>, <<"a/b/+">>, <<"a/#">>, <<"#">>, <<"$SYS/#">>],
    mnesia:transaction(fun() -> [emqttd_trie:insert(Topic) || Topic <- Topics] end),
    Matched = mnesia:async_dirty(fun emqttd_trie:match/1, [<<"a/b/c">>]),
    ?assertEqual(4, length(Matched)),
    SysMatched = mnesia:async_dirty(fun emqttd_trie:match/1, [<<"$SYS/a/b/c">>]),
    ?assertEqual([<<"$SYS/#">>], SysMatched).

t_delete() ->
    TN = #trie_node{node_id = <<"sensor/1">>,
                    edge_count = 2,
                    topic = undefined,
                    flags = undefined},
    ?assertEqual({atomic, [TN]}, mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"sensor/1/#">>),
                ?TRIE:insert(<<"sensor/1/metric/2">>),
                ?TRIE:insert(<<"sensor/1/metric/3">>),
                ?TRIE:delete(<<"sensor/1/metric/2">>),
                ?TRIE:delete(<<"sensor/1/metric">>),
                ?TRIE:delete(<<"sensor/1/metric">>),
                ?TRIE:lookup(<<"sensor/1">>)
            end)).

t_delete2() ->
    ?assertEqual({atomic, {[], []}}, mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"sensor">>),
                ?TRIE:insert(<<"sensor/1/metric/2">>),
                ?TRIE:insert(<<"sensor/1/metric/3">>),
                ?TRIE:delete(<<"sensor">>),
                ?TRIE:delete(<<"sensor/1/metric/2">>),
                ?TRIE:delete(<<"sensor/1/metric/3">>),
                {?TRIE:lookup(<<"sensor">>),
                 ?TRIE:lookup(<<"sensor/1">>)}
            end)).

t_delete3() ->
    ?assertEqual({atomic, {[], []}}, mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"sensor/+">>),
                ?TRIE:insert(<<"sensor/+/metric/2">>),
                ?TRIE:insert(<<"sensor/+/metric/3">>),
                ?TRIE:delete(<<"sensor/+/metric/2">>),
                ?TRIE:delete(<<"sensor/+/metric/3">>),
                ?TRIE:delete(<<"sensor">>),
                ?TRIE:delete(<<"sensor/+">>),
                ?TRIE:delete(<<"sensor/+/unknown">>),
                {?TRIE:lookup(<<"sensor">>), ?TRIE:lookup(<<"sensor/+">>)}
            end)).

clear() ->
    mnesia:clear_table(trie),
    mnesia:clear_table(trie_node).

-endif.
