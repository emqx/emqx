%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_trie_SUITE).

-compile(export_all).

-include("emqttd_trie.hrl").

-define(TRIE, emqttd_trie).

all() ->
    [t_insert, t_match, t_match2, t_match3, t_delete, t_delete2, t_delete3].

init_per_suite(Config) ->
    emqttd_mnesia:ensure_started(),
    ?TRIE:mnesia(boot),
    ?TRIE:mnesia(copy),
    Config.

end_per_suite(_Config) ->
    emqttd_mnesia:ensure_stopped(),
    emqttd_mnesia:delete_schema().

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_tables().

t_insert(_) ->
    TN = #trie_node{node_id = <<"sensor">>,
                    edge_count = 3,
                    topic = <<"sensor">>,
                    flags = undefined},
    {atomic, [TN]} = mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"sensor/1/metric/2">>),
                ?TRIE:insert(<<"sensor/+/#">>),
                ?TRIE:insert(<<"sensor/#">>),
                ?TRIE:insert(<<"sensor">>),
                ?TRIE:insert(<<"sensor">>),
                ?TRIE:lookup(<<"sensor">>)
            end).

t_match(_) ->
    Machted = [<<"sensor/+/#">>, <<"sensor/#">>],
    {atomic, Machted} = mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"sensor/1/metric/2">>),
                ?TRIE:insert(<<"sensor/+/#">>),
                ?TRIE:insert(<<"sensor/#">>),
                ?TRIE:match(<<"sensor/1">>)
            end).

t_match2(_) ->
    Matched = {[<<"+/+/#">>, <<"+/#">>, <<"#">>], []},
    {atomic, Matched} = mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"#">>),
                ?TRIE:insert(<<"+/#">>),
                ?TRIE:insert(<<"+/+/#">>),
                {?TRIE:match(<<"a/b/c">>),
                 ?TRIE:match(<<"$SYS/broker/zenmq">>)}
            end).

t_match3(_) ->
    Topics = [<<"d/#">>, <<"a/b/c">>, <<"a/b/+">>, <<"a/#">>, <<"#">>, <<"$SYS/#">>],
    mnesia:transaction(fun() -> [emqttd_trie:insert(Topic) || Topic <- Topics] end),
    Matched = mnesia:async_dirty(fun emqttd_trie:match/1, [<<"a/b/c">>]),
    4 = length(Matched),
    SysMatched = mnesia:async_dirty(fun emqttd_trie:match/1, [<<"$SYS/a/b/c">>]),
    [<<"$SYS/#">>] = SysMatched.

t_delete(_) ->
    TN = #trie_node{node_id = <<"sensor/1">>,
                    edge_count = 2,
                    topic = undefined,
                    flags = undefined},
    {atomic, [TN]} = mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"sensor/1/#">>),
                ?TRIE:insert(<<"sensor/1/metric/2">>),
                ?TRIE:insert(<<"sensor/1/metric/3">>),
                ?TRIE:delete(<<"sensor/1/metric/2">>),
                ?TRIE:delete(<<"sensor/1/metric">>),
                ?TRIE:delete(<<"sensor/1/metric">>),
                ?TRIE:lookup(<<"sensor/1">>)
            end).

t_delete2(_) ->
    {atomic, {[], []}} = mnesia:transaction(
            fun() ->
                ?TRIE:insert(<<"sensor">>),
                ?TRIE:insert(<<"sensor/1/metric/2">>),
                ?TRIE:insert(<<"sensor/1/metric/3">>),
                ?TRIE:delete(<<"sensor">>),
                ?TRIE:delete(<<"sensor/1/metric/2">>),
                ?TRIE:delete(<<"sensor/1/metric/3">>),
                {?TRIE:lookup(<<"sensor">>),
                 ?TRIE:lookup(<<"sensor/1">>)}
            end).

t_delete3(_) ->
    {atomic, {[], []}} = mnesia:transaction(
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
            end).

clear_tables() ->
    lists:foreach(fun mnesia:clear_table/1, [trie, trie_node]).

