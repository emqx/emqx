%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_trie_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TRIE, emqx_trie).
-define(TRIE_TABS, [emqx_trie, emqx_trie_node]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx),
    ok = ekka:start(),
    Config.

end_per_suite(_Config) ->
    ekka:stop(),
    ekka_mnesia:ensure_stopped(),
    ekka_mnesia:delete_schema().

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_tables().

t_mnesia(_) ->
    ok = ?TRIE:mnesia(copy).

t_insert(_) ->
    TN = #trie_node{node_id = <<"sensor">>,
                    edge_count = 3,
                    topic = <<"sensor">>,
                    flags = undefined
                   },
    Fun = fun() ->
              ?TRIE:insert(<<"sensor/1/metric/2">>),
              ?TRIE:insert(<<"sensor/+/#">>),
              ?TRIE:insert(<<"sensor/#">>),
              ?TRIE:insert(<<"sensor">>),
              ?TRIE:insert(<<"sensor">>),
              ?TRIE:lookup(<<"sensor">>)
          end,
    ?assertEqual({atomic, [TN]}, trans(Fun)).

t_match(_) ->
    Machted = [<<"sensor/+/#">>, <<"sensor/#">>],
    Fun = fun() ->
              ?TRIE:insert(<<"sensor/1/metric/2">>),
              ?TRIE:insert(<<"sensor/+/#">>),
              ?TRIE:insert(<<"sensor/#">>),
              ?TRIE:match(<<"sensor/1">>)
            end,
    ?assertEqual({atomic, Machted}, trans(Fun)).

t_match2(_) ->
    Matched = {[<<"+/+/#">>, <<"+/#">>, <<"#">>], []},
    Fun = fun() ->
              ?TRIE:insert(<<"#">>),
              ?TRIE:insert(<<"+/#">>),
              ?TRIE:insert(<<"+/+/#">>),
              {?TRIE:match(<<"a/b/c">>),
               ?TRIE:match(<<"$SYS/broker/zenmq">>)}
          end,
    ?assertEqual({atomic, Matched}, trans(Fun)).

t_match3(_) ->
    Topics = [<<"d/#">>, <<"a/b/c">>, <<"a/b/+">>, <<"a/#">>, <<"#">>, <<"$SYS/#">>],
    trans(fun() -> [emqx_trie:insert(Topic) || Topic <- Topics] end),
    Matched = mnesia:async_dirty(fun emqx_trie:match/1, [<<"a/b/c">>]),
    ?assertEqual(4, length(Matched)),
    SysMatched = mnesia:async_dirty(fun emqx_trie:match/1, [<<"$SYS/a/b/c">>]),
    ?assertEqual([<<"$SYS/#">>], SysMatched).

t_empty(_) ->
    ?assert(?TRIE:empty()),
    trans(fun ?TRIE:insert/1, [<<"topic/x/#">>]),
    ?assertNot(?TRIE:empty()),
    trans(fun ?TRIE:delete/1, [<<"topic/x/#">>]),
    ?assert(?TRIE:empty()).

t_delete(_) ->
    TN = #trie_node{node_id = <<"sensor/1">>,
                    edge_count = 2,
                    topic = undefined,
                    flags = undefined},
    Fun = fun() ->
              ?TRIE:insert(<<"sensor/1/#">>),
              ?TRIE:insert(<<"sensor/1/metric/2">>),
              ?TRIE:insert(<<"sensor/1/metric/3">>),
              ?TRIE:delete(<<"sensor/1/metric/2">>),
              ?TRIE:delete(<<"sensor/1/metric">>),
              ?TRIE:delete(<<"sensor/1/metric">>),
              ?TRIE:lookup(<<"sensor/1">>)
          end,
    ?assertEqual({atomic, [TN]}, trans(Fun)).

t_delete2(_) ->
    Fun = fun() ->
              ?TRIE:insert(<<"sensor">>),
              ?TRIE:insert(<<"sensor/1/metric/2">>),
              ?TRIE:insert(<<"sensor/+/metric/3">>),
              ?TRIE:delete(<<"sensor">>),
              ?TRIE:delete(<<"sensor/1/metric/2">>),
              ?TRIE:delete(<<"sensor/+/metric/3">>),
              ?TRIE:delete(<<"sensor/+/metric/3">>),
              {?TRIE:lookup(<<"sensor">>), ?TRIE:lookup(<<"sensor/1">>)}
          end,
    ?assertEqual({atomic, {[], []}}, trans(Fun)).

t_delete3(_) ->
    Fun = fun() ->
              ?TRIE:insert(<<"sensor/+">>),
              ?TRIE:insert(<<"sensor/+/metric/2">>),
              ?TRIE:insert(<<"sensor/+/metric/3">>),
              ?TRIE:delete(<<"sensor/+/metric/2">>),
              ?TRIE:delete(<<"sensor/+/metric/3">>),
              ?TRIE:delete(<<"sensor">>),
              ?TRIE:delete(<<"sensor/+">>),
              ?TRIE:delete(<<"sensor/+/unknown">>),
              {?TRIE:lookup(<<"sensor">>), ?TRIE:lookup(<<"sensor/+">>)}
          end,
    ?assertEqual({atomic, {[], []}}, trans(Fun)).

clear_tables() ->
    lists:foreach(fun mnesia:clear_table/1, ?TRIE_TABS).

trans(Fun) ->
    mnesia:transaction(Fun).
trans(Fun, Args) ->
    mnesia:transaction(Fun, Args).

