%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TRIE, emqx_trie).

all() ->
    [{group, compact},
     {group, not_compact}
    ].

groups() ->
    Cases = emqx_ct:all(?MODULE),
    [{compact, Cases}, {not_compact, Cases}].

init_per_group(compact, Config) ->
    emqx_trie:put_compaction_flag(true),
    Config;
init_per_group(not_compact, Config) ->
    emqx_trie:put_compaction_flag(false),
    Config.

end_per_group(_, _) ->
    emqx_trie:put_default_compaction_flag().

init_per_suite(Config) ->
    application:load(emqx),
    ok = ekka:start(),
    Config.

end_per_suite(_Config) ->
    ekka:stop(),
    ekka_mnesia:ensure_stopped(),
    ekka_mnesia:delete_schema().

init_per_testcase(_TestCase, Config) ->
    clear_tables(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_tables().

t_mnesia(_) ->
    ok = ?TRIE:mnesia(copy).

t_insert(_) ->
    Fun = fun() ->
              ?TRIE:insert(<<"sensor/1/metric/2">>),
              ?TRIE:insert(<<"sensor/+/#">>),
              ?TRIE:insert(<<"sensor/#">>)
          end,
    ?assertEqual({atomic, ok}, trans(Fun)),
    ?assertEqual([<<"sensor/#">>], ?TRIE:match(<<"sensor">>)).

t_match(_) ->
    Machted = [<<"sensor/#">>, <<"sensor/+/#">>],
    trans(fun() ->
              ?TRIE:insert(<<"sensor/1/metric/2">>),
              ?TRIE:insert(<<"sensor/+/#">>),
              ?TRIE:insert(<<"sensor/#">>)
            end),
    ?assertEqual(Machted, lists:sort(?TRIE:match(<<"sensor/1">>))).

t_match_invalid(_) ->
    trans(fun() ->
              ?TRIE:insert(<<"sensor/1/metric/2">>),
              ?TRIE:insert(<<"sensor/+/#">>),
              ?TRIE:insert(<<"sensor/#">>)
            end),
    ?assertEqual([], lists:sort(?TRIE:match(<<"sensor/+">>))),
    ?assertEqual([], lists:sort(?TRIE:match(<<"#">>))).


t_match2(_) ->
    Matched = [<<"#">>, <<"+/#">>, <<"+/+/#">>],
    trans(fun() ->
              ?TRIE:insert(<<"#">>),
              ?TRIE:insert(<<"+/#">>),
              ?TRIE:insert(<<"+/+/#">>)
          end),
    ?assertEqual(Matched, lists:sort(?TRIE:match(<<"a/b/c">>))),
    ?assertEqual([], ?TRIE:match(<<"$SYS/broker/zenmq">>)).

t_match3(_) ->
    Topics = [<<"d/#">>, <<"a/b/+">>, <<"a/#">>, <<"#">>, <<"$SYS/#">>],
    trans(fun() -> [emqx_trie:insert(Topic) || Topic <- Topics] end),
    Matched = mnesia:async_dirty(fun emqx_trie:match/1, [<<"a/b/c">>]),
    case length(Matched) of
        3 -> ok;
        _ -> error({unexpected, Matched})
    end,
    SysMatched = emqx_trie:match(<<"$SYS/a/b/c">>),
    ?assertEqual([<<"$SYS/#">>], SysMatched).

t_match4(_) ->
    Topics = [<<"/#">>, <<"/+">>, <<"/+/a/b/c">>],
    trans(fun() -> lists:foreach(fun emqx_trie:insert/1, Topics) end),
    ?assertEqual([<<"/#">>, <<"/+/a/b/c">>], lists:sort(emqx_trie:match(<<"/0/a/b/c">>))).

t_match5(_) ->
    T = <<"a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z">>,
    Topics = [<<"#">>, <<T/binary, "/#">>, <<T/binary, "/+">>],
    trans(fun() -> lists:foreach(fun emqx_trie:insert/1, Topics) end),
    ?assertEqual([<<"#">>, <<T/binary, "/#">>], lists:sort(emqx_trie:match(T))),
    ?assertEqual([<<"#">>, <<T/binary, "/#">>, <<T/binary, "/+">>],
                 lists:sort(emqx_trie:match(<<T/binary, "/1">>))).

t_match6(_) ->
    T = <<"a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z">>,
    W = <<"+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/+/#">>,
    trans(fun() -> emqx_trie:insert(W) end),
    ?assertEqual([W], emqx_trie:match(T)).

t_match7(_) ->
    T = <<"a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z">>,
    W = <<"a/+/c/+/e/+/g/+/i/+/k/+/m/+/o/+/q/+/s/+/u/+/w/+/y/+/#">>,
    trans(fun() -> emqx_trie:insert(W) end),
    ?assertEqual([W], emqx_trie:match(T)).

t_empty(_) ->
    ?assert(?TRIE:empty()),
    trans(fun ?TRIE:insert/1, [<<"topic/x/#">>]),
    ?assertNot(?TRIE:empty()),
    trans(fun ?TRIE:delete/1, [<<"topic/x/#">>]),
    ?assert(?TRIE:empty()).

t_delete(_) ->
    trans(fun() ->
              ?TRIE:insert(<<"sensor/1/#">>),
              ?TRIE:insert(<<"sensor/1/metric/2">>),
              ?TRIE:insert(<<"sensor/1/metric/3">>)
          end),
    trans(fun() ->
              ?TRIE:delete(<<"sensor/1/metric/2">>),
              ?TRIE:delete(<<"sensor/1/metric">>),
              ?TRIE:delete(<<"sensor/1/metric">>)
          end),
    ?assertEqual([<<"sensor/1/#">>], ?TRIE:match(<<"sensor/1/x">>)).

t_delete2(_) ->
    trans(fun() ->
              ?TRIE:insert(<<"sensor">>),
              ?TRIE:insert(<<"sensor/1/metric/2">>),
              ?TRIE:insert(<<"sensor/+/metric/3">>)
          end),
    trans(fun() ->
              ?TRIE:delete(<<"sensor">>),
              ?TRIE:delete(<<"sensor/1/metric/2">>),
              ?TRIE:delete(<<"sensor/+/metric/3">>),
              ?TRIE:delete(<<"sensor/+/metric/3">>)
          end),
    ?assertEqual([], ?TRIE:match(<<"sensor">>)),
    ?assertEqual([], ?TRIE:match(<<"sensor/1">>)).

t_delete3(_) ->
    trans(fun() ->
              ?TRIE:insert(<<"sensor/+">>),
              ?TRIE:insert(<<"sensor/+/metric/2">>),
              ?TRIE:insert(<<"sensor/+/metric/3">>)
          end),
    trans(fun() ->
              ?TRIE:delete(<<"sensor/+/metric/2">>),
              ?TRIE:delete(<<"sensor/+/metric/3">>),
              ?TRIE:delete(<<"sensor">>),
              ?TRIE:delete(<<"sensor/+">>),
              ?TRIE:delete(<<"sensor/+/unknown">>)
          end),
    ?assertEqual([], ?TRIE:match(<<"sensor">>)),
    ?assertEqual([], ?TRIE:lookup_topic(<<"sensor/+">>)).

clear_tables() -> emqx_trie:clear_tables().

trans(Fun) ->
    mnesia:transaction(Fun).
trans(Fun, Args) ->
    mnesia:transaction(Fun, Args).

