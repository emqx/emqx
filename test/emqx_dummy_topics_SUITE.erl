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

-module(emqx_dummy_topics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = application:load(emqx),
    ok = ekka:start(),
    %% for coverage
    ok = emqx_dummy_topics:mnesia(boot),
    ok = emqx_dummy_topics:mnesia(copy),
    Config.

end_per_suite(_Config) ->
    ekka:stop(),
    ekka_mnesia:ensure_stopped(),
    ekka_mnesia:delete_schema().

init_per_testcase(_, Config) ->
    {ok, _} = emqx_dummy_topics:start_link(),
    Config.

end_per_testcase(_, Config) ->
    ok = emqx_dummy_topics:stop(),
    Config.

t_topic_add_has_del_list(_) ->
    ?assertEqual([], emqx_dummy_topics:list()),
    ?assertEqual(ok, emqx_dummy_topics:add(<<"t/1">>)),
    ?assertEqual(ok, emqx_dummy_topics:add(<<"t/2">>)),
    ct:sleep(100),
    ?assert(emqx_dummy_topics:has(<<"t/1">>)),
    ?assert(emqx_dummy_topics:has(<<"t/2">>)),
    ?assertEqual(2,
        length([Elm || Elm = #{topic := Topic, created_at := CreatedAt}
                <- emqx_dummy_topics:list(),
                 is_binary(Topic), is_integer(CreatedAt)])),
    ?assertEqual(ok, emqx_dummy_topics:del(<<"t/1">>)),
    ct:sleep(100),
    ?assertNot(emqx_dummy_topics:has(<<"t/1">>)),
    ?assert(emqx_dummy_topics:has(<<"t/2">>)),
    ?assertEqual(1,
        length([Elm || Elm = #{topic := Topic, created_at := CreatedAt}
                <- emqx_dummy_topics:list(),
                 is_binary(Topic), is_integer(CreatedAt)])).

t_topic_match_1(_) ->
    ?assertEqual([], emqx_dummy_topics:list()),
    ?assertEqual(ok, emqx_dummy_topics:add(<<"a/1">>)),
    ?assertEqual(ok, emqx_dummy_topics:add(<<"a/1/1">>)),
    ?assertEqual(ok, emqx_dummy_topics:add(<<"a/1/1/1">>)),
    ?assertEqual(ok, emqx_dummy_topics:add(<<"a/2">>)),
    ?assertEqual(ok, emqx_dummy_topics:add(<<"b/1">>)),
    ?assertEqual(ok, emqx_dummy_topics:add(<<"a/1">>)), %% duplicated
    ?assertEqual(5, length(emqx_dummy_topics:list())),
    ?assert(emqx_dummy_topics:match(<<"a/1">>)),
    ?assert(emqx_dummy_topics:match(<<"a/1/1">>)),
    ?assert(emqx_dummy_topics:match(<<"a/1/1/1">>)),
    ?assert(emqx_dummy_topics:match(<<"a/2">>)),
    ?assert(emqx_dummy_topics:match(<<"b/1">>)),
    ?assertNot(emqx_dummy_topics:match(<<"a/3">>)),
    ?assertNot(emqx_dummy_topics:match(<<"b/2">>)),
    ?assertNot(emqx_dummy_topics:match(<<"t">>)),
    ?assertEqual(ok, emqx_dummy_topics:del(<<"a/1">>)),
    ?assertEqual(ok, emqx_dummy_topics:del(<<"a/1/1">>)),
    ?assertEqual(ok, emqx_dummy_topics:del(<<"a/1/1/1">>)),
    ?assertEqual(ok, emqx_dummy_topics:del(<<"a/2">>)),
    ?assertEqual(ok, emqx_dummy_topics:del(<<"b/1">>)),
    ?assertEqual(0, length(emqx_dummy_topics:list())),
    ?assertNot(emqx_dummy_topics:match(<<"a/1">>)),
    ?assertNot(emqx_dummy_topics:match(<<"a/1/1">>)),
    ?assertNot(emqx_dummy_topics:match(<<"a/1/1/1">>)),
    ?assertNot(emqx_dummy_topics:match(<<"a/2">>)),
    ?assertNot(emqx_dummy_topics:match(<<"b/1">>)).

t_topic_match_2(_) ->
    ?assertEqual([], emqx_dummy_topics:list()),
    ok = emqx_dummy_topics:add(<<"#">>),
    ct:pal("------- ~p~n", [emqx_dummy_topics:get_trie()]),
    ?assertEqual(1, length(emqx_dummy_topics:list())),
    ?assert(emqx_dummy_topics:match(<<>>)),
    ok = emqx_dummy_topics:del(<<"#">>),
    ?assertEqual(0, length(emqx_dummy_topics:list())).

t_topic_match_3(_) ->
    ?assertEqual([], emqx_dummy_topics:list()),
    ok = emqx_dummy_topics:add(<<"#">>),
    ok = emqx_dummy_topics:add(<<"+">>),
    ct:pal("------- ~p~n", [emqx_dummy_topics:get_trie()]),
    ?assertEqual(2, length(emqx_dummy_topics:list())),
    ?assertNot(emqx_dummy_topics:match(<<"$">>)),
    ?assertNot(emqx_dummy_topics:match(<<"$ab">>)),
    ?assertNot(emqx_dummy_topics:match(<<"$a/b">>)),
    ?assert(emqx_dummy_topics:match(<<"/">>)),
    ?assert(emqx_dummy_topics:match(<<"a/b">>)),
    ok = emqx_dummy_topics:del(<<"#">>),
    ok = emqx_dummy_topics:del(<<"+">>),
    ?assertEqual(0, length(emqx_dummy_topics:list())).
