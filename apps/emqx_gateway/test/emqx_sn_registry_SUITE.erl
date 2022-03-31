%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sn_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(REGISTRY, emqx_sn_registry).
-define(MAX_PREDEF_ID, 2).
-define(PREDEF_TOPICS, [
    #{id => 1, topic => <<"/predefined/topic/name/hello">>},
    #{id => 2, topic => <<"/predefined/topic/name/nice">>}
]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:ensure_all_started(ekka),
    mria:start(),
    Config.

end_per_suite(_Config) ->
    application:stop(ekka),
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, Pid} = ?REGISTRY:start_link('mqttsn', ?PREDEF_TOPICS),
    {Tab, Pid} = ?REGISTRY:lookup_name(Pid),
    [{reg, {Tab, Pid}} | Config].

end_per_testcase(_TestCase, Config) ->
    {Tab, _Pid} = proplists:get_value(reg, Config),
    mria:clear_table(Tab),
    Config.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_register(Config) ->
    Reg = proplists:get_value(reg, Config),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID + 2, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(<<"Topic1">>, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 1)),
    ?assertEqual(<<"Topic2">>, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 2)),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID + 2, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, <<"Topic2">>)),
    emqx_sn_registry:unregister_topic(Reg, <<"ClientId">>),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 2)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, <<"Topic2">>)).

t_register_case2(Config) ->
    Reg = proplists:get_value(reg, Config),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID + 2, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(<<"Topic1">>, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 1)),
    ?assertEqual(<<"Topic2">>, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 2)),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID + 2, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, <<"Topic3">>)),
    ?REGISTRY:unregister_topic(Reg, <<"ClientId">>),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 2)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, <<"Topic2">>)).

t_reach_maximum(Config) ->
    Reg = proplists:get_value(reg, Config),
    register_a_lot(?MAX_PREDEF_ID + 1, 16#ffff, Reg),
    ?assertEqual({error, too_large}, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"TopicABC">>)),
    Topic1 = iolist_to_binary(io_lib:format("Topic~p", [?MAX_PREDEF_ID + 1])),
    Topic2 = iolist_to_binary(io_lib:format("Topic~p", [?MAX_PREDEF_ID + 2])),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, Topic1)),
    ?assertEqual(?MAX_PREDEF_ID + 2, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, Topic2)),
    ?REGISTRY:unregister_topic(Reg, <<"ClientId">>),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(Reg, <<"ClientId">>, ?MAX_PREDEF_ID + 2)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, Topic1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(Reg, <<"ClientId">>, Topic2)).

t_register_case4(Config) ->
    Reg = proplists:get_value(reg, Config),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"TopicA">>)),
    ?assertEqual(?MAX_PREDEF_ID + 2, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"TopicB">>)),
    ?assertEqual(?MAX_PREDEF_ID + 3, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"TopicC">>)),
    ?REGISTRY:unregister_topic(Reg, <<"ClientId">>),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"TopicD">>)).

t_deny_wildcard_topic(Config) ->
    Reg = proplists:get_value(reg, Config),
    ?assertEqual(
        {error, wildcard_topic}, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"/TopicA/#">>)
    ),
    ?assertEqual(
        {error, wildcard_topic}, ?REGISTRY:register_topic(Reg, <<"ClientId">>, <<"/+/TopicB">>)
    ).

%%--------------------------------------------------------------------
%% Helper funcs
%%--------------------------------------------------------------------

register_a_lot(Max, Max, _Reg) ->
    ok;
register_a_lot(N, Max, Reg) when N < Max ->
    Topic = iolist_to_binary(["Topic", integer_to_list(N)]),
    ?assertEqual(N, ?REGISTRY:register_topic(Reg, <<"ClientId">>, Topic)),
    register_a_lot(N + 1, Max, Reg).
