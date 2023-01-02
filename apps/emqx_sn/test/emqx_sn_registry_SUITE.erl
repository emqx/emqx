%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(PREDEF_TOPICS, [{1, <<"/predefined/topic/name/hello">>},
                        {2, <<"/predefined/topic/name/nice">>}]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    _ = application:set_env(emqx_sn, predefined, ?PREDEF_TOPICS),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ekka_mnesia:start(),
    emqx_sn_registry:create_table(),
    mnesia:clear_table(emqx_sn_registry),
    PredefTopics = application:get_env(emqx_sn, predefined, []),
    {ok, _Pid} = ?REGISTRY:start_link(PredefTopics),
    Config.

end_per_testcase(_TestCase, Config) ->
    ?REGISTRY:stop(),
    Config.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_register(_Config) ->
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(<<"Topic1">>, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(<<"Topic2">>, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    emqx_sn_registry:unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic2">>)).

t_register_case2(_Config) ->
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(<<"Topic1">>, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(<<"Topic2">>, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic2">>)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic3">>)),
    ?REGISTRY:unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic1">>)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, <<"Topic2">>)).

t_reach_maximum(_Config) ->
    register_a_lot(?MAX_PREDEF_ID+1, 16#ffff),
    ?assertEqual({error, too_large}, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicABC">>)),
    Topic1 = iolist_to_binary(io_lib:format("Topic~p", [?MAX_PREDEF_ID+1])),
    Topic2 = iolist_to_binary(io_lib:format("Topic~p", [?MAX_PREDEF_ID+2])),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:lookup_topic_id(<<"ClientId">>, Topic1)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:lookup_topic_id(<<"ClientId">>, Topic2)),
    ?REGISTRY:unregister_topic(<<"ClientId">>),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(<<"ClientId">>, ?MAX_PREDEF_ID+2)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, Topic1)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"ClientId">>, Topic2)).

t_register_case4(_Config) ->
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicA">>)),
    ?assertEqual(?MAX_PREDEF_ID+2, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicB">>)),
    ?assertEqual(?MAX_PREDEF_ID+3, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicC">>)),
    ?REGISTRY:unregister_topic(<<"ClientId">>),
    ?assertEqual(?MAX_PREDEF_ID+1, ?REGISTRY:register_topic(<<"ClientId">>, <<"TopicD">>)).

t_deny_wildcard_topic(_Config) ->
    ?assertEqual({error, wildcard_topic}, ?REGISTRY:register_topic(<<"ClientId">>, <<"/TopicA/#">>)),
    ?assertEqual({error, wildcard_topic}, ?REGISTRY:register_topic(<<"ClientId">>, <<"/+/TopicB">>)).

t_gen_server(_) ->
    ?assertEqual(ignored, gen_server:call(emqx_sn_registry, ignored)),
    ?assertEqual(ok, gen_server:cast(emqx_sn_registry, ignored)),
    ?assertEqual(ignored, erlang:send(emqx_sn_registry, ignored)).

%%--------------------------------------------------------------------
%% Helper funcs
%%--------------------------------------------------------------------

register_a_lot(Max, Max) ->
    ok;
register_a_lot(N, Max) when N < Max ->
    Topic = iolist_to_binary(["Topic", integer_to_list(N)]),
    ?assertEqual(N, ?REGISTRY:register_topic(<<"ClientId">>, Topic)),
    register_a_lot(N+1, Max).
