%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_mqttsn.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(REGISTRY, emqx_mqttsn_registry).
-define(MAX_PREDEF_ID, ?SN_MAX_PREDEF_TOPIC_ID).
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
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    emqx_mqttsn_registry:persist_predefined_topics(?PREDEF_TOPICS),
    Config.

end_per_testcase(_TestCase, Config) ->
    emqx_mqttsn_registry:clear_predefined_topics(?PREDEF_TOPICS),
    Config.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_register(_) ->
    Reg = ?REGISTRY:init(),
    {ok, ?MAX_PREDEF_ID + 1, Reg1} = ?REGISTRY:reg(<<"Topic1">>, Reg),
    {ok, ?MAX_PREDEF_ID + 2, Reg2} = ?REGISTRY:reg(<<"Topic2">>, Reg1),
    ?assertMatch({ok, ?MAX_PREDEF_ID + 1, Reg2}, ?REGISTRY:reg(<<"Topic1">>, Reg2)),
    ?assertEqual(<<"Topic1">>, ?REGISTRY:lookup_topic(?MAX_PREDEF_ID + 1, Reg2)),
    ?assertEqual(<<"Topic2">>, ?REGISTRY:lookup_topic(?MAX_PREDEF_ID + 2, Reg2)),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:lookup_topic_id(<<"Topic1">>, Reg2)),
    ?assertEqual(?MAX_PREDEF_ID + 2, ?REGISTRY:lookup_topic_id(<<"Topic2">>, Reg2)),

    Reg3 = emqx_mqttsn_registry:unreg(<<"Topic1">>, Reg2),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic(?MAX_PREDEF_ID + 1, Reg3)),
    ?assertEqual(undefined, ?REGISTRY:lookup_topic_id(<<"Topic1">>, Reg3)),
    ?assertEqual(<<"Topic2">>, ?REGISTRY:lookup_topic(?MAX_PREDEF_ID + 2, Reg3)),
    ?assertEqual(?MAX_PREDEF_ID + 2, ?REGISTRY:lookup_topic_id(<<"Topic2">>, Reg3)),

    ?assertMatch({ok, ?MAX_PREDEF_ID + 3, _Reg4}, ?REGISTRY:reg(<<"Topic3">>, Reg3)).

t_reach_maximum(_) ->
    Reg0 = ?REGISTRY:init(),
    Reg = register_a_lot(?MAX_PREDEF_ID + 1, 16#ffff, Reg0),
    ?assertEqual({error, too_large}, ?REGISTRY:reg(<<"TopicABC">>, Reg)),
    ?assertEqual(?MAX_PREDEF_ID + 1, ?REGISTRY:lookup_topic_id(<<"Topic1025">>, Reg)),
    ?assertEqual(?MAX_PREDEF_ID + 2, ?REGISTRY:lookup_topic_id(<<"Topic1026">>, Reg)).

t_deny_wildcard_topic(_) ->
    Reg = ?REGISTRY:init(),
    ?assertEqual({error, wildcard_topic}, ?REGISTRY:reg(<<"/TopicA/#">>, Reg)),
    ?assertEqual({error, wildcard_topic}, ?REGISTRY:reg(<<"/+/TopicB">>, Reg)).

%%--------------------------------------------------------------------
%% Helper funcs
%%--------------------------------------------------------------------

register_a_lot(N, Max, Reg) when N =< Max ->
    Topic = iolist_to_binary(["Topic", integer_to_list(N)]),
    {ok, ReturnedId, Reg1} = ?REGISTRY:reg(Topic, Reg),
    ?assertEqual(N, ReturnedId),
    case N == Max of
        true ->
            Reg1;
        _ ->
            register_a_lot(N + 1, Max, Reg1)
    end.
