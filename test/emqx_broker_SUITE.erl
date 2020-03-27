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

-module(emqx_broker_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% PubSub Test
%%--------------------------------------------------------------------

t_subscribed(_) ->
    emqx_broker:subscribe(<<"topic">>),
    ?assertEqual(false, emqx_broker:subscribed(undefined, <<"topic">>)),
    ?assertEqual(true, emqx_broker:subscribed(self(), <<"topic">>)),
    emqx_broker:unsubscribe(<<"topic">>).

t_subscribed_2(_) ->
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>),
    ?assertEqual(true, emqx_broker:subscribed(<<"clientid">>, <<"topic">>)),
    ?assertEqual(true, emqx_broker:subscribed(self(), <<"topic">>)),
    emqx_broker:unsubscribe(<<"topic">>).

t_subopts(_) ->
    ?assertEqual(false, emqx_broker:set_subopts(<<"topic">>, #{qos => 1})),
    ?assertEqual(undefined, emqx_broker:get_subopts(self(), <<"topic">>)),
    ?assertEqual(undefined, emqx_broker:get_subopts(<<"clientid">>, <<"topic">>)),
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{qos => 1}),
    timer:sleep(200),
    ?assertEqual(#{qos => 1, subid => <<"clientid">>}, emqx_broker:get_subopts(self(), <<"topic">>)),
    ?assertEqual(#{qos => 1, subid => <<"clientid">>}, emqx_broker:get_subopts(<<"clientid">>,<<"topic">>)),
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{qos => 2}),
    ?assertEqual(#{qos => 2, subid => <<"clientid">>}, emqx_broker:get_subopts(self(), <<"topic">>)),
    ?assertEqual(true, emqx_broker:set_subopts(<<"topic">>, #{qos => 2})),
    ?assertEqual(#{qos => 2, subid => <<"clientid">>}, emqx_broker:get_subopts(self(), <<"topic">>)),
    emqx_broker:unsubscribe(<<"topic">>).

t_topics(_) ->
    Topics = [<<"topic">>, <<"topic/1">>, <<"topic/2">>],
    ok = emqx_broker:subscribe(lists:nth(1, Topics), <<"clientId">>),
    ok = emqx_broker:subscribe(lists:nth(2, Topics), <<"clientId">>),
    ok = emqx_broker:subscribe(lists:nth(3, Topics), <<"clientId">>),
    Topics1 = emqx_broker:topics(),
    ?assertEqual(true, lists:foldl(fun(Topic, Acc) ->
                                       case lists:member(Topic, Topics1) of
                                           true -> Acc;
                                           false -> false
                                       end
                                   end, true, Topics)),
    emqx_broker:unsubscribe(lists:nth(1, Topics)),
    emqx_broker:unsubscribe(lists:nth(2, Topics)),
    emqx_broker:unsubscribe(lists:nth(3, Topics)).

t_subscribers(_) ->
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>),
    ?assertEqual([self()], emqx_broker:subscribers(<<"topic">>)),
    emqx_broker:unsubscribe(<<"topic">>).

t_subscriptions(_) ->
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{qos => 1}),
    ok = timer:sleep(100),
    ?assertEqual(#{qos => 1, subid => <<"clientid">>},
                 proplists:get_value(<<"topic">>, emqx_broker:subscriptions(self()))),
    ?assertEqual(#{qos => 1, subid => <<"clientid">>},
                 proplists:get_value(<<"topic">>, emqx_broker:subscriptions(<<"clientid">>))),
    emqx_broker:unsubscribe(<<"topic">>).

t_sub_pub(_) ->
    ok = emqx_broker:subscribe(<<"topic">>),
    ct:sleep(10),
    emqx_broker:safe_publish(emqx_message:make(ct, <<"topic">>, <<"hello">>)),
    ?assert(
        receive
            {deliver, <<"topic">>, #message{payload = <<"hello">>}} ->
                true;
            _ ->
                false
        after 100 ->
            false
        end).

t_nosub_pub(_) ->
    ?assertEqual(0, emqx_metrics:val('messages.dropped')),
    emqx_broker:publish(emqx_message:make(ct, <<"topic">>, <<"hello">>)),
    ?assertEqual(1, emqx_metrics:val('messages.dropped')).

t_shared_subscribe(_) ->
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{share => <<"group">>}),
    ct:sleep(10),
    emqx_broker:safe_publish(emqx_message:make(ct, <<"topic">>, <<"hello">>)),
    ?assert(receive
                {deliver, <<"topic">>, #message{payload = <<"hello">>}} ->
                    true;
                Msg ->
                    ct:pal("Msg: ~p", [Msg]),
                    false
            after 100 ->
                false
            end),
    emqx_broker:unsubscribe(<<"$share/group/topic">>).

t_shared_subscribe_2(_) ->
    {ok, ConnPid} = emqtt:start_link([{clean_start, true}, {clientid, <<"clientid">>}]),
    {ok, _} = emqtt:connect(ConnPid),
    {ok, _, [0]} = emqtt:subscribe(ConnPid, <<"$share/group/topic">>, 0),

    {ok, ConnPid2} = emqtt:start_link([{clean_start, true}, {clientid, <<"clientid2">>}]),
    {ok, _} = emqtt:connect(ConnPid2),
    {ok, _, [0]} = emqtt:subscribe(ConnPid2, <<"$share/group2/topic">>, 0),

    ct:sleep(10),
    ok = emqtt:publish(ConnPid, <<"topic">>, <<"hello">>, 0),
    Msgs = recv_msgs(2),
    ?assertEqual(2, length(Msgs)),
    ?assertEqual(true, lists:foldl(fun(#{payload := <<"hello">>, topic := <<"topic">>}, Acc) ->
                                       Acc;
                                      (_, _) ->
                                       false
                                   end, true, Msgs)),
    emqtt:disconnect(ConnPid),
    emqtt:disconnect(ConnPid2).

t_shared_subscribe_3(_) ->
    {ok, ConnPid} = emqtt:start_link([{clean_start, true}, {clientid, <<"clientid">>}]),
    {ok, _} = emqtt:connect(ConnPid),
    {ok, _, [0]} = emqtt:subscribe(ConnPid, <<"$share/group/topic">>, 0),

    {ok, ConnPid2} = emqtt:start_link([{clean_start, true}, {clientid, <<"clientid2">>}]),
    {ok, _} = emqtt:connect(ConnPid2),
    {ok, _, [0]} = emqtt:subscribe(ConnPid2, <<"$share/group/topic">>, 0),

    ct:sleep(10),
    ok = emqtt:publish(ConnPid, <<"topic">>, <<"hello">>, 0),
    Msgs = recv_msgs(2),
    ?assertEqual(1, length(Msgs)),
    emqtt:disconnect(ConnPid),
    emqtt:disconnect(ConnPid2).

t_shard(_) ->
    ok = meck:new(emqx_broker_helper, [passthrough, no_history]),
    ok = meck:expect(emqx_broker_helper, get_sub_shard, fun(_, _) -> 1 end),
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>),
    ct:sleep(10),
    emqx_broker:safe_publish(emqx_message:make(ct, <<"topic">>, <<"hello">>)),
    ?assert(
        receive
            {deliver, <<"topic">>, #message{payload = <<"hello">>}} ->
                true;
            _ ->
                false
        after 100 ->
            false
        end),
    ok = meck:unload(emqx_broker_helper).

t_stats_fun(_) ->
    ?assertEqual(0, emqx_stats:getstat('subscribers.count')),
    ?assertEqual(0, emqx_stats:getstat('subscriptions.count')),
    ?assertEqual(0, emqx_stats:getstat('suboptions.count')),
    ok = emqx_broker:subscribe(<<"topic">>, <<"clientid">>),
    ok = emqx_broker:subscribe(<<"topic2">>, <<"clientid">>),
    emqx_broker:stats_fun(),
    ct:sleep(10),
    ?assertEqual(2, emqx_stats:getstat('subscribers.count')),
    ?assertEqual(2, emqx_stats:getstat('subscribers.max')),
    ?assertEqual(2, emqx_stats:getstat('subscriptions.count')),
    ?assertEqual(2, emqx_stats:getstat('subscriptions.max')),
    ?assertEqual(2, emqx_stats:getstat('suboptions.count')),
    ?assertEqual(2, emqx_stats:getstat('suboptions.max')).

recv_msgs(Count) ->
    recv_msgs(Count, []).

recv_msgs(0, Msgs) ->
    Msgs;
recv_msgs(Count, Msgs) ->
    receive
        {publish, Msg} ->
            recv_msgs(Count-1, [Msg|Msgs]);
        _Other -> recv_msgs(Count, Msgs)
    after 100 ->
        Msgs
    end.
