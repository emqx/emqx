%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

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
    %?assertEqual(true, emqx_broker:subscribed(<<"clientid">>, <<"topic">>)),
    ?assertEqual(true, emqx_broker:subscribed(self(), <<"topic">>)),
    emqx_broker:unsubscribe(<<"topic">>).

t_subopts(_) ->
    ?assertEqual(false, emqx_broker:set_subopts(<<"topic">>, #{qos => 1})),
    ?assertEqual(undefined, emqx_broker:get_subopts(self(), <<"topic">>)),
    ?assertEqual(undefined, emqx_broker:get_subopts(<<"clientid">>, <<"topic">>)),
    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{qos => 1}),
    timer:sleep(200),
    ?assertEqual(#{nl => 0, qos => 1, rap => 0, rh => 0, subid => <<"clientid">>},
                 emqx_broker:get_subopts(self(), <<"topic">>)),
    ?assertEqual(#{nl => 0, qos => 1, rap => 0, rh => 0, subid => <<"clientid">>},
                 emqx_broker:get_subopts(<<"clientid">>,<<"topic">>)),

    emqx_broker:subscribe(<<"topic">>, <<"clientid">>, #{qos => 2}),
    ?assertEqual(#{nl => 0, qos => 2, rap => 0, rh => 0, subid => <<"clientid">>},
                 emqx_broker:get_subopts(self(), <<"topic">>)),

    ?assertEqual(true, emqx_broker:set_subopts(<<"topic">>, #{qos => 0})),
    ?assertEqual(#{nl => 0, qos => 0, rap => 0, rh => 0, subid => <<"clientid">>},
                 emqx_broker:get_subopts(self(), <<"topic">>)),
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
    ?assertEqual(#{nl => 0, qos => 1, rap => 0, rh => 0, subid => <<"clientid">>},
                 proplists:get_value(<<"topic">>, emqx_broker:subscriptions(self()))),
    ?assertEqual(#{nl => 0, qos => 1, rap => 0, rh => 0, subid => <<"clientid">>},
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

t_worker_kill(_)->
    TargetBroker = hd(supervisor:which_children(
                  element(2,lists:keyfind(pool_sup, 1,
                                          supervisor:which_children(emqx_broker_sup))))),
    TargetBrokerPid = element(2, TargetBroker),
    #{worker := Worker, worker_ref := Ref} = sys:get_state(TargetBrokerPid),
    true = erlang:is_process_alive(Worker),
    spawn(fun() -> link(Worker), exit(abnormal) end),
    ok = wait_die(Worker),
    #{worker := NewWorker, worker_ref := NewRef} = sys:get_state(TargetBrokerPid),
    ?assertNotEqual(NewWorker, Worker),
    ?assertNotEqual(NewRef, Ref),
    ok.

t_worker_exit_work(_)->
    %% must be wildcard to trigger
    Topic = <<  "topic/#" >>,
    TargetBrokerPid = gproc_pool:pick_worker(broker_pool, Topic),
    #{worker := Worker, worker_ref := Ref} = sys:get_state(TargetBrokerPid),
    true = erlang:is_process_alive(Worker),
    ct:pal("~p", [erlang:process_info(Worker)]),
    ok = meck:new(mnesia, [passthrough,no_history]),
    ok = meck:expect(mnesia, transaction, fun(_,_) -> meck:exception(error, boom) end),
    emqx_broker:subscribe(Topic, <<"clientid">>),
    ok = wait_die(Worker),
    #{worker := NewWorker, worker_ref := NewRef} = sys:get_state(TargetBrokerPid),
    ?assertNotEqual(Worker, NewWorker),
    ?assertNotEqual(Ref, NewRef),
    ok = meck:unload(mnesia),
    ok.

wait_die(Worker) ->
    case erlang:is_process_alive(Worker) of
        true ->
            timer:sleep(10),
            wait_die(Worker);
        false ->
            ok
    end.
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
