%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() ->
    [{group, pubsub},
     {group, metrics},
     {group, stats}].

groups() ->
    [{pubsub, [sequence],
      [t_sub_unsub,
       t_publish,
       t_pubsub,
       t_shared_subscribe,
       t_dispatch_with_no_sub,
       't_pubsub#',
       't_pubsub+'
      ]},
     {metrics, [sequence],
      [inc_dec_metric]},
     {stats, [sequence],
      [set_get_stat]
     }].

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules([router, broker]),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% PubSub Test
%%--------------------------------------------------------------------

t_sub_unsub(_) ->
    ok = emqx_broker:subscribe(<<"topic">>, <<"clientId">>),
    ok = emqx_broker:subscribe(<<"topic/1">>, <<"clientId">>, #{qos => 1}),
    ok = emqx_broker:subscribe(<<"topic/2">>, <<"clientId">>, #{qos => 2}),
    true = emqx_broker:subscribed(<<"clientId">>, <<"topic">>),
    Topics = emqx_broker:topics(),
    lists:foreach(fun(Topic) ->
                      ?assert(lists:member(Topic, Topics))
                  end, Topics),
    ok = emqx_broker:unsubscribe(<<"topic">>),
    ok = emqx_broker:unsubscribe(<<"topic/1">>),
    ok = emqx_broker:unsubscribe(<<"topic/2">>).

t_publish(_) ->
    Msg = emqx_message:make(ct, <<"test/pubsub">>, <<"hello">>),
    ok = emqx_broker:subscribe(<<"test/+">>),
    timer:sleep(10),
    emqx_broker:publish(Msg),
    ?assert(receive {deliver, <<"test/+">>, #message{payload = <<"hello">>}} -> true after 100 -> false end).

t_dispatch_with_no_sub(_) ->
    Msg = emqx_message:make(ct, <<"no_subscribers">>, <<"hello">>),
    Delivery = #delivery{sender = self(), message = Msg},
    ?assertEqual([{node(),<<"no_subscribers">>,{error,no_subscribers}}],
                 emqx_broker:route([{<<"no_subscribers">>, node()}], Delivery)).

t_pubsub(_) ->
    true = emqx:is_running(node()),
    Self = self(),
    Subscriber = <<"clientId">>,
    ok = emqx_broker:subscribe(<<"a/b/c">>, Subscriber, #{ qos => 1 }),
    #{qos := 1} = ets:lookup_element(emqx_suboption, {Self, <<"a/b/c">>}, 2),
    #{qos := 1} = emqx_broker:get_subopts(Subscriber, <<"a/b/c">>),
    true = emqx_broker:set_subopts(<<"a/b/c">>, #{qos => 0}),
    #{qos := 0} = emqx_broker:get_subopts(Subscriber, <<"a/b/c">>),
    ok = emqx_broker:subscribe(<<"a/b/c">>, Subscriber, #{ qos => 2 }),
    %% ct:log("Emq Sub: ~p.~n", [ets:lookup(emqx_suboption, {<<"a/b/c">>, Subscriber})]),
    timer:sleep(10),
    [Self] = emqx_broker:subscribers(<<"a/b/c">>),
    emqx_broker:publish(
      emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(
        receive {deliver, <<"a/b/c">>, _ } ->
                true;
            P ->
                ct:log("Receive Message: ~p~n",[P])
        after 100 ->
            false
        end),
    spawn(fun() ->
            emqx_broker:subscribe(<<"a/b/c">>),
            emqx_broker:subscribe(<<"c/d/e">>),
            timer:sleep(10),
            emqx_broker:unsubscribe(<<"a/b/c">>)
          end),
    timer:sleep(20),
    emqx_broker:unsubscribe(<<"a/b/c">>).

t_shared_subscribe(_) ->
    emqx_broker:subscribe(<<"$share/group2/topic2">>),
    emqx_broker:subscribe(<<"$queue/topic3">>),
    timer:sleep(10),
    ct:pal("Share subscriptions: ~p",
           [emqx_broker:subscriptions(self())]),
    ?assertEqual(2, length(emqx_broker:subscriptions(self()))),
    emqx_broker:unsubscribe(<<"$share/group2/topic2">>),
    emqx_broker:unsubscribe(<<"$queue/topic3">>),
    ?assertEqual(0, length(emqx_broker:subscriptions(self()))).

't_pubsub#'(_) ->
    emqx_broker:subscribe(<<"a/#">>),
    timer:sleep(10),
    emqx_broker:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {deliver, <<"a/#">>, _} -> true after 100 -> false end),
    emqx_broker:unsubscribe(<<"a/#">>).

't_pubsub+'(_) ->
    emqx_broker:subscribe(<<"a/+/+">>),
    timer:sleep(10), %% TODO: why sleep?
    emqx_broker:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {deliver, <<"a/+/+">>, _} -> true after 100 -> false end),
    emqx_broker:unsubscribe(<<"a/+/+">>).

%%--------------------------------------------------------------------
%% Metric Group
%%--------------------------------------------------------------------

inc_dec_metric(_) ->
    emqx_metrics:inc('messages.retained', 10),
    emqx_metrics:dec('messages.retained', 10).

%%--------------------------------------------------------------------
%% Stats Group
%%--------------------------------------------------------------------

set_get_stat(_) ->
    emqx_stats:setstat('retained.max', 99),
    ?assertEqual(99, emqx_stats:getstat('retained.max')).

