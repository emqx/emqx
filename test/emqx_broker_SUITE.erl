%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
     {group, session},
     {group, metrics},
     {group, stats}].

groups() ->
    [{pubsub, [sequence], [subscribe_unsubscribe,
                           publish, pubsub,
                           t_shared_subscribe,
                           dispatch_with_no_sub,
                           'pubsub#', 'pubsub+']},
     {session, [sequence], [start_session]},
     {metrics, [sequence], [inc_dec_metric]},
     {stats, [sequence], [set_get_stat]}].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% PubSub Test
%%--------------------------------------------------------------------

subscribe_unsubscribe(_) ->
    ok = emqx:subscribe(<<"topic">>, <<"clientId">>),
    ok = emqx:subscribe(<<"topic/1">>, <<"clientId">>, #{ qos => 1 }),
    ok = emqx:subscribe(<<"topic/2">>, <<"clientId">>, #{ qos => 2 }),
    true = emqx:subscribed(<<"clientId">>, <<"topic">>),
    Topics = emqx:topics(),
    lists:foreach(fun(Topic) ->
                      ?assert(lists:member(Topic, Topics))
                  end, Topics),
    ok = emqx:unsubscribe(<<"topic">>),
    ok = emqx:unsubscribe(<<"topic/1">>),
    ok = emqx:unsubscribe(<<"topic/2">>).

publish(_) ->
    Msg = emqx_message:make(ct, <<"test/pubsub">>, <<"hello">>),
    ok = emqx:subscribe(<<"test/+">>),
    timer:sleep(10),
    emqx:publish(Msg),
    ?assert(receive {dispatch, <<"test/+">>, #message{payload = <<"hello">>}} -> true after 100 -> false end).

dispatch_with_no_sub(_) ->
    Msg = emqx_message:make(ct, <<"no_subscribers">>, <<"hello">>),
    Delivery = #delivery{sender = self(), message = Msg, results = []},
    ?assertEqual(Delivery, emqx_broker:route([{<<"no_subscribers">>, node()}], Delivery)).

pubsub(_) ->
    true = emqx:is_running(node()),
    Self = self(),
    Subscriber = <<"clientId">>,
    ok = emqx:subscribe(<<"a/b/c">>, Subscriber, #{ qos => 1 }),
    #{qos := 1} = ets:lookup_element(emqx_suboption, {Self, <<"a/b/c">>}, 2),
    #{qos := 1} = emqx_broker:get_subopts(Subscriber, <<"a/b/c">>),
    true = emqx_broker:set_subopts(<<"a/b/c">>, #{qos => 0}),
    #{qos := 0} = emqx_broker:get_subopts(Subscriber, <<"a/b/c">>),
    ok = emqx:subscribe(<<"a/b/c">>, Subscriber, #{ qos => 2 }),
    %% ct:log("Emq Sub: ~p.~n", [ets:lookup(emqx_suboption, {<<"a/b/c">>, Subscriber})]),
    timer:sleep(10),
    [Self] = emqx_broker:subscribers(<<"a/b/c">>),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(
        receive {dispatch, <<"a/b/c">>, _ } ->
                true;
            P ->
                ct:log("Receive Message: ~p~n",[P])
        after 100 ->
            false
        end),
    spawn(fun() ->
            emqx:subscribe(<<"a/b/c">>),
            emqx:subscribe(<<"c/d/e">>),
            timer:sleep(10),
            emqx:unsubscribe(<<"a/b/c">>)
          end),
    timer:sleep(20),
    emqx:unsubscribe(<<"a/b/c">>).

t_shared_subscribe(_) ->
    emqx:subscribe("$share/group2/topic2"),
    emqx:subscribe("$queue/topic3"),
    timer:sleep(10),
    ct:log("share subscriptions: ~p~n", [emqx:subscriptions(self())]),
    ?assertEqual(2, length(emqx:subscriptions(self()))),
    emqx:unsubscribe("$share/group2/topic2"),
    emqx:unsubscribe("$queue/topic3"),
    ?assertEqual(0, length(emqx:subscriptions(self()))).

'pubsub#'(_) ->
    emqx:subscribe(<<"a/#">>),
    timer:sleep(10),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/#">>, _} -> true after 100 -> false end),
    emqx:unsubscribe(<<"a/#">>).

'pubsub+'(_) ->
    emqx:subscribe(<<"a/+/+">>),
    timer:sleep(10),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/+/+">>, _} -> true after 100 -> false end),
    emqx:unsubscribe(<<"a/+/+">>).

%%--------------------------------------------------------------------
%% Session Group
%%--------------------------------------------------------------------
start_session(_) ->
    ClientId = <<"clientId">>,
    {ok, ClientPid} = emqx_mock_client:start_link(ClientId),
    {ok, SessPid} = emqx_mock_client:open_session(ClientPid, ClientId, internal),
    Message1 = emqx_message:make(<<"clientId">>, 2, <<"topic">>, <<"hello">>),
    emqx_session:publish(SessPid, 1, Message1),
    emqx_session:pubrel(SessPid, 2, reasoncode),
    emqx_session:subscribe(SessPid, [{<<"topic/session">>, #{qos => 2}}]),
    Message2 = emqx_message:make(<<"clientId">>, 1, <<"topic/session">>, <<"test">>),
    emqx_session:publish(SessPid, 3, Message2),
    emqx_session:unsubscribe(SessPid, [{<<"topic/session">>, []}]),
    %% emqx_mock_client:stop(ClientPid).
    emqx_mock_client:close_session(ClientPid).

%%--------------------------------------------------------------------
%% Broker Group
%%--------------------------------------------------------------------

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
    99 = emqx_stats:getstat('retained.max').
