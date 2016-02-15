%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_SUITE).

-compile(export_all).

-include("emqttd.hrl").

all() ->
    [{group, pubsub},
     {group, router},
     {group, session},
     {group, retainer},
     {group, broker},
     {group, metrics},
     {group, stats}].

groups() ->
    [{pubsub, [sequence],
      [create_topic,
       create_subscription,
       subscribe_unsubscribe,
       publish_message]},
     {router, [sequence],
      [add_delete_routes,
       add_delete_route,
       route_message]},
     {session, [sequence],
      [start_session]},
     {retainer, [sequence],
      [retain_message]},
     {broker, [sequence],
      [hook_unhook]},
     {metrics, [sequence],
      [inc_dec_metric]},
     {stats, [sequence],
      [set_get_stat]}].

init_per_suite(Config) ->
    application:start(lager),
    application:ensure_all_started(emqttd),
    Config.

end_per_suite(_Config) ->
    application:stop(emqttd),
    application:stop(esockd),
    application:stop(gproc),
    emqttd_mnesia:ensure_stopped().

%%--------------------------------------------------------------------
%% PubSub Group
%%--------------------------------------------------------------------

create_topic(_) ->
    Node = node(),
    ok = emqttd_pubsub:create(topic, <<"topic/create">>),
    ok = emqttd_pubsub:create(topic, <<"topic/create2">>),
    [#mqtt_topic{topic = <<"topic/create">>, node  = Node}]
        = emqttd_pubsub:lookup(topic, <<"topic/create">>).

create_subscription(_) ->
    ok = emqttd_pubsub:create(subscription, {<<"clientId">>, <<"topic/sub">>, qos2}),
    [#mqtt_subscription{subid = <<"clientId">>, topic = <<"topic/sub">>, qos = 2}]
        = emqttd_pubsub:lookup(subscription, <<"clientId">>),
    ok = emqttd_pubsub:delete(subscription, <<"clientId">>),
    [] = emqttd_pubsub:lookup(subscription, <<"clientId">>).

subscribe_unsubscribe(_) ->
    {ok, [1]} = emqttd_pubsub:subscribe({<<"topic/subunsub">>, 1}),
    {ok, [1, 2]} = emqttd_pubsub:subscribe([{<<"topic/subunsub1">>, 1}, {<<"topic/subunsub2">>, 2}]),
    ok = emqttd_pubsub:unsubscribe(<<"topic/subunsub">>),
    ok = emqttd_pubsub:unsubscribe([<<"topic/subunsub1">>, <<"topic/subunsub2">>]),

    {ok, [1]} = emqttd_pubsub:subscribe(<<"client_subunsub">>, {<<"topic/subunsub">>, 1}),
    {ok, [1,2]} = emqttd_pubsub:subscribe(<<"client_subunsub">>, [{<<"topic/subunsub1">>, 1},
                                                         {<<"topic/subunsub2">>, 2}]),
    ok = emqttd_pubsub:unsubscribe(<<"client_subunsub">>, <<"topic/subunsub">>),
    ok = emqttd_pubsub:unsubscribe(<<"client_subunsub">>, [<<"topic/subunsub1">>,
                                                           <<"topic/subunsub2">>]).

publish_message(_) ->
    Msg = emqttd_message:make(ct, <<"test/pubsub">>, <<"hello">>),
    {ok, [1]} = emqttd_pubsub:subscribe({<<"test/+">>, qos1}),
    emqttd_pubsub:publish(Msg),
    true = receive {dispatch, <<"test/+">>, Msg} -> true after 5 -> false end.

%%--------------------------------------------------------------------
%% Route Group
%%--------------------------------------------------------------------

add_delete_route(_) ->
    Self = self(),
    emqttd_router:add_route(<<"topic1">>, Self),
    true = emqttd_router:has_route(<<"topic1">>),
    emqttd_router:add_route(<<"topic2">>, Self),
    true = emqttd_router:has_route(<<"topic2">>),
    [Self] = emqttd_router:lookup_routes(<<"topic1">>),
    [Self] = emqttd_router:lookup_routes(<<"topic2">>),
    %% Del topic1
    emqttd_router:delete_route(<<"topic1">>, Self),
    erlang:yield(),
    timer:sleep(10),
    false = emqttd_router:has_route(<<"topic1">>),
    %% Del topic2
    emqttd_router:delete_route(<<"topic2">>, Self),
    erlang:yield(),
    timer:sleep(10),
    false = emqttd_router:has_route(<<"topic2">>).

add_delete_routes(_) ->
    Self = self(),
    emqttd_router:add_routes([], Self),
    emqttd_router:add_routes([<<"t0">>], Self),
    emqttd_router:add_routes([<<"t1">>,<<"t2">>,<<"t3">>], Self),
    true = emqttd_router:has_route(<<"t1">>),
    [Self] = emqttd_router:lookup_routes(<<"t1">>),
    [Self] = emqttd_router:lookup_routes(<<"t2">>),
    [Self] = emqttd_router:lookup_routes(<<"t3">>),

    emqttd_router:delete_routes([<<"t3">>], Self),
    emqttd_router:delete_routes([<<"t0">>, <<"t1">>], Self),
    erlang:yield(),
    timer:sleep(10),
    false = emqttd_router:has_route(<<"t0">>),
    false = emqttd_router:has_route(<<"t1">>),
    true = emqttd_router:has_route(<<"t2">>),
    false = emqttd_router:has_route(<<"t3">>).

route_message(_) ->
    Self = self(),
    Pid = spawn_link(fun() -> timer:sleep(1000) end),
    emqttd_router:add_routes([<<"$Q/1">>,<<"t/2">>,<<"t/3">>], Self),
    emqttd_router:add_routes([<<"t/2">>], Pid),
    Msg1 = #mqtt_message{topic = <<"$Q/1">>, payload = <<"q">>},
    Msg2 = #mqtt_message{topic = <<"t/2">>, payload = <<"t2">>},
    Msg3 = #mqtt_message{topic = <<"t/3">>, payload = <<"t3">>},
    emqttd_router:route(<<"$Q/1">>, Msg1),
    emqttd_router:route(<<"t/2">>, Msg2),
    emqttd_router:route(<<"t/3">>, Msg3),
    [Msg1, Msg2, Msg3] = recv_loop([]),
    emqttd_router:add_route(<<"$Q/1">>, Self),
    emqttd_router:route(<<"$Q/1">>, Msg1).

recv_loop(Msgs) ->
    receive
        {dispatch, _Topic, Msg} ->
            recv_loop([Msg|Msgs])
        after
            100 -> lists:reverse(Msgs)
    end.

%%--------------------------------------------------------------------
%% Session Group
%%--------------------------------------------------------------------

start_session(_) ->
    {ok, ClientPid} = emqttd_mock_client:start_link(<<"clientId">>),
    {ok, SessPid} = emqttd_mock_client:start_session(ClientPid),
    Message = emqttd_message:make(<<"clientId">>, 2, <<"topic">>, <<"hello">>),
    Message1 = Message#mqtt_message{pktid = 1},
    emqttd_session:publish(SessPid, Message1),
    emqttd_session:pubrel(SessPid, 1),
    emqttd_session:subscribe(SessPid, [{<<"topic/session">>, 2}]),
    Message2 = emqttd_message:make(<<"clientId">>, 1, <<"topic/session">>, <<"test">>),
    emqttd_session:publish(SessPid, Message2),
    emqttd_session:unsubscribe(SessPid, [<<"topic/session">>]),
    emqttd_mock_client:stop(ClientPid).

%%--------------------------------------------------------------------
%% Retainer Group
%%--------------------------------------------------------------------

retain_message(_) ->
    Msg = #mqtt_message{retain = true, topic = <<"a/b/c">>,
                        payload = <<"payload">>},
    emqttd_retainer:retain(Msg),
    emqttd_retainer:dispatch(<<"a/b/+">>, self()),
    true = receive {dispatch, <<"a/b/+">>, Msg} -> true after 10 -> false end,
    emqttd_retainer:retain(#mqtt_message{retain = true, topic = <<"a/b/c">>, payload = <<>>}),
    [] = mnesia:dirty_read({retained, <<"a/b/c">>}).

%%--------------------------------------------------------------------
%% Broker Group
%%--------------------------------------------------------------------
hook_unhook(_) ->
    ok.

%%--------------------------------------------------------------------
%% Metric Group
%%--------------------------------------------------------------------
inc_dec_metric(_) ->
    emqttd_metrics:inc(gauge, 'messages/retained', 10),
    emqttd_metrics:dec(gauge, 'messages/retained', 10).

%%--------------------------------------------------------------------
%% Stats Group
%%--------------------------------------------------------------------
set_get_stat(_) ->
    emqttd_stats:setstat('retained/max', 99),
    99 = emqttd_stats:getstat('retained/max').
