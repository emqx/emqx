%%--------------------------------------------------------------------
%% Copyright (c) 2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-include("emqx.hrl").

all() ->
    [
     {group, pubsub},
     {group, session},
     {group, broker},
     {group, metrics},
     {group, stats},
     {group, hook},
     {group, alarms}].

groups() ->
    [
     {pubsub, [sequence], [subscribe_unsubscribe,
                           publish, pubsub,
                           t_local_subscribe,
                           t_shared_subscribe,
                           'pubsub#', 'pubsub+']},
     {session, [sequence], [start_session]},
     {broker, [sequence], [hook_unhook]},
     {metrics, [sequence], [inc_dec_metric]},
     {stats, [sequence], [set_get_stat]},
     {hook, [sequence], [add_delete_hook, run_hooks]},
     {alarms, [sequence], [set_alarms]}
    ].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().
    
%%--------------------------------------------------------------------
%% PubSub Test
%%--------------------------------------------------------------------

subscribe_unsubscribe(_) ->
    ok = emqx:subscribe(<<"topic">>, <<"clientId">>),
    ok = emqx:subscribe(<<"topic/1">>, <<"clientId">>, [{qos, 1}]),
    ok = emqx:subscribe(<<"topic/2">>, <<"clientId">>, [{qos, 2}]),
    ok = emqx:unsubscribe(<<"topic">>, <<"clientId">>),
    ok = emqx:unsubscribe(<<"topic/1">>, <<"clientId">>),
    ok = emqx:unsubscribe(<<"topic/2">>, <<"clientId">>).

publish(_) ->
    Msg = emqx_message:make(ct, <<"test/pubsub">>, <<"hello">>),
    ok = emqx:subscribe(<<"test/+">>),
    timer:sleep(10),
    emqx:publish(Msg),
    ?assert(receive {dispatch, <<"test/+">>, Msg} -> true after 5 -> false end).

pubsub(_) ->
    Self = self(),
    ok = emqx:subscribe(<<"a/b/c">>, Self, [{qos, 1}]),
    ?assertMatch({error, _}, emqx:subscribe(<<"a/b/c">>, Self, [{qos, 2}])),
    timer:sleep(10),
    [{Self, <<"a/b/c">>}] = ets:lookup(mqtt_subscription, Self),
    [{<<"a/b/c">>, Self}] = ets:lookup(mqtt_subscriber, <<"a/b/c">>),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/b/c">>, _} -> true after 2 -> false end),
    spawn(fun() ->
            emqx:subscribe(<<"a/b/c">>),
            emqx:subscribe(<<"c/d/e">>),
            timer:sleep(10),
            emqx:unsubscribe(<<"a/b/c">>)
          end),
    timer:sleep(20),
    emqx:unsubscribe(<<"a/b/c">>).

t_local_subscribe(_) ->
    ok = emqx:subscribe("$local/topic0"),
    ok = emqx:subscribe("$local/topic1", <<"x">>),
    ok = emqx:subscribe("$local/topic2", <<"x">>, [{qos, 2}]),
    timer:sleep(10),
    ?assertEqual([self()], emqx:subscribers("$local/topic0")),
    ?assertEqual([{<<"x">>, self()}], emqx:subscribers("$local/topic1")),
    ?assertEqual([{{<<"x">>, self()}, <<"$local/topic1">>, []},
                  {{<<"x">>, self()}, <<"$local/topic2">>, [{qos,2}]}],
                 emqx:subscriptions(<<"x">>)),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic0")),
    ?assertMatch({error, {subscription_not_found, _}}, emqx:unsubscribe("$local/topic0")),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic1", <<"x">>)),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic2", <<"x">>)),
    ?assertEqual([], emqx:subscribers("topic1")),
    ?assertEqual([], emqx:subscriptions(<<"x">>)).

t_shared_subscribe(_) ->
    emqx:subscribe("$local/$share/group1/topic1"),
    emqx:subscribe("$share/group2/topic2"),
    emqx:subscribe("$queue/topic3"),
    timer:sleep(10),
    ?assertEqual([self()], emqx:subscribers(<<"$local/$share/group1/topic1">>)),
    ?assertEqual([{self(), <<"$local/$share/group1/topic1">>, []},
                  {self(), <<"$queue/topic3">>, []},
                  {self(), <<"$share/group2/topic2">>, []}],
                 lists:sort(emqx:subscriptions(self()))),
    emqx:unsubscribe("$local/$share/group1/topic1"),
    emqx:unsubscribe("$share/group2/topic2"),
    emqx:unsubscribe("$queue/topic3"),
    ?assertEqual([], lists:sort(emqx:subscriptions(self()))).

'pubsub#'(_) ->
    emqx:subscribe(<<"a/#">>),
    timer:sleep(10),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/#">>, _} -> true after 2 -> false end),
    emqx:unsubscribe(<<"a/#">>).

'pubsub+'(_) ->
    emqx:subscribe(<<"a/+/+">>),
    timer:sleep(10),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/+/+">>, _} -> true after 1 -> false end),
    emqx:unsubscribe(<<"a/+/+">>).

%%--------------------------------------------------------------------
%% Session Group
%%--------------------------------------------------------------------
start_session(_) ->
    {ok, ClientPid} = emqx_mock_client:start_link(<<"clientId">>),
    {ok, SessPid} = emqx_mock_client:start_session(ClientPid),
    Message = emqx_message:make(<<"clientId">>, 2, <<"topic">>, <<"hello">>),
    Message1 = Message#mqtt_message{pktid = 1},
    emqx_session:publish(SessPid, Message1),
    emqx_session:pubrel(SessPid, 1),
    emqx_session:subscribe(SessPid, [{<<"topic/session">>, [{qos, 2}]}]),
    Message2 = emqx_message:make(<<"clientId">>, 1, <<"topic/session">>, <<"test">>),
    emqx_session:publish(SessPid, Message2),
    emqx_session:unsubscribe(SessPid, [{<<"topic/session">>, []}]),
    emqx_mock_client:stop(ClientPid).

%%--------------------------------------------------------------------
%% Broker Group
%%--------------------------------------------------------------------
hook_unhook(_) ->
    ok.

%%--------------------------------------------------------------------
%% Metric Group
%%--------------------------------------------------------------------
inc_dec_metric(_) ->
    emqx_metrics:inc(gauge, 'messages/retained', 10),
    emqx_metrics:dec(gauge, 'messages/retained', 10).

%%--------------------------------------------------------------------
%% Stats Group
%%--------------------------------------------------------------------
set_get_stat(_) ->
    emqx_stats:setstat('retained/max', 99),
    99 = emqx_stats:getstat('retained/max').

%%--------------------------------------------------------------------
%% Hook Test
%%--------------------------------------------------------------------

add_delete_hook(_) ->
    ok = emqx:hook(test_hook, fun ?MODULE:hook_fun1/1, []),
    ok = emqx:hook(test_hook, {tag, fun ?MODULE:hook_fun2/1}, []),
    {error, already_hooked} = emqx:hook(test_hook, {tag, fun ?MODULE:hook_fun2/1}, []),
    Callbacks = [{callback, undefined, fun ?MODULE:hook_fun1/1, [], 0},
                 {callback, tag, fun ?MODULE:hook_fun2/1, [], 0}],
    Callbacks = emqx_hooks:lookup(test_hook),
    ok = emqx:unhook(test_hook, fun ?MODULE:hook_fun1/1),
    ct:print("Callbacks: ~p~n", [emqx_hooks:lookup(test_hook)]),
    ok = emqx:unhook(test_hook, {tag, fun ?MODULE:hook_fun2/1}),
    {error, not_found} = emqx:unhook(test_hook1, {tag, fun ?MODULE:hook_fun2/1}),
    [] = emqx_hooks:lookup(test_hook),

    ok = emqx:hook(emqx_hook, fun ?MODULE:hook_fun1/1, [], 9),
    ok = emqx:hook(emqx_hook, {"tag", fun ?MODULE:hook_fun2/1}, [], 8),
    Callbacks2 = [{callback, "tag", fun ?MODULE:hook_fun2/1, [], 8},
                  {callback, undefined, fun ?MODULE:hook_fun1/1, [], 9}],
    Callbacks2 = emqx_hooks:lookup(emqx_hook),
    ok = emqx:unhook(emqx_hook, fun ?MODULE:hook_fun1/1),
    ok = emqx:unhook(emqx_hook, {"tag", fun ?MODULE:hook_fun2/1}),
    [] = emqx_hooks:lookup(emqx_hook).

run_hooks(_) ->
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun3/4, [init]),
    ok = emqx:hook(foldl_hook, {tag, fun ?MODULE:hook_fun3/4}, [init]),
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun4/4, [init]),
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun5/4, [init]),
    {stop, [r3, r2]} = emqx:run_hooks(foldl_hook, [arg1, arg2], []),
    {ok, []} = emqx:run_hooks(unknown_hook, [], []),

    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun6/2, [initArg]),
    ok = emqx:hook(foreach_hook, {tag, fun ?MODULE:hook_fun6/2}, [initArg]),
    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun7/2, [initArg]),
    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun8/2, [initArg]),
    stop = emqx:run_hooks(foreach_hook, [arg]).

hook_fun1([]) -> ok.
hook_fun2([]) -> {ok, []}.

hook_fun3(arg1, arg2, _Acc, init) -> ok.
hook_fun4(arg1, arg2, Acc, init)  -> {ok, [r2 | Acc]}.
hook_fun5(arg1, arg2, Acc, init)  -> {stop, [r3 | Acc]}.

hook_fun6(arg, initArg) -> ok.
hook_fun7(arg, initArg) -> any.
hook_fun8(arg, initArg) -> stop.

set_alarms(_) ->
    AlarmTest = #mqtt_alarm{id = <<"1">>, severity = error, title="alarm title", summary="alarm summary"},
    emqx_alarm:set_alarm(AlarmTest),
    Alarms = emqx_alarm:get_alarms(),
    ?assertEqual(1, length(Alarms)),
    emqx_alarm:clear_alarm(<<"1">>),
    [] = emqx_alarm:get_alarms().


