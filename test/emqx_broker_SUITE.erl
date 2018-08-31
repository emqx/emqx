%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

%%--------------------------------------------------------------------
%% PubSub Test
%%--------------------------------------------------------------------

subscribe_unsubscribe(_) ->
    ok = emqx:subscribe(<<"topic">>, <<"clientId">>),
    ok = emqx:subscribe(<<"topic/1">>, <<"clientId">>, #{ qos => 1 }),
    ok = emqx:subscribe(<<"topic/2">>, <<"clientId">>, #{ qos => 2 }),
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
    Subscriber = {Self, <<"clientId">>},
    ok = emqx:subscribe(<<"a/b/c">>, <<"clientId">>, #{ qos => 1 }),
    #{ qos := 1} = ets:lookup_element(emqx_suboption, {<<"a/b/c">>, Subscriber}, 2),
    ok = emqx:subscribe(<<"a/b/c">>, <<"clientId">>, #{ qos => 2 }),
    #{ qos := 2} = ets:lookup_element(emqx_suboption, {<<"a/b/c">>, Subscriber}, 2),
    %% ct:log("Emq Sub: ~p.~n", [ets:lookup(emqx_suboption, {<<"a/b/c">>, Subscriber})]),
    timer:sleep(10),
    [{<<"a/b/c">>, #{qos := 2}}] = emqx_broker:subscriptions(Subscriber),
    [{Self, <<"clientId">>}] = emqx_broker:subscribers(<<"a/b/c">>),
    emqx:publish(emqx_message:make(ct, <<"a/b/c">>, <<"hello">>)),
    ?assert(receive {dispatch, <<"a/b/c">>, _ } -> true; P -> ct:log("Receive Message: ~p~n",[P]) after 2 -> false end),
    spawn(fun() ->
            emqx:subscribe(<<"a/b/c">>),
            emqx:subscribe(<<"c/d/e">>),
            timer:sleep(10),
            emqx:unsubscribe(<<"a/b/c">>)
          end),
    timer:sleep(20),
    emqx:unsubscribe(<<"a/b/c">>).

t_local_subscribe(_) ->
    ok = emqx:subscribe(<<"$local/topic0">>),
    ok = emqx:subscribe(<<"$local/topic1">>, <<"clientId">>),
    ok = emqx:subscribe(<<"$local/topic2">>, <<"clientId">>, #{ qos => 2 }),
    timer:sleep(10),
    ?assertEqual([{self(), undefined}], emqx:subscribers("$local/topic0")),
    ?assertEqual([{self(), <<"clientId">>}], emqx:subscribers("$local/topic1")),
    ?assertEqual([{<<"$local/topic1">>, #{}},
                  {<<"$local/topic2">>, #{ qos => 2 }}],
                 emqx:subscriptions({self(), <<"clientId">>})),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic0")),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic0")),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic1", <<"clientId">>)),
    ?assertEqual(ok, emqx:unsubscribe("$local/topic2", <<"clientId">>)),
    ?assertEqual([], emqx:subscribers("topic1")),
    ?assertEqual([], emqx:subscriptions({self(), <<"clientId">>})).

t_shared_subscribe(_) ->
    emqx:subscribe("$local/$share/group1/topic1"),
    emqx:subscribe("$share/group2/topic2"),
    emqx:subscribe("$queue/topic3"),
    timer:sleep(10),
    ct:log("share subscriptions: ~p~n", [emqx:subscriptions({self(), undefined})]),
    ?assertEqual([{self(), undefined}], emqx:subscribers(<<"$local/$share/group1/topic1">>)),
    ?assertEqual([{<<"$local/$share/group1/topic1">>, #{}},
                  {<<"$queue/topic3">>, #{}},
                  {<<"$share/group2/topic2">>, #{}}],
                 lists:sort(emqx:subscriptions({self(), undefined}))),
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
    emqx_mock_client:close_session(ClientPid, SessPid).

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
    AlarmTest = #alarm{id = <<"1">>, severity = error, title="alarm title", summary="alarm summary"},
    emqx_alarm_mgr:set_alarm(AlarmTest),
    Alarms = emqx_alarm_mgr:get_alarms(),
    ct:log("Alarms Length: ~p ~n", [length(Alarms)]),
    ?assertEqual(1, length(Alarms)),
    emqx_alarm_mgr:clear_alarm(<<"1">>),
    [] = emqx_alarm_mgr:get_alarms().

