%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

t_emqx_pubsub_api(_) ->
    true = emqx:is_running(node()),
    {ok, C} = emqtt:start_link([{host, "localhost"}, {clientid, "myclient"}]),
    {ok, _} = emqtt:connect(C),
    ClientId = <<"myclient">>,
    Payload = <<"Hello World">>,
    Topic = <<"mytopic">>,
    Topic1 = <<"mytopic1">>,
    Topic2 = <<"mytopic2">>,
    Topic3 = <<"mytopic3">>,
    emqx:subscribe(Topic, ClientId),
    emqx:subscribe(Topic1, ClientId, #{qos => 1}),
    emqx:subscribe(Topic2, ClientId, #{qos => 2}),
    ct:sleep(100),
    ?assertEqual([Topic2, Topic1, Topic], emqx:topics()),
    ?assertEqual([self()], emqx:subscribers(Topic)),
    ?assertEqual([self()], emqx:subscribers(Topic1)),
    ?assertEqual([self()], emqx:subscribers(Topic2)),

    ?assertEqual(
        [
            {Topic, #{nl => 0, qos => 0, rap => 0, rh => 0, subid => ClientId}},
            {Topic1, #{nl => 0, qos => 1, rap => 0, rh => 0, subid => ClientId}},
            {Topic2, #{nl => 0, qos => 2, rap => 0, rh => 0, subid => ClientId}}
        ],
        emqx:subscriptions(self())
    ),
    ?assertEqual(true, emqx:subscribed(self(), Topic)),
    ?assertEqual(true, emqx:subscribed(ClientId, Topic)),
    ?assertEqual(true, emqx:subscribed(self(), Topic1)),
    ?assertEqual(true, emqx:subscribed(ClientId, Topic1)),
    ?assertEqual(true, emqx:subscribed(self(), Topic2)),
    ?assertEqual(true, emqx:subscribed(ClientId, Topic2)),
    ?assertEqual(false, emqx:subscribed(self(), Topic3)),
    ?assertEqual(false, emqx:subscribed(ClientId, Topic3)),
    emqx:publish(emqx_message:make(Topic, Payload)),
    receive
        {deliver, Topic, #message{payload = Payload}} ->
            ok
    after 100 ->
        ct:fail("no_message")
    end,
    emqx:publish(emqx_message:make(Topic1, Payload)),
    receive
        {deliver, Topic1, #message{payload = Payload}} ->
            ok
    after 100 ->
        ct:fail("no_message")
    end,
    emqx:publish(emqx_message:make(Topic2, Payload)),
    receive
        {deliver, Topic2, #message{payload = Payload}} ->
            ok
    after 100 ->
        ct:fail("no_message")
    end,
    emqx:unsubscribe(Topic),
    emqx:unsubscribe(Topic1),
    emqx:unsubscribe(Topic2),
    ct:sleep(20),
    ?assertEqual([], emqx:topics()).

t_hook_unhook(_) ->
    ok = emqx_hooks:add(test_hook, {?MODULE, hook_fun1, []}, 0),
    ok = emqx_hooks:add(test_hook, {?MODULE, hook_fun2, []}, 0),
    ?assertEqual(
        {error, already_exists},
        emqx_hooks:add(test_hook, {?MODULE, hook_fun2, []}, 0)
    ),
    ok = emqx_hooks:del(test_hook, {?MODULE, hook_fun1}),
    ok = emqx_hooks:del(test_hook, {?MODULE, hook_fun2}),

    ok = emqx_hooks:add(emqx_hook, {?MODULE, hook_fun8, []}, 8),
    ok = emqx_hooks:add(emqx_hook, {?MODULE, hook_fun2, []}, 2),
    ok = emqx_hooks:add(emqx_hook, {?MODULE, hook_fun10, []}, 10),
    ok = emqx_hooks:add(emqx_hook, {?MODULE, hook_fun9, []}, 9),
    ok = emqx_hooks:del(emqx_hook, {?MODULE, hook_fun2, []}),
    ok = emqx_hooks:del(emqx_hook, {?MODULE, hook_fun8, []}),
    ok = emqx_hooks:del(emqx_hook, {?MODULE, hook_fun9, []}),
    ok = emqx_hooks:del(emqx_hook, {?MODULE, hook_fun10, []}).

t_run_hook(_) ->
    ok = emqx_hooks:add(foldl_hook, {?MODULE, hook_fun3, [init]}, 0),
    ok = emqx_hooks:add(foldl_hook, {?MODULE, hook_fun4, [init]}, 0),
    ok = emqx_hooks:add(foldl_hook, {?MODULE, hook_fun5, [init]}, 0),
    [r5, r4] = emqx:run_fold_hook(foldl_hook, [arg1, arg2], []),
    [] = emqx:run_fold_hook(unknown_hook, [], []),

    ok = emqx_hooks:add(foldl_hook2, {?MODULE, hook_fun9, []}, 0),
    ok = emqx_hooks:add(foldl_hook2, {?MODULE, hook_fun10, []}, 0),
    [r10] = emqx:run_fold_hook(foldl_hook2, [arg], []),

    ok = emqx_hooks:add(foreach_hook, {?MODULE, hook_fun6, [initArg]}, 0),
    {error, already_exists} = emqx_hooks:add(foreach_hook, {?MODULE, hook_fun6, [initArg]}, 0),
    ok = emqx_hooks:add(foreach_hook, {?MODULE, hook_fun7, [initArg]}, 0),
    ok = emqx_hooks:add(foreach_hook, {?MODULE, hook_fun8, [initArg]}, 0),
    ok = emqx:run_hook(foreach_hook, [arg]),

    ok = emqx_hooks:add(
        foreach_filter1_hook, {?MODULE, hook_fun1, []}, 0, {?MODULE, hook_filter1, []}
    ),
    %% filter passed
    ?assertEqual(ok, emqx:run_hook(foreach_filter1_hook, [arg])),
    %% filter failed
    ?assertEqual(ok, emqx:run_hook(foreach_filter1_hook, [arg1])),

    ok = emqx_hooks:add(
        foldl_filter2_hook, {?MODULE, hook_fun2, []}, 0, {?MODULE, hook_filter2, [init_arg]}
    ),
    ok = emqx_hooks:add(
        foldl_filter2_hook, {?MODULE, hook_fun2_1, []}, 0, {?MODULE, hook_filter2_1, [init_arg]}
    ),
    ?assertEqual(3, emqx:run_fold_hook(foldl_filter2_hook, [arg], 1)),
    ?assertEqual(2, emqx:run_fold_hook(foldl_filter2_hook, [arg1], 1)).

%%--------------------------------------------------------------------
%% Hook fun
%%--------------------------------------------------------------------

hook_fun1(arg) -> ok;
hook_fun1(_) -> error.

hook_fun2(arg) -> ok;
hook_fun2(_) -> error.

hook_fun2(_, Acc) -> {ok, Acc + 1}.
hook_fun2_1(_, Acc) -> {ok, Acc + 1}.

hook_fun3(arg1, arg2, _Acc, init) -> ok.
hook_fun4(arg1, arg2, Acc, init) -> {ok, [r4 | Acc]}.
hook_fun5(arg1, arg2, Acc, init) -> {ok, [r5 | Acc]}.

hook_fun6(arg, initArg) -> ok.
hook_fun7(arg, initArg) -> ok.
hook_fun8(arg, initArg) -> ok.

hook_fun9(arg, Acc) -> {stop, [r9 | Acc]}.
hook_fun10(arg, Acc) -> {stop, [r10 | Acc]}.

hook_filter1(arg) -> true;
hook_filter1(_) -> false.

hook_filter2(arg, _Acc, init_arg) -> true;
hook_filter2(_, _Acc, _IntArg) -> false.

hook_filter2_1(arg, _Acc, init_arg) -> true;
hook_filter2_1(arg1, _Acc, init_arg) -> true;
hook_filter2_1(_, _Acc, _IntArg) -> false.
