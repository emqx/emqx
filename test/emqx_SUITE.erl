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

-module(emqx_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_restart(_) ->
    ConfFile = "test.config",
    Data = "[{emqx_statsd,[{interval,15000},{push_gateway,\"http://127.0.0.1:9091\"}]}].",
    file:write_file(ConfFile, list_to_binary(Data)),
    emqx:restart(ConfFile),
    file:delete(ConfFile).

t_stop_start(_) ->
    emqx:stop(),
    false = emqx:is_running(node()),
    emqx:start(),
    true = emqx:is_running(node()),
    ok = emqx:shutdown(),
    false = emqx:is_running(node()),
    ok = emqx:reboot(),
    true = emqx:is_running(node()),
    ok = emqx:shutdown(for_test),
    false = emqx:is_running(node()).

t_get_env(_) ->
    ?assertEqual(undefined, emqx:get_env(undefined_key)),
    ?assertEqual(default_value, emqx:get_env(undefined_key, default_value)),
    application:set_env(emqx, undefined_key, hello),
    ?assertEqual(hello, emqx:get_env(undefined_key)),
    ?assertEqual(hello, emqx:get_env(undefined_key, default_value)),
    application:unset_env(emqx, undefined_key).

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
    ?assertEqual([{Topic,#{qos => 0,subid => ClientId}}, {Topic1,#{qos => 1,subid => ClientId}}, {Topic2,#{qos => 2,subid => ClientId}}], emqx:subscriptions(self())),
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
    ok = emqx:hook(test_hook, fun ?MODULE:hook_fun1/1, []),
    ok = emqx:hook(test_hook, fun ?MODULE:hook_fun2/1, []),
    ?assertEqual({error, already_exists},
                    emqx:hook(test_hook, fun ?MODULE:hook_fun2/1, [])),
    ok = emqx:unhook(test_hook, fun ?MODULE:hook_fun1/1),
    ok = emqx:unhook(test_hook, fun ?MODULE:hook_fun2/1),

    ok = emqx:hook(emqx_hook, {?MODULE, hook_fun8, []}, 8),
    ok = emqx:hook(emqx_hook, {?MODULE, hook_fun2, []}, 2),
    ok = emqx:hook(emqx_hook, {?MODULE, hook_fun10, []}, 10),
    ok = emqx:hook(emqx_hook, {?MODULE, hook_fun9, []}, 9),
    ok = emqx:unhook(emqx_hook, {?MODULE, hook_fun2, []}),
    ok = emqx:unhook(emqx_hook, {?MODULE, hook_fun8, []}),
    ok = emqx:unhook(emqx_hook, {?MODULE, hook_fun9, []}),
    ok = emqx:unhook(emqx_hook, {?MODULE, hook_fun10, []}).

t_run_hook(_) ->
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun3/4, [init]),
    ok = emqx:hook(foldl_hook, {?MODULE, hook_fun3, [init]}),
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun4/4, [init]),
    ok = emqx:hook(foldl_hook, fun ?MODULE:hook_fun5/4, [init]),
    [r5,r4] = emqx:run_fold_hook(foldl_hook, [arg1, arg2], []),
    [] = emqx:run_fold_hook(unknown_hook, [], []),

    ok = emqx:hook(foldl_hook2, fun ?MODULE:hook_fun9/2),
    ok = emqx:hook(foldl_hook2, {?MODULE, hook_fun10, []}),
    [r9] = emqx:run_fold_hook(foldl_hook2, [arg], []),

    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun6/2, [initArg]),
    {error, already_exists} = emqx:hook(foreach_hook, fun ?MODULE:hook_fun6/2, [initArg]),
    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun7/2, [initArg]),
    ok = emqx:hook(foreach_hook, fun ?MODULE:hook_fun8/2, [initArg]),
    ok = emqx:run_hook(foreach_hook, [arg]),

    ok = emqx:hook(foreach_filter1_hook, {?MODULE, hook_fun1, []}, {?MODULE, hook_filter1, []}, 0),
    ?assertEqual(ok, emqx:run_hook(foreach_filter1_hook, [arg])), %% filter passed
    ?assertEqual(ok, emqx:run_hook(foreach_filter1_hook, [arg1])), %% filter failed

    ok = emqx:hook(foldl_filter2_hook, {?MODULE, hook_fun2, []}, {?MODULE, hook_filter2, [init_arg]}),
    ok = emqx:hook(foldl_filter2_hook, {?MODULE, hook_fun2_1, []}, {?MODULE, hook_filter2_1, [init_arg]}),
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
hook_fun4(arg1, arg2, Acc, init)  -> {ok, [r4 | Acc]}.
hook_fun5(arg1, arg2, Acc, init)  -> {ok, [r5 | Acc]}.

hook_fun6(arg, initArg) -> ok.
hook_fun7(arg, initArg) -> ok.
hook_fun8(arg, initArg) -> ok.

hook_fun9(arg, Acc)  -> {stop, [r9 | Acc]}.
hook_fun10(arg, Acc) -> {stop, [r10 | Acc]}.

hook_filter1(arg) -> true;
hook_filter1(_) -> false.

hook_filter2(arg, _Acc, init_arg) -> true;
hook_filter2(_, _Acc, _IntArg) -> false.

hook_filter2_1(arg, _Acc, init_arg)  -> true;
hook_filter2_1(arg1, _Acc, init_arg) -> true;
hook_filter2_1(_, _Acc, _IntArg)     -> false.
