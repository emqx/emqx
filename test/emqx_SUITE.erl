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

t_emqx_pubsub_api(_) ->
    emqx:start(),
    true = emqx:is_running(node()),
    {ok, C} = emqx_client:start_link([{host, "localhost"}, {client_id, "myclient"}]),
    {ok, _} = emqx_client:connect(C),
    ClientId = <<"myclient">>,
    Topic = <<"mytopic">>,
    Payload = <<"Hello World">>,
    Topic1 = <<"mytopic1">>,
    emqx:subscribe(Topic, ClientId),
    ?assertEqual([Topic], emqx:topics()),
    ?assertEqual([self()], emqx:subscribers(Topic)),
    ?assertEqual([{Topic,#{qos => 0,subid => ClientId}}], emqx:subscriptions(self())),
    ?assertEqual(true, emqx:subscribed(self(), Topic)),
    ?assertEqual(true, emqx:subscribed(ClientId, Topic)),
    ?assertEqual(false, emqx:subscribed(self(), Topic1)),
    ?assertEqual(false, emqx:subscribed(ClientId, Topic1)),
    emqx:publish(emqx_message:make(Topic, Payload)),
    receive
        {deliver, Topic, #message{payload = Payload}} ->
            ok
    after 100 ->
        ct:fail("no_message")
    end,
    emqx:unsubscribe(Topic),
    ct:sleep(20),
    ?assertEqual([], emqx:topics()).

t_emqx_hook_api(_) ->
    InitArgs = ['arg2', 'arg3'],
    emqx:hook('hook.run', fun run/3, InitArgs),
    ok = emqx:run_hook('hook.run', ['arg1']),
    emqx:unhook('hook.run', fun run/3),

    emqx:hook('hook.run_fold', fun add1/1),
    emqx:hook('hook.run_fold', fun add2/1),
    4 = emqx:run_fold_hook('hook.run_fold', [], 1),
    emqx:unhook('hook.run_fold', fun add1/1),
    emqx:unhook('hook.run_fold', fun add2/1).

run('arg1', 'arg2', 'arg3') ->
    ok;
run(_, _, _) ->
    ct:fail("no_match").

add1(N) -> {ok, N + 1}.
add2(N) -> {ok, N + 2}.