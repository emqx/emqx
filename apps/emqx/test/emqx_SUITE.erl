%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

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

t_cluster_nodes(_) ->
    Expected = [node()],
    ?assertEqual(Expected, emqx:running_nodes()),
    ?assertEqual(Expected, emqx:cluster_nodes(running)),
    ?assertEqual(Expected, emqx:cluster_nodes(all)),
    ?assertEqual(Expected, emqx:cluster_nodes(cores)),
    ?assertEqual([], emqx:cluster_nodes(stopped)).

t_get_config(_) ->
    ?assertEqual(false, emqx:get_config([overload_protection, enable])),
    ?assertEqual(false, emqx:get_config(["overload_protection", <<"enable">>])).

t_get_config_default_1(_) ->
    ?assertEqual(false, emqx:get_config([overload_protection, enable], undefined)),
    ?assertEqual(false, emqx:get_config(["overload_protection", <<"enable">>], undefined)).

t_get_config_default_2(_) ->
    AtomPathRes = emqx:get_config([overload_protection, <<"_!no_@exist_">>], undefined),
    NonAtomPathRes = emqx:get_config(["doesnotexist", <<"db_backend">>], undefined),
    ?assertEqual(undefined, NonAtomPathRes),
    ?assertEqual(undefined, AtomPathRes).
