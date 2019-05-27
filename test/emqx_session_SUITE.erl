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

-module(emqx_session_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

all() -> [ignore_loop, t_session_all].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

ignore_loop(_Config) ->
    emqx_zone:set_env(external, ignore_loop_deliver, true),
    {ok, Client} = emqx_client:start_link(),
    {ok, _} = emqx_client:connect(Client),
    TestTopic = <<"Self">>,
    {ok, _, [2]} = emqx_client:subscribe(Client, TestTopic, qos2),
    ok = emqx_client:publish(Client, TestTopic, <<"testmsg">>, 0),
    {ok, _} = emqx_client:publish(Client, TestTopic, <<"testmsg">>, 1),
    {ok, _} = emqx_client:publish(Client, TestTopic, <<"testmsg">>, 2),
    ?assertEqual(0, length(emqx_client_SUITE:receive_messages(3))),
    ok = emqx_client:disconnect(Client),
    emqx_zone:set_env(external, ignore_loop_deliver, false).

t_session_all(_) ->
    emqx_zone:set_env(internal, idle_timeout, 1000),
    ClientId = <<"ClientId">>,
    {ok, ConnPid} = emqx_mock_client:start_link(ClientId),
    {ok, SPid} = emqx_mock_client:open_session(ConnPid, ClientId, internal),
    Message1 = emqx_message:make(<<"ClientId">>, 2, <<"topic">>, <<"hello">>),
    emqx_session:subscribe(SPid, [{<<"topic">>, #{qos => 2}}]),
    emqx_session:subscribe(SPid, [{<<"topic">>, #{qos => 1}}]),
    timer:sleep(200),
    [{<<"topic">>, _}] = emqx:subscriptions(SPid),
    emqx_session:publish(SPid, 1, Message1),
    timer:sleep(200),
    [{publish, 1, _}] = emqx_mock_client:get_last_message(ConnPid),
    Attrs = emqx_session:attrs(SPid),
    Info = emqx_session:info(SPid),
    Stats = emqx_session:stats(SPid),
    ClientId = proplists:get_value(client_id, Attrs),
    ClientId = proplists:get_value(client_id, Info),
    1 = proplists:get_value(subscriptions_count, Stats),
    emqx_session:unsubscribe(SPid, [<<"topic">>]),
    timer:sleep(200),
    [] = emqx:subscriptions(SPid),
    emqx_mock_client:close_session(ConnPid).
