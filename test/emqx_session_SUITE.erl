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

all() -> [ignore_loop, t_session_all, t_message_expiry_interval_1, t_message_expiry_interval_2].

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

t_message_expiry_interval_1(_) ->
    ClientA = message_expiry_interval_init(),
    [message_expiry_interval_exipred(ClientA, QoS) || QoS <- [0,1,2]].

t_message_expiry_interval_2(_) ->
    ClientA = message_expiry_interval_init(),
    [message_expiry_interval_not_exipred(ClientA, QoS) || QoS <- [0,1,2]].

message_expiry_interval_init() ->
    {ok, ClientA} = emqx_client:start_link([{proto_ver,v5}, {client_id, <<"client-a">>}, {clean_start, false},{properties, #{'Session-Expiry-Interval' => 360}}]),
    {ok, ClientB} = emqx_client:start_link([{proto_ver,v5}, {client_id, <<"client-b">>}, {clean_start, false},{properties, #{'Session-Expiry-Interval' => 360}}]),
    {ok, _} = emqx_client:connect(ClientA),
    {ok, _} = emqx_client:connect(ClientB),
     %% subscribe and disconnect client-b
    emqx_client:subscribe(ClientB, <<"t/a">>, 1),
    emqx_client:stop(ClientB),
    ClientA.

message_expiry_interval_exipred(ClientA, QoS) ->
    ct:pal("~p ~p", [?FUNCTION_NAME, QoS]),
    %% publish to t/a and waiting for the message expired
    emqx_client:publish(ClientA, <<"t/a">>, #{'Message-Expiry-Interval' => 1}, <<"this will be purged in 1s">>, [{qos, QoS}]),
    ct:sleep(2000),

    %% resume the session for client-b
    {ok, ClientB1} = emqx_client:start_link([{proto_ver,v5}, {client_id, <<"client-b">>}, {clean_start, false},{properties, #{'Session-Expiry-Interval' => 360}}]),
    {ok, _} = emqx_client:connect(ClientB1),

    %% verify client-b could not receive the publish message
    receive
        {publish,#{client_pid := ClientB1, topic := <<"t/a">>}} ->
            ct:fail(should_have_expired)
    after 300 ->
        ok
    end,
    emqx_client:stop(ClientB1).

message_expiry_interval_not_exipred(ClientA, QoS) ->
    ct:pal("~p ~p", [?FUNCTION_NAME, QoS]),
    %% publish to t/a
    emqx_client:publish(ClientA, <<"t/a">>, #{'Message-Expiry-Interval' => 20}, <<"this will be purged in 1s">>, [{qos, QoS}]),

    %% wait for 1s and then resume the session for client-b, the message should not expires
    %% as Message-Expiry-Interval = 20s
    ct:sleep(1000),
    {ok, ClientB1} = emqx_client:start_link([{proto_ver,v5}, {client_id, <<"client-b">>}, {clean_start, false},{properties, #{'Session-Expiry-Interval' => 360}}]),
    {ok, _} = emqx_client:connect(ClientB1),

    %% verify client-b could receive the publish message and the Message-Expiry-Interval is set
    receive
        {publish,#{client_pid := ClientB1, topic := <<"t/a">>,
                   properties := #{'Message-Expiry-Interval' := MsgExpItvl}}}
            when MsgExpItvl < 20 -> ok;
        {publish, _} = Msg ->
            ct:fail({incorrect_publish, Msg})
    after 300 ->
        ct:fail(no_publish_received)
    end,
    emqx_client:stop(ClientB1).
