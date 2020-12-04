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

-module(emqx_mqtt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(STATS_KYES, [recv_pkt, recv_msg, send_pkt, send_msg,
                     recv_oct, recv_cnt, send_oct, send_cnt,
                     send_pend
                    ]).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_conn_stats(_) ->
    with_client(fun(CPid) ->
                            Stats = emqx_connection:stats(CPid),
                            ct:pal("==== stats: ~p", [Stats]),
                            [?assert(proplists:get_value(Key, Stats) >= 0) || Key <- ?STATS_KYES]
                    end, []).

t_tcp_sock_passive(_) ->
    with_client(fun(CPid) -> CPid ! {tcp_passive, sock} end, []).

t_message_expiry_interval_1(_) ->
    ClientA = message_expiry_interval_init(),
    [message_expiry_interval_exipred(ClientA, QoS) || QoS <- [0,1,2]],
    emqtt:stop(ClientA).

t_message_expiry_interval_2(_) ->
    ClientA = message_expiry_interval_init(),
    [message_expiry_interval_not_exipred(ClientA, QoS) || QoS <- [0,1,2]],
    emqtt:stop(ClientA).

message_expiry_interval_init() ->
    {ok, ClientA} = emqtt:start_link([{proto_ver,v5},
                                      {clientid, <<"client-a">>},
                                      {clean_start, false},
                                      {properties, #{'Session-Expiry-Interval' => 360}}]),
    {ok, ClientB} = emqtt:start_link([{proto_ver,v5},
                                      {clientid, <<"client-b">>},
                                      {clean_start, false},
                                      {properties, #{'Session-Expiry-Interval' => 360}}]),
    {ok, _} = emqtt:connect(ClientA),
    {ok, _} = emqtt:connect(ClientB),
        %% subscribe and disconnect client-b
    emqtt:subscribe(ClientB, <<"t/a">>, 1),
    emqtt:stop(ClientB),
    ClientA.

message_expiry_interval_exipred(ClientA, QoS) ->
    ct:pal("~p ~p", [?FUNCTION_NAME, QoS]),
    %% publish to t/a and waiting for the message expired
    emqtt:publish(ClientA, <<"t/a">>, #{'Message-Expiry-Interval' => 1}, <<"this will be purged in 1s">>, [{qos, QoS}]),
    ct:sleep(1500),

    %% resume the session for client-b
    {ok, ClientB1} = emqtt:start_link([{proto_ver,v5},
                                       {clientid, <<"client-b">>},
                                       {clean_start, false},
                                       {properties, #{'Session-Expiry-Interval' => 360}}]),
    {ok, _} = emqtt:connect(ClientB1),

    %% verify client-b could not receive the publish message
    receive
        {publish,#{client_pid := ClientB1, topic := <<"t/a">>}} ->
            ct:fail(should_have_expired)
    after 300 ->
        ok
    end,
    emqtt:stop(ClientB1).

message_expiry_interval_not_exipred(ClientA, QoS) ->
    ct:pal("~p ~p", [?FUNCTION_NAME, QoS]),
    %% publish to t/a
    emqtt:publish(ClientA, <<"t/a">>, #{'Message-Expiry-Interval' => 20}, <<"this will be purged in 1s">>, [{qos, QoS}]),

    %% wait for 1s and then resume the session for client-b, the message should not expires
    %% as Message-Expiry-Interval = 20s
    ct:sleep(1000),
    {ok, ClientB1} = emqtt:start_link([{proto_ver,v5},
                                       {clientid, <<"client-b">>},
                                       {clean_start, false},
                                       {properties, #{'Session-Expiry-Interval' => 360}}]),
    {ok, _} = emqtt:connect(ClientB1),

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
	emqtt:stop(ClientB1).

with_client(TestFun, _Options) ->
    ClientId = <<"t_conn">>,
    {ok, C} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    timer:sleep(50),
    case emqx_cm:lookup_channels(ClientId) of
        [] -> ct:fail({client_not_started, ClientId});
        [ChanPid] ->
            TestFun(ChanPid),
            emqtt:stop(C)
    end.

