%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_keepalive_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx,
                "listeners {"
                "tcp.default.bind = 1883,"
                "ssl.default = marked_for_deletion,"
                "quic.default = marked_for_deletion,"
                "ws.default = marked_for_deletion,"
                "wss.default = marked_for_deletion"
                "}"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

t_check_keepalive_default_timeout(_) ->
    emqx_config:put_zone_conf(default, [mqtt, keepalive_multiplier], 1.5),
    emqx_config:put_zone_conf(default, [mqtt, keepalive_check_interval], 30000),
    erlang:process_flag(trap_exit, true),
    ClientID = <<"default">>,
    KeepaliveSec = 10,
    {ok, C} = emqtt:start_link([
        {keepalive, KeepaliveSec},
        {clientid, binary_to_list(ClientID)}
    ]),
    {ok, _} = emqtt:connect(C),
    emqtt:pause(C),
    [ChannelPid] = emqx_cm:lookup_channels(ClientID),
    erlang:link(ChannelPid),
    CheckInterval = emqx_utils:clamp(keepalive_check_interval(), 1000, 5000),
    ?assertMatch(5000, CheckInterval),
    %% when keepalive_check_interval is 30s and keepalive_multiplier is 1.5
    %% connect T0(packet = 1, idle_milliseconds = 0)
    %% check1 T1(packet = 1, idle_milliseconds = 1 * CheckInterval = 5000)
    %% check2 T2(packet = 1, idle_milliseconds = 2 * CheckInterval = 10000)
    %% check2 T3(packet = 1, idle_milliseconds = 3 * CheckInterval = 15000) -> timeout
    Timeout = CheckInterval * 3,
    %% connector but not send a packet.
    ?assertMatch(
        no_keepalive_timeout_received,
        receive_msg_in_time(ChannelPid, C, Timeout - 200),
        Timeout - 200
    ),
    ?assertMatch(ok, receive_msg_in_time(ChannelPid, C, 1200)).

t_check_keepalive_other_timeout(_) ->
    emqx_config:put_zone_conf(default, [mqtt, keepalive_multiplier], 1.5),
    emqx_config:put_zone_conf(default, [mqtt, keepalive_check_interval], 2000),
    erlang:process_flag(trap_exit, true),
    ClientID = <<"other">>,
    KeepaliveSec = 10,
    {ok, C} = emqtt:start_link([
        {keepalive, KeepaliveSec},
        {clientid, binary_to_list(ClientID)}
    ]),
    {ok, _} = emqtt:connect(C),
    emqtt:pause(C),
    {ok, _, [0]} = emqtt:subscribe(C, <<"mytopic">>, []),
    [ChannelPid] = emqx_cm:lookup_channels(ClientID),
    erlang:link(ChannelPid),
    %%CheckInterval = ceil(keepalive_check_factor() * KeepaliveSec * 1000),
    CheckInterval = emqx_utils:clamp(keepalive_check_interval(), 1000, 5000),
    ?assertMatch(2000, CheckInterval),
    %% when keepalive_check_interval is 2s and keepalive_multiplier is 1.5
    %% connect T0(packet = 1, idle_milliseconds = 0)
    %% subscribe T1(packet = 2, idle_milliseconds = 0)
    %% check1 T2(packet = 2, idle_milliseconds = 1 * CheckInterval = 2000)
    %% check2 T3(packet = 2, idle_milliseconds = 2 * CheckInterval = 4000)
    %% check3 T4(packet = 2, idle_milliseconds = 3 * CheckInterval = 6000)
    %% check4 T5(packet = 2, idle_milliseconds = 4 * CheckInterval = 8000)
    %% check4 T6(packet = 2, idle_milliseconds = 5 * CheckInterval = 10000)
    %% check4 T7(packet = 2, idle_milliseconds = 6 * CheckInterval = 12000)
    %% check4 T8(packet = 2, idle_milliseconds = 7 * CheckInterval = 14000)
    %% check4 T9(packet = 2, idle_milliseconds = 8 * CheckInterval = 16000) > 15000 timeout
    Timeout = CheckInterval * 9,
    ?assertMatch(
        no_keepalive_timeout_received,
        receive_msg_in_time(ChannelPid, C, Timeout - 200),
        Timeout - 200
    ),
    ?assertMatch(ok, receive_msg_in_time(ChannelPid, C, 1200), Timeout).

t_check_keepalive_ping_reset_timer(_) ->
    emqx_config:put_zone_conf(default, [mqtt, keepalive_multiplier], 1.5),
    emqx_config:put_zone_conf(default, [mqtt, keepalive_check_interval], 100000),
    erlang:process_flag(trap_exit, true),
    ClientID = <<"ping_reset">>,
    KeepaliveSec = 10,
    {ok, C} = emqtt:start_link([
        {keepalive, KeepaliveSec},
        {clientid, binary_to_list(ClientID)}
    ]),
    {ok, _} = emqtt:connect(C),
    emqtt:pause(C),
    ct:sleep(1000),
    emqtt:resume(C),
    pong = emqtt:ping(C),
    emqtt:pause(C),
    [ChannelPid] = emqx_cm:lookup_channels(ClientID),
    erlang:link(ChannelPid),
    CheckInterval = emqx_utils:clamp(keepalive_check_interval(), 1000, 5000),
    ?assertMatch(5000, CheckInterval),
    %% when keepalive_check_interval is 30s and keepalive_multiplier is 1.5
    %% connect T0(packet = 1, idle_milliseconds = 0)
    %% sleep 1000ms
    %% ping (packet = 2, idle_milliseconds = 0) restart timer
    %% check1 T1(packet = 1, idle_milliseconds = 1 * CheckInterval = 5000)
    %% check2 T2(packet = 1, idle_milliseconds = 2 * CheckInterval = 10000)
    %% check2 T3(packet = 1, idle_milliseconds = 3 * CheckInterval = 15000) -> timeout
    Timeout = CheckInterval * 3,
    ?assertMatch(
        no_keepalive_timeout_received,
        receive_msg_in_time(ChannelPid, C, Timeout - 200),
        Timeout - 200
    ),
    ?assertMatch(ok, receive_msg_in_time(ChannelPid, C, 1200)).

t_check(_) ->
    emqx_config:put_zone_conf(default, [mqtt, keepalive_multiplier], 1.5),
    emqx_config:put_zone_conf(default, [mqtt, keepalive_check_interval], 30000),
    Keepalive = emqx_keepalive:init(60),
    ?assertEqual(30000, emqx_keepalive:info(check_interval, Keepalive)),
    ?assertEqual(0, emqx_keepalive:info(statval, Keepalive)),
    Info = emqx_keepalive:info(Keepalive),
    ?assertEqual(
        #{
            check_interval => 30000,
            statval => 0,
            idle_milliseconds => 0,
            %% 60 * 1.5 * 1000
            max_idle_millisecond => 90000
        },
        Info
    ),
    {ok, Keepalive1} = emqx_keepalive:check(1, Keepalive),
    ?assertEqual(1, emqx_keepalive:info(statval, Keepalive1)),
    {ok, Keepalive2} = emqx_keepalive:check(1, Keepalive1),
    ?assertEqual(1, emqx_keepalive:info(statval, Keepalive2)),
    {ok, Keepalive3} = emqx_keepalive:check(1, Keepalive2),
    ?assertEqual(1, emqx_keepalive:info(statval, Keepalive3)),
    ?assertEqual({error, timeout}, emqx_keepalive:check(1, Keepalive3)),

    Keepalive4 = emqx_keepalive:init(90),
    ?assertEqual(30000, emqx_keepalive:info(check_interval, Keepalive4)),

    Keepalive5 = emqx_keepalive:init(1),
    ?assertEqual(1000, emqx_keepalive:info(check_interval, Keepalive5)),
    ok.

keepalive_multiplier() ->
    emqx_config:get_zone_conf(default, [mqtt, keepalive_multiplier]).

keepalive_check_interval() ->
    emqx_config:get_zone_conf(default, [mqtt, keepalive_check_interval]).

receive_msg_in_time(ChannelPid, C, Timeout) ->
    receive
        {'EXIT', ChannelPid, {shutdown, keepalive_timeout}} ->
            receive
                {'EXIT', C, {shutdown, tcp_closed}} ->
                    ok
            after 500 ->
                throw(no_tcp_closed_from_mqtt_client)
            end
    after Timeout ->
        no_keepalive_timeout_received
    end.
