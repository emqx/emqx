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

-module(emqx_alarm_handler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([], fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

set_special_configs(emqx) ->
    AclFile = emqx_ct_helpers:deps_path(emqx, "test/emqx_access_SUITE_data/acl_deny_action.conf"),
    application:set_env(emqx, acl_file, AclFile);
set_special_configs(_App) -> ok.

t_alarm_handler(_) ->
    with_connection(
        fun(Sock) ->
            emqtt_sock:send(Sock,
                                  raw_send_serialize(
                                      ?CONNECT_PACKET(
                                          #mqtt_packet_connect{
                                          proto_ver = ?MQTT_PROTO_V5})
                                   )),
            {ok, Data} = gen_tcp:recv(Sock, 0),
            {ok, ?CONNACK_PACKET(?RC_SUCCESS), <<>>, _} = raw_recv_parse(Data),

            Topic1 = emqx_topic:systop(<<"alarms/alert">>),
            Topic2 = emqx_topic:systop(<<"alarms/clear">>),
            SubOpts = #{rh => 1, qos => ?QOS_2, rap => 0, nl => 0, rc => 0},
            emqtt_sock:send(Sock,
                                  raw_send_serialize(
                                      ?SUBSCRIBE_PACKET(
                                          1,
                                          [{Topic1, SubOpts},
                                           {Topic2, SubOpts}])
                                      )),

            {ok, Data2} = gen_tcp:recv(Sock, 0),
            {ok, ?SUBACK_PACKET(1, #{}, [2, 2]), <<>>, _} = raw_recv_parse(Data2),

            alarm_handler:set_alarm({alarm_for_test, #alarm{id = alarm_for_test,
                                                            severity = error,
                                                            title="alarm title",
                                                            summary="alarm summary"
                                                           }}),

            {ok, Data3} = gen_tcp:recv(Sock, 0),

            {ok, ?PUBLISH_PACKET(?QOS_0, Topic1, _, _), <<>>, _} = raw_recv_parse(Data3),

            ?assertEqual(true, lists:keymember(alarm_for_test, 1, emqx_alarm_handler:get_alarms())),

            alarm_handler:clear_alarm(alarm_for_test),

            {ok, Data4} = gen_tcp:recv(Sock, 0),

            {ok, ?PUBLISH_PACKET(?QOS_0, Topic2, _, _), <<>>, _} = raw_recv_parse(Data4),

            ?assertEqual(false, lists:keymember(alarm_for_test, 1, emqx_alarm_handler:get_alarms())),

            emqx_alarm_handler:mnesia(copy),
            ?assertEqual(true, lists:keymember(alarm_for_test, 1, emqx_alarm_handler:get_alarms(history))),

            alarm_handler:clear_alarm(not_exist),

            gen_event:start({local, alarm_handler_2}, []),
            gen_event:add_handler(alarm_handler_2, emqx_alarm_handler, []),
            ?assertEqual({error,bad_query}, gen_event:call(alarm_handler_2, emqx_alarm_handler, bad_query)),
            ?assertEqual(ok, gen_event:notify(alarm_handler_2, ignored)),
            gen_event:stop(alarm_handler_2)
        end).

with_connection(DoFun) ->
    {ok, Sock} = emqtt_sock:connect({127, 0, 0, 1}, 1883,
                                          [binary, {packet, raw}, {active, false}],
                                          3000),
    try
        DoFun(Sock)
    after
        emqtt_sock:close(Sock)
    end.

raw_send_serialize(Packet) ->
    emqx_frame:serialize(Packet, ?MQTT_PROTO_V5).

raw_recv_parse(Bin) ->
    emqx_frame:parse(Bin, emqx_frame:initial_parse_state(#{version => ?MQTT_PROTO_V5})).
