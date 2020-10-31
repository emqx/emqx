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

-module(emqx_mod_presence_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    %% Ensure all the modules unloaded.
    ok = emqx_modules:unload(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%% Test case for emqx_mod_presence
t_mod_presence(_) ->
    ok = emqx_mod_presence:load([{qos, ?QOS_1}]),
    {ok, C1} = emqtt:start_link([{clientid, <<"monsys">>}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _Props, [?QOS_1]} = emqtt:subscribe(C1, <<"$SYS/brokers/+/clients/#">>, qos1),
    %% Connected Presence
    {ok, C2} = emqtt:start_link([{clientid, <<"clientid">>},
                                 {username, <<"username">>}]),
    {ok, _} = emqtt:connect(C2),
    ok = recv_and_check_presence(<<"clientid">>, <<"connected">>),
    %% Disconnected Presence
    ok = emqtt:disconnect(C2),
    ok = recv_and_check_presence(<<"clientid">>, <<"disconnected">>),
    ok = emqtt:disconnect(C1),
    ok = emqx_mod_presence:unload([{qos, ?QOS_1}]).

t_mod_presence_reason(_) ->
    ?assertEqual(normal, emqx_mod_presence:reason(normal)),
    ?assertEqual(discarded, emqx_mod_presence:reason({shutdown, discarded})),
    ?assertEqual(tcp_error, emqx_mod_presence:reason({tcp_error, einval})),
    ?assertEqual(internal_error, emqx_mod_presence:reason(<<"unknown error">>)).

recv_and_check_presence(ClientId, Presence) ->
    {ok, #{qos := ?QOS_1, topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertMatch([<<"$SYS">>, <<"brokers">>, _Node, <<"clients">>, ClientId, Presence],
                 binary:split(Topic, <<"/">>, [global])),
    case Presence of
        <<"connected">> ->
            ?assertMatch(#{<<"clientid">> := <<"clientid">>,
                           <<"username">> := <<"username">>,
                           <<"ipaddress">> := <<"127.0.0.1">>,
                           <<"proto_name">> := <<"MQTT">>,
                           <<"proto_ver">> := ?MQTT_PROTO_V4,
                           <<"connack">> := ?RC_SUCCESS,
                           <<"clean_start">> := true}, emqx_json:decode(Payload, [return_maps]));
        <<"disconnected">> ->
            ?assertMatch(#{<<"clientid">> := <<"clientid">>,
                           <<"username">> := <<"username">>,
                           <<"reason">> := <<"normal">>}, emqx_json:decode(Payload, [return_maps]))
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

receive_publish(Timeout) ->
    receive
        {publish, Publish} -> {ok, Publish}
    after
        Timeout -> {error, timeout}
    end.
