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

-module(emqx_mod_subscription_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_on_client_connected(_) ->
    ?assertEqual(ok, emqx_mod_subscription:load([{<<"connected/%c/%u">>, #{qos => ?QOS_0}}])),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                            {clientid, "myclient"},
                            {username, "admin"}]),
    {ok, _} = emqtt:connect(C),
    emqtt:publish(C, <<"connected/myclient/admin">>, <<"Hello world">>, ?QOS_0),
    {ok, #{topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertEqual(<<"connected/myclient/admin">>, Topic),
    ?assertEqual(<<"Hello world">>, Payload),
    ok = emqtt:disconnect(C),
    ?assertEqual(ok, emqx_mod_subscription:unload([{<<"connected/%c/%u">>, #{qos => ?QOS_0}}])).

t_on_undefined_client_connected(_) ->
    ?assertEqual(ok, emqx_mod_subscription:load([{<<"connected/undefined">>, #{qos => ?QOS_1}}])),
    {ok, C} = emqtt:start_link([{host, "localhost"}]),
    {ok, _} = emqtt:connect(C),
    emqtt:publish(C, <<"connected/undefined">>, <<"Hello world">>, ?QOS_1),
    {ok, #{topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertEqual(<<"connected/undefined">>, Topic),
    ?assertEqual(<<"Hello world">>, Payload),
    ok = emqtt:disconnect(C),
    ?assertEqual(ok, emqx_mod_subscription:unload([{<<"connected/undefined">>, #{qos => ?QOS_1}}])).

t_suboption(_) ->
    Client_info = fun(Key, Client) -> maps:get(Key, maps:from_list(emqtt:info(Client)), undefined) end,
    Suboption = #{qos => ?QOS_2, nl => 1, rap => 1, rh => 2},
    ?assertEqual(ok, emqx_mod_subscription:load([{<<"connected/%c/%u">>, Suboption}])),
    {ok, C1} = emqtt:start_link([{proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    timer:sleep(200),
    [CPid1] = emqx_cm:lookup_channels(Client_info(clientid, C1)),
    [ Sub1 | _ ] =  ets:lookup(emqx_subscription,CPid1),
    [ Suboption1 | _ ] = ets:lookup(emqx_suboption,Sub1),
    ?assertMatch({Sub1, #{qos := 2, nl := 1, rap := 1, rh := 2, subid := _}}, Suboption1),
    ok = emqtt:disconnect(C1),
    %% The subscription option is not valid for MQTT V3.1.1
    {ok, C2} = emqtt:start_link([{proto_ver, v4}]),
    {ok, _} = emqtt:connect(C2),
    timer:sleep(200),
    [CPid2] = emqx_cm:lookup_channels(Client_info(clientid, C2)),
    [ Sub2 | _ ] =  ets:lookup(emqx_subscription,CPid2),
    [ Suboption2 | _ ] = ets:lookup(emqx_suboption,Sub2),
    ok = emqtt:disconnect(C2),
    ?assertMatch({Sub2, #{qos := 2, nl := 0, rap := 0, rh := 0, subid := _}}, Suboption2),

    ?assertEqual(ok, emqx_mod_subscription:unload([{<<"connected/undefined">>, Suboption}])).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

receive_publish(Timeout) ->
    receive
        {publish, Publish} -> {ok, Publish}
    after
        Timeout -> {error, timeout}
    end.
