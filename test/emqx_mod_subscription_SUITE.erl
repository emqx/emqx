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
    ?assertEqual(ok, emqx_mod_subscription:load([{<<"connected/%c/%u">>, ?QOS_0}])),
    {ok, C} = emqtt:start_link([{host, "localhost"},
                            {clientid, "myclient"},
                            {username, "admin"}]),
    {ok, _} = emqtt:connect(C),
    emqtt:publish(C, <<"connected/myclient/admin">>, <<"Hello world">>, ?QOS_0),
    {ok, #{topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertEqual(<<"connected/myclient/admin">>, Topic),
    ?assertEqual(<<"Hello world">>, Payload),
    ok = emqtt:disconnect(C),
    ?assertEqual(ok, emqx_mod_subscription:unload([{<<"connected/%c/%u">>, ?QOS_0}])).

t_on_undefined_client_connected(_) ->
    ?assertEqual(ok, emqx_mod_subscription:load([{<<"connected/undefined">>, ?QOS_0}])),
    {ok, C} = emqtt:start_link([{host, "localhost"}]),
    {ok, _} = emqtt:connect(C),
    emqtt:publish(C, <<"connected/undefined">>, <<"Hello world">>, ?QOS_0),
    {ok, #{topic := Topic, payload := Payload}} = receive_publish(100),
    ?assertEqual(<<"connected/undefined">>, Topic),
    ?assertEqual(<<"Hello world">>, Payload),
    ok = emqtt:disconnect(C),
    ?assertEqual(ok, emqx_mod_subscription:unload([{<<"connected/undefined">>, ?QOS_0}])).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

receive_publish(Timeout) ->
    receive
        {publish, Publish} -> {ok, Publish}
    after
        Timeout -> {error, timeout}
    end.

