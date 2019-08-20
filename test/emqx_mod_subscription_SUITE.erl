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

-module(emqx_mod_subscription_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include("emqx.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx]).

t_mod_subscription(_) ->
    emqx_mod_subscription:load([{<<"connected/%c/%u">>, ?QOS_0}]),
    {ok, C} = emqtt:start_link([{host, "localhost"}, {client_id, "myclient"}, {username, "admin"}]),
    {ok, _} = emqtt:connect(C),
    % ct:sleep(100),
    emqtt:publish(C, <<"connected/myclient/admin">>, <<"Hello world">>, ?QOS_0),
    receive
        {publish, #{topic := Topic, payload := Payload}} ->
            ?assertEqual(<<"connected/myclient/admin">>, Topic),
            ?assertEqual(<<"Hello world">>, Payload)
    after 100 ->
        ct:fail("no_message")
    end,
    ok = emqtt:disconnect(C),
    emqx_mod_subscription:unload([]).
