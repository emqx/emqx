%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_backend_SUITE).

-include("emqttd.hrl").

-compile(export_all).

all() -> [{group, subscription}].

groups() -> [{subscription, [], [add_del_subscription]}].

init_per_suite(Config) ->
    ok = emqttd_mnesia:ensure_started(),
    emqttd_backend:mnesia(boot),
    emqttd_backend:mnesia(copy),
    Config.

end_per_suite(_Config) ->
    emqttd_mnesia:ensure_stopped().

add_del_subscription(_) ->
    Sub1 = #mqtt_subscription{subid = <<"clientId">>, topic = <<"topic">>, qos = 2},
    Sub2 = #mqtt_subscription{subid = <<"clientId">>, topic = <<"topic">>, qos = 1},
    ok = emqttd_backend:add_subscription(Sub1),
    {error, already_existed} = emqttd_backend:add_subscription(Sub1),
    ok = emqttd_backend:add_subscription(Sub2),
    [Sub2] = emqttd_backend:lookup_subscriptions(<<"clientId">>),
    emqttd_backend:del_subscription(<<"clientId">>, <<"topic">>),
    [] = emqttd_backend:lookup_subscriptions(<<"clientId">>).

