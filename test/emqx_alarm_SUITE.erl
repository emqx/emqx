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

-module(emqx_alarm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_alarm(_) ->
    ok = emqx_alarm:activate(unknown_alarm),
    {error, already_existed} = emqx_alarm:activate(unknown_alarm),
    ?assertNotEqual({error, not_found}, get_alarm(unknown_alarm, emqx_alarm:get_alarms())),
    ?assertNotEqual({error, not_found}, get_alarm(unknown_alarm, emqx_alarm:get_alarms(activated))),
    ?assertEqual({error, not_found}, get_alarm(unknown_alarm, emqx_alarm:get_alarms(deactivated))),

    ok = emqx_alarm:deactivate(unknown_alarm),
    {error, not_found} = emqx_alarm:deactivate(unknown_alarm),
    ?assertEqual({error, not_found}, get_alarm(unknown_alarm, emqx_alarm:get_alarms(activated))),
    ?assertNotEqual({error, not_found}, get_alarm(unknown_alarm, emqx_alarm:get_alarms(deactivated))),

    emqx_alarm:delete_all_deactivated_alarms(),
    ?assertEqual({error, not_found}, get_alarm(unknown_alarm, emqx_alarm:get_alarms(deactivated))).

t_deactivate_all_alarms(_) ->
    ok = emqx_alarm:activate(unknown_alarm),
    {error, already_existed} = emqx_alarm:activate(unknown_alarm),
    ?assertNotEqual({error, not_found}, get_alarm(unknown_alarm, emqx_alarm:get_alarms(activated))),

    emqx_alarm:deactivate_all_alarms(),
    ?assertNotEqual({error, not_found}, get_alarm(unknown_alarm, emqx_alarm:get_alarms(deactivated))),

    emqx_alarm:delete_all_deactivated_alarms(),
    ?assertEqual({error, not_found}, get_alarm(unknown_alarm, emqx_alarm:get_alarms(deactivated))).

get_alarm(Name, [Alarm = #{name := Name} | _More]) ->
    Alarm;
get_alarm(Name, [_Alarm | More]) ->
    get_alarm(Name, More);
get_alarm(_Name, []) ->
    {error, not_found}.

