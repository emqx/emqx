%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_vm_mon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_testcase(t_api, Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([],
        fun(emqx) ->
            application:set_env(emqx, vm_mon, [{check_interval, 1},
                                               {process_high_watermark, 80},
                                               {process_low_watermark, 75}]),
            ok;
           (_) ->
            ok
        end),
    Config;
init_per_testcase(_, Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_testcase(_, _Config) ->
    emqx_ct_helpers:stop_apps([]).

t_api(_) ->
    ?assertEqual(1, emqx_vm_mon:get_check_interval()),
    ?assertEqual(80, emqx_vm_mon:get_process_high_watermark()),
    ?assertEqual(75, emqx_vm_mon:get_process_low_watermark()),
    emqx_vm_mon:set_process_high_watermark(0),
    emqx_vm_mon:set_process_low_watermark(60),
    ?assertEqual(0, emqx_vm_mon:get_process_high_watermark()),
    ?assertEqual(60, emqx_vm_mon:get_process_low_watermark()),
    timer:sleep(emqx_vm_mon:get_check_interval() * 1000 * 2),
    ?assert(is_existing(too_many_processes, emqx_alarm:get_alarms(activated))),
    emqx_vm_mon:set_process_high_watermark(70),
    timer:sleep(emqx_vm_mon:get_check_interval() * 1000 * 2),
    ?assertNot(is_existing(too_many_processes, emqx_alarm:get_alarms(activated))).

is_existing(Name, [#{name := Name} | _More]) ->
    true;
is_existing(Name, [_Alarm | More]) ->
    is_existing(Name, More);
is_existing(_Name, []) ->
    false.
