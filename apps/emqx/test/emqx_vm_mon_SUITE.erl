%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(t_alarms, Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    emqx_config:put([sysmon, vm], #{
        process_high_watermark => 0,
        process_low_watermark => 0,
        %% 1s
        process_check_interval => 100
    }),
    ok = supervisor:terminate_child(emqx_sys_sup, emqx_vm_mon),
    {ok, _} = supervisor:restart_child(emqx_sys_sup, emqx_vm_mon),
    Config;
init_per_testcase(_, Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_testcase(_, _Config) ->
    emqx_common_test_helpers:stop_apps([]).

t_alarms(_) ->
    timer:sleep(500),
    ?assert(is_existing(too_many_processes, emqx_alarm:get_alarms(activated))),
    emqx_config:put([sysmon, vm, process_high_watermark], 70),
    emqx_config:put([sysmon, vm, process_low_watermark], 60),
    timer:sleep(500),
    ?assertNot(is_existing(too_many_processes, emqx_alarm:get_alarms(activated))).

is_existing(Name, [#{name := Name} | _More]) ->
    true;
is_existing(Name, [_Alarm | More]) ->
    is_existing(Name, More);
is_existing(_Name, []) ->
    false.
