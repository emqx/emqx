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

-module(emqx_os_mon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([],
        fun(emqx) ->
            application:set_env(emqx, os_mon, [
                {cpu_check_interval, 1},
                {cpu_high_watermark, 5},
                {cpu_low_watermark, 80},
                {mem_check_interval, 60},
                {sysmem_high_watermark, 70},
                {procmem_high_watermark, 5}]);
            (_) -> ok
        end),
    application:ensure_all_started(os_mon),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]),
    application:stop(os_mon).

t_set_mem_check_interval(_) ->
    emqx_os_mon:set_mem_check_interval(0),
    ?assertEqual(60, emqx_os_mon:get_mem_check_interval()),
    emqx_os_mon:set_mem_check_interval(61),
    ?assertEqual(61, emqx_os_mon:get_mem_check_interval()),
    ok.

t_set_sysmem_high_watermark(_) ->
    emqx_os_mon:set_sysmem_high_watermark(10),
    ?assertEqual(10, emqx_os_mon:get_sysmem_high_watermark()),
    emqx_os_mon:set_sysmem_high_watermark(100),
    ?assertEqual(100, emqx_os_mon:get_sysmem_high_watermark()),
    emqx_os_mon:set_sysmem_high_watermark(90),
    ?assertEqual(90, emqx_os_mon:get_sysmem_high_watermark()),
    emqx_os_mon:set_sysmem_high_watermark(93.2),
    ?assertEqual(93.2, emqx_os_mon:get_sysmem_high_watermark()),
    ok.

t_api(_) ->
    ?assertEqual(1, emqx_os_mon:get_cpu_check_interval()),
    ?assertEqual(5, emqx_os_mon:get_cpu_high_watermark()),
    ?assertEqual(80, emqx_os_mon:get_cpu_low_watermark()),
    ?assertEqual(60, emqx_os_mon:get_mem_check_interval()),
    ?assertEqual(70, emqx_os_mon:get_sysmem_high_watermark()),
    ?assertEqual(5, emqx_os_mon:get_procmem_high_watermark()),
    % timer:sleep(2000),
    % ?assertEqual(true, lists:keymember(cpu_high_watermark, 1, alarm_handler:get_alarms())),
    emqx_os_mon:set_cpu_check_interval(0.05),
    emqx_os_mon:set_cpu_high_watermark(80),
    emqx_os_mon:set_cpu_low_watermark(75),
    ?assertEqual(0.05, emqx_os_mon:get_cpu_check_interval()),
    ?assertEqual(80, emqx_os_mon:get_cpu_high_watermark()),
    ?assertEqual(75, emqx_os_mon:get_cpu_low_watermark()),
    % timer:sleep(3000),
    % ?assertEqual(false, lists:keymember(cpu_high_watermark, 1, alarm_handler:get_alarms())),
    ?assertEqual(ignored, gen_server:call(emqx_os_mon, ignored)),
    ?assertEqual(ok, gen_server:cast(emqx_os_mon, ignored)),
    emqx_os_mon ! ignored,
    gen_server:stop(emqx_os_mon),
    ok.
