%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(t_cpu_check_alarm, Config) ->
    SysMon = emqx_config:get([sysmon, os], #{}),
    emqx_config:put([sysmon, os], SysMon#{
        cpu_high_watermark => 0.9,
        cpu_low_watermark => 0,
        %% 200ms
        cpu_check_interval => 200
    }),
    restart_os_mon(),
    Config;
init_per_testcase(t_sys_mem_check_alarm, Config) ->
    case emqx_os_mon:is_os_check_supported() of
        true ->
            SysMon = emqx_config:get([sysmon, os], #{}),
            emqx_config:put([sysmon, os], SysMon#{
                sysmem_high_watermark => 0.51,
                %% 200ms
                mem_check_interval => 200
            });
        false ->
            ok
    end,
    restart_os_mon(),
    Config;
init_per_testcase(_, Config) ->
    restart_os_mon(),
    Config.

restart_os_mon() ->
    case emqx_os_mon:is_os_check_supported() of
        true ->
            ok = supervisor:terminate_child(emqx_sys_sup, emqx_os_mon),
            {ok, _} = supervisor:restart_child(emqx_sys_sup, emqx_os_mon);
        false ->
            _ = supervisor:terminate_child(emqx_sys_sup, emqx_os_mon),
            _ = supervisor:delete_child(emqx_sys_sup, emqx_os_mon),
            %% run test on mac/windows.
            Mod = emqx_os_mon,
            OsMon = #{
                id => Mod,
                start => {Mod, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [Mod]
            },
            {ok, _} = supervisor:start_child(emqx_sys_sup, OsMon)
    end.

t_api(_) ->
    ?assertEqual(0.7, emqx_os_mon:get_sysmem_high_watermark()),
    ?assertEqual(ok, emqx_os_mon:set_sysmem_high_watermark(0.8)),
    ?assertEqual(0.8, emqx_os_mon:get_sysmem_high_watermark()),

    ?assertEqual(5, emqx_os_mon:get_procmem_high_watermark()),
    ?assertEqual(ok, emqx_os_mon:set_procmem_high_watermark(0.11)),
    ?assertEqual(11, emqx_os_mon:get_procmem_high_watermark()),

    ?assertEqual(
        {error, {unexpected_call, ignored}},
        gen_server:call(emqx_os_mon, ignored)
    ),
    ?assertEqual(ok, gen_server:cast(emqx_os_mon, ignored)),
    emqx_os_mon ! ignored,
    gen_server:stop(emqx_os_mon),
    ok.

t_sys_mem_check_disable(Config) ->
    case emqx_os_mon:is_os_check_supported() of
        true -> do_sys_mem_check_disable(Config);
        false -> skip
    end.

do_sys_mem_check_disable(_Config) ->
    MemRef0 = maps:get(mem_time_ref, sys:get_state(emqx_os_mon)),
    ?assertEqual(true, is_reference(MemRef0), MemRef0),
    emqx_config:put([sysmon, os, mem_check_interval], 1000),
    emqx_os_mon:update(emqx_config:get([sysmon, os])),
    MemRef1 = maps:get(mem_time_ref, sys:get_state(emqx_os_mon)),
    ?assertEqual(true, is_reference(MemRef1), {MemRef0, MemRef1}),
    ?assertNotEqual(MemRef0, MemRef1),
    emqx_config:put([sysmon, os, mem_check_interval], disabled),
    emqx_os_mon:update(emqx_config:get([sysmon, os])),
    ?assertEqual(undefined, maps:get(mem_time_ref, sys:get_state(emqx_os_mon))),
    ok.

t_sys_mem_check_alarm(Config) ->
    case emqx_os_mon:is_os_check_supported() of
        true -> do_sys_mem_check_alarm(Config);
        false -> skip
    end.

do_sys_mem_check_alarm(_Config) ->
    emqx_config:put([sysmon, os, mem_check_interval], 200),
    emqx_os_mon:update(emqx_config:get([sysmon, os])),
    Mem = 0.52345,
    Usage = floor(Mem * 10000) / 100,
    emqx_common_test_helpers:with_mock(
        load_ctl,
        get_memory_usage,
        fun() -> Mem end,
        fun() ->
            %% wait for `os_mon` started
            timer:sleep(10_000),
            Alarms = emqx_alarm:get_alarms(activated),
            ?assert(
                emqx_vm_mon_SUITE:is_existing(
                    high_system_memory_usage, emqx_alarm:get_alarms(activated)
                ),
                #{
                    load_ctl_memory => load_ctl:get_memory_usage(),
                    config => emqx_config:get([sysmon, os]),
                    process => sys:get_state(emqx_os_mon),
                    alarms => Alarms
                }
            ),
            [
                #{
                    activate_at := _,
                    activated := true,
                    deactivate_at := infinity,
                    details := #{high_watermark := 51.0, usage := RealUsage},
                    message := Msg,
                    name := high_system_memory_usage
                }
            ] =
                lists:filter(
                    fun
                        (#{name := high_system_memory_usage}) -> true;
                        (_) -> false
                    end,
                    Alarms
                ),
            ?assert(RealUsage >= Usage, {RealUsage, Usage}),
            ?assert(is_binary(Msg)),
            emqx_config:put([sysmon, os, sysmem_high_watermark], 0.99999),
            ok = supervisor:terminate_child(emqx_sys_sup, emqx_os_mon),
            {ok, _} = supervisor:restart_child(emqx_sys_sup, emqx_os_mon),
            timer:sleep(600),
            Activated = emqx_alarm:get_alarms(activated),
            ?assertNot(
                emqx_vm_mon_SUITE:is_existing(high_system_memory_usage, Activated),
                #{activated => Activated, process_state => sys:get_state(emqx_os_mon)}
            )
        end
    ).

t_cpu_check_alarm(_) ->
    CpuUtil = 90.12345,
    Usage = floor(CpuUtil * 100) / 100,
    emqx_common_test_helpers:with_mock(
        cpu_sup,
        util,
        fun() -> CpuUtil end,
        fun() ->
            timer:sleep(1000),
            Alarms = emqx_alarm:get_alarms(activated),
            ?assert(
                emqx_vm_mon_SUITE:is_existing(high_cpu_usage, emqx_alarm:get_alarms(activated))
            ),
            [
                #{
                    activate_at := _,
                    activated := true,
                    deactivate_at := infinity,
                    details := #{high_watermark := 90.0, low_watermark := 0, usage := RealUsage},
                    message := Msg,
                    name := high_cpu_usage
                }
            ] =
                lists:filter(
                    fun
                        (#{name := high_cpu_usage}) -> true;
                        (_) -> false
                    end,
                    Alarms
                ),
            ?assert(RealUsage >= Usage, {RealUsage, Usage}),
            ?assert(is_binary(Msg)),
            emqx_config:put([sysmon, os, cpu_high_watermark], 1),
            emqx_config:put([sysmon, os, cpu_low_watermark], 0.96),
            timer:sleep(800),
            ?assertNot(
                emqx_vm_mon_SUITE:is_existing(high_cpu_usage, emqx_alarm:get_alarms(activated))
            )
        end
    ).
