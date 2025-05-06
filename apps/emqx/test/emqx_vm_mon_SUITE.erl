%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_vm_mon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(t_too_many_processes_alarm = TestCase, Config) ->
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    emqx_config:put([sysmon, vm], #{
        process_high_watermark => 0,
        process_low_watermark => 0,
        %% 100ms
        process_check_interval => 100
    }),
    ok = supervisor:terminate_child(emqx_sys_sup, emqx_vm_mon),
    {ok, _} = supervisor:restart_child(emqx_sys_sup, emqx_vm_mon),
    [{apps, Apps} | Config];
init_per_testcase(TestCase, Config) ->
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    [{apps, Apps} | Config].

end_per_testcase(_, Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

t_too_many_processes_alarm(_) ->
    timer:sleep(500),
    Alarms = emqx_alarm:get_alarms(activated),
    ?assert(is_existing(too_many_processes, emqx_alarm:get_alarms(activated))),
    ?assertMatch(
        [
            #{
                activate_at := _,
                activated := true,
                deactivate_at := infinity,
                details := #{high_watermark := 0, low_watermark := 0, usage := "0%"},
                message := <<"0% process usage">>,
                name := too_many_processes
            }
        ],
        lists:filter(
            fun
                (#{name := too_many_processes}) -> true;
                (_) -> false
            end,
            Alarms
        )
    ),
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
