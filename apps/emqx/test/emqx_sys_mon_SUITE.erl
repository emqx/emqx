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

-module(emqx_sys_mon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SYSMON, emqx_sys_mon).

-define(INPUTINFO, [{self(), long_gc,
                     concat_str("long_gc warning: pid = ~p, info: ~p", self(), "hello"), "hello"},
                    {self(), long_schedule,
                     concat_str("long_schedule warning: pid = ~p, info: ~p", self(), "hello"), "hello"},
                     {self(), large_heap,
                     concat_str("large_heap warning: pid = ~p, info: ~p", self(), "hello"), "hello"},
                    {self(), busy_port,
                     concat_str("busy_port warning: suspid = ~p, port = ~p",
                                self(), list_to_port("#Port<0.4>")), list_to_port("#Port<0.4>")},
                    {self(), busy_dist_port,
                     concat_str("busy_dist_port warning: suspid = ~p, port = ~p",
                                self(), list_to_port("#Port<0.4>")),list_to_port("#Port<0.4>")},
                    {list_to_port("#Port<0.4>"), long_schedule,
                     concat_str("long_schedule warning: port = ~p, info: ~p",
                                list_to_port("#Port<0.4>"), "hello"), "hello"}
                    ]).

all() -> emqx_ct:all(?MODULE).

init_per_testcase(t_sys_mon, Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([],
        fun(emqx) ->
            application:set_env(emqx, sysmon, [{busy_dist_port,true},
                                                {busy_port,false},
                                                {large_heap,8388608},
                                                {long_schedule,240},
                                                {long_gc,0}]),
            ok;
           (_) -> ok
        end),
    Config;
init_per_testcase(t_sys_mon2, Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([],
        fun(emqx) ->
            application:set_env(emqx, sysmon, [{busy_dist_port,false},
                                                {busy_port,true},
                                                {large_heap,8388608},
                                                {long_schedule,0},
                                                {long_gc,200},
                                                {nothing, 0}]),
            ok;
           (_) -> ok
        end),
    Config;
init_per_testcase(_, Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_testcase(_, _Config) ->
    emqx_ct_helpers:stop_apps([]).

t_procinfo(_) ->
    ok = meck:new(emqx_vm, [passthrough, no_history]),
    ok = meck:expect(emqx_vm, get_process_info, fun(_) -> [] end),
    ok = meck:expect(emqx_vm, get_process_gc_info, fun(_) -> [] end),
    ?assertEqual([], emqx_sys_mon:procinfo([])),
    ok = meck:expect(emqx_vm, get_process_info, fun(_) -> ok end),
    ok = meck:expect(emqx_vm, get_process_gc_info, fun(_) -> undefined end),
    ?assertEqual(undefined, emqx_sys_mon:procinfo([])),
    ok = meck:unload(emqx_vm).

t_sys_mon(_Config) ->
    lists:foreach(
      fun({PidOrPort, SysMonName,ValidateInfo, InfoOrPort}) ->
              validate_sys_mon_info(PidOrPort, SysMonName,ValidateInfo, InfoOrPort)
      end, ?INPUTINFO).

t_sys_mon2(_Config) ->
    ?SYSMON ! {timeout, ignored, reset},
    ?SYSMON ! {ignored},
    ?assertEqual(ignored, gen_server:call(?SYSMON, ignored)),
    ?assertEqual(ok, gen_server:cast(?SYSMON, ignored)),
    gen_server:stop(?SYSMON).

validate_sys_mon_info(PidOrPort, SysMonName,ValidateInfo, InfoOrPort) ->
    {ok, C} = emqtt:start_link([{host, "localhost"}]),
    {ok, _} = emqtt:connect(C),
    emqtt:subscribe(C, emqx_topic:systop(lists:concat(['sysmon/', SysMonName])), qos1),
    timer:sleep(100),
    ?SYSMON ! {monitor, PidOrPort, SysMonName, InfoOrPort},
    receive
        {publish,  #{payload := Info}} ->
            ?assertEqual(ValidateInfo, binary_to_list(Info)),
            ct:pal("OK - received msg: ~p~n", [Info])
    after
        1000 ->
            ct:fail("flase")
    end,
    emqtt:stop(C).

concat_str(ValidateInfo, InfoOrPort, Info) ->
    WarnInfo = io_lib:format(ValidateInfo, [InfoOrPort, Info]),
    lists:flatten(WarnInfo).
