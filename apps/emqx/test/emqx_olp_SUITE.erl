%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_olp_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("lc/include/lc.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

init_per_testcase(_, Config) ->
    emqx_olp:enable(),
    case wait_for(fun() -> lc_sup:whereis_runq_flagman() end, 10) of
        true -> ok;
        false -> ct:fail("runq_flagman is not up")
    end,
    ok = load_ctl:put_config(#{
        ?RUNQ_MON_F0 => true,
        ?RUNQ_MON_F1 => 5,
        ?RUNQ_MON_F2 => 1,
        ?RUNQ_MON_T1 => 200,
        ?RUNQ_MON_T2 => 50,
        ?RUNQ_MON_C1 => 2,
        ?RUNQ_MON_F5 => -1
    }),
    Config.

%% Test that olp could be enabled/disabled globally
t_disable_enable(_Config) ->
    Old = load_ctl:whereis_runq_flagman(),
    ok = emqx_olp:disable(),
    ?assert(not is_process_alive(Old)),
    {ok, Pid} = emqx_olp:enable(),
    timer:sleep(1000),
    ?assert(is_process_alive(Pid)).

%% Test that overload detection works
t_is_overloaded(_Config) ->
    P = burst_runq(),
    timer:sleep(3000),
    ?assert(emqx_olp:is_overloaded()),
    exit(P, kill),
    timer:sleep(3000),
    ?assert(not emqx_olp:is_overloaded()).

%% Test that new conn is rejected when olp is enabled
t_overloaded_conn(_Config) ->
    process_flag(trap_exit, true),
    ?assert(erlang:is_process_alive(load_ctl:whereis_runq_flagman())),
    emqx_config:put([overload_protection, enable], true),
    P = burst_runq(),
    timer:sleep(1000),
    ?assert(emqx_olp:is_overloaded()),
    true = emqx:is_running(node()),
    {ok, C} = emqtt:start_link([{host, "localhost"}, {clientid, "myclient"}]),
    ?assertNotMatch({ok, _Pid}, emqtt:connect(C)),
    exit(P, kill).

%% Test that new conn is rejected when olp is enabled
t_overload_cooldown_conn(Config) ->
    t_overloaded_conn(Config),
    timer:sleep(1000),
    ?assert(not emqx_olp:is_overloaded()),
    {ok, C} = emqtt:start_link([{host, "localhost"}, {clientid, "myclient"}]),
    ?assertMatch({ok, _Pid}, emqtt:connect(C)),
    emqtt:stop(C).

-spec burst_runq() -> ParentToKill :: pid().
burst_runq() ->
    NProc = erlang:system_info(schedulers_online),
    spawn(?MODULE, worker_parent, [NProc * 10, {?MODULE, busy_loop, []}]).

%% internal helpers
worker_parent(N, {M, F, A}) ->
    lists:foreach(
        fun(_) ->
            proc_lib:spawn_link(fun() -> apply(M, F, A) end)
        end,
        lists:seq(1, N)
    ),
    receive
        stop -> ok
    end.

busy_loop() ->
    erlang:yield(),
    busy_loop().

wait_for(_Fun, 0) ->
    false;
wait_for(Fun, Retry) ->
    case is_pid(Fun()) of
        true ->
            true;
        false ->
            timer:sleep(10),
            wait_for(Fun, Retry - 1)
    end.
