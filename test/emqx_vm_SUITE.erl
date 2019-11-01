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

-module(emqx_vm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(SYSTEM_INFO, [allocated_areas,
                      allocator,
                      alloc_util_allocators,
                      build_type,
                      check_io,
                      compat_rel,
                      creation,
                      debug_compiled,
                      dist,
                      dist_ctrl,
                      driver_version,
                      elib_malloc,
                      dist_buf_busy_limit,
                      %fullsweep_after, % included in garbage_collection
                      garbage_collection,
                      %global_heaps_size, % deprecated
                      heap_sizes,
                      heap_type,
                      info,
                      kernel_poll,
                      loaded,
                      logical_processors,
                      logical_processors_available,
                      logical_processors_online,
                      machine,
                      %min_heap_size, % included in garbage_collection
                      %min_bin_vheap_size, % included in garbage_collection
                      modified_timing_level,
                      multi_scheduling,
                      multi_scheduling_blockers,
                      otp_release,
                      port_count,
                      process_count,
                      process_limit,
                      scheduler_bind_type,
                      scheduler_bindings,
                      scheduler_id,
                      schedulers,
                      schedulers_online,
                      smp_support,
                      system_version,
                      system_architecture,
                      threads,
                      thread_pool_size,
                      trace_control_word,
                      update_cpu_info,
                      version,
                      wordsize]).

-define(PROCESS_INFO, [initial_call,
                       current_function,
                       registered_name,
                       status,
                       message_queue_len,
                       group_leader,
                       priority,
                       trap_exit,
                       reductions,
                       %%binary,
                       last_calls,
                       catchlevel,
                       trace,
                       suspending,
                       sequential_trace_token,
                       error_handler]).

-define(PROCESS_GC, [memory,
                     total_heap_size,
                     heap_size,
                     stack_size,
                     min_heap_size]).
                     %fullsweep_after]).

all() -> emqx_ct:all(?MODULE).

t_load(_Config) ->
    ?assertMatch([{load1, _},
                  {load5, _},
                  {load15, _}
                 ], emqx_vm:loads()).

t_systeminfo(_Config) ->
   Keys =  [Key || {Key, _} <- emqx_vm:get_system_info()],
   ?SYSTEM_INFO = Keys,
   ?assertEqual(undefined, emqx_vm:get_system_info(undefined)).

t_mem_info(_Config) ->
    application:ensure_all_started(os_mon),
    MemInfo = emqx_vm:mem_info(),
    [{total_memory, _},
     {used_memory, _}]= MemInfo,
    application:stop(os_mon).

t_process_list(_Config) ->
    Pid = self(),
    ProcessInfo = emqx_vm:get_process_list(),
    true = lists:member({pid, Pid}, lists:concat(ProcessInfo)).

t_process_info(_Config) ->
    ProcessInfos = emqx_vm:get_process_info(),
    ProcessInfo = lists:last(ProcessInfos),
    Keys = [K || {K, _V}<- ProcessInfo],
    ?PROCESS_INFO = Keys.

t_process_gc(_Config) ->
    ProcessGcs = emqx_vm:get_process_gc(),
    ProcessGc = lists:last(ProcessGcs),
    Keys = [K || {K, _V}<- ProcessGc],
    ?PROCESS_GC = Keys.

t_get_ets_list(_Config) ->
    ets:new(test, [named_table]),
    Ets =  emqx_vm:get_ets_list(),
    true = lists:member(test, Ets).

t_get_ets_info(_Config) ->
    ets:new(test, [named_table]),
    [] = emqx_vm:get_ets_info(test1),
    EtsInfo = emqx_vm:get_ets_info(test),
    test = proplists:get_value(name, EtsInfo),
    Tid = proplists:get_value(id, EtsInfo),
    EtsInfos = emqx_vm:get_ets_info(),
    ?assertEqual(true, lists:foldl(fun(Info, Acc) ->
                           case proplists:get_value(id, Info) of
                               Tid -> true;
                               _ -> Acc
                           end
                       end, false, EtsInfos)).

t_get_ets_object(_Config) ->
    ets:new(test, [named_table]),
    [] = emqx_vm:get_ets_object(test),
    ets:insert(test, {k, v}),
    [{k, v}] = emqx_vm:get_ets_object(test).

t_get_port_types(_Config) ->
    emqx_vm:get_port_types().

t_get_port_info(_Config) ->
    emqx_vm:get_port_info(),
    spawn(fun easy_server/0),
    ct:sleep(100),
    {ok, Sock} = gen_tcp:connect("localhost", 5678, [binary, {packet, 0}]),
    emqx_vm:get_port_info(),
    ok = gen_tcp:close(Sock),
    [Port | _] = erlang:ports(),
    [{connected, _}, {name, _}] = emqx_vm:port_info(Port, [connected, name]).

t_transform_port(_Config) ->
    [Port | _] = erlang:ports(),
    ?assertEqual(Port, emqx_vm:transform_port(Port)),
    <<131, 102, 100, NameLen:2/unit:8, _Name:NameLen/binary, N:4/unit:8, _Vsn:8>> = erlang:term_to_binary(Port),
    ?assertEqual(Port, emqx_vm:transform_port("#Port<0." ++ integer_to_list(N) ++ ">")).

t_scheduler_usage(_Config) ->
    emqx_vm:scheduler_usage(5000).

t_get_memory(_Config) ->
    emqx_vm:get_memory().

t_microsecs(_Config) ->
    emqx_vm:microsecs().

t_schedulers(_Config) ->
    emqx_vm:schedulers().

t_get_process_group_leader_info(_Config) ->
    emqx_vm:get_process_group_leader_info(self()).

t_get_process_limit(_Config) ->
    emqx_vm:get_process_limit().

t_cpu_util(_Config) ->
    _Cpu = emqx_vm:cpu_util().

easy_server() ->
    {ok, LSock} = gen_tcp:listen(5678, [binary, {packet, 0}, {active, false}]),
    {ok, Sock} = gen_tcp:accept(LSock),
    ok = do_recv(Sock),
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(LSock).

do_recv(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, _} ->
            do_recv(Sock);
        {error, closed} ->
            ok
    end.

