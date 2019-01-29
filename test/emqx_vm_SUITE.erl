%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_vm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

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



all() ->
    [load, systeminfo, mem_info, process_list, process_info, process_gc,
     get_ets_list, get_ets_info, get_ets_object, get_port_types, get_port_info,
     scheduler_usage, get_memory, microsecs, schedulers, get_process_group_leader_info,
     get_process_limit].

load(_Config) ->
    Loads = emqx_vm:loads(),
    [{load1, _}, {load5, _}, {load15, _}] = Loads.

systeminfo(_Config) ->
   Keys =  [Key || {Key, _} <- emqx_vm:get_system_info()],
   ?SYSTEM_INFO = Keys.

mem_info(_Config) ->
    application:ensure_all_started(os_mon),
    MemInfo = emqx_vm:mem_info(),
    [{total_memory, _},
     {used_memory, _}]= MemInfo,
    application:stop(os_mon).

process_list(_Config) ->
    Pid = self(),
    ProcessInfo = emqx_vm:get_process_list(),
    true = lists:member({pid, Pid}, lists:concat(ProcessInfo)).

process_info(_Config) ->
    ProcessInfos = emqx_vm:get_process_info(),
    ProcessInfo = lists:last(ProcessInfos),
    Keys = [K || {K, _V}<- ProcessInfo],
    ?PROCESS_INFO = Keys.

process_gc(_Config) ->
    ProcessGcs = emqx_vm:get_process_gc(),
    ProcessGc = lists:last(ProcessGcs),
    Keys = [K || {K, _V}<- ProcessGc],
    ?PROCESS_GC = Keys.
   
get_ets_list(_Config) ->
    ets:new(test, [named_table]),
    Ets =  emqx_vm:get_ets_list(),
    true = lists:member(test, Ets).

get_ets_info(_Config) ->
    ets:new(test, [named_table]),
    [] = emqx_vm:get_ets_info(test1),
    EtsInfo = emqx_vm:get_ets_info(test),
    test = proplists:get_value(name, EtsInfo).

get_ets_object(_Config) ->
    ets:new(test, [named_table]),
    ets:insert(test, {k, v}),
    [{k, v}] = emqx_vm:get_ets_object(test).

get_port_types(_Config) ->
    emqx_vm:get_port_types().

get_port_info(_Config) ->
    emqx_vm:get_port_info().

scheduler_usage(_Config) ->
    emqx_vm:scheduler_usage(5000).

get_memory(_Config) ->
    emqx_vm:get_memory().
   
microsecs(_Config) ->
    emqx_vm:microsecs().

schedulers(_Config) ->
    emqx_vm:schedulers().

get_process_group_leader_info(_Config) ->
    emqx_vm:get_process_group_leader_info(self()).

get_process_limit(_Config) ->
    emqx_vm:get_process_limit().
