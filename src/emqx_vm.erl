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

-module(emqx_vm).

-export([ schedulers/0
        , scheduler_usage/1
        , system_info_keys/0
        , get_system_info/0
        , get_system_info/1
        , get_memory/0
        , mem_info/0
        , loads/0
        ]).

-export([ process_info_keys/0
        , get_process_info/0
        , get_process_info/1
        , process_gc_info_keys/0
        , get_process_gc_info/0
        , get_process_gc_info/1
        , get_process_group_leader_info/1
        , get_process_limit/0
        ]).

-export([ get_ets_list/0
        , get_ets_info/0
        , get_ets_info/1
        , get_ets_object/0
        , get_ets_object/1
        ]).

-export([ get_port_types/0
        , get_port_info/0
        , get_port_info/1
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([cpu_util/0]).

-define(UTIL_ALLOCATORS, [temp_alloc,
                          eheap_alloc,
                          binary_alloc,
                          ets_alloc,
                          driver_alloc,
                          sl_alloc,
                          ll_alloc,
                          fix_alloc,
                          std_alloc
                         ]).

-define(PROCESS_INFO_KEYS, [initial_call,
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
                            error_handler
                           ]).

-define(PROCESS_GC_KEYS, [memory,
                          total_heap_size,
                          heap_size,
                          stack_size,
                          min_heap_size
                         ]).

-define(SYSTEM_INFO_KEYS, [allocated_areas,
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
                           wordsize
                          ]).

-define(SOCKET_OPTS, [active,
                      broadcast,
                      buffer,
                      delay_send,
                      dontroute,
                      exit_on_close,
                      header,
                      high_watermark,
                      ipv6_v6only,
                      keepalive,
                      linger,
                      low_watermark,
                      mode,
                      nodelay,
                      packet,
                      packet_size,
                      priority,
                      read_packets,
                      recbuf,
                      reuseaddr,
                      send_timeout,
                      send_timeout_close,
                      sndbuf,
                      tos
                     ]).

schedulers() ->
    erlang:system_info(schedulers).

loads() ->
    [{load1,  ftos(avg1()/256)},
     {load5,  ftos(avg5()/256)},
     {load15, ftos(avg15()/256)}
    ].

system_info_keys() -> ?SYSTEM_INFO_KEYS.

get_system_info() ->
    [{Key, format_system_info(Key, get_system_info(Key))} || Key <- ?SYSTEM_INFO_KEYS].

get_system_info(Key) ->
    try erlang:system_info(Key) catch error:badarg-> undefined end.

format_system_info(allocated_areas, List) ->
    [convert_allocated_areas(Value) || Value <- List];
format_system_info(allocator, {_,_,_,List}) ->
    List;
format_system_info(dist_ctrl, List) ->
    lists:map(fun({Node, Socket}) ->
                {ok, Stats} = inet:getstat(Socket), {Node, Stats}
              end, List);
format_system_info(driver_version, Value) ->
    list_to_binary(Value);
format_system_info(machine, Value) ->
    list_to_binary(Value);
format_system_info(otp_release, Value) ->
    list_to_binary(Value);
format_system_info(scheduler_bindings, Value) ->
    tuple_to_list(Value);
format_system_info(system_version, Value) ->
    list_to_binary(Value);
format_system_info(system_architecture, Value) ->
    list_to_binary(Value);
format_system_info(version, Value) ->
    list_to_binary(Value);
format_system_info(_, Value) ->
    Value.

convert_allocated_areas({Key, Value1, Value2}) ->
    {Key, [Value1, Value2]};
convert_allocated_areas({Key, Value}) ->
    {Key, Value}.

mem_info() ->
    Dataset = memsup:get_system_memory_data(),
    Total = proplists:get_value(total_memory, Dataset),
    Free = proplists:get_value(free_memory, Dataset),
    [{total_memory, Total}, {used_memory, Total - Free}].

ftos(F) ->
    S = io_lib:format("~.2f", [F]), S.

%%%% erlang vm scheduler_usage  fun copied from recon
scheduler_usage(Interval) when is_integer(Interval) ->
    %% We start and stop the scheduler_wall_time system flag
    %% if it wasn't in place already. Usually setting the flag
    %% should have a CPU impact(make it higher) only when under low usage.
    FormerFlag = erlang:system_flag(scheduler_wall_time, true),
    First = erlang:statistics(scheduler_wall_time),
    timer:sleep(Interval),
    Last = erlang:statistics(scheduler_wall_time),
    erlang:system_flag(scheduler_wall_time, FormerFlag),
    scheduler_usage_diff(First, Last).

scheduler_usage_diff(First, Last) ->
    lists:map(fun({{I, A0, T0},{I, A1, T1}}) ->
                {I, (A1 - A0)/(T1 - T0)}
              end, lists:zip(lists:sort(First), lists:sort(Last))).

get_memory()->
    [{Key, get_memory(Key, current)} || Key <- [used, allocated, unused, usage]] ++ erlang:memory().

get_memory(used, Keyword) ->
    lists:sum(lists:map(fun({_, Prop}) ->
                    container_size(Prop, Keyword, blocks_size)
            end, util_alloc()));
get_memory(allocated, Keyword) ->
    lists:sum(lists:map(fun({_, Prop})->
                    container_size(Prop, Keyword, carriers_size)
            end, util_alloc()));
get_memory(unused, Keyword) ->
    get_memory(allocated, Keyword) - get_memory(used, Keyword);
get_memory(usage, Keyword) ->
    get_memory(used, Keyword) / get_memory(allocated, Keyword).

util_alloc()->
    alloc(?UTIL_ALLOCATORS).

alloc()->
    {_Mem, Allocs} = snapshot_int(),
    Allocs.
alloc(Type) ->
    [{{T, Instance}, Props} || {{T, Instance}, Props} <- alloc(), lists:member(T, Type)].

snapshot_int() ->
    {erlang:memory(), allocators()}.

allocators() ->
    UtilAllocators = erlang:system_info(alloc_util_allocators),
    Allocators = [sys_alloc, mseg_alloc|UtilAllocators],
    [{{A, N},lists:sort(proplists:delete(versions, Props))} ||
        A <- Allocators, Allocs <- [erlang:system_info({allocator, A})],
            Allocs =/= false, {_, N, Props} <- Allocs].

container_size(Prop, Keyword, Container) ->
    Sbcs = container_value(Prop, Keyword, sbcs, Container),
    Mbcs = container_value(Prop, Keyword, mbcs, Container),
    Sbcs+Mbcs.

container_value(Prop, Keyword, Type, Container) when is_atom(Keyword)->
    container_value(Prop, 2, Type, Container);
container_value(Props, Pos, mbcs = Type, Container) when is_integer(Pos)->
    Pool = case proplists:get_value(mbcs_pool, Props) of
        PoolProps when PoolProps =/= undefined ->
            element(Pos, lists:keyfind(Container, 1, PoolProps));
        _ ->
            0
       end,
    TypeProps = proplists:get_value(Type, Props),
    Pool + element(Pos, lists:keyfind(Container, 1, TypeProps));

container_value(Props, Pos, Type, Container) ->
    TypeProps = proplists:get_value(Type, Props),
    element(Pos, lists:keyfind(Container, 1, TypeProps)).

process_info_keys() ->
    ?PROCESS_INFO_KEYS.

get_process_info() ->
    get_process_info(self()).
get_process_info(Pid) when is_pid(Pid) ->
    process_info(Pid, ?PROCESS_INFO_KEYS).

process_gc_info_keys() ->
    ?PROCESS_GC_KEYS.

get_process_gc_info() ->
    get_process_gc_info(self()).
get_process_gc_info(Pid) when is_pid(Pid) ->
    process_info(Pid, ?PROCESS_GC_KEYS).

get_process_group_leader_info(LeaderPid) when is_pid(LeaderPid) ->
    [{Key, Value}|| {Key, Value} <- process_info(LeaderPid), lists:member(Key, ?PROCESS_INFO_KEYS)].

get_process_limit() ->
    erlang:system_info(process_limit).

get_ets_list() ->
     ets:all().

get_ets_info() ->
    [get_ets_info(Tab) || Tab <- ets:all()].

get_ets_info(Tab) ->
    case ets:info(Tab) of
    undefined ->
        [];
    Entries when is_list(Entries) ->
        mapping(Entries)
    end.

get_ets_object() ->
    [{Tab, get_ets_object(Tab)} || Tab <- ets:all()].

get_ets_object(Tab) ->
    TabInfo = ets:info(Tab),
    Size = proplists:get_value(size, TabInfo),
    NameTab = proplists:get_value(named_table, TabInfo),
    if (Size == 0) or (NameTab == false) ->
        [];
    true ->
        ets:tab2list(Tab)
    end.

get_port_types() ->
    lists:usort(fun({KA, VA},{KB, VB})-> {VA, KB} >{VB, KA} end,
    ports_type_count([Type || {_Port, Type} <- ports_type_list()])).

get_port_info() ->
    [get_port_info(Port) ||Port <- erlang:ports()].

get_port_info(PortTerm) ->
    Port = transform_port(PortTerm),
    [port_info(Port, Type) || Type <- [meta, signals, io, memory_used, specific]].

port_info(Port, meta) ->
    {meta, List} = port_info_type(Port, meta, [id, name, os_pid]),
    case port_info(Port, registered_name)  of
        []   -> {meta, List};
        Name -> {meta, [Name | List]}
    end;

port_info(PortTerm, signals) ->
    port_info_type(PortTerm, signals, [connected, links, monitors]);

port_info(PortTerm, io) ->
    port_info_type(PortTerm, io, [input, output]);

port_info(PortTerm, memory_used) ->
    port_info_type(PortTerm, memory_used, [memory, queue_size]);

port_info(PortTerm, specific) ->
    Port = transform_port(PortTerm),
    Props = case erlang:port_info(Port, name) of
                {_, Type} when Type =:= "udp_inet";
                       Type =:= "tcp_inet";
                       Type =:= "sctp_inet" ->
                    try inet:getstat(Port) of
                        {ok, Stats} -> [{statistics, Stats}];
                        {error, _} -> []
                    catch
                        _Error:_Reason -> []
                    end ++
                    try inet:peername(Port) of
                        {ok, Peer} -> [{peername, Peer}];
                        _ -> []
                    catch
                        _Error:_Reason -> []
                    end ++
                    try inet:sockname(Port) of
                        {ok, Local} -> [{sockname, Local}];
                        {error, _}  -> []
                    catch
                        _Error:_Reason -> []
                    end ++
                    try inet:getopts(Port, ?SOCKET_OPTS ) of
                        {ok, Opts} -> [{options, Opts}];
                        {error, _} -> []
                    catch
                        _Error:_Reason -> []
                    end;
                {_, "efile"} ->
                    [];
                _ ->
                    []
            end,
    {specific, Props};
port_info(PortTerm, Keys) when is_list(Keys) ->
    Port = transform_port(PortTerm),
    [erlang:port_info(Port, Key) || Key <- Keys];
port_info(PortTerm, Key) when is_atom(Key) ->
    Port = transform_port(PortTerm),
    erlang:port_info(Port, Key).

port_info_type(PortTerm, Type, Keys) ->
    Port = transform_port(PortTerm),
    {Type, [erlang:port_info(Port, Key) || Key <- Keys]}.

transform_port(Port) when is_port(Port)  -> Port;
transform_port("#Port<0." ++ Id) ->
    N = list_to_integer(lists:sublist(Id, length(Id) - 1)),
    transform_port(N);
transform_port(N) when is_integer(N) ->
    Name = iolist_to_binary(atom_to_list(node())),
    NameLen = iolist_size(Name),
    Vsn = binary:last(term_to_binary(self())),
    Bin = <<131, 102, 100, NameLen:2/unit:8, Name:NameLen/binary, N:4/unit:8, Vsn:8>>,
    binary_to_term(Bin).

ports_type_list() ->
    [{Port, PortType} || Port <- erlang:ports(),
                         {_, PortType} <- [erlang:port_info(Port, name)]].

ports_type_count(Types) ->
    DictTypes = lists:foldl(fun(Type, Acc)->
            dict:update_counter(Type, 1, Acc)
        end, dict:new(), Types),
    dict:to_list(DictTypes).

mapping(Entries) ->
    mapping(Entries, []).
mapping([], Acc) -> Acc;
mapping([{owner, V}|Entries], Acc) when is_pid(V) ->
    OwnerInfo = process_info(V),
    Owner = proplists:get_value(registered_name, OwnerInfo, undefined),
    mapping(Entries, [{owner, Owner}|Acc]);
mapping([{Key, Value}|Entries], Acc) ->
    mapping(Entries, [{Key, Value}|Acc]).

avg1() ->
    compat_windows(fun cpu_sup:avg1/0).

avg5() ->
    compat_windows(fun cpu_sup:avg5/0).

avg15() ->
    compat_windows(fun cpu_sup:avg15/0).

cpu_util() ->
    compat_windows(fun cpu_sup:util/0).

compat_windows(Fun) ->
    case os:type() of
        {win32, nt} -> 0;
        _Type ->
            case catch Fun() of
                Val when is_number(Val) -> Val;
                _Error -> 0
            end
    end.

