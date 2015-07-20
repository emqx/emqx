%%------------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------
%%% @doc
%%% emqttd erlang vm.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_vm).

-export([schedulers/0]).

-export([microsecs/0]).

-export([loads/0, mem_info/0, scheduler_usage/1]).

-export([get_memory/0]).

-export([get_process_list/0,
         get_process_info/0,
         get_process_gc/0,
         get_process_group_leader_info/1,
 	 get_process_limit/0]).
	
-export([get_ets_list/0,
         get_ets_info/0,
         get_ets_info/1,
         get_ets_object/0,
         get_ets_object/1]).

-export([get_port_types/0,
         get_port_info/0,
         get_port_info/1]).

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

-define(PROCESS_LIST, [initial_call,
                       reductions,
                       memory,
                       message_queue_len,
                       current_function]).

-define(PROCESS_INFO, [initial_call,
                       current_function,
                       registered_name,
                       status,
                       message_queue_len,
                       group_leader,
                       priority,
                       trap_exit,
                       reductions,
                       binary,
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
                      tos]).

schedulers() ->
    erlang:system_info(schedulers).

microsecs() ->
    {Mega, Sec, Micro} = erlang:now(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

loads() ->
    [{load1, ftos(cpu_sup:avg1()/256)},
     {load5, ftos(cpu_sup:avg5()/256)},
     {load15, ftos(cpu_sup:avg15()/256)}].

mem_info() ->
    Dataset = memsup:get_system_memory_data(),
    [{total_memory, proplists:get_value(total_memory, Dataset)},
     {used_memory, proplists:get_value(total_memory, Dataset) - proplists:get_value(free_memory, Dataset)}].

ftos(F) -> 
    [S] = io_lib:format("~.2f", [F]), S.

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
    lists:map(
	    fun({{I, A0, T0},{I, A1, T1}}) ->{I, (A1 - A0)/(T1 - T0)}end,
	    lists:zip(lists:sort(First), lists:sort(Last))
    ).

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
        A <- Allocators,
	Allocs <- [erlang:system_info({allocator, A})],
	Allocs =/= false,
	{_, N, Props} <- Allocs].

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
	    _ -> 0
	   end,
    TypeProps = proplists:get_value(Type, Props),
    Pool + element(Pos, lists:keyfind(Container, 1, TypeProps));

container_value(Props, Pos, Type, Container) ->
    TypeProps = proplists:get_value(Type, Props),
    element(Pos, lists:keyfind(Container, 1, TypeProps)).

get_process_list()->
    [get_process_list(Pid) || Pid <- processes()].

get_process_list(Pid) when is_pid(Pid) ->
    Info =  [process_info(Pid, Key) || Key <- ?PROCESS_LIST],
    [{pid, pid_port_fun_to_atom(Pid)}] ++ lists:flatten([convert_pid_info(Item) || Item <- Info]).

get_process_info() ->
    [get_process_info(Pid) || Pid <- processes()].
get_process_info(Pid) when is_pid(Pid) ->
    ProcessInfo = [process_info(Pid, Key) || Key <- ?PROCESS_INFO],
    lists:flatten([convert_pid_info(Item) || Item <- ProcessInfo]).

get_process_gc() ->
    [get_process_gc(Pid) || Pid <- processes()].
get_process_gc(Pid) when is_pid(Pid) ->
    GcInfo = [process_info(Pid, Key) || Key <- ?PROCESS_GC],
    lists:flatten([convert_pid_info(E) || E <- GcInfo]).

get_process_group_leader_info(LeaderPid) when is_pid(LeaderPid) ->
    LeaderInfo = [{Key, Value}|| {Key, Value} <- process_info(LeaderPid), lists:member(Key, ?PROCESS_INFO)],
    lists:flatten([convert_pid_info(E) || E <- LeaderInfo]).

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
    if  (Size == 0) or (NameTab == false) ->
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
		[] -> {meta, List};
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
		{_, Type}  when Type =:= "udp_inet";
				Type =:= "tcp_inet";
				Type =:= "sctp_inet" ->
					case catch inet:getstat(Port) of
						{ok, Stats} -> [{statistics, Stats}];
						_ ->[]

		end ++
		case catch inet:peername(Port) of
			{ok, Peer} ->[{peername, Peer}];
			{error, _} ->[]
	end ++
	case catch inet:sockname(Port) of
		{ok, Local} ->[{sockname, Local}];
		{error, _} -> []
end ++
case catch inet:getopts(Port, ?SOCKET_OPTS ) of
	{ok, Opts} -> [{options, Opts}];
	{error, _} -> []
end;
	{_, "efile"} ->
		[];
	_ ->[]
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
    Bin = <<131, 102, 100,
    	    NameLen:2/unit:8,
	    Name:NameLen/binary,
	    N:4/unit:8,
	    Vsn:8>>,
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
mapping([], Acc) ->
    Acc;
mapping([{owner, V}|Entries], Acc) when is_pid(V) ->
    OwnerInfo = process_info(V),
    Owner = proplists:get_value(registered_name, OwnerInfo, undefined),
    mapping(Entries, [{owner, pid_port_fun_to_atom(Owner)}|Acc]);
mapping([{Key, Value}|Entries], Acc) ->
    mapping(Entries, [{Key, pid_port_fun_to_atom(Value)}|Acc]).

%ip_to_binary(Tuple) ->
%    iolist_to_binary(string:join(lists:map(fun integer_to_list/1, tuple_to_list(Tuple)), ".")).

convert_pid_info({initial_call,{_M, F, _A}}) ->
    {initial_call, F};
convert_pid_info({current_function, {M, F, A}}) ->
    {current_function, list_to_atom(lists:concat([atom_to_list(M),":",atom_to_list(F),"/",integer_to_list(A)]))};
convert_pid_info({suspending, List}) ->
    {suspending, [pid_port_fun_to_atom(E) || E <- List]};
convert_pid_info({binary, List}) ->
    {binary,[tuple_to_list(E) || E <- List]};
convert_pid_info({Key, Term}) when is_pid(Term) or is_port(Term) or is_function(Term) ->
    {Key, pid_port_fun_to_atom(Term)};
convert_pid_info(Item) ->
    Item.

%convert_port_info({name, Name}) ->
%    {name, list_to_binary(Name)};
%convert_port_info({links, List}) ->
%    {links, [pid_port_fun_to_atom(Item) || Item <- List]};
%convert_port_info({connected, Pid}) ->
%    erlang:process_info(Pid, registered_name);
%convert_port_info(Item) ->
%    Item.


pid_port_fun_to_atom(Term) when is_pid(Term) ->
    erlang:list_to_atom(pid_to_list(Term));
pid_port_fun_to_atom(Term) when is_port(Term) ->
    erlang:list_to_atom(erlang:port_to_list(Term));
pid_port_fun_to_atom(Term) when is_function(Term) ->
    erlang:list_to_atom(erlang:fun_to_list(Term));
pid_port_fun_to_atom(Term) ->
    Term.
