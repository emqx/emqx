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
		     min_heap_size]).%,
		     %fullsweep_after]).

-author("Feng Lee <feng@emqtt.io>").

-export([loads/0,
	scheduler_usage/1]).

-export([get_memory/0]).

-export([get_process_list/0,
 	 get_process_info/0,
 	 get_process_gc/0,
 	 get_process_group_leader_info/1]).
	
-export([get_ets_list/0,
 	 get_ets_info/0,
	 get_ets_info/1,
 	 get_ets_object/0,
	 get_ets_object/1]).

-export([get_port_types/0]).


loads() ->
    [{load1, ftos(cpu_sup:avg1()/256)},
     {load5, ftos(cpu_sup:avg5()/256)},
     {load15, ftos(cpu_sup:avg15()/256)}].

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

pid_port_fun_to_atom(Term) when is_pid(Term) ->
    erlang:list_to_atom(pid_to_list(Term));
pid_port_fun_to_atom(Term) when is_port(Term) ->
    erlang:list_to_atom(erlang:port_to_list(Term));
pid_port_fun_to_atom(Term) when is_function(Term) ->
    erlang:list_to_atom(erlang:fun_to_list(Term));
pid_port_fun_to_atom(Term) ->
    Term.
