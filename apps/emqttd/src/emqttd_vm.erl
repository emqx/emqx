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

-author("Feng Lee <feng@emqtt.io>").

-export([loads/0,
	 scheduler_usage/1,
 	 get_memory/0,
 	 get_process_list/0]).

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

convert_pid_info({initial_call,{_M, F, _A}}) ->
    {initial_call, F};
convert_pid_info({current_function, {M, F, A}}) ->
    {current_function, list_to_atom(lists:concat([atom_to_list(M),":",atom_to_list(F),"/",integer_to_list(A)]))};
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
