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

-author("Feng Lee <feng@emqtt.io>").

-export([timestamp/0, microsecs/0]).

-export([loads/0]).

-define(SYSTEM_INFO, [
                      allocated_areas,
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

-define(SOCKET_OPTS, [
                      active,
                      broadcast,
                      delay_send,
                      dontroute,
                      exit_on_close,
                      header,
                      keepalive,
                      nodelay,
                      packet,
                      packet_size,
                      read_packets,
                      recbuf,
                      reuseaddr,
                      send_timeout,
                      send_timeout_close,
                      sndbuf,
                      priority,
                      tos
                     ]).



-export([loads/0,
	 get_system_info/0,
 	% get_statistics/0, 
 	% get_process_info/0,
 	 get_ports_info/0,
 	 get_ets_info/0]).

timestamp() ->
    {MegaSecs, Secs, _MicroSecs} = os:timestamp(),
    MegaSecs * 1000000 + Secs.

microsecs() ->
    {Mega, Sec, Micro} = erlang:now(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

loads() ->
    [{load1, ftos(cpu_sup:avg1()/256)},
     {load5, ftos(cpu_sup:avg5()/256)},
     {load15, ftos(cpu_sup:avg15()/256)}].

ftos(F) -> 
    [S] = io_lib:format("~.2f", [F]), S.

get_system_info() ->
    [{Key, format_system_info(Key, get_system_info(Key))} || Key <- ?SYSTEM_INFO].

get_system_info(Key) ->
    try erlang:system_info(Key) catch
                                    error:badarg->undefined
                                end.

%% conversion functions for erlang:system_info(Key)

format_system_info(allocated_areas, List) ->
    [convert_allocated_areas(Value) || Value <- List];
format_system_info(allocator, {_,_,_,List}) ->
    List;
format_system_info(dist_ctrl, List) ->
    lists:map(fun({Node, Socket}) ->
                      {ok, Stats} = inet:getstat(Socket),
                      {Node, Stats}
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


get_ports_info()->
    [{pid_port_fun_to_atom(Port), get_port_info(Port)} || Port <- erlang:ports()].

get_port_info(Port) ->
    Stat = get_socket_getstat(Port),
    SockName = get_socket_sockname(Port),
    Opts = get_socket_opts(Port),
    Protocol = get_socket_protocol(Port),
    Status = get_socket_status(Port),
    Type = get_socket_type(Port),

    lists:flatten(lists:append([
                                Stat,
                                SockName,
                                Opts,
                                Protocol,
                                Status,
                                Type
                               ])).

get_socket_getstat(Socket) ->
    case catch inet:getstat(Socket) of
        {ok, Info} ->
            Info;
        _ ->
            []
    end.

get_socket_sockname(Socket) ->
    case catch inet:sockname(Socket) of
        {ok, {Ip, Port}} ->
            [{ip, ip_to_binary(Ip)}, {port, Port}];
        _ ->
            []
    end.

ip_to_binary(Tuple) ->
    iolist_to_binary(string:join(lists:map(fun integer_to_list/1, tuple_to_list(Tuple)), ".")).


get_socket_protocol(Socket) ->
    case erlang:port_info(Socket, name) of
        {name, "tcp_inet"} ->
            [{protocol, tcp}];
        {name, "udp_inet"} ->
            [{protocol, udp}];
        {name,"sctp_inet"} ->
            [{protocol, sctp}];
        _ ->
            []
    end.

get_socket_status(Socket) ->
    case catch prim_inet:getstatus(Socket) of
        {ok, Status} ->
            [{status, Status}];
        _ ->
         []
    end.

get_socket_type(Socket) ->
    case catch prim_inet:gettype(Socket) of
        {ok, Type} ->
            [{type, tuple_to_list(Type)}];
        _ ->
         []
    end.

get_socket_opts(Socket) ->
    [get_socket_opts(Socket, Key) || Key <- ?SOCKET_OPTS].

get_socket_opts(Socket, Key) ->
    case catch inet:getopts(Socket, [Key]) of
        {ok, Opt} ->
            Opt;
        _ ->
            []
    end.

get_ets_info() ->
    [{Tab, get_ets_dets_info(ets, Tab)} || Tab <- ets:all()].

get_ets_dets_info(Type, Tab) ->
    case Type:info(Tab) of
        undefined -> [];
        Entries when is_list(Entries) ->
            [{Key, pid_port_fun_to_atom(Value)} || {Key, Value} <- Entries]
    end. 

pid_port_fun_to_atom(Term) when is_pid(Term) ->
    erlang:list_to_atom(pid_to_list(Term));
pid_port_fun_to_atom(Term) when is_port(Term) ->
    erlang:list_to_atom(erlang:port_to_list(Term));
pid_port_fun_to_atom(Term) when is_function(Term) ->
    erlang:list_to_atom(erlang:fun_to_list(Term));
pid_port_fun_to_atom(Term) ->
    Term.
