%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Common Test Helper proxy module for slave -> peer migration.
%% OTP 26 has slave module deprecated, use peer instead.

-module(emqx_cth_peer).

-export([start/2, start/3, start/4]).
-export([start_link/2, start_link/3, start_link/4]).
-export([stop/1]).
-export([kill/1]).

start(Name, Args) ->
    start(Name, Args, []).

start(Name, Args, Envs) ->
    start(Name, Args, Envs, timer:seconds(20)).

start(Name, Args, Envs, Timeout) when is_atom(Name) ->
    do_start(Name, Args, Envs, Timeout, start).

start_link(Name, Args) ->
    start_link(Name, Args, []).

start_link(Name, Args, Envs) ->
    start_link(Name, Args, Envs, timer:seconds(20)).

start_link(Name, Args, Envs, Timeout) when is_atom(Name) ->
    do_start(Name, Args, Envs, Timeout, start_link).

do_start(Name0, Args, Envs, Timeout, Func) when is_atom(Name0) ->
    {Name, Host} = parse_node_name(Name0),
    {ok, Pid, Node} = peer:Func(#{
        name => Name,
        host => Host,
        args => Args,
        env => Envs,
        wait_boot => Timeout,
        longnames => true,
        shutdown => {halt, 1000}
    }),
    true = register(Node, Pid),
    {ok, Node}.

stop(Node) when is_atom(Node) ->
    Pid = whereis(Node),
    case is_pid(Pid) of
        true ->
            unlink(Pid),
            ok = peer:stop(Pid);
        false ->
            ct:pal("The control process for node ~p is unexpectedly down", [Node]),
            ok
    end.

%% @doc Kill a node abruptly, through mechanisms provided by OS.
%% Relies on POSIX `kill`.
kill(Node) ->
    try erpc:call(Node, os, getpid, []) of
        OSPid ->
            Pid = whereis(Node),
            _ = is_pid(Pid) andalso unlink(Pid),
            Result = kill_os_process(OSPid),
            %% Either ensure control process stops, or try to stop if not killed.
            _ = is_pid(Pid) andalso catch peer:stop(Pid),
            Result
    catch
        error:{erpc, _} = Reason ->
            {error, Reason}
    end.

kill_os_process(OSPid) ->
    Cmd = "kill -SIGKILL " ++ OSPid,
    Port = erlang:open_port({spawn, Cmd}, [binary, exit_status, hide]),
    receive
        {Port, {exit_status, 0}} ->
            ok;
        {Port, {exit_status, EC}} ->
            {error, EC}
    end.

parse_node_name(NodeName) ->
    case string:tokens(atom_to_list(NodeName), "@") of
        [Name, Host] ->
            {list_to_atom(Name), Host};
        [_] ->
            {NodeName, host()}
    end.

host() ->
    [_Name, Host] = string:tokens(atom_to_list(node()), "@"),
    Host.
