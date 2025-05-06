%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Common Test Helper proxy module for slave -> peer migration.
%% OTP 26 has slave module deprecated, use peer instead.

-module(emqx_cth_peer).

-export([start/2, start/3, start/4, start/5]).
-export([start_link/2, start_link/3, start_link/4, start_link/5]).
-export([stop/1]).
-export([kill/1]).

start(Name, Args) ->
    start(Name, Args, []).

start(Name, Args, Envs) ->
    start(Name, Args, Envs, timer:seconds(20)).

start(Name, Args, Envs, Timeout) when is_atom(Name) ->
    start(Name, Args, Envs, Timeout, _Opts = #{}).

start(Name, Args, Envs, Timeout, #{} = Opts) when is_atom(Name) ->
    do_start(Name, Args, Envs, Timeout, start, Opts).

start_link(Name, Args) ->
    start_link(Name, Args, []).

start_link(Name, Args, Envs) ->
    start_link(Name, Args, Envs, timer:seconds(20)).

start_link(Name, Args, Envs, TimeoutOrWaitBoot) when is_atom(Name) ->
    start_link(Name, Args, Envs, TimeoutOrWaitBoot, _Opts = #{}).

start_link(Name, Args, Envs, TimeoutOrWaitBoot, #{} = Opts) when is_atom(Name) ->
    do_start(Name, Args, Envs, TimeoutOrWaitBoot, start_link, Opts).

do_start(Name0, Args, Envs, TimeoutOrWaitBoot, Func, Opts0) when is_atom(Name0) ->
    {Name, Host} = parse_node_name(Name0),
    Opts = maps:merge(
        #{
            name => Name,
            host => Host,
            args => Args,
            env => Envs,
            wait_boot => TimeoutOrWaitBoot,
            longnames => true,
            shutdown => {halt, 1_000}
        },
        Opts0
    ),
    {ok, Pid, Node} = peer:Func(Opts),
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
