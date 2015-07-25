%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd control commands.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_ctl).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-define(PRINT_MSG(Msg), 
    io:format(Msg)).

-define(PRINT(Format, Args), 
    io:format(Format, Args)).

-export([status/1,
         vm/1,
         broker/1,
         stats/1,
         metrics/1,
         cluster/1,
         listeners/1,
         bridges/1,
         plugins/1,
         trace/1,
         useradd/1,
         userdel/1]).

%%------------------------------------------------------------------------------
%% @doc Query node status
%% @end
%%------------------------------------------------------------------------------
status([]) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p~n", [node(), InternalStatus]),
    case lists:keysearch(emqttd, 1, application:which_applications()) of
    false ->
        ?PRINT_MSG("emqttd is not running~n");
    {value,_Version} ->
        ?PRINT_MSG("emqttd is running~n")
    end.

%%------------------------------------------------------------------------------
%% @doc Cluster with other node
%% @end
%%------------------------------------------------------------------------------
cluster([]) ->
    Nodes = [node()|nodes()],
    ?PRINT("cluster nodes: ~p~n", [Nodes]);

cluster([SNode]) ->
    Node = node_name(SNode),
    case net_adm:ping(Node) of
    pong ->
        case emqttd:is_running(Node) of
            true ->
                %%TODO: should not unload here.
                emqttd:unload_all_plugins(),
                application:stop(emqttd),
                application:stop(esockd),
                application:stop(gproc),
                emqttd_mnesia:cluster(Node),
                application:start(gproc),
                application:start(esockd),
                application:start(emqttd),
                ?PRINT("cluster with ~p successfully.~n", [Node]);
            false ->
                ?PRINT("emqttd is not running on ~p~n", [Node])
        end;
    pang ->
        ?PRINT("failed to connect to ~p~n", [Node])
    end.

%%------------------------------------------------------------------------------
%% @doc Add user
%% @end
%%------------------------------------------------------------------------------
useradd([Username, Password]) ->
    ?PRINT("~p~n", [emqttd_auth_username:add_user(bin(Username), bin(Password))]).

%%------------------------------------------------------------------------------
%% @doc Delete user
%% @end
%%------------------------------------------------------------------------------
userdel([Username]) ->
    ?PRINT("~p~n", [emqttd_auth_username:remove_user(bin(Username))]).

vm([]) ->
    [vm([Name]) || Name <- ["load", "memory", "process", "io"]];

vm(["load"]) ->
    ?PRINT_MSG("Load: ~n"),
    [?PRINT("  ~s:~s~n", [L, V]) || {L, V} <- emqttd_vm:loads()];

vm(["memory"]) ->
    ?PRINT_MSG("Memory: ~n"),
    [?PRINT("  ~s:~p~n", [Cat, Val]) || {Cat, Val} <- erlang:memory()];

vm(["process"]) ->
    ?PRINT_MSG("Process: ~n"),
    ?PRINT("  process_limit:~p~n", [erlang:system_info(process_limit)]),
    ?PRINT("  process_count:~p~n", [erlang:system_info(process_count)]);

vm(["io"]) ->
    ?PRINT_MSG("IO: ~n"),
    ?PRINT("  max_fds:~p~n", [proplists:get_value(max_fds, erlang:system_info(check_io))]).

broker([]) ->
    Funs = [sysdescr, version, uptime, datetime],
    [?PRINT("~s: ~s~n", [Fun, emqttd_broker:Fun()]) || Fun <- Funs].

stats([]) ->
    [?PRINT("~s: ~p~n", [Stat, Val]) || {Stat, Val} <- emqttd_stats:getstats()].
    
metrics([]) ->
    [?PRINT("~s: ~p~n", [Metric, Val]) || {Metric, Val} <- emqttd_metrics:all()].
    
listeners([]) ->
    lists:foreach(fun({{Protocol, Port}, Pid}) ->
                ?PRINT("listener ~s:~p~n", [Protocol, Port]), 
                ?PRINT("  acceptors: ~p~n", [esockd:get_acceptors(Pid)]),
                ?PRINT("  max_clients: ~p~n", [esockd:get_max_clients(Pid)]),
                ?PRINT("  current_clients: ~p~n", [esockd:get_current_clients(Pid)])
        end, esockd:listeners()).

bridges(["list"]) ->
    lists:foreach(fun({{Node, Topic}, _Pid}) ->
                ?PRINT("bridge: ~s ~s~n", [Node, Topic])
        end, emqttd_bridge_sup:bridges());

bridges(["options"]) ->
    ?PRINT_MSG("Options:~n"),
    ?PRINT_MSG("  qos     = 0 | 1 | 2~n"),
    ?PRINT_MSG("  prefix  = string~n"),
    ?PRINT_MSG("  suffix  = string~n"),
    ?PRINT_MSG("  queue   = integer~n"),
    ?PRINT_MSG("Example:~n"),
    ?PRINT_MSG("  qos=2,prefix=abc/,suffix=/yxz,queue=1000~n");

bridges(["start", SNode, Topic]) ->
    case emqttd_bridge_sup:start_bridge(list_to_atom(SNode), bin(Topic)) of
        {ok, _} -> ?PRINT_MSG("bridge is started.~n");
        {error, Error} -> ?PRINT("error: ~p~n", [Error])
    end;

bridges(["start", SNode, Topic, OptStr]) ->
    Opts = parse_opts(bridge, OptStr),
    case emqttd_bridge_sup:start_bridge(list_to_atom(SNode), bin(Topic), Opts) of
        {ok, _} -> ?PRINT_MSG("bridge is started.~n");
        {error, Error} -> ?PRINT("error: ~p~n", [Error])
    end;

bridges(["stop", SNode, Topic]) ->
    case emqttd_bridge_sup:stop_bridge(list_to_atom(SNode), bin(Topic)) of
        ok -> ?PRINT_MSG("bridge is stopped.~n");
        {error, Error} -> ?PRINT("error: ~p~n", [Error])
    end.

plugins(["list"]) ->
    Plugins = emqttd:loaded_plugins(),
    lists:foreach(fun(Plugin) -> ?PRINT("~p~n", [Plugin]) end, Plugins);

plugins(["load", Name]) ->
    case emqttd:load_plugin(list_to_atom(Name)) of
        ok -> ?PRINT("plugin ~s is loaded successfully.~n", [Name]);
        {error, Reason} -> ?PRINT("error: ~s~n", [Reason])
    end;

plugins(["unload", Name]) ->
    case emqttd:unload_plugin(list_to_atom(Name)) of
        ok -> ?PRINT("plugin ~s is unloaded successfully.~n", [Name]);
        {error, Reason} -> ?PRINT("error: ~s~n", [Reason])
    end.

trace(["list"]) ->
    lists:foreach(fun({{Who, Name}, LogFile}) -> 
            ?PRINT("trace ~s ~s -> ~s~n", [Who, Name, LogFile])
        end, emqttd_trace:all_traces());

trace(["client", ClientId, "off"]) ->
    stop_trace(client, ClientId);
trace(["client", ClientId, LogFile]) ->
    start_trace(client, ClientId, LogFile);
trace(["topic", Topic, "off"]) ->
    stop_trace(topic, Topic);
trace(["topic", Topic, LogFile]) ->
    start_trace(topic, Topic, LogFile).

start_trace(Who, Name, LogFile) ->
    case emqttd_trace:start_trace({Who, bin(Name)}, LogFile) of
        ok -> 
            ?PRINT("trace ~s ~s successfully.~n", [Who, Name]);
        {error, Error} ->
            ?PRINT("trace ~s ~s error: ~p~n", [Who, Name, Error])
    end.
stop_trace(Who, Name) ->
    case emqttd_trace:stop_trace({Who, bin(Name)}) of
        ok -> 
            ?PRINT("stop to trace ~s ~s successfully.~n", [Who, Name]);
        {error, Error} ->
            ?PRINT("stop to trace ~s ~s error: ~p.~n", [Who, Name, Error])
    end.

node_name(SNode) ->
    SNode1 =
    case string:tokens(SNode, "@") of
    [_Node, _Server] ->
        SNode;
    _ ->
        case net_kernel:longnames() of
         true ->
             SNode ++ "@" ++ inet_db:gethostname() ++
                  "." ++ inet_db:res_option(domain);
         false ->
             SNode ++ "@" ++ inet_db:gethostname();
         _ ->
             SNode
         end
    end,
    list_to_atom(SNode1).

bin(S) when is_list(S) -> list_to_binary(S);
bin(B) when is_binary(B) -> B.

parse_opts(Cmd, OptStr) ->
    Tokens = string:tokens(OptStr, ","),
    [parse_opt(Cmd, list_to_atom(Opt), Val)
        || [Opt, Val] <- [string:tokens(S, "=") || S <- Tokens]].

parse_opt(bridge, qos, Qos) ->
    {qos, list_to_integer(Qos)};
parse_opt(bridge, suffix, Suffix) ->
    {topic_suffix, list_to_binary(Suffix)};
parse_opt(bridge, prefix, Prefix) ->
    {topic_prefix, list_to_binary(Prefix)};
parse_opt(bridge, queue, Len) ->
    {max_queue_len, list_to_integer(Len)};
parse_opt(_Cmd, Opt, _Val) ->
    ?PRINT("Bad Option: ~s~n", [Opt]).

