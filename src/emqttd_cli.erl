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
%%% emqttd cli.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_cli).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_cli.hrl").

-import(lists, [foreach/2]).

-import(proplists, [get_value/2]).

-export([load/0]).

-export([status/1, broker/1, cluster/1, bridges/1,
         clients/1, sessions/1, plugins/1, listeners/1,
         vm/1, mnesia/1, trace/1]).

-define(PROC_INFOKEYS, [status,
                        memory,
                        message_queue_len,
                        total_heap_size,
                        heap_size,
                        stack_size,
                        reductions]).

load() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    [emqttd_ctl:register_cmd(Cmd, {?MODULE, Cmd}, []) || Cmd <- Cmds].

is_cmd(Fun) ->
    not lists:member(Fun, [init, load, module_info]).

%%%=============================================================================
%%% Commands
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Node status
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
    end;
status(_) ->
     ?PRINT_CMD("status", "query broker status").

%%------------------------------------------------------------------------------
%% @doc Query broker
%% @end
%%------------------------------------------------------------------------------
broker([]) ->
    Funs = [sysdescr, version, uptime, datetime],
    foreach(fun(Fun) ->
                ?PRINT("~-10s: ~s~n", [Fun, emqttd_broker:Fun()])
            end, Funs);

broker(["stats"]) ->
    foreach(fun({Stat, Val}) ->
                ?PRINT("~-20s: ~w~n", [Stat, Val])
            end, emqttd_stats:getstats());

broker(["metrics"]) ->
    foreach(fun({Metric, Val}) ->
                ?PRINT("~-24s: ~w~n", [Metric, Val])
            end, lists:sort(emqttd_metrics:all()));

broker(["pubsub"]) ->
    Pubsubs = supervisor:which_children(emqttd_pubsub_sup),
    foreach(fun({{_, Id}, Pid, _, _}) ->
            ProcInfo = erlang:process_info(Pid, ?PROC_INFOKEYS),
            ?PRINT("pubsub: ~w~n", [Id]),
            foreach(fun({Key, Val}) ->
                        ?PRINT("  ~-18s: ~w~n", [Key, Val])
                    end, ProcInfo)
         end, lists:reverse(Pubsubs));

broker(_) ->
    ?USAGE([{"broker",         "query broker version, uptime and description"},
            {"broker pubsub",  "query process_info of pubsub"},
            {"broker stats",   "query broker statistics of clients, topics, subscribers"},
            {"broker metrics", "query broker metrics"}]).

%%------------------------------------------------------------------------------
%% @doc Cluster with other node
%% @end
%%------------------------------------------------------------------------------
cluster([]) ->
    Nodes = emqttd_broker:running_nodes(),
    ?PRINT("cluster nodes: ~p~n", [Nodes]);

cluster(usage) ->
    ?PRINT_CMD("cluster [<Node>]", "cluster with node, query cluster info ");

cluster([SNode]) ->
    Node = emqttd_dist:parse_node(SNode),
    case lists:member(Node, emqttd_broker:running_nodes()) of
        true ->
            ?PRINT("~s is already clustered~n", [Node]);
        false ->
            cluster(Node, fun() ->
                emqttd_plugins:unload(),
                application:stop(emqttd),
                application:stop(esockd),
                application:stop(gproc),
                emqttd_mnesia:cluster(Node),
                application:start(gproc),
                application:start(esockd),
                application:start(emqttd)
           end)
    end;

cluster(_) ->
    cluster(usage).

cluster(Node, DoCluster) ->
    cluster(net_adm:ping(Node), Node, DoCluster).

cluster(pong, Node, DoCluster) ->
    case emqttd:is_running(Node) of
        true ->
            DoCluster(),
            ?PRINT("cluster with ~s successfully.~n", [Node]);
        false ->
            ?PRINT("emqttd is not running on ~s~n", [Node])
    end;

cluster(pang, Node, _DoCluster) ->
    ?PRINT("Cannot connect to ~s~n", [Node]).

%%------------------------------------------------------------------------------
%% @doc Query clients
%% @end
%%------------------------------------------------------------------------------
clients(["list"]) ->
    emqttd_mnesia:dump(ets, mqtt_client, fun print/1);

clients(["show", ClientId]) ->
    case emqttd_cm:lookup(list_to_binary(ClientId)) of
        undefined -> ?PRINT_MSG("Not Found.~n");
        Client    -> print(Client)
    end;

clients(["kick", ClientId]) ->
    case emqttd_cm:lookup(list_to_binary(ClientId)) of
        undefined ->
            ?PRINT_MSG("Not Found.~n");
        #mqtt_client{client_pid = Pid} -> 
            emqttd_client:kick(Pid)
    end;

clients(_) ->
    ?USAGE([{"clients list",            "list all clients"},
            {"clients show <ClientId>", "show a client"},
            {"clients kick <ClientId>", "kick a client"}]).

%%------------------------------------------------------------------------------
%% @doc Sessions Command
%% @end
%%------------------------------------------------------------------------------
sessions(["list"]) ->
    [sessions(["list", Type]) || Type <- ["persistent", "transient"]];

sessions(["list", "persistent"]) ->
    emqttd_mnesia:dump(ets, mqtt_persistent_session, fun print/1);

sessions(["list", "transient"]) ->
    emqttd_mnesia:dump(ets, mqtt_transient_session,  fun print/1);

sessions(["show", ClientId]) ->
    MP = {{list_to_binary(ClientId), '_'}, '_'},
    case {ets:match_object(mqtt_transient_session,  MP),
          ets:match_object(mqtt_persistent_session, MP)} of
        {[], []} ->
            ?PRINT_MSG("Not Found.~n");
        {[SessInfo], _} ->
            print(SessInfo);
        {_, [SessInfo]} ->
            print(SessInfo)
    end;

sessions(_) ->
    ?USAGE([{"sessions list",            "list all sessions"},
            {"sessions list persistent", "list all persistent sessions"},
            {"sessions list transient",  "list all transient sessions"},
            {"sessions show <ClientId>", "show a session"}]).

plugins(["list"]) ->
    foreach(fun print/1, emqttd_plugins:list());

plugins(["load", Name]) ->
    case emqttd_plugins:load(list_to_atom(Name)) of
        {ok, StartedApps} ->
            ?PRINT("Start apps: ~p~nPlugin ~s loaded successfully.~n", [StartedApps, Name]);
        {error, Reason}   ->
            ?PRINT("load plugin error: ~p~n", [Reason])
    end;

plugins(["unload", Name]) ->
    case emqttd_plugins:unload(list_to_atom(Name)) of
        ok ->
            ?PRINT("Plugin ~s unloaded successfully.~n", [Name]);
        {error, Reason} ->
            ?PRINT("unload plugin error: ~p~n", [Reason])
    end;

plugins(_) ->
    ?USAGE([{"plugins list",            "show loaded plugins"},
            {"plugins load <Plugin>",   "load plugin"},
            {"plugins unload <Plugin>", "unload plugin"}]).

%%------------------------------------------------------------------------------
%% @doc Bridges command
%% @end
%%------------------------------------------------------------------------------

bridges(["list"]) ->
    foreach(fun({{Node, Topic}, _Pid}) ->
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
    case emqttd_bridge_sup:start_bridge(list_to_atom(SNode), list_to_binary(Topic)) of
        {ok, _}        -> ?PRINT_MSG("bridge is started.~n");
        {error, Error} -> ?PRINT("error: ~p~n", [Error])
    end;

bridges(["start", SNode, Topic, OptStr]) ->
    Opts = parse_opts(bridge, OptStr),
    case emqttd_bridge_sup:start_bridge(list_to_atom(SNode), list_to_binary(Topic), Opts) of
        {ok, _}        -> ?PRINT_MSG("bridge is started.~n");
        {error, Error} -> ?PRINT("error: ~p~n", [Error])
    end;

bridges(["stop", SNode, Topic]) ->
    case emqttd_bridge_sup:stop_bridge(list_to_atom(SNode), list_to_binary(Topic)) of
        ok             -> ?PRINT_MSG("bridge is stopped.~n");
        {error, Error} -> ?PRINT("error: ~p~n", [Error])
    end;

bridges(_) ->
    ?USAGE([{"bridges list",                 "query bridges"},
            {"bridges options",              "bridge options"},
            {"bridges start <Node> <Topic>", "start bridge"},
            {"bridges start <Node> <Topic> <Options>", "start bridge with options"},
            {"bridges stop <Node> <Topic>", "stop bridge"}]).

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

%%------------------------------------------------------------------------------
%% @doc vm command
%% @end
%%------------------------------------------------------------------------------
vm([]) ->
    vm(["all"]);

vm(["all"]) ->
    [vm([Name]) || Name <- ["load", "memory", "process", "io"]];

vm(["load"]) ->
    [?PRINT("cpu/~-20s: ~s~n", [L, V]) || {L, V} <- emqttd_vm:loads()];

vm(["memory"]) ->
    [?PRINT("memory/~-17s: ~w~n", [Cat, Val]) || {Cat, Val} <- erlang:memory()];

vm(["process"]) ->
    foreach(fun({Name, Key}) ->
                ?PRINT("process/~-16s: ~w~n", [Name, erlang:system_info(Key)])
            end, [{limit, process_limit}, {count, process_count}]);

vm(["io"]) ->
    IoInfo = erlang:system_info(check_io),
    foreach(fun(Key) ->
                ?PRINT("io/~-21s: ~w~n", [Key, get_value(Key, IoInfo)])
            end, [max_fds, active_fds]);

vm(_) ->
    ?USAGE([{"vm all",     "query info of erlang vm"},
            {"vm load",    "query load of erlang vm"},
            {"vm memory",  "query memory of erlang vm"},
            {"vm process", "query process of erlang vm"},
            {"vm io",      "queue io of erlang vm"}]).

%%------------------------------------------------------------------------------
%% @doc mnesia Command
%% @end
%%------------------------------------------------------------------------------
mnesia([]) ->
    mnesia:system_info();

mnesia(_) ->
    ?PRINT_CMD("mnesia", "mnesia system info").

%%------------------------------------------------------------------------------
%% @doc Trace Command
%% @end
%%------------------------------------------------------------------------------
trace(["list"]) ->
    foreach(fun({{Who, Name}, LogFile}) ->
                ?PRINT("trace ~s ~s -> ~s~n", [Who, Name, LogFile])
            end, emqttd_trace:all_traces());

trace(["client", ClientId, "off"]) ->
    trace_off(client, ClientId);

trace(["client", ClientId, LogFile]) ->
    trace_on(client, ClientId, LogFile);

trace(["topic", Topic, "off"]) ->
    trace_off(topic, Topic);

trace(["topic", Topic, LogFile]) ->
    trace_on(topic, Topic, LogFile);

trace(_) ->
    ?USAGE([{"trace list",                       "query all traces"},
            {"trace client <ClientId> <LogFile>","trace client with ClientId"},
            {"trace client <ClientId> off",      "stop to trace client"},
            {"trace topic <Topic> <LogFile>",    "trace topic with Topic"},
            {"trace topic <Topic> off",          "stop to trace Topic"}]).

trace_on(Who, Name, LogFile) ->
    case emqttd_trace:start_trace({Who, list_to_binary(Name)}, LogFile) of
        ok ->
            ?PRINT("trace ~s ~s successfully.~n", [Who, Name]);
        {error, Error} ->
            ?PRINT("trace ~s ~s error: ~p~n", [Who, Name, Error])
    end.

trace_off(Who, Name) ->
    case emqttd_trace:stop_trace({Who, list_to_binary(Name)}) of
        ok -> 
            ?PRINT("stop to trace ~s ~s successfully.~n", [Who, Name]);
        {error, Error} ->
            ?PRINT("stop to trace ~s ~s error: ~p.~n", [Who, Name, Error])
    end.

%%------------------------------------------------------------------------------
%% @doc Listeners Command
%% @end
%%------------------------------------------------------------------------------
listeners([]) ->
    foreach(fun({{Protocol, Port}, Pid}) ->
                Info = [{acceptors,      esockd:get_acceptors(Pid)},
                        {max_clients,    esockd:get_max_clients(Pid)},
                        {current_clients,esockd:get_current_clients(Pid)},
                        {shutdown_count, esockd:get_shutdown_count(Pid)}],
                ?PRINT("listener on ~s:~w~n", [Protocol, Port]),
                foreach(fun({Key, Val}) ->
                            ?PRINT("  ~-16s: ~w~n", [Key, Val])
                        end, Info)
            end, esockd:listeners());

listeners(_) ->
    ?PRINT_CMD("listeners", "query broker listeners").

print(#mqtt_plugin{name = Name, version = Ver, descr = Descr, active = Active}) ->
    ?PRINT("Plugin(~s, version=~s, description=~s, active=~s)~n",
               [Name, Ver, Descr, Active]);

print(#mqtt_client{client_id = ClientId, clean_sess = CleanSess,
                   username = Username, peername = Peername,
                   connected_at = ConnectedAt}) ->
    ?PRINT("Client(~s, clean_sess=~s, username=~s, peername=~s, connected_at=~p)~n",
            [ClientId, CleanSess, Username,
             emqttd_net:format(Peername),
             emqttd_util:now_to_secs(ConnectedAt)]);

print({{ClientId, _ClientPid}, SessInfo}) ->
    InfoKeys = [clean_sess, 
                max_inflight,
                inflight_queue,
                message_queue,
                message_dropped,
                awaiting_rel,
                awaiting_ack,
                awaiting_comp,
                created_at,
                subscriptions],
    ?PRINT("Session(~s, clean_sess=~s, max_inflight=~w, inflight_queue=~w, "
           "message_queue=~w, message_dropped=~w, "
           "awaiting_rel=~w, awaiting_ack=~w, awaiting_comp=~w, "
           "created_at=~w, subscriptions=~s)~n",
            [ClientId | [format(Key, proplists:get_value(Key, SessInfo)) || Key <- InfoKeys]]).

format(created_at, Val) ->
    emqttd_util:now_to_secs(Val);

format(subscriptions, List) ->
    string:join([io_lib:format("~s:~w", [Topic, Qos]) || {Topic, Qos} <- List], ",");

format(_, Val) ->
    Val.

