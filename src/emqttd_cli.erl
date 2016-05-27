%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_cli).

-include("emqttd.hrl").

-include("emqttd_cli.hrl").

-include("emqttd_protocol.hrl").

-import(lists, [foreach/2]).

-import(proplists, [get_value/2]).

-export([load/0]).

-export([status/1, broker/1, cluster/1, users/1, clients/1, sessions/1,
         routes/1, topics/1, subscriptions/1, plugins/1, bridges/1,
         listeners/1, vm/1, mnesia/1, trace/1]).

-define(PROC_INFOKEYS, [status,
                        memory,
                        message_queue_len,
                        total_heap_size,
                        heap_size,
                        stack_size,
                        reductions]).

-define(MAX_LIMIT, 10000).

-define(APP, emqttd).

load() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    [emqttd_ctl:register_cmd(Cmd, {?MODULE, Cmd}, []) || Cmd <- Cmds].

is_cmd(Fun) ->
    not lists:member(Fun, [init, load, module_info]).

%%--------------------------------------------------------------------
%% Commands
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Node status
status([]) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    ?PRINT("Node ~p is ~p~n", [node(), InternalStatus]),
    case lists:keysearch(?APP, 1, application:which_applications()) of
        false ->
            ?PRINT_MSG("emqttd is not running~n");
        {value, {?APP, _Desc, Vsn}} ->
            ?PRINT("emqttd ~s is running~n", [Vsn])
    end;
status(_) ->
     ?PRINT_CMD("status", "Show broker status").

%%--------------------------------------------------------------------
%% @doc Query broker
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
    Pubsubs = supervisor:which_children(emqttd_pubsub_sup:pubsub_pool()),
    foreach(fun({{_, Id}, Pid, _, _}) ->
                ProcInfo = erlang:process_info(Pid, ?PROC_INFOKEYS),
                ?PRINT("pubsub: ~w~n", [Id]),
                foreach(fun({Key, Val}) ->
                            ?PRINT("  ~-18s: ~w~n", [Key, Val])
                        end, ProcInfo)
             end, lists:reverse(Pubsubs));

broker(_) ->
    ?USAGE([{"broker",         "Show broker version, uptime and description"},
            {"broker pubsub",  "Show process_info of pubsub"},
            {"broker stats",   "Show broker statistics of clients, topics, subscribers"},
            {"broker metrics", "Show broker metrics"}]).

%%--------------------------------------------------------------------
%% @doc Cluster with other nodes
cluster(["join", SNode]) ->
    case emqttd_cluster:join(emqttd_node:parse_name(SNode)) of
        ok ->
            ?PRINT_MSG("Join the cluster successfully.~n"),
            cluster(["status"]);
        {error, Error} ->
            ?PRINT("Failed to join the cluster: ~p~n", [Error])
    end;

cluster(["leave"]) ->
    case emqttd_cluster:leave() of
        ok ->
            ?PRINT_MSG("Leave the cluster successfully.~n"),
            cluster(["status"]);
        {error, Error} ->
            ?PRINT("Failed to leave the cluster: ~p~n", [Error])
    end;

cluster(["remove", SNode]) ->
    case emqttd_cluster:remove(emqttd_node:parse_name(SNode)) of
        ok ->
            ?PRINT_MSG("Remove the node from cluster successfully.~n"),
            cluster(["status"]);
        {error, Error} ->
            ?PRINT("Failed to remove the node from cluster: ~p~n", [Error])
    end;

cluster(["status"]) ->
    ?PRINT("Cluster status: ~p~n", [emqttd_cluster:status()]);

cluster(_) ->
    ?USAGE([{"cluster join <Node>",  "Join the cluster"},
            {"cluster leave",        "Leave the cluster"},
            {"cluster remove <Node>","Remove the node from cluster"},
            {"cluster status",       "Cluster status"}]).

%%--------------------------------------------------------------------
%% @doc Users usage
users(Args) -> emqttd_auth_username:cli(Args).

%%--------------------------------------------------------------------
%% @doc Query clients
clients(["list"]) ->
    dump(mqtt_client);

clients(["show", ClientId]) ->
    if_client(ClientId, fun print/1);

clients(["kick", ClientId]) ->
    if_client(ClientId, fun(#mqtt_client{client_pid = Pid}) -> emqttd_client:kick(Pid) end);

clients(_) ->
    ?USAGE([{"clients list",            "List all clients"},
            {"clients show <ClientId>", "Show a client"},
            {"clients kick <ClientId>", "Kick out a client"}]).

if_client(ClientId, Fun) ->
    case emqttd_cm:lookup(bin(ClientId)) of
        undefined -> ?PRINT_MSG("Not Found.~n");
        Client    -> Fun(Client)
    end.

%%--------------------------------------------------------------------
%% @doc Sessions Command
sessions(["list"]) ->
    [sessions(["list", Type]) || Type <- ["persistent", "transient"]];

sessions(["list", "persistent"]) ->
    dump(mqtt_persistent_session);

sessions(["list", "transient"]) ->
    dump(mqtt_transient_session);

sessions(["show", ClientId]) ->
    MP = {{bin(ClientId), '_'}, '_'},
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
    ?USAGE([{"sessions list",            "List all sessions"},
            {"sessions list persistent", "List all persistent sessions"},
            {"sessions list transient",  "List all transient sessions"},
            {"sessions show <ClientId>", "Show a session"}]).

%%--------------------------------------------------------------------
%% @doc Routes Command
routes(["list"]) ->
    if_could_print(route, fun print/1);

routes(["show", Topic]) ->
    print(mnesia:dirty_read(route, bin(Topic)));

routes(_) ->
    ?USAGE([{"routes list",         "List all routes"},
            {"routes show <Topic>", "Show a route"}]).

%%--------------------------------------------------------------------
%% @doc Topics Command
topics(["list"]) ->
    if_could_print(topic, fun print/1);

topics(["show", Topic]) ->
    print(mnesia:dirty_read(topic, bin(Topic)));

topics(_) ->
    ?USAGE([{"topics list",         "List all topics"},
            {"topics show <Topic>", "Show a topic"}]).

subscriptions(["list"]) ->
    if_could_print(subscription, fun print/1);

subscriptions(["list", "static"]) ->
    if_could_print(backend_subscription, fun print/1);

subscriptions(["show", ClientId]) ->
    case mnesia:dirty_read(subscription, bin(ClientId)) of
        []      -> ?PRINT_MSG("Not Found.~n");
        Records -> print(Records)
    end;

subscriptions(["add", ClientId, Topic, QoS]) ->
    Add = fun(IntQos) ->
            Subscription = #mqtt_subscription{subid = bin(ClientId),
                                              topic = bin(Topic),
                                              qos   = IntQos},
            case emqttd_backend:add_subscription(Subscription) of
                ok ->
                    ?PRINT_MSG("ok~n");
                {error, already_existed} ->
                    ?PRINT_MSG("Error: already existed~n");
                {error, Reason} ->
                    ?PRINT("Error: ~p~n", [Reason])
            end
          end,
    if_valid_qos(QoS, Add);

subscriptions(["del", ClientId]) ->
    Ok = emqttd_backend:del_subscriptions(bin(ClientId)),
    ?PRINT("~p~n", [Ok]);

subscriptions(["del", ClientId, Topic]) ->
    Ok = emqttd_backend:del_subscription(bin(ClientId), bin(Topic)),
    ?PRINT("~p~n", [Ok]);

subscriptions(_) ->
    ?USAGE([{"subscriptions list",                         "List all subscriptions"},
            {"subscriptions list static",                  "List all static subscriptions"},
            {"subscriptions show <ClientId>",              "Show subscriptions of a client"},
            {"subscriptions add <ClientId> <Topic> <QoS>", "Add a static subscription manually"},
            {"subscriptions del <ClientId>",               "Delete static subscriptions manually"},
            {"subscriptions del <ClientId> <Topic>",       "Delete a static subscription manually"}]).

if_could_print(Tab, Fun) ->
    case mnesia:table_info(Tab, size) of
        Size when Size >= ?MAX_LIMIT ->
            ?PRINT("Could not list, too many ~ss: ~p~n", [Tab, Size]);
        _Size ->
            Keys = mnesia:dirty_all_keys(Tab),
            foreach(fun(Key) -> Fun(ets:lookup(Tab, Key)) end, Keys)
    end.

if_valid_qos(QoS, Fun) ->
    try list_to_integer(QoS) of
        Int when ?IS_QOS(Int) -> Fun(Int);
        _ -> ?PRINT_MSG("QoS should be 0, 1, 2~n")
    catch _:_ ->
        ?PRINT_MSG("QoS should be 0, 1, 2~n")
    end.

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
    ?USAGE([{"plugins list",            "Show loaded plugins"},
            {"plugins load <Plugin>",   "Load plugin"},
            {"plugins unload <Plugin>", "Unload plugin"}]).

%%--------------------------------------------------------------------
%% @doc Bridges command
bridges(["list"]) ->
    foreach(fun({{Node, Topic}, _Pid}) ->
                ?PRINT("bridge: ~s--~s-->~s~n", [node(), Topic, Node])
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
    ?USAGE([{"bridges list",                 "List bridges"},
            {"bridges options",              "Bridge options"},
            {"bridges start <Node> <Topic>", "Start a bridge"},
            {"bridges start <Node> <Topic> <Options>", "Start a bridge with options"},
            {"bridges stop <Node> <Topic>", "Stop a bridge"}]).

parse_opts(Cmd, OptStr) ->
    Tokens = string:tokens(OptStr, ","),
    [parse_opt(Cmd, list_to_atom(Opt), Val)
        || [Opt, Val] <- [string:tokens(S, "=") || S <- Tokens]].
parse_opt(bridge, qos, Qos) ->
    {qos, list_to_integer(Qos)};
parse_opt(bridge, suffix, Suffix) ->
    {topic_suffix, bin(Suffix)};
parse_opt(bridge, prefix, Prefix) ->
    {topic_prefix, bin(Prefix)};
parse_opt(bridge, queue, Len) ->
    {max_queue_len, list_to_integer(Len)};
parse_opt(_Cmd, Opt, _Val) ->
    ?PRINT("Bad Option: ~s~n", [Opt]).

%%--------------------------------------------------------------------
%% @doc vm command
vm([]) ->
    vm(["all"]);

vm(["all"]) ->
    [vm([Name]) || Name <- ["load", "memory", "process", "io", "ports"]];

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

vm(["ports"]) ->
    foreach(fun({Name, Key}) ->
                ?PRINT("ports/~-16s: ~w~n", [Name, erlang:system_info(Key)])
            end, [{count, port_count}, {limit, port_limit}]);

vm(_) ->
    ?USAGE([{"vm all",     "Show info of Erlang VM"},
            {"vm load",    "Show load of Erlang VM"},
            {"vm memory",  "Show memory of Erlang VM"},
            {"vm process", "Show process of Erlang VM"},
            {"vm io",      "Show IO of Erlang VM"},
            {"vm ports",   "Show Ports of Erlang VM"}]).

%%--------------------------------------------------------------------
%% @doc mnesia Command
mnesia([]) ->
    mnesia:system_info();

mnesia(_) ->
    ?PRINT_CMD("mnesia", "Mnesia system info").

%%--------------------------------------------------------------------
%% @doc Trace Command
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
    ?USAGE([{"trace list",                       "List all traces"},
            {"trace client <ClientId> <LogFile>","Trace a client"},
            {"trace client <ClientId> off",      "Stop tracing a client"},
            {"trace topic <Topic> <LogFile>",    "Trace a topic"},
            {"trace topic <Topic> off",          "Stop tracing a Topic"}]).

trace_on(Who, Name, LogFile) ->
    case emqttd_trace:start_trace({Who, iolist_to_binary(Name)}, LogFile) of
        ok ->
            ?PRINT("trace ~s ~s successfully.~n", [Who, Name]);
        {error, Error} ->
            ?PRINT("trace ~s ~s error: ~p~n", [Who, Name, Error])
    end.

trace_off(Who, Name) ->
    case emqttd_trace:stop_trace({Who, iolist_to_binary(Name)}) of
        ok -> 
            ?PRINT("stop tracing ~s ~s successfully.~n", [Who, Name]);
        {error, Error} ->
            ?PRINT("stop tracing ~s ~s error: ~p.~n", [Who, Name, Error])
    end.

%%--------------------------------------------------------------------
%% @doc Listeners Command
listeners([]) ->
    foreach(fun({{Protocol, ListenOn}, Pid}) ->
                Info = [{acceptors,      esockd:get_acceptors(Pid)},
                        {max_clients,    esockd:get_max_clients(Pid)},
                        {current_clients,esockd:get_current_clients(Pid)},
                        {shutdown_count, esockd:get_shutdown_count(Pid)}],
                ?PRINT("listener on ~s:~s~n", [Protocol, esockd:to_string(ListenOn)]),
                foreach(fun({Key, Val}) ->
                            ?PRINT("  ~-16s: ~w~n", [Key, Val])
                        end, Info)
            end, esockd:listeners());

listeners(_) ->
    ?PRINT_CMD("listeners", "List listeners").

%%--------------------------------------------------------------------
%% Dump ETS
%%--------------------------------------------------------------------

dump(Table) ->
    dump(Table, ets:first(Table)).

dump(_Table, '$end_of_table') ->
    ok;

dump(Table, Key) ->
    case ets:lookup(Table, Key) of
        [Record] -> print(Record);
        [] -> ok
    end,
    dump(Table, ets:next(Table, Key)).

print([]) ->
    ok;

print(Routes = [#mqtt_route{topic = Topic} | _]) ->
    Nodes = [atom_to_list(Node) || #mqtt_route{node = Node} <- Routes],
    ?PRINT("~s -> ~s~n", [Topic, string:join(Nodes, ",")]);

print(Subscriptions = [#mqtt_subscription{subid = ClientId} | _]) ->
    TopicTable = [io_lib:format("~s:~w", [Topic, Qos])
                  || #mqtt_subscription{topic = Topic, qos = Qos} <- Subscriptions],
    ?PRINT("~s -> ~s~n", [ClientId, string:join(TopicTable, ",")]);

print(Topics = [#mqtt_topic{}|_]) ->
    foreach(fun print/1, Topics);

print(#mqtt_plugin{name = Name, version = Ver, descr = Descr, active = Active}) ->
    ?PRINT("Plugin(~s, version=~s, description=~s, active=~s)~n",
           [Name, Ver, Descr, Active]);

print(#mqtt_client{client_id = ClientId, clean_sess = CleanSess, username = Username,
                   peername = Peername, connected_at = ConnectedAt}) ->
    ?PRINT("Client(~s, clean_sess=~s, username=~s, peername=~s, connected_at=~p)~n",
           [ClientId, CleanSess, Username, emqttd_net:format(Peername),
            emqttd_time:now_to_secs(ConnectedAt)]);

print(#mqtt_topic{topic = Topic, flags = Flags}) ->
    ?PRINT("~s: ~s~n", [Topic, string:join([atom_to_list(F) || F <- Flags], ",")]);

print(#mqtt_route{topic = Topic, node = Node}) ->
    ?PRINT("~s -> ~s~n", [Topic, Node]);

print({{ClientId, _ClientPid}, SessInfo}) ->
    InfoKeys = [clean_sess, 
                max_inflight,
                inflight_queue,
                message_queue,
                message_dropped,
                awaiting_rel,
                awaiting_ack,
                awaiting_comp,
                created_at],
    ?PRINT("Session(~s, clean_sess=~s, max_inflight=~w, inflight_queue=~w, "
           "message_queue=~w, message_dropped=~w, "
           "awaiting_rel=~w, awaiting_ack=~w, awaiting_comp=~w, "
           "created_at=~w)~n",
            [ClientId | [format(Key, get_value(Key, SessInfo)) || Key <- InfoKeys]]).

format(created_at, Val) ->
    emqttd_time:now_to_secs(Val);

format(_, Val) ->
    Val.

bin(S) -> iolist_to_binary(S).

