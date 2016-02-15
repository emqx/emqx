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

-export([status/1, broker/1, cluster/1, bridges/1,
         clients/1, sessions/1, topics/1, subscriptions/1,
         plugins/1, listeners/1, vm/1, mnesia/1, trace/1]).

-define(PROC_INFOKEYS, [status,
                        memory,
                        message_queue_len,
                        total_heap_size,
                        heap_size,
                        stack_size,
                        reductions]).

-define(MAX_LINES, 20000).

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
     ?PRINT_CMD("status", "query broker status").

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
    ?USAGE([{"broker",         "query broker version, uptime and description"},
            {"broker pubsub",  "query process_info of pubsub"},
            {"broker stats",   "query broker statistics of clients, topics, subscribers"},
            {"broker metrics", "query broker metrics"}]).

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
%% @doc Query clients
clients(["list"]) ->
    dump(ets, mqtt_client, fun print/1);

clients(["show", ClientId]) ->
    if_client(ClientId, fun print/1);

clients(["kick", ClientId]) ->
    if_client(ClientId, fun(#mqtt_client{client_pid = Pid}) -> emqttd_client:kick(Pid) end);

clients(_) ->
    ?USAGE([{"clients list",            "list all clients"},
            {"clients show <ClientId>", "show a client"},
            {"clients kick <ClientId>", "kick a client"}]).

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
    dump(ets, mqtt_persistent_session, fun print/1);

sessions(["list", "transient"]) ->
    dump(ets, mqtt_transient_session,  fun print/1);

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
    ?USAGE([{"sessions list",            "list all sessions"},
            {"sessions list persistent", "list all persistent sessions"},
            {"sessions list transient",  "list all transient sessions"},
            {"sessions show <ClientId>", "show a session"}]).

%%--------------------------------------------------------------------
%% @doc Topics Command
topics(["list"]) ->
    Print = fun(Topic, Records) -> print(topic, Topic, Records) end,
    if_could_print(topic, Print);

topics(["show", Topic]) ->
    print(topic, Topic, ets:lookup(topic, bin(Topic)));

topics(_) ->
    ?USAGE([{"topics list",         "list all topics"},
            {"topics show <Topic>", "show a topic"}]).

subscriptions(["list"]) ->
    Print = fun(ClientId, Records) -> print(subscription, ClientId, Records) end,
    if_subscription(fun() -> if_could_print(subscription, Print) end);

subscriptions(["show", ClientId]) ->
    if_subscription(fun() ->
            case emqttd_pubsub:lookup(subscription, bin(ClientId)) of
                []      -> ?PRINT_MSG("Not Found.~n");
                Records -> print(subscription, ClientId, Records)
            end
        end);

subscriptions(["add", ClientId, Topic, QoS]) ->
    Create = fun(IntQos) ->
                Subscription = {bin(ClientId), bin(Topic), IntQos},
                case emqttd_pubsub:create(subscription, Subscription) of
                    ok             -> ?PRINT_MSG("ok~n");
                    {error, Error} -> ?PRINT("Error: ~p~n", [Error])
                end
             end,
    if_subscription(fun() -> if_valid_qos(QoS, Create) end);

subscriptions(["del", ClientId, Topic]) ->
    if_subscription(fun() ->
            Ok = emqttd_pubsub:delete(subscription, {bin(ClientId), bin(Topic)}),
            ?PRINT("~p~n", [Ok])
        end);

subscriptions(_) ->
    ?USAGE([{"subscriptions list",                         "list all subscriptions"},
            {"subscriptions show <ClientId>",              "show subscriptions of a client"},
            {"subscriptions add <ClientId> <Topic> <QoS>", "add subscription"},
            {"subscriptions del <ClientId> <Topic>",       "delete subscription"}]).

if_subscription(Fun) ->
    case ets:info(subscription, name) of
        undefined -> ?PRINT_MSG("Error: subscription table not found!~n");
        _         -> Fun()
    end.

if_could_print(Tab, Fun) ->
    case mnesia:table_info(Tab, size) of
        Size when Size >= ?MAX_LINES ->
            ?PRINT("Could not list, too many ~ss: ~p~n", [Tab, Size]);
        _Size ->
            Keys = mnesia:dirty_all_keys(Tab),
            foreach(fun(Key) -> Fun(Key, ets:lookup(Tab, Key)) end, Keys)
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
    ?USAGE([{"plugins list",            "show loaded plugins"},
            {"plugins load <Plugin>",   "load plugin"},
            {"plugins unload <Plugin>", "unload plugin"}]).

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

%%--------------------------------------------------------------------
%% @doc mnesia Command
mnesia([]) ->
    mnesia:system_info();

mnesia(_) ->
    ?PRINT_CMD("mnesia", "mnesia system info").

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
    ?USAGE([{"trace list",                       "query all traces"},
            {"trace client <ClientId> <LogFile>","trace client with ClientId"},
            {"trace client <ClientId> off",      "stop to trace client"},
            {"trace topic <Topic> <LogFile>",    "trace topic with Topic"},
            {"trace topic <Topic> off",          "stop to trace Topic"}]).

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
            ?PRINT("stop to trace ~s ~s successfully.~n", [Who, Name]);
        {error, Error} ->
            ?PRINT("stop to trace ~s ~s error: ~p.~n", [Who, Name, Error])
    end.

%%--------------------------------------------------------------------
%% @doc Listeners Command
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
             emqttd_time:now_to_secs(ConnectedAt)]);

print(#mqtt_topic{topic = Topic, node = Node}) ->
    ?PRINT("~s on ~s~n", [Topic, Node]);

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
            [ClientId | [format(Key, proplists:get_value(Key, SessInfo)) || Key <- InfoKeys]]).

print(topic, Topic, Records) ->
    Nodes = [Node || #mqtt_topic{node = Node} <- Records],
    ?PRINT("~s: ~p~n", [Topic, Nodes]);

print(subscription, ClientId, Subscriptions) ->
    TopicTable = [{Topic, Qos} || #mqtt_subscription{topic = Topic, qos = Qos} <- Subscriptions],
    ?PRINT("~s: ~p~n", [ClientId, TopicTable]).

format(created_at, Val) ->
    emqttd_time:now_to_secs(Val);

format(subscriptions, List) ->
    string:join([io_lib:format("~s:~w", [Topic, Qos]) || {Topic, Qos} <- List], ",");

format(_, Val) ->
    Val.

bin(S) -> iolist_to_binary(S).

%%TODO: ...
dump(ets, Table, Fun) ->
    dump(ets, Table, ets:first(Table), Fun).

dump(ets, _Table, '$end_of_table', _Fun) ->
    ok;

dump(ets, Table, Key, Fun) ->
    case ets:lookup(Table, Key) of
        [Record] -> Fun(Record);
        [] -> ignore
    end,
    dump(ets, Table, ets:next(Table, Key), Fun).

