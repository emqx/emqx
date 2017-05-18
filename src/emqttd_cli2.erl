-module (emqttd_cli2).

-export([register_cli/0]).

-include("emqttd.hrl").

-include("emqttd_cli.hrl").

-include("emqttd_protocol.hrl").

-export([run/1]).

-behaviour(clique_handler).

-import(proplists, [get_value/2]).

-define(APP, emqttd).

-define(PROC_INFOKEYS, [status,
                        memory,
                        message_queue_len,
                        total_heap_size,
                        heap_size,
                        stack_size,
                        reductions]).

register_cli() ->
    F = fun() -> emqttd_mnesia:running_nodes() end,
    clique:register_node_finder(F),
    clique:register_writer("json", emqttd_cli_format),
    register_usage(),
    register_cmd().

run([]) ->
    All = clique_usage:find_all(),
    io:format("--------------------------------------------------------------------------------~n"),
    lists:foreach(fun({Cmd, Usage}) -> 
        io:format("~p usage:", [Cmd]),
        io:format("~ts", [Usage]),
        io:format("--------------------------------------------------------------------------------~n")
    end, lists:sort(All));
    
run(Cmd) ->
    clique:run(Cmd).

register_usage() ->
    clique:register_usage(["broker"],        broker_usage()),
    clique:register_usage(["cluster"],       cluster_usage()),
    clique:register_usage(["acl"],           acl_usage()),
    clique:register_usage(["clients"],       clients_usage()),
    clique:register_usage(["sessions"],      sessions_usage()),
    clique:register_usage(["routes"],        routes_usage()),
    clique:register_usage(["topics"],        topics_usage()),
    clique:register_usage(["subscriptions"], subscriptions_usage()),
    clique:register_usage(["plugins"],       plugins_usage()),
    clique:register_usage(["bridges"],       bridges_usage()),
    clique:register_usage(["vm"],            vm_usage()),
    clique:register_usage(["trace"],         trace_usage()),
    clique:register_usage(["status"],        status_usage()),
    clique:register_usage(["listeners"],     listeners_usage()),
    clique:register_usage(["listeners", "stop"],listener_stop_usage()),
    clique:register_usage(["mnesia"],        mnesia_usage()).

register_cmd() ->

    node_status(),

    broker_status(),
    broker_stats(),
    broker_metrics(),
    broker_pubsub(),

    cluster_join(),
    cluster_leave(),
    cluster_remove(),

    acl_reload(),

    clients_list(),
    clients_show(),
    clients_kick(),

    sessions_list(),
    sessions_list_persistent(),
    sessions_list_transient(),
    sessions_show(),

    routes_list(),
    routes_show(),
    topics_list(),
    topics_show(),

    subscriptions_list(),
    subscriptions_show(),
    subscriptions_subscribe(),
    subscriptions_del(),
    subscriptions_unsubscribe(),

    plugins_list(),
    plugins_load(),
    plugins_unload(),

    bridges_list(),
    bridges_start(),
    bridges_stop(),

    vm_all(),
    vm_load(),
    vm_memory(),
    vm_process(),
    vm_io(),
    vm_ports(),

    mnesia_info(),

    trace_list(),
    trace_on(),
    trace_off(),

    listeners(),
    listeners_stop().

node_status() ->
    Cmd = ["status", "info"],
    Callback =
        fun (_, _, _) ->
            {Status, Vsn} = case lists:keysearch(?APP, 1, application:which_applications()) of
                false -> 
                    {"not running", undefined};
                {value, {?APP, _Desc, Vsn0}} -> 
                    {"running", Vsn0}
            end,
            [clique_status:table([[{node, node()}, {status, Status}, {version, Vsn}]])]
        end,
    clique:register_command(Cmd, [], [], Callback).

%%--------------------------------------------------------------------
%% @doc Query broker

broker_status() ->
    Cmd = ["broker", "info"],
    Callback =
        fun (_, _, _) ->
            Funs = [sysdescr, version, uptime, datetime],
            Table = lists:map(fun(Fun) ->
                        {Fun, emqttd_broker:Fun()}
                    end, Funs),
            [clique_status:table([Table])]
        end,
    clique:register_command(Cmd, [], [], Callback).

broker_stats() ->
    Cmd = ["broker", "stats"],
    Callback =
        fun (_, _, _) ->
            lists:map(
                fun({Key, Val}) ->
                clique_status:list(Key, io_lib:format("~p", [Val]))
                end, emqttd_stats:getstats())
        end,
    clique:register_command(Cmd, [], [], Callback).

broker_metrics() ->
    Cmd = ["broker", "metrics"],
    Callback =
        fun (_, _, _) ->
            lists:map(
                fun({Key, Val}) ->
                clique_status:list(Key, io_lib:format("~p", [Val]))
                end, lists:sort(emqttd_metrics:all()))
        end,
    clique:register_command(Cmd, [], [], Callback).

broker_pubsub() ->
    Cmd = ["broker", "pubsub"],
    Callback =
        fun (_, _, _) ->
            Pubsubs = supervisor:which_children(emqttd_pubsub_sup:pubsub_pool()),
            Table = lists:map(
                fun({{_, Id}, Pid, _, _}) -> 
                    ProcInfo = erlang:process_info(Pid, ?PROC_INFOKEYS),
                    [{id, Id}] ++ ProcInfo
                end, lists:reverse(Pubsubs)),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).


%%--------------------------------------------------------------------
%% @doc Cluster with other nodes

cluster_join() ->
    Cmd = ["cluster", "join"],
    KeySpecs = [{'node', [{typecast, fun(Node) -> list_to_atom(Node) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Text = case get_value('node', Params) of
                undefined -> 
                    io_lib:format("Invalid params node is undefined~n", []);
                Node ->
                    case emqttd_cluster:join(Node) of
                        ok ->
                            ["Join the cluster successfully.\n", cluster(["status"])];
                        {error, Error} ->
                            io_lib:format("Failed to join the cluster: ~p~n", [Error])
                    end
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

cluster_leave() ->
    Cmd = ["cluster", "leave"],
    Callback =
    fun(_, _, _) ->
        Text = case emqttd_cluster:leave() of
            ok ->
                ["Leave the cluster successfully.\n", cluster(["status"])];
            {error, Error} ->
                io_lib:format("Failed to leave the cluster: ~p~n", [Error])
        end,
        [clique_status:text(Text)]
    end,
    clique:register_command(Cmd, [], [], Callback).

cluster_remove() ->
    Cmd = ["cluster", "remove"],
    KeySpecs = [{'node', [{typecast, fun(Node) -> list_to_atom(Node) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Text = case get_value('node', Params) of
                undefined -> 
                    io_lib:format("Invalid params node is undefined~n", []);
                Node ->
                    case emqttd_cluster:remove(Node) of
                        ok ->
                            ["Remove the cluster successfully.\n", cluster(["status"])];
                        {error, Error} ->
                            io_lib:format("Failed to remove the cluster: ~p~n", [Error])
                    end
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

cluster(["status"]) ->
    io_lib:format("Cluster status: ~p~n", [emqttd_cluster:status()]).

%%--------------------------------------------------------------------
%% @doc acl

acl_reload() ->
    Cmd = ["acl", "reload"],
    Callback =
        fun (_, _, _) ->
            emqttd_access_control:reload_acl(),
            [clique_status:text("")]
        end,
    clique:register_command(Cmd, [], [], Callback).

%%--------------------------------------------------------------------
%% @doc Query clients

clients_list() ->
    Cmd = ["clients", "list"],
    Callback =
        fun (_, _, _) ->
            [clique_status:table(dump(mqtt_client))]
        end,
    clique:register_command(Cmd, [], [], Callback).

clients_show() ->
    Cmd = ["clients", "show"],
    KeySpecs = [{'client_id', [{typecast, fun(ClientId) -> list_to_binary(ClientId) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            case get_value('client_id', Params) of
                undefined -> 
                    [clique_status:text(io_lib:format("Invalid params client_id is undefined~n", []))];
                ClientId ->
                    [clique_status:table(if_client(ClientId, fun print/1))]
            end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

clients_kick() ->
    Cmd = ["clients", "kick"],
    KeySpecs = [{'client_id', [{typecast, fun(ClientId) -> list_to_binary(ClientId) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            case get_value('client_id', Params) of
                undefined -> 
                    [clique_status:text(io_lib:format("Invalid params client_id is undefined~n", []))];
                ClientId ->
                    Result = if_client(ClientId, fun(#mqtt_client{client_pid = Pid}) -> emqttd_client:kick(Pid) end),
                    case Result of
                        [ok] -> [clique_status:text(io_lib:format("Kick client_id: ~p successfully~n", [ClientId]))];
                        _ -> [clique_status:text("")]
                    end
            end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

if_client(ClientId, Fun) ->
    case emqttd_cm:lookup(ClientId) of
        undefined -> ?PRINT_MSG("Not Found.~n"), [];
        Client    -> [Fun(Client)]
    end.

%%--------------------------------------------------------------------
%% @doc Sessions Command

sessions_list() ->
    Cmd = ["sessions", "list"],
    Callback =
        fun (_, _, _) ->
            [clique_status:table(dump(mqtt_local_session))]
        end,
    clique:register_command(Cmd, [], [], Callback).

%% performance issue?

sessions_list_persistent() ->
    Cmd = ["sessions", "list", "persistent"],
    Callback =
        fun (_, _, _) ->
            Table = lists:map(fun print/1, ets:match_object(mqtt_local_session, {'_', '_', false, '_'})),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

%% performance issue?

sessions_list_transient() ->
    Cmd = ["sessions", "list", "transient"],
    Callback =
        fun (_, _, _) ->
            Table = lists:map(fun print/1, ets:match_object(mqtt_local_session, {'_', '_', true, '_'})),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

sessions_show() ->
    Cmd = ["sessions", "show"],
    KeySpecs = [{'client_id', [{typecast, fun(ClientId) -> list_to_binary(ClientId) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            case get_value('client_id', Params) of
                undefined -> 
                    [clique_status:text(io_lib:format("Invalid params client_id is undefined~n", []))];
                ClientId ->
                    case ets:lookup(mqtt_local_session, ClientId) of
                        []         -> 
                            ?PRINT_MSG("Not Found.~n"),
                            [clique_status:table([])]; 
                        [SessInfo] -> 
                            [clique_status:table([print(SessInfo)])]
                    end
            end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

%%--------------------------------------------------------------------
%% @doc Routes Command

routes_list() ->
        Cmd = ["routes", "list"],
        Callback =
            fun (_, _, _) ->
                Table = lists:flatten(lists:map(fun print/1, emqttd_router:dump())),
                [clique_status:table([Table])]
            end,
        clique:register_command(Cmd, [], [], Callback).

routes_show() ->
    Cmd = ["routes", "show"],
    KeySpecs = [{'topic', [{typecast, fun(Topic) -> list_to_binary(Topic) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            case get_value('topic', Params) of
                undefined -> 
                    [clique_status:text(io_lib:format("Invalid params topic is undefined~n", []))];
                Topic ->
                    [clique_status:table([print(mnesia:dirty_read(mqtt_route, Topic))])]
            end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).
    
%%--------------------------------------------------------------------
%% @doc Topics Command

topics_list() ->
    Cmd = ["topics", "list"],
    Callback =
        fun (_, _, _) ->
            Table = lists:map(fun(Topic) -> [{topic, Topic}] end, emqttd:topics()),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

topics_show() ->
    Cmd = ["topics", "show"],
    KeySpecs = [{'topic', [{typecast, fun(Topic) -> list_to_binary(Topic) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            case get_value('client_id', Params) of
                undefined -> 
                    [clique_status:text(io_lib:format("Invalid params topic is undefined~n", []))];
                Topic ->
                    Table = print(mnesia:dirty_read(mqtt_route, Topic)),
                    [clique_status:table([Table])]
            end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

%%--------------------------------------------------------------------
%% @doc Subscriptions Command
subscriptions_list() ->
    Cmd = ["subscriptions", "list"],
    Callback =
        fun (_, _, _) ->
            Table = lists:map(fun(Subscription) ->
                                  print(subscription, Subscription)
                              end, ets:tab2list(mqtt_subscription)),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

subscriptions_show() ->
    Cmd = ["subscriptions", "show"],
    KeySpecs = [{'client_id', [{typecast, fun(Topic) -> list_to_binary(Topic) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            case get_value('client_id', Params) of
                undefined -> 
                    [clique_status:text(io_lib:format("Invalid params client_id is undefined~n", []))];
                ClientId ->
                    case ets:lookup(mqtt_subscription, ClientId) of
                        []      -> 
                            ?PRINT_MSG("Not Found.~n"),
                            [clique_status:table([])];
                        Records -> 
                            Table = [print(subscription, Subscription) || Subscription <- Records],
                            [clique_status:table(Table)]
                    end
            end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

subscriptions_subscribe() ->
    Cmd = ["subscriptions", "subscribe"],
    KeySpecs = [{'client_id', [{typecast, fun(ClientId) -> list_to_binary(ClientId) end}]},
                {'topic',     [{typecast, fun(Topic) -> list_to_binary(Topic) end}]},
                {'qos',       [{typecast, fun(QoS) -> list_to_integer(QoS) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Topic = get_value('topic', Params),
            ClientId = get_value('client_id', Params),
            QoS = get_value('qos', Params),
            Text = case {Topic, ClientId, QoS} of
                {undefined, _, _} ->
                    io_lib:format("Invalid params topic is undefined~n", []);
                {_, undefined, _} ->
                    io_lib:format("Invalid params client_id is undefined~n", []);
                {_, _, undefined} ->
                    io_lib:format("Invalid params qos is undefined~n", []);
                {_, _, _} ->
                    case emqttd:subscribe(Topic, ClientId, [{qos, QoS}]) of
                        ok ->
                            io_lib:format("Client_id: ~p subscribe topic: ~p qos: ~p successfully~n", [ClientId, Topic, QoS]);
                        {error, already_existed} ->
                            io_lib:format("Error: client_id: ~p subscribe topic: ~p already existed~n", [ClientId, Topic]);
                        {error, Reason} ->
                            io_lib:format("Error: ~p~n", [Reason])
                    end
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

subscriptions_del() ->
    Cmd = ["subscriptions", "del"],
    KeySpecs = [{'client_id', [{typecast, fun(ClientId) -> list_to_binary(ClientId) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            case get_value('client_id', Params) of
                undefined -> 
                    [clique_status:text(io_lib:format("Invalid params client_id is undefined~n", []))];
                ClientId ->
                    emqttd:subscriber_down(ClientId),
                    Text = io_lib:format("Client_id del subscriptions:~p successfully~n", [ClientId]),
                    [clique_status:text(Text)]
            end
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

subscriptions_unsubscribe() ->
    Cmd = ["subscriptions", "unsubscribe"],
    KeySpecs = [{'client_id', [{typecast, fun(ClientId) -> list_to_binary(ClientId) end}]},
                {'topic',     [{typecast, fun(Topic) -> list_to_binary(Topic) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Topic = get_value('topic', Params),
            ClientId = get_value('client_id', Params),
            QoS = get_value('qos', Params),
            Text = case {Topic, ClientId, QoS} of
                {undefined, _, _} ->
                    io_lib:format("Invalid params topic is undefined~n", []);
                {_, undefined, _} ->
                    io_lib:format("Invalid params client_id is undefined~n", []);
                {_, _, undefined} ->
                    io_lib:format("Invalid params qos is undefined~n", []);
                    
                {_, _, _} ->
                    emqttd:unsubscribe(Topic, ClientId),
                    io_lib:format("Client_id: ~p unsubscribe topic: ~p successfully~n", [ClientId, Topic])
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).
   
%%--------------------------------------------------------------------
%% @doc Plugins Command
plugins_list() ->
    Cmd = ["plugins", "list"],
    Callback =
        fun (_, _, _) ->
                Text = lists:map(fun(Plugin) -> print(Plugin) end, emqttd_plugins:list()),
                [clique_status:table(Text)]
        end,
    clique:register_command(Cmd, [], [], Callback).

plugins_load() ->
    Cmd = ["plugins", "load"],
    KeySpecs = [{'plugin_name', [{typecast, fun(PluginName) -> list_to_atom(PluginName) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Text = case get_value('plugin_name', Params) of
                undefined -> 
                    io_lib:format("Invalid params plugin_name is undefined~n", []);
                PluginName ->
                    case emqttd_plugins:load(PluginName) of
                        {ok, StartedApps} ->
                            io_lib:format("Start apps: ~p~nPlugin ~s loaded successfully.~n", [StartedApps, PluginName]);
                        {error, Reason}   ->
                            io_lib:format("load plugin error: ~p~n", [Reason])
                    end
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

plugins_unload() ->
    Cmd = ["plugins", "unload"],
    KeySpecs = [{'plugin_name', [{typecast, fun(PluginName) -> list_to_atom(PluginName) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Text = case get_value('plugin_name', Params) of
                undefined -> 
                    io_lib:format("Invalid params plugin_name is undefined~n", []);
                PluginName ->
                    case emqttd_plugins:unload(PluginName) of
                        ok ->
                            io_lib:format("Plugin ~s unloaded successfully.~n", [PluginName]);
                        {error, Reason} ->
                            io_lib:format("unload plugin error: ~p~n", [Reason])
                    end
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).


%%--------------------------------------------------------------------
%% @doc Bridges command

bridges_list() ->
    Cmd = ["bridges", "list"],
    Callback =
        fun (_, _, _) ->
                Text = lists:map(
                    fun({Node, Topic, _Pid}) -> 
                        [{bridge, node()}, {topic, Topic}, {node, Node}]
                    end, emqttd_bridge_sup_sup:bridges()),
                [clique_status:table(Text)]
        end,
    clique:register_command(Cmd, [], [], Callback).

bridges_start() ->
    Cmd = ["bridges", "start"],
    KeySpecs = [{'snode',         [{typecast, fun(SNode)  -> list_to_atom(SNode) end}]},
                {'topic',         [{typecast, fun(Topic)  -> list_to_binary(Topic) end}]},
                {'qos',           [{typecast, fun(Qos)    -> list_to_integer(Qos) end}]},
                {'topic_suffix',  [{typecast, fun(Prefix) -> list_to_binary(Prefix) end}]},
                {'topic_prefix',  [{typecast, fun(Suffix) -> list_to_binary(Suffix) end}]},
                {'max_queue_len', [{typecast, fun(Queue)  -> list_to_integer(Queue) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Text = case {get_value('snode', Params), get_value('topic', Params)} of
                {undefined, _} ->
                    io_lib:format("Invalid params snode is undefined~n", []);
                {_, undefined} ->
                    io_lib:format("Invalid params topic is undefined~n", []);
                {SNode, Topic} ->
                    Opts = Params -- [{'snode', SNode}, {'topic', Topic}],
                    case emqttd_bridge_sup_sup:start_bridge(SNode, Topic, Opts) of
                        {ok, _} -> 
                            io_lib:format("bridge is started.~n", []);
                        {error, Error} -> 
                            io_lib:format("error: ~p~n", [Error])
                    end
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

bridges_stop() ->
    Cmd = ["bridges", "stop"],
    KeySpecs = [{'snode',  [{typecast, fun(SNode) -> list_to_atom(SNode) end}]},
                {'topic',  [{typecast, fun(Topic) -> list_to_binary(Topic) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Text = case {get_value('snode', Params), get_value('topic', Params)} of
                {undefined, _} ->
                    io_lib:format("Invalid params snode is undefined~n", []);
                {_, undefined} ->
                    io_lib:format("Invalid params topic is undefined~n", []);
                {SNode, Topic} ->
                    case emqttd_bridge_sup_sup:stop_bridge(SNode, Topic) of
                        ok             -> io_lib:format("bridge is stopped.~n", []);
                        {error, Error} -> io_lib:format("error: ~p~n", [Error])
                    end
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

%%--------------------------------------------------------------------
%% @doc vm command

vm_all() ->
    Cmd = ["vm","info"],
    Callback =
        fun (_, _, _) ->
            Cpu = [vm_info("cpu", K, list_to_float(V)) || {K, V} <- emqttd_vm:loads()],
            Memory = [vm_info("memory", K, V) || {K, V} <- erlang:memory()],
            Process = [vm_info("process", K, erlang:system_info(V)) || {K, V} <- [{limit, process_limit}, {count, process_count}]],
            IoInfo = erlang:system_info(check_io),
            Io = [vm_info("io", K, get_value(K, IoInfo)) || K <- [max_fds, active_fds]],
            Ports = [vm_info("ports", K, erlang:system_info(V)) || {K, V} <- [{count, port_count}, {limit, port_limit}]],
            lists:flatten([Cpu, Memory, Process, Io, Ports])
        end,
    clique:register_command(Cmd, [], [], Callback).

vm_info(Item, K, V) ->
    clique_status:list(format_key(Item, K), io_lib:format("~p", [V])).

format_key(Item, K) ->
    list_to_atom(lists:concat([Item, "/", K])).

vm_load() ->
    Cmd = ["vm","load"],
    Callback =
        fun (_, _, _) ->
            Table = lists:map(
                fun({Name, Val}) -> 
                    [{name, Name}, {val, Val}]
                end, emqttd_vm:loads()),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

vm_memory() ->
    Cmd = ["vm","memory"],
    Callback =
        fun (_, _, _) ->
            Table = lists:map(
                        fun({Name, Val}) -> 
                            [{name, Name}, {val, Val}]
                        end, erlang:memory()),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

vm_process() ->
    Cmd = ["vm","process"],
    Callback =
        fun (_, _, _) ->
            Table = lists:map(
                        fun({Name, Val}) -> 
                            [{name, Name}, {val, erlang:system_info(Val)}]
                        end, [{limit, process_limit}, {count, process_count}]),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

vm_io() ->
    Cmd = ["vm","io"],
    Callback =
        fun (_, _, _) ->
            IoInfo = erlang:system_info(check_io),
            Table = lists:map(
                        fun(Key) -> 
                            [{name, Key}, {val, get_value(Key, IoInfo)}]
                        end, [max_fds, active_fds]),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

vm_ports() ->
    Cmd = ["vm","ports"],
    Callback =
        fun (_, _, _) ->
            Table = lists:map(
                        fun({Name, Val}) -> 
                            [{name, Name}, {val, erlang:system_info(Val)}]
                        end, [{count, port_count}, {limit, port_limit}]),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

% %%--------------------------------------------------------------------
%% @doc mnesia Command

mnesia_info() ->
    Cmd = ["mnesia", "info"],
    Callback =
        fun (_, _, _) ->
            mnesia:system_info(),
            [clique_status:text("")]
        end,
    clique:register_command(Cmd, [], [], Callback).

%%--------------------------------------------------------------------
%% @doc Trace Command

trace_list() ->
    Cmd = ["trace", "list"],
    Callback =
        fun (_, _, _) ->
            Table = lists:map(fun({{Who, Name}, LogFile}) ->
                [{trace, Who}, {name, Name}, {log_file, LogFile}]
            end, emqttd_trace:all_traces()),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

trace_on() ->
    Cmd = ["trace"],
    KeySpecs = [{'type',      [{typecast, fun(Type)     -> list_to_atom(Type) end}]},
                {'client_id', [{typecast, fun(ClientId) -> list_to_binary(ClientId) end}]},
                {'topic',     [{typecast, fun(Topic)    -> list_to_binary(Topic) end}]},
                {'log_file',  [{typecast, fun(LogFile)  -> list_to_binary(LogFile) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Text = case get_value('type', Params) of
                client -> 
                    trace_on(client, get_value('client_id', Params), get_value('log_file', Params));
                topic  -> 
                    trace_on(topic, get_value('topic', Params), get_value('log_file', Params));
                Type   ->
                    io_lib:format("Invalid params type: ~p error~n", [Type])
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

trace_off() ->
    Cmd = ["trace", "off"],
    KeySpecs = [{'type',      [{typecast, fun(Type) -> list_to_atom(Type) end}]},
                {'client_id', [{typecast, fun(ClientId) -> list_to_binary(ClientId) end}]},
                {'topic',     [{typecast, fun(Topic) -> list_to_binary(Topic) end}]}],
    FlagSpecs = [],
    Callback =
        fun (_, Params, _) ->
            Text = case get_value('type', Params) of
                client -> 
                    trace_off(client, get_value('client_id', Params));
                topic  -> 
                    trace_off(topic, get_value('topic', Params));
                Type   ->
                    io_lib:format("Invalid params type: ~p error~n", [Type])
            end,
            [clique_status:text(Text)]
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

trace_on(Who, Name, LogFile) ->
    case emqttd_trace:start_trace({Who, Name}, LogFile) of
        ok ->
            io_lib:format("trace ~s ~s successfully.~n", [Who, Name]);
        {error, Error} ->
            io_lib:format("trace ~s ~s error: ~p~n", [Who, Name, Error])
    end.

trace_off(Who, Name) ->
    case emqttd_trace:stop_trace({Who, Name}) of
        ok -> 
            io_lib:format("stop tracing ~s ~s successfully.~n", [Who, Name]);
        {error, Error} ->
            io_lib:format("stop tracing ~s ~s error: ~p.~n", [Who, Name, Error])
    end.

%%--------------------------------------------------------------------
%% @doc Listeners Command

listeners() ->
    Cmd = ["listeners", "info"],
    Callback =
        fun (_, _, _) ->
            Table = 
                lists:map(fun({{Protocol, ListenOn}, Pid}) ->
                    Info = [{acceptors,      esockd:get_acceptors(Pid)},
                            {max_clients,    esockd:get_max_clients(Pid)},
                            {current_clients,esockd:get_current_clients(Pid)},
                            {shutdown_count, esockd:get_shutdown_count(Pid)}],
                    Listener = io_lib:format("~s:~s~n", [Protocol, esockd:to_string(ListenOn)]),
                    [{listener, Listener}| Info]
                end, esockd:listeners()),
            [clique_status:table(Table)]
        end,
    clique:register_command(Cmd, [], [], Callback).

listeners_stop() ->
    Cmd = ["listeners", "stop"],
    KeySpecs = [{'address',  [{typecast, fun parse_addr/1}]},
                {'port',  [{typecast, fun parse_port/1}]},
                {'type',  [{typecast, fun parse_type/1}]}],
    FlagSpecs = [{kill, [{shortname, "k"},
                         {longname, "kill_sessions"}]}],
    Callback =
        fun (_, Params, Flag) ->
            Address = get_value('address', Params),
            Port  = get_value('port', Params),
            Type = get_value('type', Params),
            case Address of
                undefined -> emqttd_app:stop_listener({Type, Port, []});
                Address -> emqttd_app:stop_listener({Type, {Address, Port}, []})
            end,
            [clique_status:text("aaa")]  
        end,
    clique:register_command(Cmd, KeySpecs, FlagSpecs, Callback).

parse_port(Port) ->
    case catch list_to_integer(Port) of
        P when (P >= 0) and (P=<65535) -> P;
        _ -> {error, {invalid_args,[{port, Port}]}}
    end.

parse_addr(Addr) ->
    case inet:parse_address(Addr) of
        {ok, Ip} -> Ip;
        {error, einval} ->
            {error, {invalid_args,[{address, Addr}]}}
    end.

parse_type(Type) ->
    case catch list_to_atom(Type) of
        T when (T=:=tcp) orelse 
               (T=:=ssl) orelse 
               (T=:=ws) orelse 
               (T=:=wss) orelse 
               (T=:=http) orelse 
               (T=:=https) -> T;
        _ -> {error, {invalid_args,[{type, Type}]}}
    end.


%%-------------------------------------------------------------
%% usage
%%-------------------------------------------------------------
broker_usage() ->
    ["\n  broker info       Show broker version, uptime and description\n",
     "  broker pubsub     Show process_info of pubsub\n",
     "  broker stats      Show broker statistics of clients, topics, subscribers\n",
     "  broker metrics    Show broker metrics\n"].

cluster_usage() ->
    ["\n  cluster join node=<Node>     Join the cluster\n",
     "  cluster leave                Leave the cluster\n",
     "  cluster remove node=<Node>   Remove the node from cluster\n",
     "  cluster status               Cluster status\n"].

acl_usage() ->
    ["\n  acl reload    reload etc/acl.conf\n"].

clients_usage() ->
    ["\n  clients list                         List all clients\n",
     "  clients show client_id=<ClientId>    Show a client\n",
     "  clients kick client_id=<ClientId>    Kick out a client\n"].

sessions_usage() ->
    ["\n  sessions list                         List all sessions\n",
     "  sessions list persistent              List all persistent sessions\n",
     "  sessions list transient               List all transient sessions\n",
     "  sessions show client_id=<ClientId>    Show a session\n"].

routes_usage() ->
    ["\n  routes list                  List all routes\n",
     "  routes show topic=<Topic>    Show a route\n"].

topics_usage() ->
    ["\n  topics list                  List all topics\n",
     "  topics show topic=<Topic>    Show a topic\n"].

subscriptions_usage() ->
    ["\n  subscriptions list                                                      List all subscriptions\n",
     "  subscriptions show client_id=<ClientId>                                 Show subscriptions of a client\n",
     "  subscriptions subscribe client_id=<ClientId> topic=<Topic> qos=<Qos>    Add a static subscription manually\n",
     "  subscriptions del client_id=<ClientId>                                  Delete static subscriptions manually\n",
     "  subscriptions unsubscribe client_id=<ClientId> topic=<Topic>            Delete a static subscription manually\n"].

plugins_usage() ->
    ["\n  plugins list                           Show loaded plugins\n",
     "  plugins load plugin_name=<Plugin>      Load plugin\n",
     "  plugins unload plugin_name=<Plugin>    Unload plugin\n"].

bridges_usage() ->
    ["\n  bridges list                              List bridges\n",
     "  bridges start snode=<Node> topic=<Topic>    Start a bridge
    options:
      qos=<Qos>
      topic_prefix=<Prefix>
      topic_suffix=<Suffix>
      queue=<Queue>\n",
     "  bridges stop snode=<Node> topic=<Topic>     Stop a bridge\n"].

vm_usage() ->
    ["\n  vm info       Show info of Erlang VM\n",
     "  vm load       Show load of Erlang VM\n",
     "  vm memory     Show memory of Erlang VM\n",
     "  vm process    Show process of Erlang VM\n",
     "  vm io         Show IO of Erlang VM\n",
     "  vm ports      Show Ports of Erlang VM\n"].

trace_usage() ->
    ["\n  trace list                                                                      List all traces\n",
     "  trace type=client|topic client_id=<ClientId> topic=<Topic> log_file=<LogFile>   Start tracing\n",
     "  trace off type=client|topic client_id=<ClientId> topic=<Topic>                  Stop tracing\n"].

status_usage() ->
    ["\n  status info   Show broker status\n"].

listeners_usage() ->
    ["\n  listeners info     List listeners\n",
     "  listeners start    Create and start a listener\n",
     "  listeners stop     Stop accepting new connections for a running listener\n",
     "  listeners restart  Restart accepting new connections for a stopped listener\n",
     "  listeners delete   Delete a stopped listener\n"].

listener_stop_usage() ->
    ["\n  listeners stop address=IpAddr port=Port\n",
     "  Stops accepting new connections on a running listener.\n",
     "Options\n",
     "  -k, --kill_sessions\n"
     "      kills all sessions accepted with this listener.\n"].

mnesia_usage() ->
    ["\n  mnesia info   Mnesia system info\n"].

%%--------------------------------------------------------------------
%% Dump ETS
%%--------------------------------------------------------------------

dump(Table) ->
    dump(Table, []).

dump(Table, Acc) ->
    dump(Table, ets:first(Table), Acc).

dump(_Table, '$end_of_table', Acc) ->
    lists:reverse(Acc);

dump(Table, Key, Acc) ->
    case ets:lookup(Table, Key) of
        [Record] -> dump(Table, ets:next(Table, Key), [print(Record)|Acc]);
        [] -> dump(Table, ets:next(Table, Key), Acc)
    end.

print([]) ->
    [];

print(Routes = [#mqtt_route{topic = Topic} | _]) ->
    Nodes = [atom_to_list(Node) || #mqtt_route{node = Node} <- Routes],
    [{topic, Topic}, {routes, string:join(Nodes, ",")}];

print(#mqtt_plugin{name = Name, version = Ver, descr = Descr, active = Active}) ->
    [{plugin, Name}, {version, Ver}, {description, Descr}, {active, Active}];

print(#mqtt_client{client_id = ClientId, clean_sess = CleanSess, username = Username,
                   peername = Peername, connected_at = ConnectedAt}) ->
           [{client_id, ClientId}, 
            {clean_sess, CleanSess}, 
            {username, Username}, 
            {ip, emqttd_net:format(Peername)}, 
            {connected_at, emqttd_time:now_secs(ConnectedAt)}];

print({route, Routes}) ->
    lists:map(fun print/1, Routes);
print({local_route, Routes}) ->
    lists:map(fun print/1, Routes);
print(#mqtt_route{topic = Topic, node = Node}) ->
    [{topic,  Topic}, {node, Node}];
print({Topic, Node}) ->
    [{topic,  Topic}, {node, Node}];

print({ClientId, _ClientPid, _Persistent, SessInfo}) ->
    Data = lists:append(SessInfo, emqttd_stats:get_session_stats(ClientId)),
    InfoKeys = [clean_sess,
                subscriptions,
                max_inflight,
                inflight_len,
                mqueue_len,
                mqueue_dropped,
                awaiting_rel_len,
                deliver_msg,
                enqueue_msg,
                created_at],
            [{client_id, ClientId} | [{Key, format(Key, get_value(Key, Data))} || Key <- InfoKeys]].

print(subscription, {Sub, {_Share, Topic}}) when is_pid(Sub) ->
    [{subscription, Sub}, {topic, Topic}];
print(subscription, {Sub, Topic}) when is_pid(Sub) ->
    [{subscription, Sub}, {topic, Topic}];
print(subscription, {Sub, {_Share, Topic}}) ->
    [{subscription, Sub}, {topic, Topic}];
print(subscription, {Sub, Topic}) ->
    [{subscription, Sub}, {topic, Topic}].

format(created_at, Val) ->
    emqttd_time:now_secs(Val);

format(_, Val) ->
    Val.
