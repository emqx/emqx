%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_cli).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("emqx_mgmt.hrl").

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-define(PRINT_CMD(Cmd, Desc), io:format("~-48s# ~s~n", [Cmd, Desc])).

-export([load/0]).

-export([ status/1
        , broker/1
        , cluster/1
        , clients/1
        , routes/1
        , subscriptions/1
        , plugins/1
        , listeners/1
        , vm/1
        , mnesia/1
        , trace/1
        , traces/1
        , log/1
        , mgmt/1
        , data/1
        , acl/1
        , pem_cache/1
        ]).

-define(PROC_INFOKEYS, [status,
                        memory,
                        message_queue_len,
                        total_heap_size,
                        heap_size,
                        stack_size,
                        reductions]).

-define(MAX_LIMIT, 10000).

-define(APP, emqx).

-spec(load() -> ok).
load() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    lists:foreach(fun(Cmd) -> emqx_ctl:register_command(Cmd, {?MODULE, Cmd}, []) end, Cmds).

is_cmd(Fun) ->
    not lists:member(Fun, [init, load, module_info]).

mgmt(["insert", AppId, Name]) ->
    case emqx_mgmt_auth:add_app(list_to_binary(AppId), list_to_binary(Name)) of
        {ok, Secret} ->
            emqx_ctl:print("AppSecret: ~s~n", [Secret]);
        {error, already_existed} ->
            emqx_ctl:print("Error: already existed~n");
        {error, Reason} ->
            emqx_ctl:print("Error: ~p~n", [Reason])
    end;

mgmt(["lookup", AppId]) ->
    case emqx_mgmt_auth:lookup_app(list_to_binary(AppId)) of
        undefined -> emqx_ctl:print("Not Found.~n");
        App -> print_app_info(App)
    end;

mgmt(["update", AppId, Status]) ->
    case emqx_mgmt_auth:update_app(list_to_binary(AppId), list_to_atom(Status)) of
        ok ->
            emqx_ctl:print("update successfully.~n");
        {error, Reason} ->
            emqx_ctl:print("Error: ~p~n", [Reason])
    end;

mgmt(["delete", AppId]) ->
    case emqx_mgmt_auth:del_app(list_to_binary(AppId)) of
        ok -> emqx_ctl:print("ok~n");
        {error, not_found} ->
            emqx_ctl:print("Error: app not found~n");
        {error, Reason} ->
            emqx_ctl:print("Error: ~p~n", [Reason])
    end;

mgmt(["list"]) ->
    lists:foreach(fun print_app_info/1, emqx_mgmt_auth:list_apps());

mgmt(_) ->
    emqx_ctl:usage([{"mgmt list",                    "List Applications"},
                    {"mgmt insert <AppId> <Name>",   "Add Application of REST API"},
                    {"mgmt update <AppId> <status>", "Update Application of REST API"},
                    {"mgmt lookup <AppId>",          "Get Application of REST API"},
                    {"mgmt delete <AppId>",          "Delete Application of REST API"}]).

%%--------------------------------------------------------------------
%% @doc Node status

status([]) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    emqx_ctl:print("Node ~p ~s is ~p~n", [node(), emqx_app:get_release(), InternalStatus]);
status(_) ->
     emqx_ctl:usage("status", "Show broker status").

%%--------------------------------------------------------------------
%% @doc Query broker

broker([]) ->
    Funs = [sysdescr, version, uptime, datetime],
    [emqx_ctl:print("~-10s: ~s~n", [Fun, emqx_sys:Fun()]) || Fun <- Funs];

broker(["stats"]) ->
    [emqx_ctl:print("~-30s: ~w~n", [Stat, Val]) ||
        {Stat, Val} <- lists:sort(emqx_stats:getstats())];

broker(["metrics"]) ->
    [emqx_ctl:print("~-30s: ~w~n", [Metric, Val]) ||
        {Metric, Val} <- lists:sort(emqx_metrics:all())];

broker(_) ->
    emqx_ctl:usage([{"broker",         "Show broker version, uptime and description"},
                    {"broker stats",   "Show broker statistics of clients, topics, subscribers"},
                    {"broker metrics", "Show broker metrics"}]).

%%-----------------------------------------------------------------------------
%% @doc Cluster with other nodes

cluster(["join", SNode]) ->
    case ekka:join(ekka_node:parse_name(SNode)) of
        ok ->
            emqx_ctl:print("Join the cluster successfully.~n"),
            cluster(["status"]);
        ignore ->
            emqx_ctl:print("Ignore.~n");
        {error, Error} ->
            emqx_ctl:print("Failed to join the cluster: ~0p~n", [Error])
    end;

cluster(["leave"]) ->
    case ekka:leave() of
        ok ->
            emqx_ctl:print("Leave the cluster successfully.~n"),
            cluster(["status"]);
        {error, Error} ->
            emqx_ctl:print("Failed to leave the cluster: ~0p~n", [Error])
    end;

cluster(["force-leave", SNode]) ->
    case ekka:force_leave(ekka_node:parse_name(SNode)) of
        ok ->
            emqx_ctl:print("Remove the node from cluster successfully.~n"),
            cluster(["status"]);
        ignore ->
            emqx_ctl:print("Ignore.~n");
        {error, Error} ->
            emqx_ctl:print("Failed to remove the node from cluster: ~0p~n", [Error])
    end;

cluster(["status"]) ->
    emqx_ctl:print("Cluster status: ~p~n", [ekka_cluster:info()]);

cluster(_) ->
    emqx_ctl:usage([{"cluster join <Node>",       "Join the cluster"},
                    {"cluster leave",             "Leave the cluster"},
                    {"cluster force-leave <Node>","Force the node leave from cluster"},
                    {"cluster status",            "Cluster status"}]).

%%--------------------------------------------------------------------
%% @doc Query clients

clients(["list"]) ->
    dump(emqx_channel, client);

clients(["show", ClientId]) ->
    if_client(ClientId, fun print/1);

clients(["kick", ClientId]) ->
    ok = emqx_cm:kick_session(bin(ClientId)),
    emqx_ctl:print("ok~n");

clients(_) ->
    emqx_ctl:usage([{"clients list",            "List all clients"},
                    {"clients show <ClientId>", "Show a client"},
                    {"clients kick <ClientId>", "Kick out a client"}]).

if_client(ClientId, Fun) ->
    case ets:lookup(emqx_channel, (bin(ClientId))) of
        [] -> emqx_ctl:print("Not Found.~n");
        [Channel]    -> Fun({client, Channel})
    end.

%%--------------------------------------------------------------------
%% @doc Routes Command

routes(["list"]) ->
    dump(emqx_route);

routes(["show", Topic]) ->
    Routes = ets:lookup(emqx_route, bin(Topic)),
    [print({emqx_route, Route}) || Route <- Routes];

routes(_) ->
    emqx_ctl:usage([{"routes list",         "List all routes"},
                    {"routes show <Topic>", "Show a route"}]).

subscriptions(["list"]) ->
    lists:foreach(fun(Suboption) ->
                        print({emqx_suboption, Suboption})
                  end, ets:tab2list(emqx_suboption));

subscriptions(["show", ClientId]) ->
    case ets:lookup(emqx_subid, bin(ClientId)) of
        [] ->
            emqx_ctl:print("Not Found.~n");
        [{_, Pid}] ->
            case ets:match_object(emqx_suboption, {{Pid, '_'}, '_'}) of
                [] -> emqx_ctl:print("Not Found.~n");
                Suboption ->
                    [print({emqx_suboption, Sub}) || Sub <- Suboption]
            end
    end;

subscriptions(["add", ClientId, Topic, QoS]) ->
   if_valid_qos(QoS, fun(IntQos) ->
                        case ets:lookup(emqx_channel, bin(ClientId)) of
                            [] -> emqx_ctl:print("Error: Channel not found!");
                            [{_, Pid}] ->
                                {Topic1, Options} = emqx_topic:parse(bin(Topic)),
                                Pid ! {subscribe, [{Topic1, Options#{qos => IntQos}}]},
                                emqx_ctl:print("ok~n")
                        end
                     end);

subscriptions(["del", ClientId, Topic]) ->
    case ets:lookup(emqx_channel, bin(ClientId)) of
        [] -> emqx_ctl:print("Error: Channel not found!");
        [{_, Pid}] ->
            Pid ! {unsubscribe, [emqx_topic:parse(bin(Topic))]},
            emqx_ctl:print("ok~n")
    end;

subscriptions(_) ->
    emqx_ctl:usage([{"subscriptions list", "List all subscriptions"},
                    {"subscriptions show <ClientId>", "Show subscriptions of a client"},
                    {"subscriptions add <ClientId> <Topic> <QoS>",
                        "Add a static subscription manually"},
                    {"subscriptions del <ClientId> <Topic>",
                        "Delete a static subscription manually"}]).

if_valid_qos(QoS, Fun) ->
    try list_to_integer(QoS) of
        Int when ?IS_QOS(Int) -> Fun(Int);
        _ -> emqx_ctl:print("QoS should be 0, 1, 2~n")
    catch _:_ ->
        emqx_ctl:print("QoS should be 0, 1, 2~n")
    end.

plugins(["list"]) ->
    lists:foreach(fun print/1, emqx_plugins:list());

plugins(["load", Name]) ->
    case emqx_plugins:load(list_to_atom(Name)) of
        ok ->
            emqx_ctl:print("Plugin ~s loaded successfully.~n", [Name]);
        {error, Reason}   ->
            emqx_ctl:print("Load plugin ~s error: ~p.~n", [Name, Reason])
    end;

plugins(["unload", "emqx_management"])->
    emqx_ctl:print("Plugin emqx_management can not be unloaded.~n");

plugins(["unload", Name]) ->
    case emqx_plugins:unload(list_to_atom(Name)) of
        ok ->
            emqx_ctl:print("Plugin ~s unloaded successfully.~n", [Name]);
        {error, Reason} ->
            emqx_ctl:print("Unload plugin ~s error: ~p.~n", [Name, Reason])
    end;

plugins(["reload", Name]) ->
    try list_to_existing_atom(Name) of
        PluginName ->
            case emqx_mgmt:reload_plugin(node(), PluginName) of
                ok ->
                    emqx_ctl:print("Plugin ~s reloaded successfully.~n", [Name]);
                {error, Reason} ->
                    emqx_ctl:print("Reload plugin ~s error: ~p.~n", [Name, Reason])
            end
    catch
        error:badarg ->
            emqx_ctl:print("Reload plugin ~s error: The plugin doesn't exist.~n", [Name])
    end;

plugins(_) ->
    emqx_ctl:usage([{"plugins list",            "Show loaded plugins"},
                    {"plugins load <Plugin>",   "Load plugin"},
                    {"plugins unload <Plugin>", "Unload plugin"},
                    {"plugins reload <Plugin>", "Reload plugin"}
                   ]).

%%--------------------------------------------------------------------
%% @doc vm command

vm([]) ->
    vm(["all"]);

vm(["all"]) ->
    [vm([Name]) || Name <- ["load", "memory", "process", "io", "ports"]];

vm(["load"]) ->
    [emqx_ctl:print("cpu/~-20s: ~s~n", [L, V]) || {L, V} <- emqx_vm:loads()];

vm(["memory"]) ->
    [emqx_ctl:print("memory/~-17s: ~w~n", [Cat, Val]) || {Cat, Val} <- erlang:memory()];

vm(["process"]) ->
    [emqx_ctl:print("process/~-16s: ~w~n",
        [Name, erlang:system_info(Key)]) ||
        {Name, Key} <- [{limit, process_limit}, {count, process_count}]];

vm(["io"]) ->
    IoInfo = lists:usort(lists:flatten(erlang:system_info(check_io))),
    [emqx_ctl:print("io/~-21s: ~w~n",
        [Key, proplists:get_value(Key, IoInfo)]) ||
        Key <- [max_fds, active_fds]];

vm(["ports"]) ->
    [emqx_ctl:print("ports/~-16s: ~w~n",
        [Name, erlang:system_info(Key)]) ||
        {Name, Key} <- [{count, port_count}, {limit, port_limit}]];

vm(_) ->
    emqx_ctl:usage([{"vm all",     "Show info of Erlang VM"},
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
    emqx_ctl:usage([{"mnesia", "Mnesia system info"}]).

%%--------------------------------------------------------------------
%% @doc Logger Command

log(["set-level", Level]) ->
    case emqx_logger:set_log_level(list_to_atom(Level)) of
        ok -> emqx_ctl:print("~s~n", [Level]);
        Error -> emqx_ctl:print("[error] set overall log level failed: ~p~n", [Error])
    end;

log(["primary-level"]) ->
    Level = emqx_logger:get_primary_log_level(),
    emqx_ctl:print("~s~n", [Level]);

log(["primary-level", Level]) ->
    _ = emqx_logger:set_primary_log_level(list_to_atom(Level)),
    emqx_ctl:print("~s~n", [emqx_logger:get_primary_log_level()]);

log(["handlers", "list"]) ->
    _ = [emqx_ctl:print("LogHandler(id=~s, level=~s, destination=~s, status=~s)~n",
        [Id, Level, Dst, Status]) || #{id := Id, level := Level, dst := Dst, status := Status}
        <- emqx_logger:get_log_handlers()],
    ok;

log(["handlers", "start", HandlerId]) ->
    case emqx_logger:start_log_handler(list_to_atom(HandlerId)) of
        ok -> emqx_ctl:print("log handler ~s started~n", [HandlerId]);
        {error, Reason} ->
            emqx_ctl:print("[error] failed to start log handler ~s: ~p~n", [HandlerId, Reason])
    end;

log(["handlers", "stop", HandlerId]) ->
    case emqx_logger:stop_log_handler(list_to_atom(HandlerId)) of
        ok -> emqx_ctl:print("log handler ~s stopped~n", [HandlerId]);
        {error, Reason} ->
            emqx_ctl:print("[error] failed to stop log handler ~s: ~p~n", [HandlerId, Reason])
    end;

log(["handlers", "set-level", HandlerId, Level]) ->
    case emqx_logger:set_log_handler_level(list_to_atom(HandlerId), list_to_atom(Level)) of
        ok ->
            #{level := NewLevel} = emqx_logger:get_log_handler(list_to_atom(HandlerId)),
            emqx_ctl:print("~s~n", [NewLevel]);
        {error, Error} ->
            emqx_ctl:print("[error] ~p~n", [Error])
    end;

log(_) ->
    emqx_ctl:usage([{"log set-level <Level>", "Set the overall log level"},
                    {"log primary-level", "Show the primary log level now"},
                    {"log primary-level <Level>","Set the primary log level"},
                    {"log handlers list", "Show log handlers"},
                    {"log handlers start <HandlerId>", "Start a log handler"},
                    {"log handlers stop  <HandlerId>", "Stop a log handler"},
                    {"log handlers set-level <HandlerId> <Level>",
                        "Set log level of a log handler"}]).

%%--------------------------------------------------------------------
%% @doc Trace Command

trace(["list"]) ->
    lists:foreach(fun(Trace) ->
        #{type := Type, filter := Filter, level := Level, dst := Dst} = Trace,
        emqx_ctl:print("Trace(~s=~s, level=~s, destination=~p)~n", [Type, Filter, Level, Dst])
                  end, emqx_trace_handler:running());

trace(["stop", Operation, ClientId]) ->
    case trace_type(Operation) of
        {ok, Type} -> trace_off(Type, ClientId);
        error -> trace([])
    end;

trace(["start", Operation, ClientId, LogFile]) ->
    trace(["start", Operation, ClientId, LogFile, "all"]);

trace(["start", Operation, ClientId, LogFile, Level]) ->
    case trace_type(Operation) of
        {ok, Type} -> trace_on(Type, ClientId, list_to_existing_atom(Level), LogFile);
        error -> trace([])
    end;

trace(_) ->
    emqx_ctl:usage([{"trace list", "List all traces started on local node"},
                    {"trace start client <ClientId> <File> [<Level>]",
                        "Traces for a client on local node"},
                    {"trace stop  client <ClientId>",
                        "Stop tracing for a client on local node"},
                    {"trace start topic  <Topic>    <File> [<Level>] ",
                        "Traces for a topic on local node"},
                    {"trace stop  topic  <Topic> ",
                        "Stop tracing for a topic on local node"},
                    {"trace start ip_address  <IP>    <File> [<Level>] ",
                        "Traces for a client ip on local node"},
                    {"trace stop  ip_addresss  <IP> ",
                        "Stop tracing for a client ip on local node"}
                   ]).

trace_on(Who, Name, Level, LogFile) ->
    case emqx_trace_handler:install(Who, Name, Level, LogFile) of
        ok ->
            emqx_ctl:print("trace ~s ~s successfully~n", [Who, Name]);
        {error, Error} ->
            emqx_ctl:print("[error] trace ~s ~s: ~p~n", [Who, Name, Error])
    end.

trace_off(Who, Name) ->
    case emqx_trace_handler:uninstall(Who, Name) of
        ok ->
            emqx_ctl:print("stop tracing ~s ~s successfully~n", [Who, Name]);
        {error, Error} ->
            emqx_ctl:print("[error] stop tracing ~s ~s: ~p~n", [Who, Name, Error])
    end.

%%--------------------------------------------------------------------
%% @doc Trace Cluster Command
traces(["list"]) ->
    {ok, List} = emqx_trace_api:list_trace(get, []),
    case List of
        [] ->
            emqx_ctl:print("Cluster Trace is empty~n", []);
        _ ->
            lists:foreach(fun(Trace) ->
                #{type := Type, name := Name, status := Status,
                    log_size := LogSize} = Trace,
                emqx_ctl:print("Trace(~s: ~s=~s, ~s, LogSize:~p)~n",
                    [Name, Type, maps:get(Type, Trace), Status, LogSize])
                          end, List)
    end,
    length(List);

traces(["stop", Name]) ->
    trace_cluster_off(Name);

traces(["delete", Name]) ->
    trace_cluster_del(Name);

traces(["start", Name, Operation, Filter]) ->
    traces(["start", Name, Operation, Filter, "900"]);

traces(["start", Name, Operation, Filter, DurationS]) ->
    case trace_type(Operation) of
        {ok, Type} ->  trace_cluster_on(Name, Type, Filter, DurationS);
        error -> traces([])
    end;

traces(_) ->
    emqx_ctl:usage([{"traces list", "List all cluster traces started"},
        {"traces start <Name> client <ClientId>", "Traces for a client in cluster"},
        {"traces start <Name> topic <Topic>", "Traces for a topic in cluster"},
        {"traces start <Name> ip_address <IPAddr>", "Traces for a IP in cluster"},
        {"traces stop  <Name>", "Stop trace in cluster"},
        {"traces delete  <Name>", "Delete trace in cluster"}
    ]).

trace_cluster_on(Name, Type, Filter, DurationS0) ->
    case erlang:whereis(emqx_trace) of
        undefined ->
            emqx_ctl:print("[error] Tracer module not started~n"
            "Please run `emqx_ctl modules start tracer` "
            "or `emqx_ctl modules start emqx_mod_trace` first~n", []);
        _ ->
            DurationS = list_to_integer(DurationS0),
            Now = erlang:system_time(second),
            Trace = #{ name => list_to_binary(Name)
                     , type => atom_to_binary(Type)
                     , Type => list_to_binary(Filter)
                     , start_at => list_to_binary(calendar:system_time_to_rfc3339(Now))
                     , end_at => list_to_binary(calendar:system_time_to_rfc3339(Now + DurationS))
                     },
            case emqx_trace:create(Trace) of
                ok ->
                    emqx_ctl:print("Cluster_trace ~p ~s ~s successfully~n", [Type, Filter, Name]);
                {error, Error} ->
                    emqx_ctl:print("[error] Cluster_trace ~s ~s=~s ~p~n",
                        [Name, Type, Filter, Error])
            end
    end.

trace_cluster_del(Name) ->
    case emqx_trace:delete(list_to_binary(Name)) of
        ok -> emqx_ctl:print("Del cluster_trace ~s successfully~n", [Name]);
        {error, Error} -> emqx_ctl:print("[error] Del cluster_trace ~s: ~p~n", [Name, Error])
    end.

trace_cluster_off(Name) ->
    case emqx_trace:update(list_to_binary(Name), false) of
        ok -> emqx_ctl:print("Stop cluster_trace ~s successfully~n", [Name]);
        {error, Error} -> emqx_ctl:print("[error] Stop cluster_trace ~s: ~p~n", [Name, Error])
    end.

trace_type("client") -> {ok, clientid};
trace_type("topic") -> {ok, topic};
trace_type("ip_address") -> {ok, ip_address};
trace_type(_) -> error.
%%--------------------------------------------------------------------
%% @doc Listeners Command

listeners([]) ->
    lists:foreach(fun({{Protocol, ListenOn}, _Pid}) ->
                Info = [{listen_on,      {string, emqx_listeners:format_listen_on(ListenOn)}},
                        {acceptors,      esockd:get_acceptors({Protocol, ListenOn})},
                        {max_conns,      esockd:get_max_connections({Protocol, ListenOn})},
                        {current_conn,   esockd:get_current_connections({Protocol, ListenOn})},
                        {shutdown_count, esockd:get_shutdown_count({Protocol, ListenOn})}
                       ],
                    emqx_ctl:print("~s~n", [listener_identifier(Protocol, ListenOn)]),
                lists:foreach(fun indent_print/1, Info)
            end, esockd:listeners()),
    lists:foreach(fun({Protocol, Opts}) ->
        Port = proplists:get_value(port, Opts),
        Acceptors = maps:get(num_acceptors, proplists:get_value(transport_options, Opts, #{}), 0),
        Info = [{listen_on,      {string, emqx_listeners:format_listen_on(Port)}},
            {acceptors,      Acceptors},
            {max_conns,      proplists:get_value(max_connections, Opts)},
            {current_conn,   proplists:get_value(all_connections, Opts)},
            {shutdown_count, []}],
        emqx_ctl:print("~s~n", [listener_identifier(Protocol, Port)]),
        lists:foreach(fun indent_print/1, Info)
                  end, ranch:info());

listeners(["stop",  Name = "http" ++ _N | _MaybePort]) ->
    %% _MaybePort is to be backward compatible, to stop http listener,
    %% there is no need for the port number
    case minirest:stop_http(list_to_atom(Name)) of
        ok ->
            emqx_ctl:print("Stop ~s listener successfully.~n", [Name]);
        {error, Error} ->
            emqx_ctl:print("Failed to stop ~s listener: ~0p~n", [Name, Error])
    end;

listeners(["stop", "mqtt:" ++ _ = Identifier]) ->
    stop_listener(emqx_listeners:find_by_id(Identifier), Identifier);

listeners(["stop", _Proto, ListenOn]) ->
    %% this clause is kept to be backward compatible
    ListenOn1 = case string:tokens(ListenOn, ":") of
        [Port]     -> list_to_integer(Port);
        [IP, Port] -> {IP, list_to_integer(Port)}
    end,
    stop_listener(emqx_listeners:find_by_listen_on(ListenOn1), ListenOn1);

listeners(["restart", "http:management"]) ->
    restart_http_listener(http, emqx_management);

listeners(["restart", "https:management"]) ->
    restart_http_listener(https, emqx_management);

listeners(["restart", "http:dashboard"]) ->
    restart_http_listener(http, emqx_dashboard);

listeners(["restart", "https:dashboard"]) ->
    restart_http_listener(https, emqx_dashboard);

listeners(["restart", Identifier]) ->
    case emqx_listeners:restart_listener(Identifier) of
        ok ->
            emqx_ctl:print("Restarted ~s listener successfully.~n", [Identifier]);
        {error, Error} ->
            emqx_ctl:print("Failed to restart ~s listener: ~0p~n", [Identifier, Error])
    end;

listeners(_) ->
    emqx_ctl:usage([{"listeners",                        "List listeners"},
                    {"listeners stop    <Identifier>",   "Stop a listener"},
                    {"listeners stop    <Proto> <Port>", "Stop a listener"},
                    {"listeners restart <Identifier>",   "Restart a listener"}
                   ]).

stop_listener(false, Input) ->
    emqx_ctl:print("No such listener ~p~n", [Input]);
stop_listener(#{listen_on := ListenOn} = Listener, _Input) ->
    ID = emqx_listeners:identifier(Listener),
    ListenOnStr = emqx_listeners:format_listen_on(ListenOn),
    case emqx_listeners:stop_listener(Listener) of
        ok ->
            emqx_ctl:print("Stop ~s listener on ~s successfully.~n", [ID, ListenOnStr]);
        {error, Reason} ->
            emqx_ctl:print("Failed to stop ~s listener on ~s: ~0p~n",
                           [ID, ListenOnStr, Reason])
    end.

%%--------------------------------------------------------------------
%% @doc data Command

data(["export"]) ->
    case emqx_mgmt_data_backup:export() of
        {ok, #{filename := Filename}} ->
            emqx_ctl:print("The emqx data has been successfully exported to ~s.~n", [Filename]);
        {error, Reason} ->
            emqx_ctl:print("The emqx data export failed due to ~p.~n", [Reason])
    end;

data(["import", Filename]) ->
    data(["import", Filename, "--env", "{}"]);
data(["import", Filename, "--env", Env]) ->
    case emqx_mgmt_data_backup:import(Filename, Env) of
        ok ->
            emqx_ctl:print("The emqx data has been imported successfully.~n");
        {error, import_failed} ->
            emqx_ctl:print("The emqx data import failed.~n");
        {error, unsupported_version} ->
            emqx_ctl:print("The emqx data import failed: Unsupported version.~n");
        {error, Reason} ->
            emqx_ctl:print("The emqx data import failed: ~0p while reading ~s.~n",
                [Reason, Filename])
    end;

data(_) ->
    emqx_ctl:usage([{"data import <File> [--env '<json>']",
                     "Import data from the specified file, possibly with overrides"},
                    {"data export", "Export data"}]).

%%--------------------------------------------------------------------
%% @doc acl Command

acl(["cache-clean", "node", Node]) ->
    with_log(fun() -> for_node(fun emqx_mgmt:clean_acl_cache_all/1, Node) end,
             "ACL cache drain start");
acl(["cache-clean", "all"]) ->
    with_log(fun emqx_mgmt:clean_acl_cache_all/0,
             "ACL cache drain start");
acl(["cache-clean", ClientId]) ->
    emqx_mgmt:clean_acl_cache(ClientId);

acl(_) ->
    emqx_ctl:usage([{"acl cache-clean all",             "Clears acl cache on all nodes"},
                    {"acl cache-clean node <Node>",     "Clears acl cache on given node"},
                    {"acl cache-clean <ClientId>",      "Clears acl cache for given client"}
                   ]).

pem_cache(["clean", "all"]) ->
    with_log(fun emqx_mgmt:clean_pem_cache/0, "PEM cache clean");
pem_cache(["clean", "node", Node]) ->
    with_log(fun() -> for_node(fun emqx_mgmt:clean_pem_cache/1, Node) end, "PEM cache clean");
pem_cache(_) ->
    emqx_ctl:usage([{"pem_cache clean all",         "Clears x509 certificate cache on all nodes"},
                    {"pem_cache clean node <Node>", "Clears x509 certificate cache on given node"}
                   ]).

%%--------------------------------------------------------------------
%% Dump ETS
%%--------------------------------------------------------------------

dump(Table) ->
    dump(Table, Table, ets:first(Table), []).

dump(Table, Tag) ->
    dump(Table, Tag, ets:first(Table), []).

dump(_Table, _, '$end_of_table', Result) ->
    lists:reverse(Result);

dump(Table, Tag, Key, Result) ->
    Ls = lists:foldl(fun(Record, Acc) ->
            try
                [print({Tag, Record}) | Acc]
            catch
                Class : Reason : Stk ->
                    logger:error("Failed to print ~p, error: {~p, ~p}. "
                                 "Stacktrace: ~0p",
                                 [Record, Class, Reason, Stk]),
                    Acc
            end
         end, [], ets:lookup(Table, Key)),
    dump(Table, Tag, ets:next(Table, Key), [lists:reverse(Ls) | Result]).

print({_, []}) ->
    ok;

print({client, {ClientId, ChanPid}}) ->
    Attrs = case emqx_cm:get_chan_info(ClientId, ChanPid) of
                undefined -> #{};
                Attrs0 -> Attrs0
            end,
    Stats = case emqx_cm:get_chan_stats(ClientId, ChanPid) of
                undefined -> #{};
                Stats0 -> maps:from_list(Stats0)
            end,
    ClientInfo = maps:get(clientinfo, Attrs, #{}),
    ConnInfo = maps:get(conninfo, Attrs, #{}),
    Session = maps:get(session, Attrs, #{}),
    Connected = case maps:get(conn_state, Attrs, undefined) of
                    connected -> true;
                    _ -> false
                end,
    Info = lists:foldl(fun(Items, Acc) ->
                               maps:merge(Items, Acc)
                       end, #{connected => Connected},
                       [maps:with([subscriptions_cnt, inflight_cnt, awaiting_rel_cnt,
                                   mqueue_len, mqueue_dropped, send_msg], Stats),
                        maps:with([clientid, username], ClientInfo),
                        maps:with([peername, clean_start, keepalive, expiry_interval,
                                   connected_at, disconnected_at], ConnInfo),
                        maps:with([created_at], Session)]),
    InfoKeys = [clientid, username, peername,
                clean_start, keepalive, expiry_interval,
                subscriptions_cnt, inflight_cnt, awaiting_rel_cnt,
                send_msg, mqueue_len, mqueue_dropped,
                connected, created_at, connected_at] ++
                case maps:is_key(disconnected_at, Info) of
                    true  -> [disconnected_at];
                    false -> []
                end,
    emqx_ctl:print("Client(~s, username=~s, peername=~s, "
                   "clean_start=~s, keepalive=~w, session_expiry_interval=~w, "
                   "subscriptions=~w, inflight=~w, awaiting_rel=~w, "
                   "delivered_msgs=~w, enqueued_msgs=~w, dropped_msgs=~w, "
                   "connected=~s, created_at=~w, connected_at=~w" ++
                   case maps:is_key(disconnected_at, Info) of
                       true  -> ", disconnected_at=~w)~n";
                       false -> ")~n"
                   end,
        [format(K, maps:get(K, Info)) || K <- InfoKeys]);

print({emqx_route, #route{topic = Topic, dest = {_, Node}}}) ->
    emqx_ctl:print("~s -> ~s~n", [Topic, Node]);
print({emqx_route, #route{topic = Topic, dest = Node}}) ->
    emqx_ctl:print("~s -> ~s~n", [Topic, Node]);

print(#plugin{name = Name, descr = Descr, active = Active}) ->
    emqx_ctl:print("Plugin(~s, description=~s, active=~s)~n",
                  [Name, Descr, Active]);

print({emqx_suboption, {{Pid, Topic}, Options}}) when is_pid(Pid) ->
    emqx_ctl:print("~s -> ~s~n", [maps:get(subid, Options), Topic]).

format(_, undefined) ->
    undefined;

format(peername, {IPAddr, Port}) ->
    IPStr = emqx_mgmt_util:ntoa(IPAddr),
    io_lib:format("~s:~p", [IPStr, Port]);

format(_, Val) ->
    Val.

bin(S) -> iolist_to_binary(S).

indent_print({Key, {string, Val}}) ->
    emqx_ctl:print("  ~-16s: ~s~n", [Key, Val]);
indent_print({Key, Val}) ->
    emqx_ctl:print("  ~-16s: ~w~n", [Key, Val]).

listener_identifier(Protocol, ListenOn) ->
    case emqx_listeners:find_id_by_listen_on(ListenOn) of
        false ->
            atom_to_list(Protocol);
        ID ->
            ID
    end.

restart_http_listener(Scheme, AppName) ->
    Listeners = application:get_env(AppName, listeners, []),
    case lists:keyfind(Scheme, 1, Listeners) of
        false ->
            emqx_ctl:print("Listener ~s not exists!~n", [AppName]);
        {Scheme, Port, Options} ->
            ModName = http_mod_name(AppName),
            ModName:stop_listener({Scheme, Port, Options}),
            ModName:start_listener({Scheme, Port, Options})
    end.

http_mod_name(emqx_management) -> emqx_mgmt_http;
http_mod_name(Name) -> Name.

print_app_info({AppId, AppSecret, Name, Desc, Status, Expired}) ->
    emqx_ctl:print("app_id: ~s, secret: ~s, name: ~s, desc: ~s, status: ~s, expired: ~p~n",
        [AppId, AppSecret, Name, Desc, Status, Expired]).

for_node(Fun, Node) ->
    try list_to_existing_atom(Node) of
        NodeAtom ->
            Fun(NodeAtom)
    catch
        error : badarg ->
            {error, unknown_node}
    end.

with_log(Fun, Msg) ->
    case Fun() of
        ok ->
            emqx_ctl:print("~s OK~n", [Msg]);
        {error, Reason} ->
            emqx_ctl:print("~s FAILED~n~p~n", [Msg, Reason])
    end.
