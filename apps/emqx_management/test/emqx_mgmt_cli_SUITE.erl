%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

-define(ON(NODES, BODY),
    emqx_ds_test_helpers:on(NODES, fun() -> BODY end)
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_mgmt_cli:load(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(TC, Config) ->
    try
        emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config)
    catch
        throw:{skip, Reason} -> {skip, Reason}
    end.

end_per_testcase(TC, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config).

t_status(_Config) ->
    emqx_ctl:run_command([]),
    emqx_ctl:run_command(["status"]),
    ok.

t_broker(_Config) ->
    %% broker         # Show broker version, uptime and description
    emqx_ctl:run_command(["broker"]),
    %% broker stats   # Show broker statistics of clients, topics, subscribers
    emqx_ctl:run_command(["broker", "stats"]),
    %% broker metrics # Show broker metrics
    emqx_ctl:run_command(["broker", "metrics"]),
    ok.

t_cluster(_Config) ->
    SelfNode = node(),
    FakeNode = 'fake@127.0.0.1',
    MFA = {?MODULE, format, [""]},
    meck:new(mria_mnesia, [non_strict, passthrough, no_link]),
    meck:expect(mria_mnesia, running_nodes, 0, [SelfNode, FakeNode]),
    {atomic, {ok, TnxId, _}} =
        mria:transaction(
            emqx_cluster_rpc_shard,
            fun emqx_cluster_rpc:init_mfa/2,
            [SelfNode, MFA]
        ),
    emqx_cluster_rpc:maybe_init_tnx_id(FakeNode, TnxId),
    ?assertMatch(
        {atomic, [
            #{
                node := SelfNode,
                mfa := MFA,
                created_at := _,
                tnx_id := TnxId,
                initiator := SelfNode
            },
            #{
                node := FakeNode,
                mfa := MFA,
                created_at := _,
                tnx_id := TnxId,
                initiator := SelfNode
            }
        ]},
        emqx_cluster_rpc:status()
    ),
    %% cluster join <Node>        # Join the cluster
    %% cluster leave              # Leave the cluster
    %% cluster force-leave <Node> # Force the node leave from cluster
    %% cluster status             # Cluster status
    emqx_ctl:run_command(["cluster", "status"]),

    ?assertEqual(
        {error, {node_down, 'nosuchnode@127.0.0.1'}},
        emqx_ctl:run_command(["cluster", "join", "nosuchnode@127.0.0.1"])
    ),

    emqx_ctl:run_command(["cluster", "force-leave", atom_to_list(FakeNode)]),
    ?assertMatch(
        {atomic, [
            #{
                node := SelfNode,
                mfa := MFA,
                created_at := _,
                tnx_id := TnxId,
                initiator := SelfNode
            }
        ]},
        emqx_cluster_rpc:status()
    ),
    meck:unload(mria_mnesia),
    ok.

t_clients(_Config) ->
    %% clients list            # List all clients
    emqx_ctl:run_command(["clients", "list"]),
    %% clients show <ClientId> # Show a client
    %% clients kick <ClientId> # Kick out a client
    ok.

t_routes(_Config) ->
    %% routes list         # List all routes
    emqx_ctl:run_command(["routes", "list"]),
    %% routes show <Topic> # Show a route
    ok.

t_subscriptions(_Config) ->
    %% subscriptions list                         # List all subscriptions
    emqx_ctl:run_command(["subscriptions", "list"]),
    %% subscriptions show <ClientId>              # Show subscriptions of a client
    %% subscriptions add <ClientId> <Topic> <QoS> # Add a static subscription manually
    %% subscriptions del <ClientId> <Topic>       # Delete a static subscription manually
    ok.

t_plugins(_Config) ->
    %% plugins <command> [Name-Vsn]          # e.g. 'start emqx_plugin_template-5.0-rc.1'
    %% plugins list                          # List all installed plugins
    emqx_ctl:run_command(["plugins", "list"]),
    %% plugins describe  Name-Vsn            # Describe an installed plugins
    %% plugins install   Name-Vsn            # Install a plugin package placed
    %%                                       # in plugin'sinstall_dir
    %% plugins uninstall Name-Vsn            # Uninstall a plugin. NOTE: it deletes
    %%                                       # all files in install_dir/Name-Vsn
    %% plugins start     Name-Vsn            # Start a plugin
    %% plugins stop      Name-Vsn            # Stop a plugin
    %% plugins restart   Name-Vsn            # Stop then start a plugin
    %% plugins disable   Name-Vsn            # Disable auto-boot
    %% plugins enable    Name-Vsn [Position] # Enable auto-boot at Position in the boot list, where Position could be
    %%                                       # 'front', 'rear', or 'before Other-Vsn' to specify a relative position.
    %%                                       # The Position parameter can be used to adjust the boot order.
    %%                                       # If no Position is given, an already configured plugin
    %%                                       # will stay at is old position; a newly plugin is appended to the rear
    %%                                       # e.g. plugins disable foo-0.1.0 front
    %%                                       #      plugins enable bar-0.2.0 before foo-0.1.0
    ok.

t_vm(_Config) ->
    %% vm all     # Show info of Erlang VM
    emqx_ctl:run_command(["vm", "all"]),
    %% vm load    # Show load of Erlang VM
    emqx_ctl:run_command(["vm", "load"]),
    %% vm memory  # Show memory of Erlang VM
    emqx_ctl:run_command(["vm", "memory"]),
    %% vm process # Show process of Erlang VM
    emqx_ctl:run_command(["vm", "process"]),
    %% vm io      # Show IO of Erlang VM
    emqx_ctl:run_command(["vm", "io"]),
    %% vm ports   # Show Ports of Erlang VM
    emqx_ctl:run_command(["vm", "ports"]),
    ok.

t_mnesia(_Config) ->
    %% mnesia # Mnesia system info
    emqx_ctl:run_command(["mnesia"]),
    ok.

t_log(_Config) ->
    %% log set-level <Level>                      # Set the overall log level
    %% log primary-level                          # Show the primary log level now
    emqx_ctl:run_command(["log", "primary-level"]),
    %% log primary-level <Level>                  # Set the primary log level
    %% log handlers list                          # Show log handlers
    emqx_ctl:run_command(["log", "handlers", "list"]),
    %% log handlers start <HandlerId>             # Start a log handler
    %% log handlers stop  <HandlerId>             # Stop a log handler
    %% log handlers set-level <HandlerId> <Level> # Set log level of a log handler
    ok.

t_trace(_Config) ->
    %% trace list                                        # List all traces started on local node
    emqx_ctl:run_command(["trace", "list"]),
    %% trace start client <ClientId> <File> [<Level>]    # Traces for a client on local node
    %% trace stop  client <ClientId>                     # Stop tracing for a client on local node
    %% trace start topic  <Topic>    <File> [<Level>]    # Traces for a topic on local node
    %% trace stop  topic  <Topic>                        # Stop tracing for a topic on local node
    %% trace start ip_address  <IP>    <File> [<Level>]  # Traces for a client ip on local node
    %% trace stop  ip_addresss  <IP>                     # Stop tracing for a client ip on local node
    ok.

t_traces(_Config) ->
    %% traces list                             # List all cluster traces started
    emqx_ctl:run_command(["traces", "list"]),
    %% traces start <Name> client <ClientId>   # Traces for a client in cluster
    %% traces start <Name> topic <Topic>       # Traces for a topic in cluster
    %% traces start <Name> ip_address <IPAddr> # Traces for a IP in cluster
    %% traces stop  <Name>                     # Stop trace in cluster
    %% traces delete  <Name>                   # Delete trace in cluster
    ok.

t_traces_client(_Config) ->
    TraceC = "TraceNameClientID",
    emqx_ctl:run_command(["traces", "start", TraceC, "client", "ClientID"]),
    emqx_ctl:run_command(["traces", "stop", TraceC]),
    emqx_ctl:run_command(["traces", "delete", TraceC]).

t_traces_client_with_duration(_Config) ->
    TraceC = "TraceNameClientID",
    Duration = "1000",
    emqx_ctl:run_command(["traces", "start", TraceC, "client", "ClientID", Duration]),
    emqx_ctl:run_command(["traces", "stop", TraceC]),
    emqx_ctl:run_command(["traces", "delete", TraceC]).

t_traces_topic(_Config) ->
    TraceT = "TraceNameTopic",
    emqx_ctl:run_command(["traces", "start", TraceT, "topic", "a/b"]),
    emqx_ctl:run_command(["traces", "stop", TraceT]),
    emqx_ctl:run_command(["traces", "delete", TraceT]).

t_traces_ip(_Config) ->
    TraceI = "TraceNameIP",
    emqx_ctl:run_command(["traces", "start", TraceI, "ip_address", "127.0.0.1"]),
    emqx_ctl:run_command(["traces", "stop", TraceI]),
    emqx_ctl:run_command(["traces", "delete", TraceI]).

t_listeners(_Config) ->
    %% listeners                      # List listeners
    emqx_ctl:run_command(["listeners"]),
    %% listeners stop    <Identifier> # Stop a listener
    %% listeners start   <Identifier> # Start a listener
    %% listeners restart <Identifier> # Restart a listener
    ok.

t_authz(_Config) ->
    %% authz cache-clean all         # Clears authorization cache on all nodes
    ?assertMatch(ok, emqx_ctl:run_command(["authz", "cache-clean", "all"])),
    ClientId = "authz_clean_test",
    ClientIdBin = list_to_binary(ClientId),
    %% authz cache-clean <ClientId>  # Clears authorization cache for given client
    ?assertMatch({error, not_found}, emqx_ctl:run_command(["authz", "cache-clean", ClientId])),
    {ok, C} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"topic/1">>, 1),
    [Pid] = emqx_cm:lookup_channels(ClientIdBin),
    ?assertMatch([_], gen_server:call(Pid, list_authz_cache)),

    ?assertMatch(ok, emqx_ctl:run_command(["authz", "cache-clean", ClientId])),
    ?assertMatch([], gen_server:call(Pid, list_authz_cache)),
    %% authz cache-clean node <Node> # Clears authorization cache on given node
    {ok, _, _} = emqtt:subscribe(C, <<"topic/2">>, 1),
    ?assertMatch([_], gen_server:call(Pid, list_authz_cache)),
    ?assertMatch(ok, emqx_ctl:run_command(["authz", "cache-clean", "node", atom_to_list(node())])),
    ?assertMatch([], gen_server:call(Pid, list_authz_cache)),
    ok = emqtt:disconnect(C),
    ok.

t_olp(_Config) ->
    %% olp status  # Return OLP status if system is overloaded
    emqx_ctl:run_command(["olp", "status"]),
    %% olp enable  # Enable overload protection
    %% olp disable # Disable overload protection
    ok.

t_admin(_Config) ->
    %% admins add <Username> <Password> <Description> # Add dashboard user
    %% admins passwd <Username> <Password>            # Reset dashboard user password
    %% admins del <Username>                          # Delete dashboard user
    ok.

t_autocluster_leave('init', Config) ->
    [Core1, Core2, Repl1, Repl2] =
        Nodes = [
            t_autocluster_leave_core1,
            t_autocluster_leave_core2,
            t_autocluster_leave_replicant1,
            t_autocluster_leave_replicant2
        ],
    NodeNames = [emqx_cth_cluster:node_name(N) || N <- Nodes],
    AppSpec = [
        emqx,
        {emqx_conf, #{
            config => #{
                cluster => #{
                    discovery_strategy => static,
                    static => #{seeds => NodeNames}
                }
            }
        }},
        emqx_management
    ],
    Cluster = emqx_cth_cluster:start(
        [
            {Core1, #{role => core, apps => AppSpec}},
            {Core2, #{role => core, apps => AppSpec}},
            {Repl1, #{role => replicant, apps => AppSpec}},
            {Repl2, #{role => replicant, apps => AppSpec}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    [{cluster, Cluster} | Config];
t_autocluster_leave('end', Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)).

t_autocluster_leave(Config) ->
    [Core1, Core2, Repl1, Repl2] = Cluster = ?config(cluster, Config),
    ClusterView = [lists:sort(rpc:call(N, emqx, running_nodes, [])) || N <- Cluster],
    [View1, View2, View3, View4] = ClusterView,
    ?assertEqual(lists:sort(Cluster), View1),
    ?assertEqual(View1, View2),
    ?assertEqual(View1, View3),
    ?assertEqual(View1, View4),

    rpc:call(Core2, emqx_mgmt_cli, cluster, [["leave"]]),
    timer:sleep(1000),
    %% Replicant nodes can discover Core2 which is now split from [Core1, Core2],
    %% but they are  expected to ignore Core2,
    %% since mria_lb must filter out core nodes that disabled discovery.
    ?assertMatch([Core2], rpc:call(Core2, emqx, running_nodes, [])),
    ?assertEqual(undefined, rpc:call(Core1, erlang, whereis, [ekka_autocluster])),
    ?assertEqual(lists:sort([Core1, Repl1, Repl2]), rpc:call(Core1, emqx, running_nodes, [])),
    ?assertEqual(lists:sort([Core1, Repl1, Repl2]), rpc:call(Repl1, emqx, running_nodes, [])),
    ?assertEqual(lists:sort([Core1, Repl1, Repl2]), rpc:call(Repl2, emqx, running_nodes, [])),

    rpc:call(Repl1, emqx_mgmt_cli, cluster, [["leave"]]),
    timer:sleep(1000),
    ?assertEqual(lists:sort([Core1, Repl2]), rpc:call(Core1, emqx, running_nodes, [])),

    ct:pal("enabling discovery for core2"),
    rpc:call(Core2, emqx_mgmt_cli, cluster, [["discovery", "enable"]]),
    ct:pal("enabling discovery for repl1"),
    rpc:call(Repl1, emqx_mgmt_cli, cluster, [["discovery", "enable"]]),
    %% nodes will join and restart asyncly, may need more time to re-cluster
    ct:pal("waiting recovery"),
    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                [lists:sort(rpc:call(N, emqx, running_nodes, [])) || N <- Cluster] =:= ClusterView
            end,
            10_000
        )
    ).

t_leave_rejected_ds_nonempty('init', Config) ->
    ok = snabbkaffe:start_trace(),
    AppSpec = [
        emqx_conf,
        {emqx,
            "durable_sessions.enable = true \n"
            "durable_storage.messages.backend = builtin_raft \n"
            "durable_storage.messages.n_sites = 2 \n"},
        emqx_management
    ],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_leave_rejected_ds_nonempty1, #{role => core, apps => AppSpec}},
            {t_leave_rejected_ds_nonempty2, #{role => core, apps => AppSpec}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    Cluster = emqx_cth_cluster:start(NodeSpecs),
    [{cluster, Cluster}, {nodespecs, NodeSpecs} | Config];
t_leave_rejected_ds_nonempty('end', Config) ->
    ok = snabbkaffe:stop(),
    emqx_cth_cluster:stop(?config(cluster, Config)).

t_leave_rejected_ds_nonempty(Config) ->
    _Specs = [_, NS2] = ?config(nodespecs, Config),
    Nodes = [N1, N2] = ?config(cluster, Config),
    S2 = ?ON(N2, emqx_ds_builtin_raft_meta:this_site()),
    DB = messages,
    DBArg = "messages",
    S2Arg = binary_to_list(S2),

    %% Ensure DS has been bootstrapped.
    ok = emqx_ds_raft_test_helpers:wait_db_bootstrapped(Nodes, DB),
    ?ON(N1, emqx_mgmt_cli:ds(["info"])),

    %% Should not be possible to leave, because there are shard replicas.
    ?assertEqual(
        {error, [nonempty_ds_site]},
        ?ON(N2, emqx_mgmt_cli:cluster(["leave"]))
    ),

    %% Ask to leave DS DB replication, wait until transitions are finished.
    ?assertEqual(ok, ?ON(N1, emqx_mgmt_cli:ds(["leave", DBArg, S2Arg]))),
    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(DB)),

    %% Now leave the cluster again.
    ?assertEqual(ok, ?ON(N2, emqx_mgmt_cli:cluster(["leave"]))),
    %% Make N1 forget about S2
    LostSite = binary_to_list(?ON(N2, emqx_ds_builtin_raft_meta:this_site())),
    ?assertEqual(ok, ?ON(N1, emqx_mgmt_cli:ds(["forget", LostSite]))),
    ?retry(500, 10, undefined = ?ON(N1, emqx_ds_builtin_raft_meta:node(S2))),
    ?ON(N1, emqx_mgmt_cli:ds(["info"])),

    %% Join the cluster again.
    ?assertEqual(ok, ?ON(N2, emqx_mgmt_cli:cluster(["join", atom_to_list(N1)]))),
    [N2] = emqx_cth_cluster:restart(NS2),
    ?ON(N1, emqx_mgmt_cli:ds(["info"])),

    %% Ask to be DS DB replication site again.
    ?assertEqual(ok, ?ON(N1, emqx_mgmt_cli:ds(["join", DBArg, S2Arg]))),
    %% Should not be possible to leave, even if transitions are still in-progress.
    ?assertEqual(
        {error, [nonempty_ds_site]},
        ?ON(N1, emqx_mgmt_cli:cluster(["leave"]))
    ).

t_exclusive(_Config) ->
    emqx_ctl:run_command(["exclusive", "list"]),
    emqx_ctl:run_command(["exclusive", "delete", "t/1"]),
    ok.

%%

format(Str, Opts) ->
    io:format("str:~s: Opts:~p", [Str, Opts]).
