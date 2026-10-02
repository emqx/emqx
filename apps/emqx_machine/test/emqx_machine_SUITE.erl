%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_machine_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(BRIDGE_NAME, <<"stop_sources">>).

-define(APPS, [
    emqx_prometheus,
    emqx_modules,
    emqx_dashboard,
    emqx_gateway,
    emqx_resource,
    emqx_rule_engine,
    emqx_bridge,
    emqx_management,
    emqx_retainer,
    emqx_exhook,
    emqx_auth,
    emqx_plugin
]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

app_specs() ->
    [
        emqx_conf,
        emqx_prometheus,
        emqx_modules,
        emqx_dashboard,
        emqx_gateway,
        emqx_resource,
        emqx_rule_engine,
        emqx_bridge,
        emqx_management,
        emqx_retainer,
        emqx_exhook,
        emqx_auth,
        emqx_plugins,
        {emqx_license, "license { key = \"default\" }"}
    ].

init_per_testcase(t_custom_shard_transports, Config) ->
    OldConfig = application:get_env(emqx_machine, custom_shard_transports),
    [{old_config, OldConfig} | Config];
init_per_testcase(t_open_ports_check = TestCase, Config) ->
    AppSpecs = [emqx],
    Cluster = [
        {emqx_machine_SUITE1, #{role => core, apps => AppSpecs}},
        {emqx_machine_SUITE2, #{role => core, apps => AppSpecs}},
        {emqx_machine_SUITE3, #{role => replicant, apps => AppSpecs}}
    ],
    Nodes = emqx_cth_cluster:start(Cluster, #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}),
    [{nodes, Nodes} | Config];
init_per_testcase(t_sorted_reboot_apps, Config) ->
    application:set_env(emqx_machine, applications, ?APPS ++ [emqx_license]),
    Config;
init_per_testcase(_TestCase, Config) ->
    application:set_env(emqx_machine, applications, ?APPS),
    Config.

end_per_testcase(t_custom_shard_transports, Config) ->
    OldConfig0 = ?config(old_config, Config),
    application:stop(ekka),
    case OldConfig0 of
        {ok, OldConfig} ->
            application:set_env(emqx_machine, custom_shard_transports, OldConfig);
        undefined ->
            application:unset_env(emqx_machine, custom_shard_transports)
    end,
    ok;
end_per_testcase(t_open_ports_check, Config) ->
    Nodes = ?config(nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

t_shutdown_reboot(Config) ->
    [Node] = emqx_cth_cluster:start(
        [{machine_reboot_SUITE1, #{role => core, apps => app_specs()}}],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    try
        erpc:call(Node, fun() ->
            true = emqx:is_running(node()),
            emqx_machine_boot:stop_apps(),
            false = emqx:is_running(node()),
            %% This is to emulate the presence of `emqx.conf' or `cluster.hocon' files,
            %% which are not present in the peer.
            %% This is done by `emqx_cth_suite' initially.
            ok = emqx_app:set_config_loader(emqx_cth_suite),
            emqx_machine_boot:ensure_apps_started(),
            true = emqx:is_running(node()),
            %% managed boot marks the node ready once apps and plugins are started
            true = emqx_node_readiness:is_ready(),
            ok = emqx_machine_boot:stop_apps(),
            false = emqx:is_running(node()),
            ok
        end)
    after
        catch emqx_cth_cluster:stop([Node])
    end.

-doc """
Verify `emqx_machine_boot:stop_apps/0' shuts the readiness gate and stops the
MQTT listeners before it stops the applications, so no client traffic reaches
hook callbacks (e.g. the rule engine) while their state is torn down.  Also
verify `stop_apps/0' does not raise when called twice, and that
`ensure_apps_started/0' restores listeners and readiness.
""".
t_stop_apps_stops_listeners_first(Config) ->
    [Node] = emqx_cth_cluster:start(
        [{machine_listeners_SUITE1, #{role => core, apps => app_specs()}}],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    try
        Port = emqx_cth_cluster:get_tcp_mqtt_port(Node),
        %% The listener accepts connections before shutdown.
        {ok, Sock0} = gen_tcp:connect("127.0.0.1", Port, [], 5000),
        ok = gen_tcp:close(Sock0),
        ?check_trace(
            ok = erpc:call(Node, emqx_machine_boot, stop_apps, []),
            fun(Trace) ->
                %% Listeners must be stopped before the rule engine app.
                Events = [
                    Event
                 || #{?snk_kind := Kind} = Event <- Trace,
                    Kind =:= emqx_listeners_stopped orelse
                        (Kind =:= machine_stopping_app andalso
                            maps:get(app, Event) =:= emqx_rule_engine)
                ],
                ?assertMatch([#{?snk_kind := emqx_listeners_stopped} | _], Events)
            end
        ),
        %% The readiness gate is shut and the listener no longer accepts.
        ?assertNot(erpc:call(Node, emqx_node_readiness, is_ready, [])),
        ?assertEqual({error, econnrefused}, gen_tcp:connect("127.0.0.1", Port, [], 5000)),
        %% A second stop does not raise.
        ok = erpc:call(Node, emqx_machine_boot, stop_apps, []),
        %% The reboot path (cluster join/leave) restores listeners and readiness.
        ok = erpc:call(Node, emqx_app, set_config_loader, [emqx_cth_suite]),
        ok = erpc:call(Node, emqx_machine_boot, ensure_apps_started, []),
        ?assert(erpc:call(Node, emqx_node_readiness, is_ready, [])),
        {ok, Sock1} = gen_tcp:connect("127.0.0.1", Port, [], 5000),
        ok = gen_tcp:close(Sock1)
    after
        catch emqx_cth_cluster:stop([Node])
    end.

-doc """
Verify `emqx_machine_boot:stop_apps/0' removes every source from its connector
before it stops the listeners, and leaves the actions and the source
configuration in place.  Also verify `ensure_apps_started/0' installs the
source again.
""".
t_stop_apps_stops_sources_before_listeners(Config) ->
    [Node] = emqx_cth_cluster:start(
        [{machine_sources_SUITE1, #{role => core, apps => app_specs() ++ [emqx_bridge_mqtt]}}],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    try
        Port = emqx_cth_cluster:get_tcp_mqtt_port(Node),
        {ConnectorResId, SourceId, ActionId} =
            erpc:call(Node, fun() -> create_mqtt_source_and_action(Port) end),
        ?assertEqual(lists:sort([SourceId, ActionId]), installed_channels(Node, ConnectorResId)),
        TestPid = self(),
        ?check_trace(
            begin
                %% `emqx_listeners:stop/0' runs again when the `emqx' app
                %% stops; only the first call is inspected.
                snabbkaffe_nemesis:inject_crash(
                    ?match_event(#{?snk_kind := emqx_listeners_stopped}),
                    fun
                        (1) ->
                            TestPid !
                                {listeners_stopped, installed_channels(Node, ConnectorResId),
                                    source_exists(Node)},
                            false;
                        (_) ->
                            false
                    end
                ),
                ok = erpc:call(Node, emqx_machine_boot, stop_apps, []),
                receive
                    {listeners_stopped, Channels, SourceExists} -> {Channels, SourceExists}
                after 5_000 ->
                    ct:fail(listeners_not_stopped)
                end
            end,
            fun({Channels, SourceExists}, _Trace) ->
                ?assertEqual([ActionId], Channels),
                ?assert(SourceExists)
            end
        ),
        %% The reboot path (cluster join/leave) installs the source again.
        ok = erpc:call(Node, emqx_app, set_config_loader, [emqx_cth_suite]),
        ok = erpc:call(Node, emqx_machine_boot, ensure_apps_started, []),
        ?retry(
            100,
            50,
            ?assertEqual(
                lists:sort([SourceId, ActionId]), installed_channels(Node, ConnectorResId)
            )
        )
    after
        catch emqx_cth_cluster:stop([Node])
    end.

t_sorted_reboot_apps(_Config) ->
    Apps = emqx_machine_boot:sorted_reboot_apps(),
    SortApps = [App || App <- Apps, (App =:= emqx_dashboard orelse App =:= emqx_license)],
    %% make sure emqx_license start early than emqx_dashboard
    ?assertEqual([emqx_license, emqx_dashboard], SortApps).

-doc """
`gproc` holds the registry `mria` reads, so restarting it when the node joins or
leaves the cluster crashes `mria`. It must stay out of the reboot list: it is
started once, from the OTP application list in `reboot_lists.eterm`.
""".
t_gproc_is_not_a_reboot_app(_Config) ->
    ?assertNot(lists:member(gproc, emqx_machine_boot:sorted_reboot_apps())).

t_custom_shard_transports(_Config) ->
    %% used to ensure the atom exists
    Shard = test_shard,
    %% the config keys are binaries
    ShardBin = atom_to_binary(Shard),
    DefaultTransport = distr,
    ?assertEqual(DefaultTransport, mria_config:shard_transport(Shard)),
    application:set_env(emqx_machine, custom_shard_transports, #{ShardBin => distr}),
    emqx_machine:start(),
    ?assertEqual(distr, mria_config:shard_transport(Shard)),
    ok.

t_node_status(_Config) ->
    JSON = emqx_machine:node_status(),
    ?assertMatch(
        #{
            <<"backend">> := _,
            <<"role">> := <<"core">>
        },
        emqx_utils_json:decode(JSON)
    ).

t_open_ports_check(Config) ->
    [Core1, Core2, Replicant] = ?config(nodes, Config),

    Plan = erpc:call(Core1, emqx_machine, create_plan, []),
    ?assertMatch(
        [{Core2, #{ports_to_check := [_GenRPC0, _Ekka0], resolved_ips := [_]}}],
        Plan
    ),
    [{Core2, #{ports_to_check := [GenRPCPort, EkkaPort], resolved_ips := [_]}}] = Plan,
    ?assertMatch(
        [{Core1, #{ports_to_check := [_GenRPC1, _Ekka1], resolved_ips := [_]}}],
        erpc:call(Core2, emqx_machine, create_plan, [])
    ),
    ?assertMatch(
        [],
        erpc:call(Replicant, emqx_machine, create_plan, [])
    ),

    ?assertEqual(ok, erpc:call(Core1, emqx_machine, open_ports_check, [])),
    ?assertEqual(ok, erpc:call(Core2, emqx_machine, open_ports_check, [])),
    ?assertEqual(ok, erpc:call(Replicant, emqx_machine, open_ports_check, [])),

    true = erlang:monitor_node(Core2, true),
    ok = emqx_cth_cluster:stop_node(Core2),
    receive
        {nodedown, Core2} -> ok
    after 10000 ->
        ct:fail("nodedown message not received after 10 seconds.")
    end,

    ?assertEqual(ok, erpc:call(Replicant, emqx_machine, open_ports_check, [])),
    ?retry(200, 20, begin
        Results = erpc:call(Core1, emqx_machine, open_ports_check, []),
        ?assertMatch(
            #{
                msg := "some ports are unreachable",
                results :=
                    #{
                        Core2 :=
                            #{
                                open_ports := #{},
                                ports_to_check := [_, _],
                                resolved_ips := [_],
                                status := bad_ports
                            }
                    }
            },
            Results,
            #{core2 => Core2, gen_rpc_port => GenRPCPort, ekka_port => EkkaPort}
        ),
        %% 2 ports to check; we don't assert the exact ekka port because, when running
        %% multiple nodes on the same machine as we do in tests, the order of returned ports
        %% might change between invocations.
        NumPorts = 2,
        ?assertEqual(
            NumPorts, map_size(emqx_utils_maps:deep_get([results, Core2, open_ports], Results))
        ),
        ok
    end),
    ok.

create_mqtt_source_and_action(Port) ->
    Name = ?BRIDGE_NAME,
    ConnectorConfig = emqx_bridge_schema_testlib:mqtt_connector_config(#{
        <<"server">> => iolist_to_binary(["127.0.0.1:", integer_to_list(Port)]),
        <<"pool_size">> => 1
    }),
    {ok, _} = emqx_connector:create(?global_ns, mqtt, Name, ConnectorConfig),
    SourceConfig = emqx_bridge_schema_testlib:mqtt_source_config(#{
        <<"connector">> => Name,
        <<"parameters">> => #{<<"topic">> => <<"t/source">>}
    }),
    {ok, _} = emqx_bridge_v2:create(?global_ns, sources, mqtt, Name, SourceConfig),
    ActionConfig = emqx_bridge_schema_testlib:mqtt_action_config(#{
        <<"connector">> => Name,
        <<"parameters">> => #{<<"topic">> => <<"t/action">>}
    }),
    {ok, _} = emqx_bridge_v2:create(?global_ns, actions, mqtt, Name, ActionConfig),
    lists:foreach(
        fun(Kind) ->
            ?retry(
                100,
                50,
                ?assertMatch(
                    {ok, #{status := connected}},
                    emqx_bridge_v2:lookup(?global_ns, Kind, mqtt, Name)
                )
            )
        end,
        [sources, actions]
    ),
    {
        emqx_connector_resource:resource_id(?global_ns, mqtt, Name),
        emqx_bridge_v2:lookup_chan_id_in_conf(?global_ns, sources, mqtt, Name),
        emqx_bridge_v2:lookup_chan_id_in_conf(?global_ns, actions, mqtt, Name)
    }.

source_exists(Node) ->
    erpc:call(Node, emqx_bridge_v2, is_source_exist, [?global_ns, mqtt, ?BRIDGE_NAME]).

installed_channels(Node, ConnectorResId) ->
    {ok, _, #{added_channels := Channels}} =
        erpc:call(Node, emqx_resource, get_instance, [ConnectorResId]),
    lists:sort(maps:keys(Channels)).
