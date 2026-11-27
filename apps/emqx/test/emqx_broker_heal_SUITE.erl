%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_broker_heal_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_cm.hrl").
-include("emqx_shared_sub.hrl").

-define(GROUP, <<"heal_group">>).
-define(TOPIC, <<"t/heal">>).
-define(SHARE, <<"$share/heal_group/t/heal">>).

suite() -> [{timetrap, {minutes, 2}}].

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(Name, Config) ->
    Apps = [emqx],
    Cluster = emqx_cth_cluster:mk_nodespecs(
        [
            {core1, #{role => core, apps => Apps}},
            {core2, #{role => core, apps => Apps}},
            {repl1, #{role => replicant, apps => Apps}},
            {repl2, #{role => replicant, apps => Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Name, Config)}
    ),
    [{cluster, Cluster} | Config].

end_per_testcase(_, Config) ->
    emqx_cth_cluster:stop(proplists:get_value(cluster, Config)).

%% This testcase verifies basic functioning of emqx_broker_heal
%% process. It emulates loss of client information that occurs after
%% network partition is healed, then it triggers the heal and verifies
%% that clients restore their information.
t_heal_on_core_partition(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    [N1 | Rest] = Nodes = emqx_cth_cluster:start(Cluster),
    %% Verify roles:
    {[core, core, replicant, replicant], []} = rpc:multicall(Nodes, mria_rlog, role, []),
    %% Connect a client to each node:
    Clients = [
        element(
            2,
            emqtt:start_link(
                [{port, emqx_cth_cluster:get_tcp_mqtt_port(Node)}]
            )
        )
     || Node <- Nodes
    ],
    [{ok, _} = emqtt:connect(Conn) || Conn <- Clients],
    %% 1. Verify that all 4 clients are present:
    ?assertEqual(
        4,
        erpc:call(N1, emqx_cm_registry, table_size, [])
    ),
    %% 2. Emulate a simple case of table corruption that may happen
    %% after cluster heal:
    ?assertEqual(
        {atomic, ok},
        erpc:call(N1, mria, clear_table, [?CHAN_REG_TAB])
    ),
    ?assertEqual(
        0,
        erpc:call(N1, emqx_cm_registry, table_size, [])
    ),
    %% 3. Trigger heal:
    ok = erpc:call(N1, emqx_broker_heal, on_autoheal, [{[N1], [Rest]}]),
    ct:sleep(5_000),
    %% 4. Clients should re-register:
    ?assertMatch(
        [#channel{}, #channel{}, #channel{}, #channel{}],
        erpc:call(N1, ets, tab2list, [?CHAN_REG_TAB])
    ),
    [emqtt:stop(Conn) || Conn <- Clients],
    Config.

%% This testcase verifies that replicants attempt to heal channel registry after mria re-bootstrap.
t_heal_on_replicant_bootstrap(Config) ->
    ?check_trace(
        #{timetrap => 60_000},
        begin
            Cluster = proplists:get_value(cluster, Config),
            NClients = 10,
            [C1, C2, R1, _] = Nodes = emqx_cth_cluster:start(Cluster),
            #{shards_in_sync := Shards} = erpc:call(R1, mria_rlog, status, []),
            erpc:call(
                R1,
                fun() ->
                    %% Tune mria for faster reconnects:
                    application:set_env(mria, rlog_lb_update_timeout, 10),
                    application:set_env(mria, rlog_lb_update_timeout, 100)
                end
            ),
            %% Connect a number of clients to R1:
            Clients = [
                element(
                    2,
                    emqtt:start_link(
                        [{port, emqx_cth_cluster:get_tcp_mqtt_port(R1)}]
                    )
                )
             || _ <- lists:seq(1, NClients)
            ],
            [{ok, _} = emqtt:connect(Conn) || Conn <- Clients],
            ct:sleep(1_000),
            ?assertEqual(
                [{ok, NClients}, {ok, NClients}, {ok, NClients}, {ok, NClients}],
                erpc:multicall(Nodes, emqx_cm_registry, table_size, [])
            ),
            %% Shut down mria upstream on the cores:
            [
                ?assertMatch(
                    ok,
                    erpc:call(N, supervisor, terminate_child, [mria_sup, mria_rlog_sup])
                )
             || N <- [C1, C2]
            ],
            %% While the upstream is down, clear the table to imitate registry cleanup logic:
            ?assertEqual(
                {atomic, ok},
                erpc:call(C1, mria, clear_table, [?CHAN_REG_TAB])
            ),
            ?assertEqual(
                0,
                erpc:call(C1, emqx_cm_registry, table_size, [])
            ),
            %% Restart replication. R1 should re-boostrap, execute
            %% consistency check, and repair the registry:
            [
                erpc:call(
                    N,
                    fun() ->
                        ?assertMatch(
                            {ok, _},
                            erpc:call(N, supervisor, restart_child, [mria_sup, mria_rlog_sup])
                        ),
                        ok = mria_rlog:wait_for_shards(Shards, 5_000)
                    end
                )
             || N <- [C1, C2]
            ],
            ?retry(
                1_000,
                10,
                ?assertEqual(
                    [{ok, NClients}, {ok, NClients}, {ok, NClients}, {ok, NClients}],
                    erpc:multicall(Nodes, emqx_cm_registry, table_size, [])
                )
            ),
            [emqtt:stop(Conn) || Conn <- Clients],
            Config
        end,
        []
    ).

%% This testcase verifies that the clients write their shared
%% subscriptions to the cluster again when the heal is triggered. It
%% emulates the loss of shared subscription members that occurs after
%% a network partition is healed: the member tables of the nodes keep
%% the members, and the cluster table does not.
t_heal_shared_sub_on_core_partition(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    [N1 | Rest] = Nodes = emqx_cth_cluster:start(Cluster),
    Clients = [connect(Node) || Node <- Nodes],
    [?assertMatch({ok, _, [1]}, emqtt:subscribe(Conn, ?SHARE, 1)) || Conn <- Clients],
    %% 1. Verify that all nodes have the 4 members:
    Members = wait_shared_members(4, Nodes),
    %% 2. Emulate the loss of the members from the cluster:
    ?assertEqual(
        {atomic, ok},
        erpc:call(N1, mria, clear_table, [?SHARED_SUBSCRIPTION])
    ),
    ?assertEqual([], wait_shared_members(0, Nodes)),
    %% 3. Trigger heal:
    ok = erpc:call(N1, emqx_broker_heal, on_autoheal, [{[N1], [Rest]}]),
    %% 4. Clients should write the same members again:
    ?assertEqual(Members, wait_shared_members(4, Nodes)),
    %% 5. A message to the group should reach one member:
    _ = erpc:call(N1, emqx, publish, [emqx_message:make(<<"heal">>, 1, ?TOPIC, <<"1">>)]),
    ?assertMatch([_], receive_publishes(2_000)),
    [emqtt:stop(Conn) || Conn <- Clients],
    Config.

%% This testcase verifies that the clients of a replicant write their
%% shared subscriptions to the cluster again after mria re-bootstrap.
t_heal_shared_sub_on_replicant_bootstrap(Config) ->
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Cluster = proplists:get_value(cluster, Config),
            NClients = 10,
            [C1, C2, R1, _] = Nodes = emqx_cth_cluster:start(Cluster),
            #{shards_in_sync := Shards} = erpc:call(R1, mria_rlog, status, []),
            %% Connect a number of clients to R1:
            Clients = [connect(R1) || _ <- lists:seq(1, NClients)],
            [?assertMatch({ok, _, [1]}, emqtt:subscribe(Conn, ?SHARE, 1)) || Conn <- Clients],
            Members = wait_shared_members(NClients, Nodes),
            %% Shut down mria upstream on the cores:
            [
                ?assertMatch(
                    ok,
                    erpc:call(N, supervisor, terminate_child, [mria_sup, mria_rlog_sup])
                )
             || N <- [C1, C2]
            ],
            %% While the upstream is down, clear the table to imitate
            %% the purge of the members of a replicant that is down:
            ?assertEqual(
                {atomic, ok},
                erpc:call(C1, mria, clear_table, [?SHARED_SUBSCRIPTION])
            ),
            ?assertEqual([], wait_shared_members(0, [C1, C2])),
            %% Restart replication. R1 should re-boostrap, and its
            %% clients should write their members again:
            [
                erpc:call(
                    N,
                    fun() ->
                        ?assertMatch(
                            {ok, _},
                            erpc:call(N, supervisor, restart_child, [mria_sup, mria_rlog_sup])
                        ),
                        ok = mria_rlog:wait_for_shards(Shards, 5_000)
                    end
                )
             || N <- [C1, C2]
            ],
            ?assertEqual(Members, wait_shared_members(NClients, Nodes)),
            [emqtt:stop(Conn) || Conn <- Clients],
            Config
        end,
        []
    ).

connect(Node) ->
    {ok, Conn} = emqtt:start_link([{port, emqx_cth_cluster:get_tcp_mqtt_port(Node)}]),
    {ok, _} = emqtt:connect(Conn),
    Conn.

%% Wait until every node has the same `N` members of the group, and return them.
wait_shared_members(N, Nodes) ->
    ?retry(
        500,
        30,
        begin
            [Members | _] =
                All = [
                    lists:sort(erpc:call(Node, emqx_shared_sub, subscribers, [?GROUP, ?TOPIC]))
                 || Node <- Nodes
                ],
            ?assertEqual(N, length(Members)),
            ?assertEqual([Members || _ <- Nodes], All),
            Members
        end
    ).

receive_publishes(Timeout) ->
    receive
        {publish, Msg} -> [Msg | receive_publishes(Timeout)]
    after Timeout -> []
    end.
