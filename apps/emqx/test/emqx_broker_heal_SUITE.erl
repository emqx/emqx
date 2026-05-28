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

%% This testcase verifies functioning of `emqx_broker_heal:consistency_check' function.
t_consistency_check(Config) ->
    Cluster = proplists:get_value(cluster, Config),
    NClients = 10,
    [_, _, R1, _] = emqx_cth_cluster:start(Cluster),
    %% No clients. Consistency check should pass:
    ?assertEqual(
        true,
        erpc:call(R1, emqx_broker_heal, consistency_check, [])
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
    ct:sleep(100),
    ?assertEqual(
        NClients,
        erpc:call(R1, emqx_cm_registry, table_size, [])
    ),
    %% Consistency check should return `true':
    ?assertEqual(
        true,
        erpc:call(R1, emqx_broker_heal, consistency_check, [])
    ),
    %% Delete channels from the registry, consistency check should detect that:
    ?assertEqual(
        {atomic, ok},
        erpc:call(R1, mria, clear_table, [?CHAN_REG_TAB])
    ),
    ct:sleep(100),
    ?assertEqual(
        false,
        erpc:call(R1, emqx_broker_heal, consistency_check, [])
    ),
    [emqtt:stop(Conn) || Conn <- Clients],
    Config.

%% This testcase verifies that replicants attempt to heal channel registry after mria re-bootstrap.
t_heal_on_replicant_bootstrap(Config) ->
    ?check_trace(
        #{timetrap => 30_000},
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
