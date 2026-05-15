%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_broker_heal_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

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
t_renew_client_regs_on_heal(Config) ->
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
