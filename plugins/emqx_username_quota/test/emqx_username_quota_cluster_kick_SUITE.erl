%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_cluster_kick_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(USER, <<"clustered-user">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    RegistryEnabled = registry_enabled(TestCase),
    {Port1, Port2} = ports_for(TestCase),
    AppSpec = fun(Port) ->
        [
            emqx_conf,
            {emqx, listener_and_broker_conf(Port, RegistryEnabled)},
            emqx_management,
            emqx_username_quota
        ]
    end,
    Cluster = emqx_cth_cluster:start(
        [
            {node_name(TestCase, 1), #{role => core, apps => AppSpec(Port1)}},
            {node_name(TestCase, 2), #{role => core, apps => AppSpec(Port2)}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    [{cluster, Cluster}, {ports, {Port1, Port2}} | Config].

end_per_testcase(_TestCase, Config) ->
    case ?config(cluster, Config) of
        undefined -> ok;
        Nodes -> ok = emqx_cth_cluster:stop(Nodes)
    end,
    ok.

registry_enabled(t_kick_username_kicks_clients_across_nodes) -> false;
registry_enabled(t_kick_username_kicks_clients_with_registry_enabled) -> true.

ports_for(t_kick_username_kicks_clients_across_nodes) -> {21883, 21884};
ports_for(t_kick_username_kicks_clients_with_registry_enabled) -> {21885, 21886}.

node_name(TestCase, N) ->
    list_to_atom(atom_to_list(TestCase) ++ "_n" ++ integer_to_list(N)).

listener_and_broker_conf(Port, RegistryEnabled) ->
    io_lib:format(
        "listeners.tcp.default.bind = ~p~n"
        "listeners.ssl.default.enable = false~n"
        "listeners.ws.default.enable = false~n"
        "listeners.wss.default.enable = false~n"
        "broker.enable_session_registry = ~s~n",
        [Port, atom_to_list(RegistryEnabled)]
    ).

t_kick_username_kicks_clients_across_nodes(Config) ->
    do_kick_test(Config).

t_kick_username_kicks_clients_with_registry_enabled(Config) ->
    do_kick_test(Config).

do_kick_test(Config) ->
    [N1, N2] = ?config(cluster, Config),
    {Port1, Port2} = ?config(ports, Config),
    C1 = connect_client(Port1, ?USER, <<"c-n1-1">>),
    C2 = connect_client(Port1, ?USER, <<"c-n1-2">>),
    C3 = connect_client(Port2, ?USER, <<"c-n2-1">>),
    C4 = connect_client(Port2, ?USER, <<"c-n2-2">>),
    Clients = [C1, C2, C3, C4],
    MRefs = [erlang:monitor(process, C) || C <- Clients],
    try
        ok = wait_username_count(N1, ?USER, 4, 200),
        ok = wait_username_count(N2, ?USER, 4, 200),
        {ok, 4} = erpc:call(N1, emqx_username_quota_state, kick_username, [?USER]),
        ok = wait_clients_down(MRefs, 5000),
        ok = wait_username_count(N1, ?USER, 0, 200),
        ok = wait_username_count(N2, ?USER, 0, 200)
    after
        lists:foreach(fun stop_client/1, Clients)
    end.

connect_client(Port, Username, ClientId) ->
    {ok, Pid} = emqtt:start_link([
        {host, "127.0.0.1"},
        {port, Port},
        {proto_ver, v5},
        {clientid, ClientId},
        {username, Username},
        {clean_start, true}
    ]),
    true = unlink(Pid),
    case emqtt:connect(Pid) of
        {ok, _} ->
            Pid;
        {error, Reason} ->
            stop_client(Pid),
            erlang:error({connect_failed, Reason})
    end.

stop_client(Pid) when is_pid(Pid) ->
    catch emqtt:stop(Pid),
    catch exit(Pid, kill),
    ok.

wait_username_count(Node, Username, Expected, Retries) when Retries > 0 ->
    case erpc:call(Node, emqx_username_quota_state, count, [Username]) of
        Expected ->
            ok;
        _ ->
            timer:sleep(25),
            wait_username_count(Node, Username, Expected, Retries - 1)
    end;
wait_username_count(Node, Username, Expected, 0) ->
    ?assertEqual(
        Expected,
        erpc:call(Node, emqx_username_quota_state, count, [Username])
    ),
    ok.

wait_clients_down([], _Timeout) ->
    ok;
wait_clients_down([MRef | Rest], Timeout) ->
    receive
        {'DOWN', MRef, process, _Pid, _Reason} ->
            wait_clients_down(Rest, Timeout)
    after Timeout ->
        ?assert(false, "timeout waiting for client DOWN")
    end.
