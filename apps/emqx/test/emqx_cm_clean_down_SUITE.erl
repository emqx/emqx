%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Clean-down of disconnected channels on a replicant: the global registry
%% rows are removed in batches, with one call to a core node per batch.
-module(emqx_cm_clean_down_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_cm.hrl").

-define(N_CLIENTS, 100).
-define(CORE_RPC, {mria_lib, rpc_call_nothrow, 4}).
%% The same value as in emqx_cm.
-define(MAX_BATCH_SIZE, 1000).

suite() -> [{timetrap, {minutes, 3}}].

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = [emqx],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {cm_clean_down_core1, #{role => core, apps => Apps}},
            {cm_clean_down_repl1, #{role => replicant, apps => Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [Core, Repl] = emqx_cth_cluster:start(Specs),
    [{core, Core}, {repl, Repl}, {specs, Specs} | Config].

end_per_suite(Config) ->
    emqx_cth_cluster:stop(?config(specs, Config)).

init_per_testcase(_TestCase, Config) ->
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TestCase, Config) ->
    Repl = ?config(repl, Config),
    snabbkaffe:stop(),
    _ = erpc:call(Repl, erlang, trace_pattern, [?CORE_RPC, false, [call_count]]),
    _ = erpc:call(Repl, meck, unload, []),
    ok = resume_pool(Repl),
    ok = set_batch_size(Repl, ?MAX_BATCH_SIZE),
    ok = set_history_retain(Config, 0),
    %% A failed case can leave channels on the replicant.
    ok = erpc:call(Repl, ?MODULE, kill_all_channels, []),
    ?retry(100, 50, ?assertEqual(0, erpc:call(Repl, ets, info, [?CHAN_TAB, size]))),
    ok = erpc:call(?config(core, Config), emqx_cm_registry_keeper, purge, []).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
An `emqx_cm_pool` worker on a replicant removes the down channels that are queued
in its mailbox from the global registry in one batch, not with one call to a core
node for each channel.
""".
t_batch_unregister(Config) ->
    Core = ?config(core, Config),
    Repl = ?config(repl, Config),
    ClientIds = client_ids(<<"batch">>),
    Workers = disconnect_with_pool_suspended(Repl, ClientIds),
    %% Count the calls from the replicant to a core node during the clean-down.
    _ = erpc:call(Repl, erlang, trace_pattern, [?CORE_RPC, true, [call_count]]),
    ok = wait_clean_down(Repl, Core),
    {call_count, Rpcs} = erpc:call(Repl, erlang, trace_info, [?CORE_RPC, call_count]),
    _ = erpc:call(Repl, erlang, trace_pattern, [?CORE_RPC, false, [call_count]]),
    Batches = ?of_kind(emqx_cm_registry_unregister_channels, snabbkaffe:collect_trace()),
    ct:pal("core RPCs: ~p, batches: ~p, pool workers: ~p", [Rpcs, length(Batches), Workers]),
    ?assert(Rpcs < ?N_CLIENTS div 2, #{rpcs => Rpcs, clients => ?N_CLIENTS}),
    ?assertEqual(?N_CLIENTS, lists:sum([C || #{count := C} <- Batches])),
    ?assert(length(Batches) =< Workers, #{batches => length(Batches), workers => Workers}).

-doc """
A client that reconnects to another node while its old channel on the
replicant waits for clean-down keeps its new registry entry.
""".
t_reconnect_to_other_node(Config) ->
    Core = ?config(core, Config),
    Repl = ?config(repl, Config),
    [ClientId] = ClientIds = [<<"reconnect-1">>],
    _ = disconnect_with_pool_suspended(Repl, ClientIds),
    [OldPid] = erpc:call(Core, emqx_cm_registry, lookup_all_channels, [ClientId]),
    ?assertEqual(Repl, node(OldPid)),
    {ok, C} = emqtt:start_link([
        {clientid, ClientId}, {port, emqx_cth_cluster:get_tcp_mqtt_port(Core)}
    ]),
    {ok, _} = emqtt:connect(C),
    ok = resume_pool(Repl),
    ?retry(100, 50, ?assertEqual(0, erpc:call(Repl, ets, info, [?CHAN_TAB, size]))),
    [NewPid] = erpc:call(Core, emqx_cm_registry, lookup_all_channels, [ClientId]),
    ?assertEqual(Core, node(NewPid)),
    ok = emqtt:stop(C).

-doc """
With the session registration history on, the batched clean-down leaves one
history row and no channel row for each client.
""".
t_batch_unregister_history(Config) ->
    Core = ?config(core, Config),
    Repl = ?config(repl, Config),
    ok = set_history_retain(Config, 3600),
    ClientIds = client_ids(<<"hist">>),
    _ = disconnect_with_pool_suspended(Repl, ClientIds),
    ok = resume_pool(Repl),
    ?retry(
        100,
        50,
        ?assertEqual(
            [[hist] || _ <- ClientIds],
            [
                [row_kind(R) || R <- erpc:call(Core, mnesia, dirty_read, [?CHAN_REG_TAB, Id])]
             || Id <- ClientIds
            ]
        )
    ).

-doc """
`node.channel_cleanup_batch_size` limits the channels of one registry call:
it is the batch size of the clean-down workers.
""".
t_batch_size_config(Config) ->
    Core = ?config(core, Config),
    Repl = ?config(repl, Config),
    BatchSize = 10,
    ok = set_batch_size(Repl, BatchSize),
    ok = fake_channels_down_with_pool_suspended(Repl, <<"small">>, ?N_CLIENTS),
    ok = wait_clean_down(Repl, Core),
    Counts = batch_counts(),
    ct:pal("batches: ~w", [Counts]),
    ?assertEqual(?N_CLIENTS, lists:sum(Counts)),
    ?assert(lists:max(Counts) =< 2 * BatchSize, #{counts => Counts}).

-doc """
A configured batch size above the maximum, such as the default of an older
release, is capped. One registry call holds at most two times the maximum.
""".
t_batch_size_cap(Config) ->
    Core = ?config(core, Config),
    Repl = ?config(repl, Config),
    N = 2 * ?MAX_BATCH_SIZE + 500,
    ok = set_batch_size(Repl, 100_000),
    ok = fake_channels_down_with_pool_suspended(Repl, <<"cap">>, N),
    ok = wait_clean_down(Repl, Core),
    Counts = batch_counts(),
    ct:pal("batches: ~w", [Counts]),
    ?assertEqual(N, lists:sum(Counts)),
    ?assert(lists:max(Counts) =< 2 * ?MAX_BATCH_SIZE, #{counts => Counts}).

-doc """
When the registry call fails, the clean-down still removes the local rows of
the channels. The registry rows stay until the registry keeper of the replicant
removes them.
""".
t_unregister_failure(Config) ->
    Core = ?config(core, Config),
    Repl = ?config(repl, Config),
    ok = erpc:call(Repl, meck, new, [emqx_cm_registry, [passthrough, no_link, no_history]]),
    ok = erpc:call(Repl, meck, expect, [
        emqx_cm_registry, unregister_channels, 1, {error, simulated}
    ]),
    ok = fake_channels_down_with_pool_suspended(Repl, <<"fail">>, ?N_CLIENTS),
    ok = resume_pool(Repl),
    ?retry(100, 50, ?assertEqual(0, erpc:call(Repl, ets, info, [?CHAN_TAB, size]))),
    ?assertEqual(0, erpc:call(Repl, ets, info, [?CHAN_CONN_TAB, size])),
    ?assertEqual(?N_CLIENTS, erpc:call(Core, emqx_cm_registry, table_size, [])),
    ok = erpc:call(Repl, emqx_cm_registry_keeper, ensure_started, []),
    ?retry(100, 100, ?assertEqual(0, erpc:call(Core, emqx_cm_registry, table_size, []))).

-doc """
A failure of the subscriber clean-down of a channel does not stop the
clean-down of the batch: the registry rows and the local rows are removed.
""".
t_subscriber_clean_down_failure(Config) ->
    Core = ?config(core, Config),
    Repl = ?config(repl, Config),
    ok = erpc:call(Repl, meck, new, [emqx_broker_helper, [passthrough, no_link, no_history]]),
    ok = erpc:call(Repl, meck, expect, [
        emqx_broker_helper, clean_down, 1, meck:raise(error, simulated)
    ]),
    ok = fake_channels_down_with_pool_suspended(Repl, <<"subfail">>, ?N_CLIENTS),
    ok = wait_clean_down(Repl, Core),
    ?assertEqual(0, erpc:call(Repl, ets, info, [?CHAN_CONN_TAB, size])).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_ids(Prefix) ->
    [<<Prefix/binary, "-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, ?N_CLIENTS)].

%% Connect the clients to the replicant, suspend the clean-down workers, and
%% disconnect the clients. Return when every clean-down task is queued.
disconnect_with_pool_suspended(Repl, ClientIds) ->
    Port = emqx_cth_cluster:get_tcp_mqtt_port(Repl),
    Clients = [connect(Id, Port) || Id <- ClientIds],
    N = length(ClientIds),
    ?retry(100, 50, ?assertEqual(N, erpc:call(Repl, ets, info, [?CHAN_LIVE_TAB, size]))),
    ChanPids = [Pid || {_, Pid} <- erpc:call(Repl, ets, tab2list, [?CHAN_TAB])],
    Workers = pool_workers(Repl),
    ok = lists:foreach(fun(W) -> ok = erpc:call(Repl, sys, suspend, [W]) end, Workers),
    lists:foreach(fun emqtt:stop/1, Clients),
    ok = wait_clean_down_queued(Repl, ChanPids),
    length(Workers).

%% Return when emqx_cm has queued the clean-down task of every pid. A channel
%% process removes its own row from the live table before it exits, so that
%% table does not tell when emqx_cm got the DOWN message. Wait for the processes
%% to exit instead: their DOWN messages are then in the mailbox of emqx_cm, in
%% front of the system message of sys:get_state/1.
wait_clean_down_queued(Repl, Pids) ->
    ok = erpc:call(Repl, ?MODULE, wait_down, [Pids]),
    _ = erpc:call(Repl, sys, get_state, [emqx_cm]),
    ?assertEqual(0, erpc:call(Repl, ets, info, [?CHAN_LIVE_TAB, size])),
    ?assertEqual(length(Pids), erpc:call(Repl, ets, info, [?CHAN_TAB, size])),
    ok.

%% Register N processes as channels on the replicant, suspend the clean-down
%% workers, and kill the processes. Return when every clean-down task is queued.
fake_channels_down_with_pool_suspended(Repl, Prefix, N) ->
    Pids = erpc:call(Repl, ?MODULE, start_fake_channels, [Prefix, N]),
    ?retry(100, 50, ?assertEqual(N, erpc:call(Repl, ets, info, [?CHAN_LIVE_TAB, size]))),
    ok = lists:foreach(fun(W) -> ok = erpc:call(Repl, sys, suspend, [W]) end, pool_workers(Repl)),
    ok = erpc:call(Repl, ?MODULE, kill_all, [Pids]),
    ok = wait_clean_down_queued(Repl, Pids).

%% Runs on the replicant.
start_fake_channels(Prefix, N) ->
    Parent = self(),
    Pids = [
        spawn(fun() ->
            ClientId = <<Prefix/binary, "-", (integer_to_binary(I))/binary>>,
            ok = emqx_cm:register_channel(ClientId, self(), #{conn_mod => ?MODULE}),
            Parent ! {registered, self()},
            receive
                stop -> ok
            end
        end)
     || I <- lists:seq(1, N)
    ],
    lists:foreach(
        fun(Pid) ->
            receive
                {registered, Pid} -> ok
            after 30_000 -> error({not_registered, Pid})
            end
        end,
        Pids
    ),
    Pids.

%% Runs on the replicant.
kill_all(Pids) ->
    lists:foreach(fun(Pid) -> exit(Pid, kill) end, Pids).

%% Runs on the replicant.
kill_all_channels() ->
    kill_all([Pid || {_, Pid} <- ets:tab2list(?CHAN_TAB)]).

%% Runs on the replicant.
wait_down(Pids) ->
    Refs = [monitor(process, Pid) || Pid <- Pids],
    lists:foreach(
        fun(Ref) ->
            receive
                {'DOWN', Ref, process, _, _} -> ok
            after 30_000 -> error(timeout)
            end
        end,
        Refs
    ).

batch_counts() ->
    [
        Count
     || #{count := Count} <- ?of_kind(
            emqx_cm_registry_unregister_channels, snabbkaffe:collect_trace()
        )
    ].

%% Set the batch size, and restart the clean-down workers when it changes: a
%% worker reads the batch size when it starts.
set_batch_size(Node, Size) ->
    Path = [node, channel_cleanup_batch_size],
    %% The nodes of this suite do not load the schema of the `node` root.
    case erpc:call(Node, emqx_config, get, [Path, ?MAX_BATCH_SIZE]) of
        Size ->
            ok;
        _ ->
            ok = erpc:call(Node, emqx_config, put, [Path, Size]),
            ok = erpc:call(Node, supervisor, terminate_child, [emqx_cm_sup, emqx_cm_pool_sup]),
            ?retry(100, 50, ?assertEqual([], pool_workers(Node))),
            {ok, _} = erpc:call(Node, supervisor, restart_child, [emqx_cm_sup, emqx_cm_pool_sup]),
            NWorkers = erpc:call(Node, emqx_vm, schedulers, []),
            ?retry(100, 50, ?assertEqual(NWorkers, length(pool_workers(Node)))),
            ok
    end.

connect(ClientId, Port) ->
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {port, Port}]),
    unlink(C),
    {ok, _} = emqtt:connect(C),
    C.

pool_workers(Node) ->
    [Pid || {_, Pid} <- erpc:call(Node, gproc_pool, active_workers, [?CM_POOL])].

resume_pool(Node) ->
    lists:foreach(
        fun(W) ->
            case erpc:call(Node, sys, get_status, [W]) of
                {status, _, _, [_, suspended | _]} -> ok = erpc:call(Node, sys, resume, [W]);
                _ -> ok
            end
        end,
        pool_workers(Node)
    ).

wait_clean_down(Repl, Core) ->
    ok = resume_pool(Repl),
    ?retry(100, 50, ?assertEqual(0, erpc:call(Repl, ets, info, [?CHAN_TAB, size]))),
    ?retry(100, 50, ?assertEqual(0, erpc:call(Core, emqx_cm_registry, table_size, []))),
    ok.

row_kind(#channel{pid = Pid}) when is_pid(Pid) -> Pid;
row_kind(#channel{pid = Ts}) when is_integer(Ts) -> hist.

set_history_retain(Config, Seconds) ->
    lists:foreach(
        fun(Node) ->
            ok = erpc:call(Node, emqx_config, put, [[broker, session_history_retain], Seconds])
        end,
        [?config(core, Config), ?config(repl, Config)]
    ).
