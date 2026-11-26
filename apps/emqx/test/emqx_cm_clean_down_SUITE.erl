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
%% The same value as in emqx_cm_worker.
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
    ok = erpc:call(Repl, ?MODULE, use_one_worker, []),
    ok = disconnect_with_pool_suspended(Repl, ClientIds),
    %% Count the calls from the replicant to a core node during the clean-down.
    _ = erpc:call(Repl, erlang, trace_pattern, [?CORE_RPC, true, [call_count]]),
    ok = wait_clean_down(Repl, Core),
    {call_count, Rpcs} = erpc:call(Repl, erlang, trace_info, [?CORE_RPC, call_count]),
    _ = erpc:call(Repl, erlang, trace_pattern, [?CORE_RPC, false, [call_count]]),
    ct:pal("core RPCs: ~p", [Rpcs]),
    ?assert(Rpcs < ?N_CLIENTS div 2, #{rpcs => Rpcs, clients => ?N_CLIENTS}),
    ?assertEqual([?N_CLIENTS], batch_counts()).

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
    ok = erpc:call(Repl, ?MODULE, use_one_worker, []),
    ok = fake_channels_down_with_pool_suspended(Repl, <<"small">>, ?N_CLIENTS),
    ok = wait_clean_down(Repl, Core),
    ?assertEqual(lists:duplicate(?N_CLIENTS div BatchSize, BatchSize), batch_counts()).

-doc """
A configured batch size above the maximum, such as the default of an older
release, is capped.
""".
t_batch_size_cap(Config) ->
    Core = ?config(core, Config),
    Repl = ?config(repl, Config),
    N = 2 * ?MAX_BATCH_SIZE + 500,
    ok = set_batch_size(Repl, 100_000),
    ok = erpc:call(Repl, ?MODULE, use_one_worker, []),
    ok = fake_channels_down_with_pool_suspended(Repl, <<"cap">>, N),
    ok = wait_clean_down(Repl, Core),
    ?assertEqual([?MAX_BATCH_SIZE, ?MAX_BATCH_SIZE, 500], batch_counts()).

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
        emqx_cm_registry, unregister_channels, 1, meck:raise(error, simulated)
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

-doc """
A worker that starts again monitors its channels again. It cleans up after the
channels whose `DOWN` messages were lost with the old process, and after the
channels that exit later.
""".
t_worker_restart(Config) ->
    Core = ?config(core, Config),
    Repl = ?config(repl, Config),
    Pids = erpc:call(Repl, ?MODULE, start_fake_channels, [<<"restart">>, ?N_CLIENTS]),
    ok = erpc:call(Repl, emqx_cm_worker, sync, []),
    {Id, Worker, Mine} = erpc:call(Repl, ?MODULE, busiest_worker, [Pids]),
    {Lost, _} = lists:split(length(Mine) div 2, Mine),
    ?assert(length(Lost) > 0, #{mine => length(Mine)}),
    %% The worker exits with the `DOWN` messages of some of its channels queued.
    ok = erpc:call(Repl, sys, suspend, [Worker]),
    ok = erpc:call(Repl, ?MODULE, kill_all, [Lost]),
    ok = erpc:call(Repl, ?MODULE, wait_down, [Lost]),
    ?retry(100, 50, ?assert(length(Lost) =< queue_len(Repl, Worker))),
    true = erpc:call(Repl, erlang, exit, [Worker, kill]),
    ?retry(
        100,
        50,
        ?assertMatch(
            {_, NewWorker} when is_pid(NewWorker) andalso NewWorker =/= Worker,
            lists:keyfind(
                {?CM_POOL, Id}, 1, erpc:call(Repl, gproc_pool, active_workers, [?CM_POOL])
            )
        )
    ),
    %% The new process cleans up after them.
    ?retry(
        100,
        50,
        ?assertEqual(
            ?N_CLIENTS - length(Lost), erpc:call(Repl, ets, info, [?CHAN_CONN_TAB, size])
        )
    ),
    ?assertEqual(
        [], [P || P <- Lost, erpc:call(Repl, ets, member, [?CHAN_CONN_TAB, P])]
    ),
    %% It also monitors the channels that are still alive.
    ok = erpc:call(Repl, ?MODULE, kill_all, [Pids -- Lost]),
    ok = wait_clean_down(Repl, Core),
    ?assertEqual(0, erpc:call(Repl, ets, info, [?CHAN_CONN_TAB, size])).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_ids(Prefix) ->
    [<<Prefix/binary, "-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, ?N_CLIENTS)].

%% Connect the clients to the replicant, suspend the workers, and disconnect
%% the clients. Return when the workers have the `DOWN` message of every channel.
disconnect_with_pool_suspended(Repl, ClientIds) ->
    Port = emqx_cth_cluster:get_tcp_mqtt_port(Repl),
    Clients = [connect(Id, Port) || Id <- ClientIds],
    N = length(ClientIds),
    ?retry(100, 50, ?assertEqual(N, erpc:call(Repl, ets, info, [?CHAN_LIVE_TAB, size]))),
    ChanPids = [Pid || {_, Pid} <- erpc:call(Repl, ets, tab2list, [?CHAN_TAB])],
    ok = suspend_pool(Repl),
    lists:foreach(fun emqtt:stop/1, Clients),
    ok = wait_clean_down_queued(Repl, ChanPids).

%% Return when the suspended workers have the `DOWN` message of every pid in
%% their mailboxes. They have no other message: suspend_pool/1 lets them handle
%% what was queued before.
wait_clean_down_queued(Repl, Pids) ->
    ok = erpc:call(Repl, ?MODULE, wait_down, [Pids]),
    N = length(Pids),
    ?retry(
        100,
        50,
        ?assert(N =< lists:sum([queue_len(Repl, W) || W <- pool_workers(Repl)]))
    ),
    ?assertEqual(N, erpc:call(Repl, ets, info, [?CHAN_TAB, size])),
    ok.

%% Suspend the workers after they have set the monitors of the channels that
%% are registered now.
suspend_pool(Node) ->
    ok = erpc:call(Node, emqx_cm_worker, sync, []),
    lists:foreach(fun(W) -> ok = erpc:call(Node, sys, suspend, [W]) end, pool_workers(Node)).

queue_len(Node, Pid) ->
    {message_queue_len, Len} = erpc:call(Node, erlang, process_info, [Pid, message_queue_len]),
    Len.

%% Register N processes as channels on the replicant, suspend the workers, and
%% kill the processes. Return when the workers have the `DOWN` message of every
%% channel.
fake_channels_down_with_pool_suspended(Repl, Prefix, N) ->
    Pids = erpc:call(Repl, ?MODULE, start_fake_channels, [Prefix, N]),
    ok = suspend_pool(Repl),
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

%% Runs on the replicant. Make one worker monitor every new channel, so that
%% the test knows how many `DOWN` messages that worker has queued.
use_one_worker() ->
    ok = meck:new(emqx_cm_worker, [passthrough, no_link, no_history]),
    ok = meck:expect(emqx_cm_worker, monitor_channel, fun(Pid) ->
        emqx_cm_worker:monitor_channel(1, Pid)
    end).

%% Runs on the replicant. Return the worker that monitors the most of the
%% channels, and those channels.
busiest_worker(Pids) ->
    ById = maps:groups_from_list(fun emqx_cm_worker:worker_id/1, Pids),
    [{_, Id, Mine} | _] = lists:reverse(lists:sort([{length(Ps), I, Ps} || I := Ps <- ById])),
    {_, Worker} = lists:keyfind({?CM_POOL, Id}, 1, gproc_pool:active_workers(?CM_POOL)),
    {Id, Worker, Mine}.

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
