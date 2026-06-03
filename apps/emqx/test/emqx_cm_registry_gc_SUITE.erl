%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Tests for the stale-pid garbage collection sweep implemented by
%% emqx_cm_registry_keeper. Session history retention is disabled in this
%% suite so that asserting "row gone" is unambiguous; a separate case
%% temporarily enables hist to check the hist predicate still fires.
-module(emqx_cm_registry_gc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_cm.hrl").

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{config => "broker.session_history_retain = 0s"}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    catch meck:unload(),
    %% drain any tombstones written by failed assertions
    mnesia:clear_table(?CHAN_REG_TAB),
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

%% A registry row pointing at a local dead pid is deleted by the sweep.
t_purges_local_dead_pid(_) ->
    ClientId = <<"local-dead">>,
    Pid = dead_local_pid(),
    ok = emqx_cm_registry:register_channel({ClientId, Pid}),
    ?assertEqual([Pid], all_pids(ClientId)),
    Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    ?assertMatch(#{deleted_local_dead := 1, deleted_remote_orphan := 0}, Counters),
    ?assertEqual([], mnesia:dirty_read(?CHAN_REG_TAB, ClientId)),
    ok.

%% A registry row pointing at a live local pid is left alone.
t_keeps_local_alive_pid(_) ->
    ClientId = <<"local-alive">>,
    Pid = self(),
    ok = emqx_cm_registry:register_channel({ClientId, Pid}),
    Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    ?assertMatch(#{deleted_local_dead := 0, deleted_remote_orphan := 0}, Counters),
    ?assertEqual([Pid], all_pids(ClientId)),
    ok = emqx_cm_registry:unregister_channel({ClientId, Pid}),
    ok.

%% A fresh history row (integer ts) is not removed even when hist is enabled,
%% because the hist predicate only deletes expired rows. The stale-pid
%% predicate must never delete a hist row.
t_skips_history_rows_when_hist_enabled(_) ->
    OldRetain = emqx_config:get([broker, session_history_retain]),
    emqx_config:put([broker, session_history_retain], 3600),
    try
        ClientId = <<"hist-keep">>,
        FreshTs = erlang:system_time(seconds),
        ok = mria:dirty_write(?CHAN_REG_TAB, #channel{chid = ClientId, pid = FreshTs}),
        Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
        ?assertMatch(
            #{deleted_hist := 0, deleted_local_dead := 0, deleted_remote_orphan := 0},
            Counters
        ),
        ?assertEqual(
            [#channel{chid = ClientId, pid = FreshTs}],
            mnesia:dirty_read(
                ?CHAN_REG_TAB, ClientId
            )
        )
    after
        emqx_config:put([broker, session_history_retain], OldRetain)
    end,
    ok.

%% A remote pid whose node is reported gone by mria:is_peer_alive/1 is purged.
t_purges_remote_orphan_pid(_) ->
    FakeNode = 'fake-orphan@127.0.0.1',
    Pid = fake_remote_pid(FakeNode),
    ClientId = <<"remote-orphan">>,
    meck:new(mria, [passthrough, no_history]),
    meck:expect(mria, is_peer_alive, fun(_) -> {ok, false} end),
    ok = mria:dirty_write(?CHAN_REG_TAB, #channel{chid = ClientId, pid = Pid}),
    Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    ?assertMatch(#{deleted_remote_orphan := 1, deleted_local_dead := 0}, Counters),
    ?assertEqual([], mnesia:dirty_read(?CHAN_REG_TAB, ClientId)),
    ok.

%% A remote pid whose node is reported alive by the cluster consensus is kept.
t_keeps_remote_alive_per_consensus(_) ->
    FakeNode = 'fake-alive@127.0.0.1',
    Pid = fake_remote_pid(FakeNode),
    ClientId = <<"remote-alive">>,
    meck:new(mria, [passthrough, no_history]),
    meck:expect(mria, is_peer_alive, fun(_) -> {ok, true} end),
    ok = mria:dirty_write(?CHAN_REG_TAB, #channel{chid = ClientId, pid = Pid}),
    Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    ?assertMatch(#{deleted_remote_orphan := 0}, Counters),
    ?assertEqual(
        [#channel{chid = ClientId, pid = Pid}],
        mnesia:dirty_read(
            ?CHAN_REG_TAB, ClientId
        )
    ),
    ok.

%% Any non-{ok, false} return (e.g. {aborted, _} from RPC error) must not
%% purge. Preserves the safety bias of emqx_cm_registry:can_run_cleanup/1.
t_treats_aborted_is_peer_alive_as_keep(_) ->
    FakeNode = 'fake-aborted@127.0.0.1',
    Pid = fake_remote_pid(FakeNode),
    ClientId = <<"remote-aborted">>,
    meck:new(mria, [passthrough, no_history]),
    meck:expect(mria, is_peer_alive, fun(_) -> {aborted, simulated_rpc_error} end),
    ok = mria:dirty_write(?CHAN_REG_TAB, #channel{chid = ClientId, pid = Pid}),
    Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    ?assertMatch(#{deleted_remote_orphan := 0}, Counters),
    ?assertEqual(
        [#channel{chid = ClientId, pid = Pid}],
        mnesia:dirty_read(
            ?CHAN_REG_TAB, ClientId
        )
    ),
    ok.

%% mria:is_peer_alive/1 must be called at most once per unique remote node
%% per sweep, regardless of how many rows point at that node.
t_caches_is_peer_alive_per_sweep(_) ->
    FakeNode = 'fake-cached@127.0.0.1',
    N = 50,
    %% NB: do not pass `no_history' here; we assert call count below.
    meck:new(mria, [passthrough]),
    meck:expect(mria, is_peer_alive, fun(_) -> {ok, false} end),
    lists:foreach(
        fun(I) ->
            ClientId = <<"cached-", (integer_to_binary(I))/binary>>,
            Pid = fake_remote_pid(FakeNode),
            ok = mria:dirty_write(?CHAN_REG_TAB, #channel{chid = ClientId, pid = Pid})
        end,
        lists:seq(1, N)
    ),
    Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    ?assertMatch(#{deleted_remote_orphan := N}, Counters),
    ?assertEqual(1, meck:num_calls(mria, is_peer_alive, ['_'])),
    ok.

%% Two back-to-back force sweeps must both return cleanly. gen_server:call
%% serializes them; the second sees an already-clean table.
t_no_concurrent_scans(_) ->
    ClientId = <<"two-sweeps">>,
    Pid = dead_local_pid(),
    ok = emqx_cm_registry:register_channel({ClientId, Pid}),
    C1 = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    C2 = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    ?assertMatch(#{deleted_local_dead := 1}, C1),
    ?assertMatch(#{deleted_local_dead := 0}, C2),
    ok.

%% On a replicant the sweep walks but must never purge a remote pid, even
%% when the cluster consensus would say the peer is gone.
t_replicant_does_not_act_on_remote(_) ->
    FakeNode = 'fake-on-replicant@127.0.0.1',
    Pid = fake_remote_pid(FakeNode),
    ClientId = <<"replicant-skip">>,
    meck:new(mria_rlog, [passthrough, no_history]),
    meck:expect(mria_rlog, role, fun() -> replicant end),
    meck:new(mria, [passthrough, no_history]),
    meck:expect(mria, is_peer_alive, fun(_) -> {ok, false} end),
    ok = mria:dirty_write(?CHAN_REG_TAB, #channel{chid = ClientId, pid = Pid}),
    Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    ?assertMatch(#{deleted_remote_orphan := 0}, Counters),
    ?assertEqual(
        [#channel{chid = ClientId, pid = Pid}],
        mnesia:dirty_read(
            ?CHAN_REG_TAB, ClientId
        )
    ),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

all_pids(ClientId) ->
    [P || #channel{pid = P} <- mnesia:dirty_read(?CHAN_REG_TAB, ClientId), is_pid(P)].

dead_local_pid() ->
    Pid = spawn(fun() -> ok end),
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 ->
        error(spawn_did_not_die)
    end,
    false = is_process_alive(Pid),
    Pid.

%% Hack: build a pid term whose node atom is `Node'. Used to simulate a row
%% left behind by a remote channel without booting an Erlang slave.
%% See https://www.erlang.org/doc/apps/erts/erl_ext_dist.html#new_pid_ext
fake_remote_pid(Node) ->
    <<131, NodeAtom/binary>> = term_to_binary(Node),
    PidBin = <<131, 88, NodeAtom/binary, 1:32/big, 1:32/big, 1:32/big>>,
    binary_to_term(PidBin).
