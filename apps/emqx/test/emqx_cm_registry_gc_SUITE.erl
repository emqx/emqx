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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
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
    %% a failed case may leave the keeper suspended
    catch sys:resume(emqx_cm_registry_keeper),
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

%% On a replicant the sweep walks and purges local-dead rows so a zombie
%% registration on the replicant does not survive until node restart.
t_replicant_purges_local_dead_pid(_) ->
    ClientId = <<"replicant-local-dead">>,
    Pid = dead_local_pid(),
    meck:new(mria_rlog, [passthrough, no_history]),
    meck:expect(mria_rlog, role, fun() -> replicant end),
    ok = emqx_cm_registry:register_channel({ClientId, Pid}),
    Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
    ?assertMatch(#{deleted_local_dead := 1, deleted_remote_orphan := 0}, Counters),
    ?assertEqual([], mnesia:dirty_read(?CHAN_REG_TAB, ClientId)),
    ok.

%% Hist tombstones are a cluster-wide concern: on a replicant the keeper
%% must skip them even when the retention predicate would otherwise fire.
%% Cores remain the single deleter for that table state.
t_replicant_skips_hist_tombstones(_) ->
    OldRetain = emqx_config:get([broker, session_history_retain]),
    emqx_config:put([broker, session_history_retain], 1),
    meck:new(mria_rlog, [passthrough, no_history]),
    meck:expect(mria_rlog, role, fun() -> replicant end),
    try
        ClientId = <<"replicant-hist">>,
        ExpiredTs = erlang:system_time(seconds) - 3600,
        ok = mria:dirty_write(?CHAN_REG_TAB, #channel{chid = ClientId, pid = ExpiredTs}),
        Counters = emqx_cm_registry_keeper:force_sweep_stale_pids(),
        ?assertMatch(#{deleted_hist := 0}, Counters),
        ?assertEqual(
            [#channel{chid = ClientId, pid = ExpiredTs}],
            mnesia:dirty_read(?CHAN_REG_TAB, ClientId)
        )
    after
        emqx_config:put([broker, session_history_retain], OldRetain)
    end,
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

-doc """
A scheduled sweep resumes after the key stored as its cursor is deleted
during the pause between two chunks. It continues from a recently scanned
key and still deletes every dead row.
""".
t_resumes_after_cursor_deleted_between_chunks(_) ->
    Dead = dead_local_pid(),
    {AliveIds, DeadIds} = write_mixed_rows(<<"between-chunks-">>, 150, Dead),
    Keeper = whereis(emqx_cm_registry_keeper),
    Cursor = run_first_chunk_and_suspend(Keeper),
    ok = mria:dirty_delete(?CHAN_REG_TAB, Cursor),
    ?check_trace(
        begin
            {_, {ok, _}} = ?wait_async_action(
                ok = sys:resume(Keeper),
                #{?snk_kind := cm_registry_gc_finished},
                5000
            ),
            ?assertEqual(Keeper, whereis(emqx_cm_registry_keeper)),
            ?assertEqual([], [Id || Id <- DeadIds, all_pids(Id) =/= []]),
            ?assertEqual(
                [],
                [Id || Id <- AliveIds, Id =/= Cursor, all_pids(Id) =:= []]
            ),
            %% a finished sweep erases its resume keys
            ?assertMatch(#{resume_keys := {0, _}}, sys:get_state(Keeper))
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(cm_registry_gc_cursor_lost, Trace))
        end
    ),
    ok.

-doc """
When the cursor key and every recorded resume key are gone, the keeper
ends the current sweep instead of crashing. The next sweep starts from the
first key and deletes the rows the ended sweep did not reach.
""".
t_ends_sweep_when_no_resume_key_left(_) ->
    Dead = dead_local_pid(),
    %% Every row is dead, so the sweep deletes every key it scans and
    %% records no resume key.
    {[], DeadIds} = write_mixed_rows(<<"no-resume-">>, 150, Dead, _AliveEvery = 0),
    Keeper = whereis(emqx_cm_registry_keeper),
    Cursor = run_first_chunk_and_suspend(Keeper),
    ?assertMatch(#{resume_keys := {0, _}}, sys:get_state(Keeper)),
    ok = mria:dirty_delete(?CHAN_REG_TAB, Cursor),
    ?check_trace(
        begin
            {_, {ok, _}} = ?wait_async_action(
                ok = sys:resume(Keeper),
                #{?snk_kind := cm_registry_gc_cursor_lost},
                5000
            ),
            ?assertMatch(#{next_clientid := undefined}, sys:get_state(Keeper)),
            ?assertEqual(Keeper, whereis(emqx_cm_registry_keeper)),
            ?assertNotEqual([], [Id || Id <- DeadIds, all_pids(Id) =/= []]),
            _ = emqx_cm_registry_keeper:force_sweep_stale_pids(),
            ?assertEqual([], [Id || Id <- DeadIds, all_pids(Id) =/= []])
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(cm_registry_gc_chunk_failed, Trace))
        end
    ),
    ok.

-doc """
The sweep continues when another process deletes a key after the sweep
reads its rows and before the sweep looks up the next key.
""".
t_resumes_when_row_deleted_between_read_and_next(_) ->
    Dead = dead_local_pid(),
    {AliveIds, DeadIds} = write_mixed_rows(<<"read-next-">>, 40, Dead),
    Order = walk_order(),
    %% The target follows an alive key, so a resume key exists when the
    %% target is deleted.
    [_FirstAlive, Target | After] = lists:dropwhile(
        fun(Id) -> not lists:member(Id, AliveIds) end, Order
    ),
    ?assertNotEqual([], [Id || Id <- After, lists:member(Id, DeadIds)]),
    ok = meck:new(mnesia, [passthrough, no_link, no_history]),
    ok = meck:expect(mnesia, dirty_read, fun(Tab, Key) ->
        Rows = meck:passthrough([Tab, Key]),
        case Tab =:= ?CHAN_REG_TAB andalso Key =:= Target of
            true -> ok = mria:dirty_delete(Tab, Key);
            false -> ok
        end,
        Rows
    end),
    ?check_trace(
        begin
            _ = emqx_cm_registry_keeper:force_sweep_stale_pids(),
            ?assertEqual([], [Id || Id <- DeadIds, all_pids(Id) =/= []]),
            ?assertEqual(
                [],
                [Id || Id <- AliveIds, Id =/= Target, all_pids(Id) =:= []]
            )
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(cm_registry_gc_cursor_lost, Trace))
        end
    ),
    ok.

-doc """
An unexpected error in a scheduled chunk does not stop the keeper. The
keeper logs the error, drops the cursor and schedules the next sweep.
""".
t_keeper_survives_chunk_error(_) ->
    ClientId = <<"chunk-error">>,
    ok = emqx_cm_registry:register_channel({ClientId, dead_local_pid()}),
    Keeper = whereis(emqx_cm_registry_keeper),
    ok = meck:new(emqx_cm_registry, [passthrough, no_link, no_history]),
    ok = meck:expect(emqx_cm_registry, unregister_channel, fun(_) -> error(injected) end),
    ?check_trace(
        begin
            {_, {ok, _}} = ?wait_async_action(
                Keeper ! start,
                #{?snk_kind := cm_registry_gc_chunk_failed, reason := injected},
                5000
            ),
            ?assertEqual(Keeper, whereis(emqx_cm_registry_keeper)),
            #{next_clientid := undefined, timer_ref := TimerRef} = sys:get_state(Keeper),
            ?assert(is_integer(erlang:read_timer(TimerRef))),
            ok = meck:unload(emqx_cm_registry),
            ?assertMatch(
                #{deleted_local_dead := 1},
                emqx_cm_registry_keeper:force_sweep_stale_pids()
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{exception := error, stacktrace := [_ | _]}],
                ?of_kind(cm_registry_gc_chunk_failed, Trace)
            )
        end
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

%% Write `N' rows. Every `AliveEvery'-th row points at the test process;
%% the rest point at `Dead'. Returns `{AliveIds, DeadIds}'.
write_mixed_rows(Prefix, N, Dead) ->
    write_mixed_rows(Prefix, N, Dead, 2).

write_mixed_rows(Prefix, N, Dead, AliveEvery) ->
    lists:foldr(
        fun(I, {Alive, DeadIds}) ->
            ClientId = <<Prefix/binary, (integer_to_binary(I))/binary>>,
            case AliveEvery > 0 andalso I rem AliveEvery =:= 0 of
                true ->
                    ok = mria:dirty_write(?CHAN_REG_TAB, #channel{chid = ClientId, pid = self()}),
                    {[ClientId | Alive], DeadIds};
                false ->
                    ok = mria:dirty_write(?CHAN_REG_TAB, #channel{chid = ClientId, pid = Dead}),
                    {Alive, [ClientId | DeadIds]}
            end
        end,
        {[], []},
        lists:seq(1, N)
    ).

walk_order() ->
    walk_order(mnesia:dirty_first(?CHAN_REG_TAB), []).

walk_order('$end_of_table', Acc) ->
    lists:reverse(Acc);
walk_order(Key, Acc) ->
    walk_order(mnesia:dirty_next(?CHAN_REG_TAB, Key), [Key | Acc]).

%% Run one scheduled chunk, then suspend the keeper before its next chunk.
%% Messages from one sender arrive in order, so the keeper handles `start'
%% before the suspend request. Returns the stored cursor.
run_first_chunk_and_suspend(Keeper) ->
    Keeper ! start,
    ok = sys:suspend(Keeper),
    #{next_clientid := Cursor} = sys:get_state(Keeper),
    ?assert(is_binary(Cursor)),
    Cursor.
