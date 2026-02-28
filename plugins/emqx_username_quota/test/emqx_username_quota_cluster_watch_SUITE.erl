%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_cluster_watch_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, Config) ->
    meck:unload(),
    Config.

t_init_terminate(_Config) ->
    ok = meck:new(ekka, [non_strict, passthrough]),
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(ekka, monitor, fun(membership) -> ok end),
    ok = meck:expect(ekka, unmonitor, fun(membership) -> ok end),
    ok = meck:expect(emqx_username_quota_state, clear_self_node, fun() -> ok end),
    {ok, State} = emqx_username_quota_cluster_watch:init([]),
    ?assertEqual(#{}, State),
    %% Drain the bootstrap message sent by init to self()
    receive
        bootstrap -> ok
    after 0 -> ok
    end,
    ok = emqx_username_quota_cluster_watch:terminate(normal, State).

t_clear_for_node(_Config) ->
    ok = meck:new(emqx, [non_strict, passthrough]),
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    Node = 'node1@127.0.0.1',
    ok = meck:expect(emqx, running_nodes, fun() -> [Node] end),
    ok = meck:expect(emqx_username_quota_state, clear_for_node, fun(_N) -> ok end),
    ?assertEqual(
        {noreply, #{}}, emqx_username_quota_cluster_watch:handle_info({clear_for_node, Node}, #{})
    ),
    ?assertNot(meck:called(emqx_username_quota_state, clear_for_node, [Node])),
    ok = meck:expect(emqx, running_nodes, fun() -> [] end),
    ?assertEqual(
        {noreply, #{}}, emqx_username_quota_cluster_watch:handle_info({clear_for_node, Node}, #{})
    ),
    ?assert(meck:called(emqx_username_quota_state, clear_for_node, [Node])).

t_nodedown_core_and_replicant(_Config) ->
    ok = meck:new(mria_rlog, [non_strict, passthrough]),
    Node = 'node2@127.0.0.1',
    ok = meck:expect(mria_rlog, role, fun() -> core end),
    ?assertEqual(
        {noreply, #{}}, emqx_username_quota_cluster_watch:handle_info({nodedown, Node}, #{})
    ),
    receive
        {clear_for_node, Node} ->
            ok
    after 1000 ->
        ct:fail(timeout_waiting_for_delayed_clear)
    end,
    ok = meck:expect(mria_rlog, role, fun() -> replicant end),
    ?assertEqual(
        {noreply, #{}}, emqx_username_quota_cluster_watch:handle_info({nodedown, Node}, #{})
    ).

t_membership_events(_Config) ->
    ok = meck:new(mria_rlog, [non_strict, passthrough]),
    ok = meck:expect(mria_rlog, role, fun() -> replicant end),
    Node = 'node3@127.0.0.1',
    ?assertEqual(
        {noreply, #{}},
        emqx_username_quota_cluster_watch:handle_info({membership, {mnesia, down, Node}}, #{})
    ),
    ?assertEqual(
        {noreply, #{}},
        emqx_username_quota_cluster_watch:handle_info({membership, {node, down, Node}}, #{})
    ),
    ?assertEqual(
        {noreply, #{}},
        emqx_username_quota_cluster_watch:handle_info({membership, up}, #{})
    ),
    ?assertEqual({noreply, #{}}, emqx_username_quota_cluster_watch:handle_info(unknown, #{})).

t_call_cast_code_change(_Config) ->
    State = #{key => value},
    ?assertEqual(
        {reply, ignored, State},
        emqx_username_quota_cluster_watch:handle_call(req, {self(), make_ref()}, State)
    ),
    ?assertEqual({noreply, State}, emqx_username_quota_cluster_watch:handle_cast(msg, State)),
    ?assertEqual({ok, State}, emqx_username_quota_cluster_watch:code_change(old, State, extra)).

t_immediate_node_clear(_Config) ->
    ok = meck:new(ekka, [non_strict, passthrough]),
    ok = meck:new(emqx, [non_strict, passthrough]),
    ok = meck:new(emqx_cm, [non_strict, passthrough]),
    ok = meck:new(emqx_username_quota_state, [non_strict, passthrough]),
    ok = meck:expect(ekka, monitor, fun(membership) -> ok end),
    ok = meck:expect(ekka, unmonitor, fun(membership) -> ok end),
    ok = meck:expect(emqx_username_quota_state, clear_self_node, fun() -> ok end),
    ok = meck:expect(emqx, running_nodes, fun() -> [] end),
    ok = meck:expect(emqx_cm, all_channels_stream, fun(_) -> [] end),
    ok = meck:expect(emqx_username_quota_state, clear_for_node, fun(_N) -> ok end),
    {ok, Pid} = emqx_username_quota_cluster_watch:start_link(),
    true = unlink(Pid),
    Node = 'node4@127.0.0.1',
    ?assertEqual(async, emqx_username_quota_cluster_watch:immediate_node_clear(Node)),
    ok = wait_until(fun() -> meck:called(emqx_username_quota_state, clear_for_node, [Node]) end),
    exit(Pid, shutdown),
    receive
        {'EXIT', Pid, shutdown} -> ok
    after 1000 ->
        ok
    end.

wait_until(Pred) ->
    wait_until(Pred, 40).

wait_until(Pred, 0) ->
    ?assert(Pred());
wait_until(Pred, N) ->
    case Pred() of
        true ->
            ok;
        false ->
            timer:sleep(25),
            wait_until(Pred, N - 1)
    end.
