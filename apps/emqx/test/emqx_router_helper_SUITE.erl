%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_router_helper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HELPER, emqx_router_helper).
-define(GROUP, <<"g">>).

%% `-ifdef(TEST)' values in `emqx_router_helper': reconcile every 2 s (± 0.5 s),
%% purge a replicant that is down for 3 s. Allow several reconcile rounds.
-define(PURGE_WAIT_TIMEOUT, 15_000).

all() ->
    [
        {group, schema_v3},
        {group, schema_v2}
    ].

groups() ->
    TCs = [
        t_replicant_down,
        t_orphan_members,
        t_replicant_restart,
        t_dispatch_skips_down_member,
        t_dispatch_skips_orphan_member,
        t_replicant_to_replicant_dispatch,
        t_reregister_members,
        t_reregister_unsubscribed_member,
        t_ensure_helper
    ],
    [
        {schema_v3, [], TCs},
        {schema_v2, [], TCs}
    ].

init_per_group(Schema, Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    Apps = emqx_cth_suite:start(
        [
            {mria, #{override_env => [{node_role, core}, {db_backend, rlog}]}},
            {emqx, #{
                config => schema_config(Schema),
                override_env => [{boot_modules, [broker]}]
            }}
        ],
        #{work_dir => WorkDir}
    ),
    [{schema, Schema}, {group_apps, Apps} | Config].

end_per_group(_Schema, Config) ->
    ok = emqx_cth_suite:stop(?config(group_apps, Config)).

init_per_testcase(TC, Config) ->
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config).

end_per_testcase(TC, Config) ->
    Res = emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config),
    ok = snabbkaffe:stop(),
    Res.

schema_config(schema_v3) ->
    "broker.routing.storage_schema = v3";
schema_config(schema_v2) ->
    "broker.routing.storage_schema = v2".

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% `emqx_router_sup:ensure_helper/0' starts the helper when it is missing (as on a
%% node that runs an older version), and does nothing when it is running.
t_ensure_helper(_Config) ->
    Pid0 = whereis(?HELPER),
    ?assert(is_pid(Pid0)),
    ?assertEqual(ok, emqx_router_sup:ensure_helper()),
    ?assertEqual(Pid0, whereis(?HELPER)),
    %% Child spec removed, as when the supervisor did not start it.
    ok = supervisor:terminate_child(emqx_router_sup, helper),
    ok = supervisor:delete_child(emqx_router_sup, helper),
    ?assertEqual(undefined, whereis(?HELPER)),
    ?assertEqual(ok, emqx_router_sup:ensure_helper()),
    Pid1 = whereis(?HELPER),
    ?assert(is_pid(Pid1)),
    ?assertNotEqual(Pid0, Pid1),
    %% Child spec kept, but the child is not running.
    ok = supervisor:terminate_child(emqx_router_sup, helper),
    ?assertEqual(undefined, whereis(?HELPER)),
    ?assertEqual(ok, emqx_router_sup:ensure_helper()),
    ?assert(is_pid(whereis(?HELPER))),
    ?assertEqual(ok, emqx_router_sup:ensure_helper()).

t_replicant_down('init', Config) ->
    start_replicant(?FUNCTION_NAME, Config);
t_replicant_down('end', Config) ->
    stop_replicant(Config).

-doc """
A replicant goes down without a chance to remove its shared subscription members.
The core node keeps the members while the replicant is down for less than the
timeout, then purges them.
""".
t_replicant_down(Config) ->
    Replicant = ?config(replicant, Config),
    Topic = <<"t/down/#">>,
    _SubPid = start_shared_sub(Replicant, ?GROUP, Topic),
    ?assertMatch([_], emqx_shared_sub:subscribers(?GROUP, Topic)),
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := broker_node_purged, node := Replicant}),
        1,
        ?PURGE_WAIT_TIMEOUT
    ),
    ok = emqx_cth_peer:kill(Replicant),
    ?retry(100, 50, ?assertEqual(down, node_status(Replicant))),
    %% Down for less than the timeout: the members stay.
    ?assertMatch([_], emqx_shared_sub:subscribers(?GROUP, Topic)),
    {ok, [Event]} = snabbkaffe:receive_events(SRef),
    ?assertMatch(#{reason := {replicant_down_for, _}}, Event),
    ?assertEqual([], emqx_shared_sub:subscribers(?GROUP, Topic)).

t_orphan_members('init', Config) ->
    start_replicant(?FUNCTION_NAME, Config);
t_orphan_members('end', Config) ->
    stop_replicant(Config).

-doc """
The core node does not know the status of a node that went down, for example
because the core node restarted after it. The periodic reconcile finds the node
from its shared subscription members and purges them.
""".
t_orphan_members(Config) ->
    Replicant = ?config(replicant, Config),
    Topic = <<"t/orphan/#">>,
    _SubPid = start_shared_sub(Replicant, ?GROUP, Topic),
    ok = emqx_cth_peer:kill(Replicant),
    ?retry(100, 50, ?assertEqual(down, node_status(Replicant))),
    %% Restart the helper process: this loses the node status table.
    ok = supervisor:terminate_child(emqx_router_sup, helper),
    {ok, _} = supervisor:restart_child(emqx_router_sup, helper),
    ?assertEqual(notfound, node_status(Replicant)),
    ?assertMatch([_], emqx_shared_sub:subscribers(?GROUP, Topic)),
    ?assertMatch(
        {ok, #{reason := {replicant_down_for, _}}},
        ?block_until(#{?snk_kind := broker_node_purged, node := Replicant}, ?PURGE_WAIT_TIMEOUT)
    ),
    ?assertEqual([], emqx_shared_sub:subscribers(?GROUP, Topic)).

t_replicant_restart('init', Config) ->
    start_replicant(?FUNCTION_NAME, Config);
t_replicant_restart('end', Config) ->
    stop_replicant(Config).

-doc """
A replicant goes down and comes back with the same node name before the timeout.
It removes the members of its previous incarnation itself, and the core node does
not purge the members of the new incarnation.
""".
t_replicant_restart(Config) ->
    Replicant = ?config(replicant, Config),
    Spec = ?config(replicant_spec, Config),
    Topic = <<"t/restart/#">>,
    OldSubPid = start_shared_sub(Replicant, ?GROUP, Topic),
    ok = emqx_cth_peer:kill(Replicant),
    ?retry(100, 50, ?assertEqual(down, node_status(Replicant))),
    [Replicant] = emqx_cth_cluster:restart(Spec),
    ?retry(100, 50, ?assertEqual(notfound, node_status(Replicant))),
    ?retry(100, 50, ?assertEqual([], emqx_shared_sub:subscribers(?GROUP, Topic))),
    NewSubPid = start_shared_sub(Replicant, ?GROUP, Topic),
    ?assertNotEqual(OldSubPid, NewSubPid),
    %% Purge every node that is known to be down, regardless of the timeout.
    scheduled = emqx_router_helper:schedule_force_purge(),
    _ = sys:get_state(?HELPER),
    ?assertEqual([NewSubPid], emqx_shared_sub:subscribers(?GROUP, Topic)),
    ?assertEqual(
        [],
        [
            E
         || E = #{node := N} <- ?of_kind(broker_node_purged, snabbkaffe:collect_trace(0)),
            N == Replicant
        ]
    ).

t_dispatch_skips_down_member('init', Config) ->
    start_replicants([dispatch_1, dispatch_2], Config);
t_dispatch_skips_down_member('end', Config) ->
    _ = sys:resume(?HELPER),
    stop_replicant(Config).

-doc """
Dispatch on a replicant skips a member on another replicant that is down, before
a core node purges the member.
""".
t_dispatch_skips_down_member(Config) ->
    [Replicant1, Replicant2] = ?config(replicants, Config),
    Topic = <<"t/dispatch/#">>,
    Sub1 = start_shared_sub(Replicant1, ?GROUP, Topic),
    Sub2 = start_shared_sub(Replicant2, ?GROUP, Topic),
    ?retry(
        100,
        50,
        ?assertMatch([_, _], erpc:call(Replicant1, emqx_shared_sub, subscribers, [?GROUP, Topic]))
    ),
    %% Round robin: both members get messages while both replicants run.
    ok = publish_n(Replicant1, <<"t/dispatch/1">>, 10),
    Before = collect_deliveries(10),
    ?assertEqual(lists:sort([Sub1, Sub2]), lists:usort(Before)),
    %% Keep the core node from purging the member during the test.
    ok = sys:suspend(?HELPER),
    ok = emqx_cth_peer:kill(Replicant2),
    ?retry(100, 50, ?assertEqual(down, node_status(Replicant1, Replicant2))),
    %% Other nodes can be listed too: replicants of earlier cases that left members.
    ?assert(lists:member(Replicant2, erpc:call(Replicant1, ?HELPER, unreachable_nodes, []))),
    %% The member is not purged yet.
    ?assertMatch([_, _], erpc:call(Replicant1, emqx_shared_sub, subscribers, [?GROUP, Topic])),
    %% Every message goes to the member on the running node.
    ok = publish_n(Replicant1, <<"t/dispatch/1">>, 10),
    ?assertEqual(lists:duplicate(10, Sub1), collect_deliveries(10)).

t_dispatch_skips_orphan_member('init', Config) ->
    start_replicants([orphan_1, orphan_2], Config);
t_dispatch_skips_orphan_member('end', Config) ->
    _ = sys:resume(?HELPER),
    stop_replicant(Config).

-doc """
A replicant that does not know that another replicant went down, for example
because it started after it, finds the node from its shared subscription members
in the periodic reconcile. Dispatch then skips the member.
""".
t_dispatch_skips_orphan_member(Config) ->
    [Replicant1, Replicant2] = ?config(replicants, Config),
    Topic = <<"t/orphan_dispatch/#">>,
    Sub1 = start_shared_sub(Replicant1, ?GROUP, Topic),
    _Sub2 = start_shared_sub(Replicant2, ?GROUP, Topic),
    ?retry(
        100,
        50,
        ?assertMatch([_, _], erpc:call(Replicant1, emqx_shared_sub, subscribers, [?GROUP, Topic]))
    ),
    %% Keep the core node from purging the member during the test.
    ok = sys:suspend(?HELPER),
    ok = emqx_cth_peer:kill(Replicant2),
    ?retry(100, 50, ?assertEqual(down, node_status(Replicant1, Replicant2))),
    %% Restart the helper process on the first replicant: this loses the node
    %% status table, and no membership event marks the node down again.
    ok = erpc:call(Replicant1, supervisor, terminate_child, [emqx_router_sup, helper]),
    {ok, _} = erpc:call(Replicant1, supervisor, restart_child, [emqx_router_sup, helper]),
    ?assertEqual(notfound, node_status(Replicant1, Replicant2)),
    ?assertNot(lists:member(Replicant2, erpc:call(Replicant1, ?HELPER, unreachable_nodes, []))),
    %% The reconcile on the replicant finds the node from its member.
    ?retry(200, 50, ?assertEqual(down, node_status(Replicant1, Replicant2))),
    ok = publish_n(Replicant1, <<"t/orphan_dispatch/1">>, 10),
    ?assertEqual(lists:duplicate(10, Sub1), collect_deliveries(10)).

t_replicant_to_replicant_dispatch('init', Config) ->
    start_replicants([r2r_1, r2r_2], Config);
t_replicant_to_replicant_dispatch('end', Config) ->
    stop_replicant(Config).

-doc """
A replicant dispatches to a member on another replicant: each replicant sees the
other one as a running cluster member.
""".
t_replicant_to_replicant_dispatch(Config) ->
    [Replicant1, Replicant2] = ?config(replicants, Config),
    Topic = <<"t/r2r/#">>,
    Sub = start_shared_sub(Replicant2, ?GROUP, Topic),
    ?retry(
        100, 50, ?assert(lists:member(Replicant2, erpc:call(Replicant1, mria, running_nodes, [])))
    ),
    ?retry(
        100,
        50,
        ?assertEqual([Sub], erpc:call(Replicant1, emqx_shared_sub, subscribers, [?GROUP, Topic]))
    ),
    %% The shared route may reach the first replicant later than the member row.
    ?retry(
        200,
        25,
        begin
            ok = publish_n(Replicant1, <<"t/r2r/1">>, 1),
            ?assertEqual([Sub], collect_deliveries(1))
        end
    ).

t_reregister_members('init', Config) ->
    start_replicant(?FUNCTION_NAME, Config);
t_reregister_members('end', Config) ->
    stop_replicant(Config).

-doc """
Another node deletes the shared subscription member of a live replicant, as a core
node does when it purges a replicant that it saw down during a partition, or when
it rejoins the cluster to heal one. The replicant writes it back on its next
reconcile.
""".
t_reregister_members(Config) ->
    Replicant = ?config(replicant, Config),
    Topic = <<"t/rereg/#">>,
    Dest = {?GROUP, Replicant},
    Sub = start_shared_sub(Replicant, ?GROUP, Topic),
    ?retry(100, 50, ?assert(emqx_router:has_route(Topic, Dest))),
    %% More members: a second topic of the same group, and a second group.
    Topic2 = <<"t/rereg2/#">>,
    Topic3 = <<"t/rereg3/#">>,
    Group2 = <<"g2">>,
    Sub2 = start_shared_sub(Replicant, ?GROUP, Topic2),
    Sub3 = start_shared_sub(Replicant, Group2, Topic3),
    %% Pause the reconcile of the replicant, and let a running worker finish, so that
    %% the replicant does not write the member back before the deletion is seen.
    ok = erpc:call(Replicant, sys, suspend, [?HELPER]),
    ?retry(
        100,
        50,
        ?assertEqual(undefined, erpc:call(Replicant, erlang, whereis, [emqx_shared_sub_reregister]))
    ),
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := shared_subscription_reregistered}),
        1,
        ?PURGE_WAIT_TIMEOUT
    ),
    ok = emqx_shared_sub:purge_node(Replicant),
    ?assertEqual([], emqx_shared_sub:subscribers(?GROUP, Topic)),
    ?assertEqual([], emqx_shared_sub:subscribers(?GROUP, Topic2)),
    ?assertEqual([], emqx_shared_sub:subscribers(Group2, Topic3)),
    ?retry(
        100,
        50,
        ?assertEqual([], erpc:call(Replicant, emqx_shared_sub, subscribers, [?GROUP, Topic]))
    ),
    ok = erpc:call(Replicant, sys, resume, [?HELPER]),
    {ok, [Event]} = snabbkaffe:receive_events(SRef),
    ?assertMatch(#{members := 3}, Event),
    ?retry(100, 50, ?assertEqual([Sub], emqx_shared_sub:subscribers(?GROUP, Topic))),
    ?retry(100, 50, ?assertEqual([Sub2], emqx_shared_sub:subscribers(?GROUP, Topic2))),
    ?retry(100, 50, ?assertEqual([Sub3], emqx_shared_sub:subscribers(Group2, Topic3))),
    ?assert(emqx_router:has_route(Topic, Dest)),
    %% Publish on the replicant: it dispatches with its copy of both tables.
    ?retry(
        100,
        50,
        ?assertEqual([Sub], erpc:call(Replicant, emqx_shared_sub, subscribers, [?GROUP, Topic]))
    ),
    ?retry(
        200,
        25,
        begin
            ok = publish_n(Replicant, <<"t/rereg/1">>, 1),
            ?assertEqual([Sub], collect_deliveries(1))
        end
    ).

-doc """
The worker that writes lost members back reads the member table of the shared
subscription server, then writes outside the server. A member that is unsubscribed
meanwhile can get its row back after the unsubscribe removed it. When the server
confirms the members that the worker wrote, it removes such a member again, and
keeps a member that is still subscribed.
""".
t_reregister_unsubscribed_member(_Config) ->
    Topic = <<"t/rereg/confirm">>,
    Sub = start_shared_sub(node(), ?GROUP, Topic),
    %% A live process that is not subscribed, with a row in the table: as a member
    %% that was unsubscribed before the write of the worker reached the table.
    Gone = spawn_link(fun() ->
        receive
            stop -> ok
        end
    end),
    Row = {emqx_shared_subscription, ?GROUP, Topic, Gone},
    ok = mria:dirty_write(emqx_shared_subscription, Row),
    ?assertEqual(lists:sort([Sub, Gone]), lists:sort(emqx_shared_sub:subscribers(?GROUP, Topic))),
    Members = [{?GROUP, Topic, Sub}, {?GROUP, Topic, Gone}],
    ok = gen_server:cast(emqx_shared_sub, {reregistered, Members}),
    _ = sys:get_state(emqx_shared_sub),
    ?assertEqual([Sub], emqx_shared_sub:subscribers(?GROUP, Topic)),
    Gone ! stop,
    exit(Sub, kill),
    ?retry(100, 50, ?assertEqual([], emqx_shared_sub:subscribers(?GROUP, Topic))).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

start_replicant(Name, Config) ->
    Config1 = start_replicants([Name], Config),
    [Node] = ?config(replicants, Config1),
    [Spec] = ?config(replicant_specs, Config1),
    [{replicant, Node}, {replicant_spec, Spec} | Config1].

start_replicants(Names, Config) ->
    AppSpec = {emqx, #{config => schema_config(?config(schema, Config))}},
    Specs = emqx_cth_cluster:mk_nodespecs(
        [{Name, #{apps => [AppSpec], role => replicant, join_to => node()}} || Name <- Names],
        #{work_dir => emqx_cth_suite:work_dir(hd(Names), Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    [{replicants, Nodes}, {replicant_specs, Specs} | Config].

stop_replicant(Config) ->
    ok = emqx_cth_cluster:stop(?config(replicants, Config)).

node_status(Node) ->
    case ets:lookup(?HELPER, Node) of
        [Status] -> element(3, Status);
        [] -> notfound
    end.

%% The status of `Node' in the router helper of `OnNode'.
node_status(OnNode, Node) ->
    erpc:call(OnNode, ?MODULE, node_status, [Node]).

%% Start a shared subscriber on the node. It sends each delivered message to
%% the test process.
start_shared_sub(Node, Group, Topic) ->
    Owner = self(),
    ShareTopic = emqx_topic:make_shared_record(Group, Topic),
    SubPid = erpc:call(Node, erlang, spawn, [fun() -> shared_subscriber(Owner, ShareTopic) end]),
    ?assertReceive({SubPid, ok}, 5_000),
    %% NOTE: Anticipate replication delay.
    ?retry(100, 50, ?assert(lists:member(SubPid, emqx_shared_sub:subscribers(Group, Topic)))),
    SubPid.

shared_subscriber(Owner, ShareTopic) ->
    ok = emqx_broker:subscribe(ShareTopic),
    Owner ! {self(), ok},
    shared_subscriber_loop(Owner).

shared_subscriber_loop(Owner) ->
    receive
        {deliver, _Topic, #message{}} ->
            Owner ! {delivered, self()},
            shared_subscriber_loop(Owner)
    end.

%% Publish from one process on the node, so that round robin alternates.
publish_n(Node, Topic, N) ->
    erpc:call(Node, fun() ->
        lists:foreach(
            fun(I) ->
                Msg = emqx_message:make(<<?MODULE_STRING>>, Topic, integer_to_binary(I)),
                _ = emqx_broker:publish(Msg)
            end,
            lists:seq(1, N)
        )
    end).

%% Collect up to N deliveries, in the order they arrive. Stop early when no
%% delivery arrives for 1 s.
collect_deliveries(0) ->
    [];
collect_deliveries(N) ->
    receive
        {delivered, SubPid} -> [SubPid | collect_deliveries(N - 1)]
    after 1_000 ->
        []
    end.
