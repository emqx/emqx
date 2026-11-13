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
        t_replicant_restart
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
    _SubPid = start_remote_shared_sub(Replicant, ?GROUP, Topic),
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
    _SubPid = start_remote_shared_sub(Replicant, ?GROUP, Topic),
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
    OldSubPid = start_remote_shared_sub(Replicant, ?GROUP, Topic),
    ok = emqx_cth_peer:kill(Replicant),
    ?retry(100, 50, ?assertEqual(down, node_status(Replicant))),
    [Replicant] = emqx_cth_cluster:restart(Spec),
    ?retry(100, 50, ?assertEqual(notfound, node_status(Replicant))),
    ?retry(100, 50, ?assertEqual([], emqx_shared_sub:subscribers(?GROUP, Topic))),
    NewSubPid = start_remote_shared_sub(Replicant, ?GROUP, Topic),
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

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

start_replicant(Name, Config) ->
    [Spec] = emqx_cth_cluster:mk_nodespecs(
        [
            {Name, #{
                apps => [{emqx, #{config => schema_config(?config(schema, Config))}}],
                role => replicant,
                join_to => node()
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Name, Config)}
    ),
    [Node] = emqx_cth_cluster:start([Spec]),
    [{replicant, Node}, {replicant_spec, Spec} | Config].

stop_replicant(Config) ->
    ok = emqx_cth_cluster:stop([?config(replicant, Config)]).

node_status(Node) ->
    case ets:lookup(?HELPER, Node) of
        [Status] -> element(3, Status);
        [] -> notfound
    end.

start_remote_shared_sub(Node, Group, Topic) ->
    Pid = self(),
    ShareTopic = emqx_topic:make_shared_record(Group, Topic),
    SubPid = erpc:call(Node, erlang, spawn, [
        fun() ->
            ok = emqx_broker:subscribe(ShareTopic),
            Pid ! {self(), ok},
            timer:sleep(infinity)
        end
    ]),
    ?assertReceive({SubPid, ok}, 5_000),
    %% NOTE: Anticipate replication delay.
    ?retry(100, 50, ?assert(lists:member(SubPid, emqx_shared_sub:subscribers(Group, Topic)))),
    SubPid.
