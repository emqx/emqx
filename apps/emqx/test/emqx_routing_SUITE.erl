%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_routing_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_router.hrl").

all() ->
    [
        {group, routing_schema_v1},
        {group, routing_schema_v2},
        t_routing_schema_switch_v1,
        t_routing_schema_switch_v2,
        t_routing_schema_consistent_clean_cluster
    ].

groups() ->
    GroupVsn = [
        {group, batch_sync_on},
        {group, batch_sync_replicants},
        {group, batch_sync_off}
    ],
    ClusterTCs = [
        t_cluster_routing,
        t_slow_rlog_routing_consistency
    ],
    SingleTCs = [t_concurrent_routing_updates],
    BatchSyncTCs = lists:duplicate(5, t_concurrent_routing_updates_with_errors),
    [
        {routing_schema_v1, [], GroupVsn},
        {routing_schema_v2, [], GroupVsn},
        {batch_sync_on, [], [{group, cluster}, {group, single_batch_on}]},
        {batch_sync_replicants, [], [{group, cluster}, {group, single}]},
        {batch_sync_off, [], [{group, cluster}, {group, single}]},
        {cluster, [], ClusterTCs},
        {single_batch_on, [], SingleTCs ++ BatchSyncTCs},
        {single, [], SingleTCs}
    ].

init_per_group(routing_schema_v1, Config) ->
    [{emqx_config, "broker.routing.storage_schema = v1"} | Config];
init_per_group(routing_schema_v2, Config) ->
    [{emqx_config, "broker.routing.storage_schema = v2"} | Config];
init_per_group(batch_sync_on, Config) ->
    [{emqx_config, "broker.routing.batch_sync.enable_on = all"} | Config];
init_per_group(batch_sync_replicants, Config) ->
    case emqx_cth_suite:skip_if_oss() of
        false ->
            [{emqx_config, "broker.routing.batch_sync.enable_on = replicant"} | Config];
        True ->
            True
    end;
init_per_group(batch_sync_off, Config) ->
    [{emqx_config, "broker.routing.batch_sync.enable_on = none"} | Config];
init_per_group(cluster, Config) ->
    case emqx_cth_suite:skip_if_oss() of
        false ->
            WorkDir = emqx_cth_suite:work_dir(Config),
            NodeSpecs = [
                {emqx_routing_SUITE1, #{apps => [mk_emqx_appspec(1, Config)], role => core}},
                {emqx_routing_SUITE2, #{apps => [mk_emqx_appspec(2, Config)], role => core}},
                {emqx_routing_SUITE3, #{apps => [mk_emqx_appspec(3, Config)], role => replicant}}
            ],
            Nodes = emqx_cth_cluster:start(NodeSpecs, #{work_dir => WorkDir}),
            [{cluster, Nodes} | Config];
        True ->
            True
    end;
init_per_group(GroupName, Config) when
    GroupName =:= single_batch_on;
    GroupName =:= single
->
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                config => mk_config_broker(Config),
                %% NOTE
                %% Artificially increasing pool workers contention by forcing small pool size.
                before_start => fun() ->
                    % NOTE
                    % This one is actually defined on `emqx_conf_schema` level, but used
                    % in `emqx_broker`. Thus we have to resort to this ugly hack.
                    emqx_config:force_put([node, broker_pool_size], 2),
                    emqx_app:set_config_loader(?MODULE)
                end
            }}
        ],
        #{work_dir => WorkDir}
    ),
    [{group_apps, Apps} | Config].

end_per_group(cluster, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config));
end_per_group(GroupName, Config) when
    GroupName =:= single_batch_on;
    GroupName =:= single
->
    emqx_cth_suite:stop(?config(group_apps, Config));
end_per_group(_, _Config) ->
    ok.

init_per_testcase(TC, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config).

end_per_testcase(TC, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config).

mk_emqx_appspec(N, Config) ->
    {emqx, #{
        config => mk_config(N, Config),
        after_start => fun() ->
            % NOTE
            % This one is actually defined on `emqx_conf_schema` level, but used
            % in `emqx_broker`. Thus we have to resort to this ugly hack.
            emqx_config:force_put([rpc, mode], async)
        end
    }}.

mk_genrpc_appspec() ->
    {gen_rpc, #{
        override_env => [{port_discovery, stateless}]
    }}.

mk_config(N, ConfigOrVsn) ->
    emqx_cth_suite:merge_config(
        mk_config_broker(ConfigOrVsn),
        mk_config_listeners(N)
    ).

mk_config_broker(v1) ->
    "broker.routing.storage_schema = v1";
mk_config_broker(v2) ->
    "broker.routing.storage_schema = v2";
mk_config_broker(CTConfig) ->
    string:join(proplists:get_all_values(emqx_config, CTConfig), "\n").

mk_config_listeners(N) ->
    Port = 1883 + N,
    #{
        listeners => #{
            tcp => #{default => #{bind => "127.0.0.1:" ++ integer_to_list(Port)}},
            ssl => #{default => #{enable => false}},
            ws => #{default => #{enable => false}},
            wss => #{default => #{enable => false}}
        }
    }.

%%

t_cluster_routing(Config) ->
    Cluster = ?config(cluster, Config),
    Clients = [C1, C2, C3] = lists:sort([start_client(N) || N <- Cluster]),
    Commands = [
        {fun publish/3, [C1, <<"a/b/c">>, <<"wontsee">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"wontsee">>]},
        {fun subscribe/2, [C3, <<"a/+/c/#">>]},
        {fun publish/3, [C1, <<"a/b/c">>, <<"01">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"wontsee">>]},
        {fun subscribe/2, [C1, <<"a/b/c">>]},
        {fun subscribe/2, [C2, <<"a/b/+">>]},
        {fun publish/3, [C3, <<"a/b/c">>, <<"02">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"03">>]},
        {fun publish/3, [C2, <<"a/b/c/d">>, <<"04">>]},
        {fun subscribe/2, [C3, <<"a/b/d">>]},
        {fun publish/3, [C1, <<"a/b/d">>, <<"05">>]},
        {fun unsubscribe/2, [C3, <<"a/+/c/#">>]},
        {fun publish/3, [C1, <<"a/b/c">>, <<"06">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"07">>]},
        {fun publish/3, [C2, <<"a/b/c/d">>, <<"08">>]},
        {fun unsubscribe/2, [C2, <<"a/b/+">>]},
        {fun publish/3, [C1, <<"a/b/c">>, <<"09">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"10">>]},
        {fun publish/3, [C2, <<"a/b/c/d">>, <<"11">>]},
        {fun unsubscribe/2, [C3, <<"a/b/d">>]},
        {fun unsubscribe/2, [C1, <<"a/b/c">>]},
        {fun publish/3, [C1, <<"a/b/c">>, <<"wontsee">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"wontsee">>]}
    ],
    ok = lists:foreach(fun({F, Args}) -> erlang:apply(F, Args) end, Commands),
    _ = [emqtt:stop(C) || C <- Clients],
    Deliveries = ?drainMailbox(),
    ?assertMatch(
        [
            {pub, C1, #{topic := <<"a/b/c">>, payload := <<"02">>}},
            {pub, C1, #{topic := <<"a/b/c">>, payload := <<"06">>}},
            {pub, C1, #{topic := <<"a/b/c">>, payload := <<"09">>}},
            {pub, C2, #{topic := <<"a/b/c">>, payload := <<"02">>}},
            {pub, C2, #{topic := <<"a/b/d">>, payload := <<"03">>}},
            {pub, C2, #{topic := <<"a/b/d">>, payload := <<"05">>}},
            {pub, C2, #{topic := <<"a/b/c">>, payload := <<"06">>}},
            {pub, C2, #{topic := <<"a/b/d">>, payload := <<"07">>}},
            {pub, C3, #{topic := <<"a/b/c">>, payload := <<"01">>}},
            {pub, C3, #{topic := <<"a/b/c">>, payload := <<"02">>}},
            {pub, C3, #{topic := <<"a/b/c/d">>, payload := <<"04">>}},
            {pub, C3, #{topic := <<"a/b/d">>, payload := <<"05">>}},
            {pub, C3, #{topic := <<"a/b/d">>, payload := <<"07">>}},
            {pub, C3, #{topic := <<"a/b/d">>, payload := <<"10">>}}
        ],
        lists:sort(
            fun({pub, CL, #{payload := PL}}, {pub, CR, #{payload := PR}}) ->
                {CL, PL} < {CR, PR}
            end,
            Deliveries
        )
    ).

start_client(Node) ->
    Self = self(),
    {ok, C} = emqtt:start_link(#{
        port => get_mqtt_tcp_port(Node),
        msg_handler => #{
            publish => fun(Msg) -> Self ! {pub, self(), Msg} end
        }
    }),
    {ok, _Props} = emqtt:connect(C),
    C.

publish(C, Topic, Payload) ->
    {ok, #{reason_code := 0}} = emqtt:publish(C, Topic, Payload, 1).

subscribe(C, Topic) ->
    % NOTE: sleeping here as lazy way to wait for subscribe to replicate
    {ok, _Props, [0]} = emqtt:subscribe(C, Topic),
    ok = timer:sleep(200).

unsubscribe(C, Topic) ->
    % NOTE: sleeping here as lazy way to wait for unsubscribe to replicate
    {ok, _Props, undefined} = emqtt:unsubscribe(C, Topic),
    ok = timer:sleep(200).

%%

-define(SUBSCRIBE_TOPICS, [
    <<"t/#">>,
    <<"t/fixed">>,
    <<"t/1/+">>,
    <<"t/2/+">>,
    <<"t/42/+/+">>,
    <<"client/${i}/+">>,
    <<"client/${i}/fixed">>,
    <<"client/${i}/#">>,
    <<"rand/${r}/+">>,
    <<"rand/${r}/fixed">>
]).

t_concurrent_routing_updates(init, Config) ->
    ok = snabbkaffe:start_trace(),
    Config;
t_concurrent_routing_updates('end', _Config) ->
    ok = snabbkaffe:stop().

t_concurrent_routing_updates(_Config) ->
    NClients = 400,
    NRTopics = 250,
    MCommands = 8,
    Port = get_mqtt_tcp_port(node()),
    Clients = [
        spawn_link(?MODULE, run_concurrent_client, [I, Port, MCommands, NRTopics])
     || I <- lists:seq(1, NClients)
    ],
    ok = lists:foreach(fun ping_concurrent_client/1, Clients),
    ok = timer:sleep(200),
    Subscribers = ets:tab2list(?SUBSCRIBER),
    Topics = maps:keys(maps:from_list(Subscribers)),
    ?assertEqual(lists:sort(Topics), lists:sort(emqx_router:topics())),
    ok = lists:foreach(fun stop_concurrent_client/1, Clients),
    ok = timer:sleep(1000),
    ct:pal("Trace: ~p", [?of_kind(router_syncer_new_batch, snabbkaffe:collect_trace())]),
    ?assertEqual([], ets:tab2list(?SUBSCRIBER)),
    ?assertEqual([], emqx_router:topics()).

t_concurrent_routing_updates_with_errors(init, Config) ->
    ok = snabbkaffe:start_trace(),
    ok = meck:new(emqx_router, [passthrough, no_history]),
    Config;
t_concurrent_routing_updates_with_errors('end', _Config) ->
    ok = meck:unload(emqx_router),
    ok = snabbkaffe:stop().

t_concurrent_routing_updates_with_errors(_Config) ->
    NClients = 100,
    NRTopics = 80,
    MCommands = 6,
    PSyncError = 0.1,
    Port = get_mqtt_tcp_port(node()),
    %% Crash the batch sync operation with some small probability.
    ok = meck:expect(emqx_router, mria_batch_run, fun(Vsn, Batch) ->
        case rand:uniform() < PSyncError of
            false -> meck:passthrough([Vsn, Batch]);
            true -> error(overload)
        end
    end),
    Clients = [
        spawn_link(?MODULE, run_concurrent_client, [I, Port, MCommands, NRTopics])
     || I <- lists:seq(1, NClients)
    ],
    ok = lists:foreach(fun ping_concurrent_client/1, Clients),
    0 = ?retry(
        _Interval = 500,
        _NTimes = 10,
        0 = lists:sum([S || #{size := S} <- emqx_router_syncer:stats()])
    ),
    Subscribers = ets:tab2list(?SUBSCRIBER),
    Topics = maps:keys(maps:from_list(Subscribers)),
    ?assertEqual(lists:sort(Topics), lists:sort(emqx_router:topics())),
    ok = lists:foreach(fun stop_concurrent_client/1, Clients),
    ok = timer:sleep(100),
    0 = ?retry(
        500,
        10,
        0 = lists:sum([S || #{size := S} <- emqx_router_syncer:stats()])
    ),
    ct:pal("Trace: ~p", [?of_kind(router_syncer_new_batch, snabbkaffe:collect_trace())]),
    ?assertEqual([], ets:tab2list(?SUBSCRIBER)),
    ?assertEqual([], emqx_router:topics()).

run_concurrent_client(I, Port, MCommands, NRTopics) ->
    % _ = rand:seed(default, I),
    Ctx = #{
        i => I,
        r => rand:uniform(NRTopics)
    },
    {ok, C} = emqtt:start_link(#{port => Port, clientid => render("client:${i}", Ctx)}),
    {ok, _Props} = emqtt:connect(C),
    NCommands = rand:uniform(MCommands),
    Commands = gen_concurrent_client_plan(NCommands, Ctx),
    ok = subscribe_concurrent_client(C, Commands),
    run_concurrent_client_loop(C).

gen_concurrent_client_plan(N, Ctx) ->
    lists:foldl(
        fun(_, Acc) -> mixin(pick_random_command(Ctx), Acc) end,
        [],
        lists:seq(1, N)
    ).

subscribe_concurrent_client(C, Commands) ->
    lists:foreach(
        fun
            ({subscribe, Topic}) ->
                {ok, _Props, [0]} = emqtt:subscribe(C, Topic);
            ({unsubscribe, Topic}) ->
                {ok, _Props, undefined} = emqtt:unsubscribe(C, Topic)
        end,
        Commands
    ).

pick_random_command(Ctx) ->
    Topic = render(randpick(?SUBSCRIBE_TOPICS), Ctx),
    randpick([
        [{subscribe, Topic}],
        [{subscribe, Topic}, {unsubscribe, Topic}]
    ]).

render(Template, Ctx) ->
    iolist_to_binary(emqx_template:render_strict(emqx_template:parse(Template), Ctx)).

run_concurrent_client_loop(C) ->
    receive
        {From, Ref, F} ->
            Reply = F(C),
            From ! {Ref, Reply},
            run_concurrent_client_loop(C)
    end.

ping_concurrent_client(Pid) ->
    Ref = make_ref(),
    Pid ! {self(), Ref, fun emqtt:ping/1},
    receive
        {Ref, Reply} -> Reply
    after 5000 ->
        error(timeout)
    end.

stop_concurrent_client(Pid) ->
    MRef = erlang:monitor(process, Pid),
    true = erlang:unlink(Pid),
    true = erlang:exit(Pid, shutdown),
    receive
        {'DOWN', MRef, process, Pid, Reason} -> Reason
    end.

randpick(List) ->
    lists:nth(rand:uniform(length(List)), List).

mixin(L = [H | T], Into = [HInto | TInto]) ->
    case rand:uniform(length(Into) + 1) of
        1 -> [H | mixin(T, Into)];
        _ -> [HInto | mixin(L, TInto)]
    end;
mixin(L, Into) ->
    L ++ Into.

%%

t_routing_schema_switch_v1(Config) ->
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    t_routing_schema_switch(_From = v2, _To = v1, WorkDir).

t_routing_schema_switch_v2(Config) ->
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    t_routing_schema_switch(_From = v1, _To = v2, WorkDir).

t_routing_schema_switch(VFrom, VTo, WorkDir) ->
    % Start first node with routing schema VTo (e.g. v1)
    [Node1] = emqx_cth_cluster:start(
        [
            {routing_schema_switch1, #{
                apps => [mk_genrpc_appspec(), mk_emqx_appspec(1, VTo)]
            }}
        ],
        #{work_dir => WorkDir}
    ),
    % Ensure there's at least 1 route on Node1
    C1 = start_client(Node1),
    ok = subscribe(C1, <<"a/+/c">>),
    ok = subscribe(C1, <<"d/e/f/#">>),
    % Start rest of nodes with routing schema VFrom (e.g. v2)
    [Node2, Node3] = emqx_cth_cluster:start(
        [
            {routing_schema_switch2, #{
                apps => [mk_genrpc_appspec(), mk_emqx_appspec(2, VFrom)],
                base_port => 20000,
                join_to => Node1
            }},
            {routing_schema_switch3, #{
                apps => [mk_genrpc_appspec(), mk_emqx_appspec(3, VFrom)],
                base_port => 20100,
                join_to => Node1
            }}
        ],
        #{work_dir => WorkDir}
    ),
    Nodes = [Node1, Node2, Node3],
    try
        % Verify that new nodes switched to schema v1/v2 in presence of v1/v2 routes respectively
        ?assertEqual(
            [{ok, VTo}, {ok, VTo}, {ok, VTo}],
            erpc:multicall(Nodes, emqx_router, get_schema_vsn, [])
        ),
        % Wait for all nodes to agree on cluster state
        ?retry(
            500,
            10,
            ?assertMatch(
                [{ok, [Node1, Node2, Node3]}],
                lists:usort(erpc:multicall(Nodes, emqx, running_nodes, []))
            )
        ),
        % Verify that routing works as expected
        C2 = start_client(Node2),
        ok = subscribe(C2, <<"a/+/d">>),
        C3 = start_client(Node3),
        ok = subscribe(C3, <<"d/e/f/#">>),
        {ok, _} = publish(C1, <<"a/b/d">>, <<"hey-newbies">>),
        {ok, _} = publish(C2, <<"a/b/c">>, <<"hi">>),
        {ok, _} = publish(C3, <<"d/e/f/42">>, <<"hello">>),
        ?assertReceive({pub, C2, #{topic := <<"a/b/d">>, payload := <<"hey-newbies">>}}),
        ?assertReceive({pub, C1, #{topic := <<"a/b/c">>, payload := <<"hi">>}}),
        ?assertReceive({pub, C1, #{topic := <<"d/e/f/42">>, payload := <<"hello">>}}),
        ?assertReceive({pub, C3, #{topic := <<"d/e/f/42">>, payload := <<"hello">>}}),
        ?assertNotReceive(_),
        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3)
    after
        ok = emqx_cth_cluster:stop(Nodes)
    end.

t_routing_schema_consistent_clean_cluster(Config) ->
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    % Start first node with routing schema v1
    [Node1] = emqx_cth_cluster:start(
        [
            {routing_schema_consistent1, #{
                apps => [mk_genrpc_appspec(), mk_emqx_appspec(1, v1)]
            }}
        ],
        #{work_dir => WorkDir}
    ),
    % Start rest of nodes with routing schema v2
    NodesRest = emqx_cth_cluster:start(
        [
            {routing_schema_consistent2, #{
                apps => [mk_genrpc_appspec(), mk_emqx_appspec(2, v2)],
                base_port => 20000,
                join_to => Node1
            }},
            {routing_schema_consistent3, #{
                apps => [mk_genrpc_appspec(), mk_emqx_appspec(3, v2)],
                base_port => 20100,
                join_to => Node1
            }}
        ],
        #{work_dir => WorkDir}
    ),
    Nodes = [Node1 | NodesRest],
    try
        % Verify that cluser is still on v1
        ?assertEqual(
            [{ok, v1} || _ <- Nodes],
            erpc:multicall(Nodes, emqx_router, get_schema_vsn, [])
        ),
        % Wait for all nodes to agree on cluster state
        ?retry(
            500,
            10,
            ?assertEqual(
                [{ok, Nodes} || _ <- Nodes],
                erpc:multicall(Nodes, emqx, running_nodes, [])
            )
        ),
        C1 = start_client(Node1),
        C2 = start_client(hd(NodesRest)),
        ok = subscribe(C2, <<"t/#">>),
        {ok, _} = publish(C1, <<"t/a/b/c">>, <<"yayconsistency">>),
        ?assertReceive({pub, C2, #{topic := <<"t/a/b/c">>, payload := <<"yayconsistency">>}}),
        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2)
    after
        ok = emqx_cth_cluster:stop(Nodes)
    end.

t_slow_rlog_routing_consistency(init, Config) ->
    [Core1, _Core2, _Replicant] = ?config(cluster, Config),
    MnesiaHook = rpc:call(Core1, persistent_term, get, [{mnesia_hook, post_commit}]),
    [{original_mnesia_hook, MnesiaHook} | Config];
t_slow_rlog_routing_consistency('end', Config) ->
    [Core1, Core2, _Replicant] = ?config(cluster, Config),
    MnesiaHook = ?config(original_mnesia_hook, Config),
    ok = register_mria_hook(MnesiaHook, [Core1, Core2]).

t_slow_rlog_routing_consistency(Config) ->
    [Core1, Core2, Replicant] = ?config(cluster, Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Topic = <<"t/", ClientId/binary>>,
    Self = self(),
    ?assertEqual(ok, rpc:call(Replicant, emqx_broker, do_subscribe, [Topic, Self, #{}])),
    %% Wait for normal route replication (must be fast enough)
    emqx_common_test_helpers:wait_for(
        ?FUNCTION_NAME,
        ?LINE,
        fun() ->
            rpc:call(Replicant, emqx_router, has_route, [Topic, Replicant])
        end,
        2_000
    ),
    DelayMs = 3_000,
    slowdown_mria_rlog(?config(original_mnesia_hook, Config), [Core1, Core2], DelayMs),
    {ok, _} = rpc:call(Replicant, mnesia_subscr, subscribe, [Self, {table, ?ROUTE_TAB, simple}]),
    UnSubSubFun = fun() ->
        %% Unsubscribe must remove a route, but the effect
        %% is expected to be delayed on the replicant node
        ok = emqx_broker:do_unsubscribe(Topic, Self, #{}),
        %% Wait a little (less than introduced delay),
        %% just to reduce the risk of delete/add routes ops being re-ordered
        timer:sleep(100),
        %% Subscribe must add a route again, even though the previosus
        %% route may be still present on the replicant at the time of
        %% this re-subscription
        ok = emqx_broker:do_subscribe(Topic, Self, #{})
    end,
    ?assertEqual(ok, erpc:call(Replicant, UnSubSubFun)),
    receive
        %% Can't match route record, since table name =/= record name,
        {mnesia_table_event, {write, {?ROUTE_TAB, Topic, Replicant}, _}} ->
            %% Event is reported before Mnesia writes a record, need to wait again...
            timer:sleep(100),
            ?assert(rpc:call(Replicant, emqx_router, has_route, [Topic, Replicant]))
    after DelayMs * 3 ->
        ct:pal("Received messages: ~p", [process_info(Self, messages)]),
        ct:fail("quick re-subscribe failed to add a route")
    end.

%%

get_mqtt_tcp_port(Node) ->
    {_, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

slowdown_mria_rlog(MnesiaHook, Nodes, DelayMs) ->
    MnesiaHook1 = fun(Tid, CommitData) ->
        spawn(fun() ->
            timer:sleep(DelayMs),
            MnesiaHook(Tid, CommitData)
        end),
        ok
    end,
    register_mria_hook(MnesiaHook1, Nodes).

register_mria_hook(MnesiaHook, Nodes) ->
    [ok, ok] = [
        rpc:call(N, mnesia_hook, register_hook, [post_commit, MnesiaHook])
     || N <- Nodes
    ],
    ok.
