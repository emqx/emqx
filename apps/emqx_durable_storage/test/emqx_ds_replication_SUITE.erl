%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_replication_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(DB, testdb).

-define(ON(NODE, BODY),
    erpc:call(NODE, erlang, apply, [fun() -> BODY end, []])
).

opts() ->
    opts(#{}).

opts(Overrides) ->
    maps:merge(
        #{
            backend => builtin,
            %% storage => {emqx_ds_storage_reference, #{}},
            storage => {emqx_ds_storage_bitfield_lts, #{epoch_bits => 10}},
            n_shards => 16,
            n_sites => 1,
            replication_factor => 3,
            replication_options => #{
                wal_max_size_bytes => 64,
                wal_max_batch_size => 1024,
                snapshot_interval => 128
            }
        },
        Overrides
    ).

appspec(emqx_durable_storage) ->
    {emqx_durable_storage, #{
        before_start => fun snabbkaffe:fix_ct_logging/0,
        override_env => [{egress_flush_interval, 1}]
    }}.

t_replication_transfers_snapshots(init, Config) ->
    Apps = [appspec(emqx_durable_storage)],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_replication_transfers_snapshots1, #{apps => Apps}},
            {t_replication_transfers_snapshots2, #{apps => Apps}},
            {t_replication_transfers_snapshots3, #{apps => Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    [{nodes, Nodes}, {specs, NodeSpecs} | Config];
t_replication_transfers_snapshots('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_replication_transfers_snapshots(Config) ->
    NMsgs = 400,
    NClients = 5,
    {Stream, TopicStreams} = emqx_ds_test_helpers:interleaved_topic_messages(
        ?FUNCTION_NAME, NClients, NMsgs
    ),

    Nodes = [Node, NodeOffline | _] = ?config(nodes, Config),
    _Specs = [_, SpecOffline | _] = ?config(specs, Config),
    ?check_trace(
        begin
            %% Initialize DB on all nodes and wait for it to be online.
            Opts = opts(#{n_shards => 1, n_sites => 3}),
            ?assertEqual(
                [{ok, ok} || _ <- Nodes],
                erpc:multicall(Nodes, emqx_ds, open_db, [?DB, Opts])
            ),
            ?retry(
                500,
                10,
                ?assertMatch([[_], [_], [_]], [shards_online(N, ?DB) || N <- Nodes])
            ),

            %% Stop the DB on the "offline" node.
            ok = emqx_cth_cluster:stop_node(NodeOffline),

            %% Fill the storage with messages and few additional generations.
            emqx_ds_test_helpers:apply_stream(?DB, Nodes -- [NodeOffline], Stream),

            %% Restart the node.
            [NodeOffline] = emqx_cth_cluster:restart(SpecOffline),
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{
                    ?snk_kind := dsrepl_snapshot_accepted,
                    ?snk_meta := #{node := NodeOffline}
                })
            ),
            ?assertEqual(
                ok,
                erpc:call(NodeOffline, emqx_ds, open_db, [?DB, opts()])
            ),

            %% Trigger storage operation and wait the replica to be restored.
            _ = add_generation(Node, ?DB),
            ?assertMatch(
                {ok, _},
                snabbkaffe:receive_events(SRef)
            ),

            %% Wait until any pending replication activities are finished (e.g. Raft log entries).
            ok = timer:sleep(3_000),

            %% Check that the DB has been restored:
            emqx_ds_test_helpers:verify_stream_effects(?DB, ?FUNCTION_NAME, Nodes, TopicStreams)
        end,
        []
    ).

t_rebalance(init, Config) ->
    Apps = [appspec(emqx_durable_storage)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_rebalance1, #{apps => Apps}},
            {t_rebalance2, #{apps => Apps}},
            {t_rebalance3, #{apps => Apps}},
            {t_rebalance4, #{apps => Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    [{nodes, Nodes} | Config];
t_rebalance('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

%% This testcase verifies that the storage rebalancing works correctly:
%% 1. Join/leave operations are applied successfully.
%% 2. Message data survives the rebalancing.
%% 3. Shard cluster membership converges to the target replica allocation.
%% 4. Replication factor is respected.
t_rebalance(Config) ->
    NMsgs = 50,
    NClients = 5,
    {Stream0, TopicStreams} = emqx_ds_test_helpers:interleaved_topic_messages(
        ?FUNCTION_NAME, NClients, NMsgs
    ),
    Nodes = [N1, N2 | _] = ?config(nodes, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% 1. Initialize DB on the first node.
            Opts = opts(#{n_shards => 16, n_sites => 1, replication_factor => 3}),
            ?assertEqual(ok, ?ON(N1, emqx_ds:open_db(?DB, Opts))),
            ?assertMatch(Shards when length(Shards) == 16, shards_online(N1, ?DB)),

            %% 1.1 Open DB on the rest of the nodes:
            [
                ?assertEqual(ok, ?ON(Node, emqx_ds:open_db(?DB, Opts)))
             || Node <- Nodes
            ],

            Sites = [S1, S2 | _] = [ds_repl_meta(N, this_site) || N <- Nodes],
            ct:pal("Sites: ~p~n", [Sites]),

            Sequence = [
                %% Join the second site to the DB replication sites:
                {N1, join_db_site, S2},
                %% Should be a no-op:
                {N2, join_db_site, S2},
                %% Now join the rest of the sites:
                {N2, assign_db_sites, Sites}
            ],
            Stream1 = emqx_utils_stream:interleave(
                [
                    {50, Stream0},
                    emqx_utils_stream:const(add_generation)
                ],
                false
            ),
            Stream = emqx_utils_stream:interleave(
                [
                    {50, Stream0},
                    emqx_utils_stream:list(Sequence)
                ],
                true
            ),

            %% 1.2 Verify that all nodes have the same view of metadata storage:
            [
                ?defer_assert(
                    ?assertEqual(
                        [S1],
                        ?ON(Node, emqx_ds_replication_layer_meta:db_sites(?DB)),
                        #{
                            msg => "Initially, only S1 should be responsible for all shards",
                            node => Node
                        }
                    )
                )
             || Node <- Nodes
            ],

            %% 2. Start filling the storage:
            emqx_ds_test_helpers:apply_stream(?DB, Nodes, Stream),
            timer:sleep(5000),
            emqx_ds_test_helpers:verify_stream_effects(?DB, ?FUNCTION_NAME, Nodes, TopicStreams),
            [
                ?defer_assert(
                    ?assertEqual(
                        16 * 3 div length(Nodes),
                        n_shards_online(Node, ?DB),
                        "Each node is now responsible for 3/4 of the shards"
                    )
                )
             || Node <- Nodes
            ],

            %% Verify that the set of shard servers matches the target allocation.
            Allocation = [ds_repl_meta(N, my_shards, [?DB]) || N <- Nodes],
            ShardServers = [
                shard_server_info(N, ?DB, Shard, Site, readiness)
             || {N, Site, Shards} <- lists:zip3(Nodes, Sites, Allocation),
                Shard <- Shards
            ],
            ?assert(
                lists:all(fun({_Server, Status}) -> Status == ready end, ShardServers),
                ShardServers
            ),

            %% Scale down the cluster by removing the first node.
            ?assertMatch({ok, _}, ds_repl_meta(N1, leave_db_site, [?DB, S1])),
            ct:pal("Transitions (~p -> ~p): ~p~n", [
                Sites, tl(Sites), emqx_ds_test_helpers:transitions(N1, ?DB)
            ]),
            ?retry(1000, 10, ?assertEqual([], emqx_ds_test_helpers:transitions(N2, ?DB))),

            %% Verify that at the end each node is now responsible for each shard.
            ?defer_assert(
                ?assertEqual(
                    [0, 16, 16, 16],
                    [n_shards_online(N, ?DB) || N <- Nodes]
                )
            ),

            %% Verify that the messages are once again preserved after the rebalance:
            emqx_ds_test_helpers:verify_stream_effects(?DB, ?FUNCTION_NAME, Nodes, TopicStreams)
        end,
        []
    ).

t_join_leave_errors(init, Config) ->
    Apps = [appspec(emqx_durable_storage)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_join_leave_errors1, #{apps => Apps}},
            {t_join_leave_errors2, #{apps => Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    [{nodes, Nodes} | Config];
t_join_leave_errors('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_join_leave_errors(Config) ->
    %% This testcase verifies that logical errors arising during handling of
    %% join/leave operations are reported correctly.

    [N1, N2] = ?config(nodes, Config),

    Opts = opts(#{n_shards => 16, n_sites => 1, replication_factor => 3}),
    ?assertEqual(ok, erpc:call(N1, emqx_ds, open_db, [?DB, Opts])),
    ?assertEqual(ok, erpc:call(N2, emqx_ds, open_db, [?DB, Opts])),

    [S1, S2] = [ds_repl_meta(N, this_site) || N <- [N1, N2]],

    ?assertEqual([S1], ds_repl_meta(N1, db_sites, [?DB])),

    %% Attempts to join a nonexistent DB / site.
    ?assertEqual(
        {error, {nonexistent_db, boo}},
        ds_repl_meta(N1, join_db_site, [_DB = boo, S1])
    ),
    ?assertEqual(
        {error, {nonexistent_sites, [<<"NO-MANS-SITE">>]}},
        ds_repl_meta(N1, join_db_site, [?DB, <<"NO-MANS-SITE">>])
    ),
    %% NOTE: Leaving a non-existent site is not an error.
    ?assertEqual(
        {ok, unchanged},
        ds_repl_meta(N1, leave_db_site, [?DB, <<"NO-MANS-SITE">>])
    ),

    %% Should be no-op.
    ?assertEqual({ok, unchanged}, ds_repl_meta(N1, join_db_site, [?DB, S1])),
    ?assertEqual([], emqx_ds_test_helpers:transitions(N1, ?DB)),

    %% Impossible to leave the last site.
    ?assertEqual(
        {error, {too_few_sites, []}},
        ds_repl_meta(N1, leave_db_site, [?DB, S1])
    ),

    %% "Move" the DB to the other node.
    ?assertMatch({ok, _}, ds_repl_meta(N1, join_db_site, [?DB, S2])),
    ?assertMatch({ok, _}, ds_repl_meta(N2, leave_db_site, [?DB, S1])),
    ?assertMatch([_ | _], emqx_ds_test_helpers:transitions(N1, ?DB)),
    ?retry(1000, 10, ?assertEqual([], emqx_ds_test_helpers:transitions(N1, ?DB))),

    %% Should be no-op.
    ?assertMatch({ok, _}, ds_repl_meta(N2, leave_db_site, [?DB, S1])),
    ?assertEqual([], emqx_ds_test_helpers:transitions(N1, ?DB)).

t_rebalance_chaotic_converges(init, Config) ->
    Apps = [appspec(emqx_durable_storage)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_rebalance_chaotic_converges1, #{apps => Apps}},
            {t_rebalance_chaotic_converges2, #{apps => Apps}},
            {t_rebalance_chaotic_converges3, #{apps => Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    [{nodes, Nodes} | Config];
t_rebalance_chaotic_converges('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_rebalance_chaotic_converges(Config) ->
    %% This testcase verifies that even a very chaotic sequence of join/leave
    %% operations will still be handled consistently, and that the shard
    %% allocation will converge to the target state.

    NMsgs = 500,
    Nodes = [N1, N2, N3] = ?config(nodes, Config),

    NClients = 5,
    {Stream0, TopicStreams} = emqx_ds_test_helpers:interleaved_topic_messages(
        ?FUNCTION_NAME, NClients, NMsgs
    ),

    ?check_trace(
        #{},
        begin
            %% Initialize DB on first two nodes.
            Opts = opts(#{n_shards => 16, n_sites => 2, replication_factor => 3}),

            ?assertEqual(
                [{ok, ok}, {ok, ok}],
                erpc:multicall([N1, N2], emqx_ds, open_db, [?DB, Opts])
            ),

            %% Open DB on the last node.
            ?assertEqual(
                ok,
                erpc:call(N3, emqx_ds, open_db, [?DB, Opts])
            ),

            %% Find out which sites there are.
            Sites = [S1, S2, S3] = [ds_repl_meta(N, this_site) || N <- Nodes],
            ct:pal("Sites: ~p~n", [Sites]),

            Sequence = [
                {N1, join_db_site, S3},
                {N2, leave_db_site, S2},
                {N3, leave_db_site, S1},
                {N1, join_db_site, S2},
                {N2, join_db_site, S1},
                {N3, leave_db_site, S3},
                {N1, leave_db_site, S1},
                {N2, join_db_site, S3}
            ],

            %% Interleaved list of events:
            Stream = emqx_utils_stream:interleave(
                [
                    {50, Stream0},
                    emqx_utils_stream:list(Sequence)
                ],
                true
            ),

            ?retry(500, 10, ?assertEqual([16, 16], [n_shards_online(N, ?DB) || N <- [N1, N2]])),
            ?assertEqual(
                lists:sort([S1, S2]),
                ds_repl_meta(N1, db_sites, [?DB]),
                "Initially, the DB is assigned to [S1, S2]"
            ),

            emqx_ds_test_helpers:apply_stream(?DB, Nodes, Stream),

            %% Wait for the last transition to complete.
            ?retry(500, 20, ?assertEqual([], emqx_ds_test_helpers:transitions(N1, ?DB))),

            ?defer_assert(
                ?assertEqual(
                    lists:sort([S2, S3]),
                    ds_repl_meta(N1, db_sites, [?DB])
                )
            ),

            %% Wait until the LTS timestamp is updated:
            timer:sleep(5000),

            %% Check that all messages are still there.
            emqx_ds_test_helpers:verify_stream_effects(?DB, ?FUNCTION_NAME, Nodes, TopicStreams)
        end,
        []
    ).

t_rebalance_offline_restarts(init, Config) ->
    Apps = [appspec(emqx_durable_storage)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_rebalance_offline_restarts1, #{apps => Apps}},
            {t_rebalance_offline_restarts2, #{apps => Apps}},
            {t_rebalance_offline_restarts3, #{apps => Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    [{nodes, Nodes}, {nodespecs, Specs} | Config];
t_rebalance_offline_restarts('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_rebalance_offline_restarts(Config) ->
    %% This testcase verifies that rebalancing progresses if nodes restart or
    %% go offline and never come back.

    Nodes = [N1, N2, N3] = ?config(nodes, Config),
    _Specs = [NS1, NS2, _] = ?config(nodespecs, Config),

    %% Initialize DB on all 3 nodes.
    Opts = opts(#{n_shards => 8, n_sites => 3, replication_factor => 3}),
    ?assertEqual(
        [{ok, ok} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_ds, open_db, [?DB, Opts])
    ),
    ?retry(
        1000,
        5,
        ?assertEqual([8 || _ <- Nodes], [n_shards_online(N, ?DB) || N <- Nodes])
    ),

    %% Find out which sites are there.
    Sites = [S1, S2, S3] = [ds_repl_meta(N, this_site) || N <- Nodes],
    ct:pal("Sites: ~p~n", [Sites]),

    %% Shut down N3 and then remove it from the DB.
    ok = emqx_cth_cluster:stop_node(N3),
    ?assertMatch({ok, _}, ds_repl_meta(N1, leave_db_site, [?DB, S3])),
    Transitions = emqx_ds_test_helpers:transitions(N1, ?DB),
    ct:pal("Transitions: ~p~n", [Transitions]),

    %% Wait until at least one transition completes.
    ?block_until(#{?snk_kind := dsrepl_shard_transition_end}),

    %% Restart N1 and N2.
    [N1] = emqx_cth_cluster:restart(NS1),
    [N2] = emqx_cth_cluster:restart(NS2),
    ?assertEqual(
        [{ok, ok}, {ok, ok}],
        erpc:multicall([N1, N2], emqx_ds, open_db, [?DB, Opts])
    ),

    %% Target state should still be reached eventually.
    ?retry(1000, 20, ?assertEqual([], emqx_ds_test_helpers:transitions(N1, ?DB))),
    ?assertEqual(lists:sort([S1, S2]), ds_repl_meta(N1, db_sites, [?DB])).

%%

shard_server_info(Node, DB, Shard, Site, Info) ->
    Server = shard_server(Node, DB, Shard, Site),
    {Server, ds_repl_shard(Node, server_info, [Info, Server])}.

shard_server(Node, DB, Shard, Site) ->
    ds_repl_shard(Node, shard_server, [DB, Shard, Site]).

ds_repl_meta(Node, Fun) ->
    ds_repl_meta(Node, Fun, []).

ds_repl_meta(Node, Fun, Args) ->
    try
        erpc:call(Node, emqx_ds_replication_layer_meta, Fun, Args)
    catch
        EC:Err:Stack ->
            ct:pal("emqx_ds_replication_layer_meta:~p(~p) @~p failed:~n~p:~p~nStack: ~p", [
                Fun, Args, Node, EC, Err, Stack
            ]),
            error(meta_op_failed)
    end.

ds_repl_shard(Node, Fun, Args) ->
    erpc:call(Node, emqx_ds_replication_layer_shard, Fun, Args).

shards(Node, DB) ->
    erpc:call(Node, emqx_ds_replication_layer_meta, shards, [DB]).

shards_online(Node, DB) ->
    erpc:call(Node, emqx_ds_builtin_db_sup, which_shards, [DB]).

n_shards_online(Node, DB) ->
    length(shards_online(Node, DB)).

add_generation(Node, DB) ->
    ok = erpc:call(Node, emqx_ds, add_generation, [DB]),
    [].

message(ClientId, Topic, Payload, PublishedAt) ->
    #message{
        from = ClientId,
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

compare_message(M1, M2) ->
    {M1#message.from, M1#message.timestamp} < {M2#message.from, M2#message.timestamp}.

consume(Node, DB, TopicFilter, StartTime) ->
    erpc:call(Node, emqx_ds_test_helpers, consume, [DB, TopicFilter, StartTime]).

consume_shard(Node, DB, Shard, TopicFilter, StartTime) ->
    erpc:call(Node, emqx_ds_test_helpers, storage_consume, [{DB, Shard}, TopicFilter, StartTime]).

probably(P, Fun) ->
    case rand:uniform() of
        X when X < P -> Fun();
        _ -> []
    end.

sample(N, List) ->
    L = length(List),
    case L =< N of
        true ->
            L;
        false ->
            H = N div 2,
            Filler = integer_to_list(L - N) ++ " more",
            lists:sublist(List, H) ++ [Filler] ++ lists:sublist(List, L - H, L)
    end.

%%

suite() -> [{timetrap, {seconds, 60}}].

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TCName, Config0) ->
    Config = emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, Config0),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(TCName, Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config).
