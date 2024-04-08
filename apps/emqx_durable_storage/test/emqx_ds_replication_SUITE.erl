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
-include_lib("snabbkaffe/include/test_macros.hrl").

-define(DB, testdb).

opts() ->
    opts(#{}).

opts(Overrides) ->
    maps:merge(
        #{
            backend => builtin,
            storage => {emqx_ds_storage_bitfield_lts, #{}},
            n_shards => 16,
            n_sites => 1,
            replication_factor => 3,
            replication_options => #{
                wal_max_size_bytes => 64 * 1024,
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
    NMsgs = 4000,
    Nodes = [Node, NodeOffline | _] = ?config(nodes, Config),
    _Specs = [_, SpecOffline | _] = ?config(specs, Config),

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
    Messages = fill_storage(Node, ?DB, NMsgs, #{p_addgen => 0.01}),

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

    %% Check that the DB has been restored.
    Shard = hd(shards(NodeOffline, ?DB)),
    MessagesOffline = lists:keysort(
        #message.timestamp,
        consume_shard(NodeOffline, ?DB, Shard, ['#'], 0)
    ),
    ?assertEqual(
        sample(40, Messages),
        sample(40, MessagesOffline)
    ),
    ?assertEqual(
        Messages,
        MessagesOffline
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

t_rebalance(Config) ->
    NMsgs = 800,
    NClients = 5,
    Nodes = [N1, N2, N3, N4] = ?config(nodes, Config),

    %% Initialize DB on the first node.
    Opts = opts(#{n_shards => 16, n_sites => 1, replication_factor => 3}),
    ?assertEqual(ok, erpc:call(N1, emqx_ds, open_db, [?DB, Opts])),
    ?assertMatch(
        Shards when length(Shards) == 16,
        shards_online(N1, ?DB)
    ),

    %% Open DB on the rest of the nodes.
    ?assertEqual(
        [{ok, ok} || _ <- [N2, N3, N4]],
        erpc:multicall([N2, N3, N4], emqx_ds, open_db, [?DB, Opts])
    ),

    Sites = [S1, S2 | _Rest] = [ds_repl_meta(N, this_site) || N <- Nodes],
    ct:pal("Sites: ~p~n", [Sites]),

    %% Only N1 should be responsible for all shards initially.
    ?assertEqual(
        [[S1] || _ <- Nodes],
        [ds_repl_meta(N, db_sites, [?DB]) || N <- Nodes]
    ),

    %% Fill the storage with messages and few additional generations.
    %% This will force shards to trigger snapshot transfers during rebalance.
    ClientMessages = emqx_utils:pmap(
        fun(CID) ->
            N = lists:nth(1 + (CID rem length(Nodes)), Nodes),
            fill_storage(N, ?DB, NMsgs, #{client_id => integer_to_binary(CID)})
        end,
        lists:seq(1, NClients),
        infinity
    ),
    Messages1 = lists:sort(fun compare_message/2, lists:append(ClientMessages)),

    %% Join the second site to the DB replication sites.
    ?assertEqual(ok, ds_repl_meta(N1, join_db_site, [?DB, S2])),
    %% Should be no-op.
    ?assertEqual(ok, ds_repl_meta(N2, join_db_site, [?DB, S2])),
    ct:pal("Transitions (~p -> ~p): ~p~n", [[S1], [S1, S2], transitions(N1, ?DB)]),

    %% Fill in some more messages *during* the rebalance.
    MessagesRB1 = fill_storage(N4, ?DB, NMsgs, #{client_id => <<"RB1">>}),

    ?retry(1000, 10, ?assertEqual([], transitions(N1, ?DB))),

    %% Now join the rest of the sites.
    ?assertEqual(ok, ds_repl_meta(N2, assign_db_sites, [?DB, Sites])),
    ct:pal("Transitions (~p -> ~p): ~p~n", [[S1, S2], Sites, transitions(N1, ?DB)]),

    %% Fill in some more messages *during* the rebalance.
    MessagesRB2 = fill_storage(N4, ?DB, NMsgs, #{client_id => <<"RB2">>}),

    ?retry(1000, 10, ?assertEqual([], transitions(N2, ?DB))),

    %% Verify that each node is now responsible for 3/4 of the shards.
    ?assertEqual(
        [(16 * 3) div length(Nodes) || _ <- Nodes],
        [n_shards_online(N, ?DB) || N <- Nodes]
    ),

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

    %% Verify that the messages are preserved after the rebalance.
    Messages = Messages1 ++ MessagesRB1 ++ MessagesRB2,
    MessagesN4 = lists:sort(fun compare_message/2, consume(N4, ?DB, ['#'], 0)),
    ?assertEqual(sample(20, Messages), sample(20, MessagesN4)),
    ?assertEqual(Messages, MessagesN4),

    %% Scale down the cluster by removing the first node.
    ?assertEqual(ok, ds_repl_meta(N1, leave_db_site, [?DB, S1])),
    ct:pal("Transitions (~p -> ~p): ~p~n", [Sites, tl(Sites), transitions(N1, ?DB)]),

    ?retry(1000, 10, ?assertEqual([], transitions(N2, ?DB))),

    %% Verify that each node is now responsible for each shard.
    ?assertEqual(
        [0, 16, 16, 16],
        [n_shards_online(N, ?DB) || N <- Nodes]
    ),

    %% Verify that the messages are once again preserved after the rebalance.
    MessagesN3 = lists:sort(fun compare_message/2, consume(N3, ?DB, ['#'], 0)),
    ?assertEqual(sample(20, Messages), sample(20, MessagesN3)),
    ?assertEqual(Messages, MessagesN3).

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
        ok,
        ds_repl_meta(N1, leave_db_site, [?DB, <<"NO-MANS-SITE">>])
    ),

    %% Should be no-op.
    ?assertEqual(ok, ds_repl_meta(N1, join_db_site, [?DB, S1])),
    ?assertEqual([], transitions(N1, ?DB)),

    %% Impossible to leave the last site.
    ?assertEqual(
        {error, {too_few_sites, []}},
        ds_repl_meta(N1, leave_db_site, [?DB, S1])
    ),

    %% "Move" the DB to the other node.
    ?assertEqual(ok, ds_repl_meta(N1, join_db_site, [?DB, S2])),
    ?assertEqual(ok, ds_repl_meta(N2, leave_db_site, [?DB, S1])),
    ?assertMatch([_ | _], transitions(N1, ?DB)),
    ?retry(1000, 10, ?assertEqual([], transitions(N1, ?DB))),

    %% Should be no-op.
    ?assertEqual(ok, ds_repl_meta(N2, leave_db_site, [?DB, S1])),
    ?assertEqual([], transitions(N1, ?DB)).

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
    NMsgs = 500,
    Nodes = [N1, N2, N3] = ?config(nodes, Config),

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

    %% Initially, the DB is assigned to [S1, S2].
    ?retry(500, 10, ?assertEqual([16, 16], [n_shards_online(N, ?DB) || N <- [N1, N2]])),
    ?assertEqual(
        lists:sort([S1, S2]),
        ds_repl_meta(N1, db_sites, [?DB])
    ),

    %% Fill the storage with messages and few additional generations.
    Messages0 = lists:append([
        fill_storage(N1, ?DB, NMsgs, #{client_id => <<"C1">>}),
        fill_storage(N2, ?DB, NMsgs, #{client_id => <<"C2">>}),
        fill_storage(N3, ?DB, NMsgs, #{client_id => <<"C3">>})
    ]),

    %% Construct a chaotic transition sequence that changes assignment to [S2, S3].
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

    %% Apply the sequence while also filling the storage with messages.
    TransitionMessages = lists:map(
        fun({N, Transition, Site}) ->
            %% Apply the transition.
            ?assertEqual(ok, ds_repl_meta(N, Transition, [?DB, Site])),
            %% Give some time for at least one transition to complete.
            Transitions = transitions(N, ?DB),
            ct:pal("Transitions after ~p: ~p", [N, Transitions]),
            ?retry(200, 10, ?assertNotEqual(Transitions, transitions(N, ?DB))),
            %% Fill the storage with messages.
            CID = integer_to_binary(erlang:system_time()),
            fill_storage(N, ?DB, NMsgs, #{client_id => CID})
        end,
        Sequence
    ),

    %% Wait for the last transition to complete.
    ?retry(500, 20, ?assertEqual([], transitions(N1, ?DB))),

    ?assertEqual(
        lists:sort([S2, S3]),
        ds_repl_meta(N1, db_sites, [?DB])
    ),

    %% Check that all messages are still there.
    Messages = lists:append(TransitionMessages) ++ Messages0,
    MessagesDB = lists:sort(fun compare_message/2, consume(N1, ?DB, ['#'], 0)),
    ?assertEqual(sample(20, Messages), sample(20, MessagesDB)),
    ?assertEqual(Messages, MessagesDB).

%%

shard_server_info(Node, DB, Shard, Site, Info) ->
    Server = shard_server(Node, DB, Shard, Site),
    {Server, ds_repl_shard(Node, server_info, [Info, Server])}.

shard_server(Node, DB, Shard, Site) ->
    ds_repl_shard(Node, shard_server, [DB, Shard, Site]).

ds_repl_meta(Node, Fun) ->
    ds_repl_meta(Node, Fun, []).

ds_repl_meta(Node, Fun, Args) ->
    erpc:call(Node, emqx_ds_replication_layer_meta, Fun, Args).

ds_repl_shard(Node, Fun, Args) ->
    erpc:call(Node, emqx_ds_replication_layer_shard, Fun, Args).

transitions(Node, DB) ->
    Shards = shards(Node, DB),
    [{S, T} || S <- Shards, T <- ds_repl_meta(Node, replica_set_transitions, [DB, S])].

shards(Node, DB) ->
    erpc:call(Node, emqx_ds_replication_layer_meta, shards, [DB]).

shards_online(Node, DB) ->
    erpc:call(Node, emqx_ds_builtin_db_sup, which_shards, [DB]).

n_shards_online(Node, DB) ->
    length(shards_online(Node, DB)).

fill_storage(Node, DB, NMsgs, Opts) ->
    fill_storage(Node, DB, NMsgs, 0, Opts).

fill_storage(Node, DB, NMsgs, I, Opts) when I < NMsgs ->
    PAddGen = maps:get(p_addgen, Opts, 0.001),
    R1 = push_message(Node, DB, I, Opts),
    R2 = probably(PAddGen, fun() -> add_generation(Node, DB) end),
    R1 ++ R2 ++ fill_storage(Node, DB, NMsgs, I + 1, Opts);
fill_storage(_Node, _DB, NMsgs, NMsgs, _Opts) ->
    [].

push_message(Node, DB, I, Opts) ->
    Topic = emqx_topic:join([<<"topic">>, <<"foo">>, integer_to_binary(I)]),
    {Bytes, _} = rand:bytes_s(120, rand:seed_s(default, I)),
    ClientId = maps:get(client_id, Opts, <<?MODULE_STRING>>),
    Message = message(ClientId, Topic, Bytes, I * 100),
    ok = erpc:call(Node, emqx_ds, store_batch, [DB, [Message], #{sync => true}]),
    [Message].

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
    H = N div 2,
    Filler = integer_to_list(L - N) ++ " more",
    lists:sublist(List, H) ++ [Filler] ++ lists:sublist(List, L - H, L).

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
