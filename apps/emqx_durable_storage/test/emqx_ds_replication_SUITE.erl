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
    #{
        backend => builtin,
        storage => {emqx_ds_storage_bitfield_lts, #{}},
        n_shards => 1,
        n_sites => 3,
        replication_factor => 3,
        replication_options => #{
            wal_max_size_bytes => 128 * 1024,
            wal_max_batch_size => 1024,
            snapshot_interval => 128
        }
    }.

t_replication_transfers_snapshots(Config) ->
    NMsgs = 4000,
    Nodes = [Node, NodeOffline | _] = ?config(nodes, Config),
    _Specs = [_, SpecOffline | _] = ?config(specs, Config),

    %% Initialize DB on all nodes and wait for it to be online.
    ?assertEqual(
        [{ok, ok} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_ds, open_db, [?DB, opts()])
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
        consume(NodeOffline, ?DB, Shard, ['#'], 0)
    ),
    ?assertEqual(
        sample(40, Messages),
        sample(40, MessagesOffline)
    ),
    ?assertEqual(
        Messages,
        MessagesOffline
    ).

shards(Node, DB) ->
    erpc:call(Node, emqx_ds_replication_layer_meta, shards, [DB]).

shards_online(Node, DB) ->
    erpc:call(Node, emqx_ds_builtin_db_sup, which_shards, [DB]).

fill_storage(Node, DB, NMsgs, Opts) ->
    fill_storage(Node, DB, NMsgs, 0, Opts).

fill_storage(Node, DB, NMsgs, I, Opts = #{p_addgen := PAddGen}) when I < NMsgs ->
    R1 = push_message(Node, DB, I),
    R2 = probably(PAddGen, fun() -> add_generation(Node, DB) end),
    R1 ++ R2 ++ fill_storage(Node, DB, NMsgs, I + 1, Opts);
fill_storage(_Node, _DB, NMsgs, NMsgs, _Opts) ->
    [].

push_message(Node, DB, I) ->
    Topic = emqx_topic:join([<<"topic">>, <<"foo">>, integer_to_binary(I)]),
    {Bytes, _} = rand:bytes_s(120, rand:seed_s(default, I)),
    Message = message(Topic, Bytes, I * 100),
    ok = erpc:call(Node, emqx_ds, store_batch, [DB, [Message], #{sync => true}]),
    [Message].

add_generation(Node, DB) ->
    ok = erpc:call(Node, emqx_ds, add_generation, [DB]),
    [].

message(Topic, Payload, PublishedAt) ->
    #message{
        from = <<?MODULE_STRING>>,
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

consume(Node, DB, Shard, TopicFilter, StartTime) ->
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

init_per_testcase(TCName, Config) ->
    Apps = [
        {emqx_durable_storage, #{
            before_start => fun snabbkaffe:fix_ct_logging/0,
            override_env => [{egress_flush_interval, 1}]
        }}
    ],
    WorkDir = emqx_cth_suite:work_dir(TCName, Config),
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {emqx_ds_replication_SUITE1, #{apps => Apps}},
            {emqx_ds_replication_SUITE2, #{apps => Apps}},
            {emqx_ds_replication_SUITE3, #{apps => Apps}}
        ],
        #{work_dir => WorkDir}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    ok = snabbkaffe:start_trace(),
    [{nodes, Nodes}, {specs, NodeSpecs} | Config].

end_per_testcase(_TCName, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).
