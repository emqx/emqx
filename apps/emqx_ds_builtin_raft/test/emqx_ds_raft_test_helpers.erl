%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_raft_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ON(NODES, BODY),
    emqx_ds_test_helpers:on(NODES, fun() -> BODY end)
).

%%

-spec assert_db_open([node()], emqx_ds:db(), emqx_ds:create_db_opts()) -> _Ok.
assert_db_open(Nodes, DB, Opts) ->
    ?assertEqual(
        [{ok, ok} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_ds, open_db, [DB, Opts])
    ),
    wait_db_bootstrapped(Nodes, DB).

wait_db_bootstrapped(Nodes, DB) ->
    wait_db_bootstrapped(Nodes, DB, infinity, infinity).

wait_db_bootstrapped(Nodes, DB, Timeout, BackInTime) ->
    SRefs = [
        snabbkaffe:subscribe(
            ?match_event(#{
                ?snk_kind := emqx_ds_replshard_bootstrapped,
                ?snk_meta := #{node := Node},
                db := DB,
                shard := Shard
            }),
            1,
            Timeout,
            BackInTime
        )
     || Node <- Nodes,
        Shard <- ?ON(Node, emqx_ds_replication_layer_meta:my_shards(DB))
    ],
    lists:foreach(
        fun({ok, SRef}) ->
            ?assertMatch({ok, [_]}, snabbkaffe:receive_events(SRef))
        end,
        SRefs
    ).

-spec assert_db_stable([node()], emqx_ds:db()) -> _Ok.
assert_db_stable([Node | _], DB) ->
    Shards = ?ON(Node, emqx_ds_replication_layer_meta:shards(DB)),
    ?assertMatch(
        _Leadership = [_ | _],
        ?ON(Node, db_leadership(DB, Shards))
    ).

db_leadership(DB, Shards) ->
    Leadership = [{S, shard_leadership(DB, S)} || S <- Shards],
    Inconsistent = [SL || SL = {_, Leaders} <- Leadership, map_size(Leaders) > 1],
    case Inconsistent of
        [] ->
            Leadership;
        [_ | _] ->
            {error, inconsistent, Inconsistent}
    end.

shard_leadership(DB, Shard) ->
    ReplicaSet = emqx_ds_replication_layer_meta:replica_set(DB, Shard),
    Nodes = [emqx_ds_replication_layer_meta:node(Site) || Site <- ReplicaSet],
    lists:foldl(
        fun({Site, SN}, Acc) -> Acc#{shard_leader(SN, DB, Shard, Site) => SN} end,
        #{},
        lists:zip(ReplicaSet, Nodes)
    ).

shard_leader(Node, DB, Shard, Site) ->
    shard_server_info(Node, DB, Shard, Site, leader).

shard_readiness(Node, DB, Shard, Site) ->
    shard_server_info(Node, DB, Shard, Site, readiness).

shard_server_info(Node, DB, Shard, Site, Info) ->
    ?ON(
        Node,
        emqx_ds_replication_layer_shard:server_info(
            Info,
            emqx_ds_replication_layer_shard:shard_server(DB, Shard, Site)
        )
    ).

-spec db_transitions(emqx_ds:db()) ->
    [{emqx_ds_replication_layer:shard_id(), emqx_ds_replication_layer_meta:transition()}].
db_transitions(DB) ->
    Shards = emqx_ds_replication_layer_meta:shards(DB),
    [
        {S, T}
     || S <- Shards, T <- emqx_ds_replication_layer_meta:replica_set_transitions(DB, S)
    ].

wait_db_transitions_done(DB) ->
    ?retry(1000, 20, ?assertEqual([], db_transitions(DB))).
