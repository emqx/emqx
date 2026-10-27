%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bcast_test_cluster).

-export([wait_ready/2]).

-include_lib("eunit/include/eunit.hrl").

%% App start returns before all nodes agree on the plugin readiness set.
%% Peer probes are cached for three seconds, after which index ownership can
%% move and rebuild shards. Wait for the complete topology to be active and
%% stay unchanged across one ownership poll before starting a test.
wait_ready(Nodes, CoreNodes) ->
    ExpectedNodes = lists:sort(Nodes),
    ExpectedCores = lists:sort(CoreNodes),
    [Coordinator | _] = ExpectedCores,
    Shards = lists:seq(
        0, erpc:call(Coordinator, emqx_bcast_index_owner, shard_count, []) - 1
    ),
    ?assert(
        wait_until(
            fun() -> cluster_ready(ExpectedNodes, ExpectedCores, Shards) end,
            200
        )
    ),
    ct:sleep(1000),
    ?assert(cluster_ready(ExpectedNodes, ExpectedCores, Shards)).

cluster_ready(Nodes, CoreNodes, Shards) ->
    try
        lists:all(
            fun(Node) ->
                lists:sort(erpc:call(Node, emqx_bcast, core_nodes, [])) =:= CoreNodes andalso
                    has_all_plugin_nodes(Node, Nodes)
            end,
            Nodes
        ) andalso
            all_owned_partitions_active(hd(CoreNodes), Nodes, Shards)
    catch
        _:_ -> false
    end.

has_all_plugin_nodes(Node, Expected) ->
    Running = lists:sort(erpc:call(Node, emqx_bcast, plugin_running_nodes, [])),
    lists:all(fun(N) -> lists:member(N, Running) end, Expected).

all_owned_partitions_active(Coordinator, Nodes, Shards) ->
    Owners = erpc:call(Coordinator, fun() ->
        [
            {Shard, emqx_bcast_index_owner:shard_owner(Shard)}
         || Shard <- Shards
        ]
    end),
    case lists:all(fun({_Shard, Owner}) -> lists:member(Owner, Nodes) end, Owners) of
        false ->
            false;
        true ->
            ByOwner = lists:foldl(
                fun({Shard, Owner}, Acc) ->
                    maps:update_with(Owner, fun(L) -> [Shard | L] end, [Shard], Acc)
                end,
                #{},
                Owners
            ),
            lists:all(
                fun({Owner, OwnedShards}) -> owned_partitions_active(Owner, OwnedShards) end,
                maps:to_list(ByOwner)
            )
    end.

owned_partitions_active(Owner, Shards) ->
    erpc:call(
        Owner,
        fun() ->
            lists:all(
                fun(Shard) ->
                    case emqx_bcast_index_owner:local_handle(Shard, {shard_status}, 1000) of
                        {true, _, _} -> true;
                        _ -> false
                    end
                end,
                Shards
            )
        end,
        5000
    ).

wait_until(Fun, Attempts) when Attempts > 0 ->
    case Fun() of
        true ->
            true;
        _ ->
            ct:sleep(100),
            wait_until(Fun, Attempts - 1)
    end;
wait_until(_Fun, 0) ->
    false.
