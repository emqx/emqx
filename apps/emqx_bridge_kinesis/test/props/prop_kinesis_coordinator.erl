%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(prop_kinesis_coordinator).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

node_assignments() ->
    ?LET(
        {NumNodes, NumShardIds, Seed},
        {non_neg_integer(), non_neg_integer(), integer()},
        begin
            Nodes = make_ids(<<"d">>, NumNodes),
            ShardIds = make_ids(<<"s">>, NumShardIds),
            maps:from_list(distribute(ShardIds, Nodes, rand:seed_s(exsss, Seed)))
        end
    ).

make_ids(Prefix, N) ->
    lists:map(
        fun(M) -> <<Prefix/binary, (integer_to_binary(M))/binary>> end,
        lists:seq(1, N)
    ).

distribute(_ShardIds = [], Nodes, _Rng) ->
    [{Node, []} || Node <- Nodes];
distribute(_ShardIds, _Nodes = [], _Rng) ->
    [];
distribute(ShardIds, [Node], _Rng) ->
    [{Node, ShardIds}];
distribute(ShardIds, [Node | Rest], Rng0) ->
    {NumToTake, Rng} = rand:uniform_s(length(ShardIds), Rng0),
    {Taken, Remaining} = lists:split(NumToTake - 1, ShardIds),
    [{Node, Taken} | distribute(Remaining, Rest, Rng)].

%%--------------------------------------------------------------------
%% Helper fns
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_donation_plan_preserves_shards() ->
    ?FORALL(
        {NumRecipients, DesiredMinPerNode, NodeAssignments},
        {pos_integer(), non_neg_integer(), node_assignments()},
        begin
            ShardIds = lists:sort(lists:concat(maps:values(NodeAssignments))),
            Recipients = make_ids(<<"r">>, NumRecipients),
            Plan = emqx_bridge_kinesis_consumer_coordinator:compute_donation_plan(
                Recipients, DesiredMinPerNode, NodeAssignments
            ),
            FinalAssignments = maps:merge(NodeAssignments, Plan),
            OutputShardIds = lists:sort(lists:concat(maps:values(FinalAssignments))),
            ?WHENFAIL(
                begin
                    ?debugFmt("~n*** Original assignments: ~p", [NodeAssignments]),
                    ?debugFmt("~n*** Plan: ~p", [Plan])
                end,
                OutputShardIds =:= ShardIds
            )
        end
    ).
