%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% TODO: documentation.
-module(emqx_streams_allocation).

-export([
    new/2,
    add_member/2,
    remove_member/2,
    occupy_resource/3,
    release_resource/3,
    lookup_resource/2,
    lookup_member/2,
    unallocated_resources/1,
    allocate/2,
    rebalance/2
]).

%%

-type alloc(R, M) :: #{
    resources := #{R => {M} | unallocated},
    members := #{M => list(R)}
}.

-type cost_estimate(R, M) :: fun((R, M) -> number()).

-define(COST_LOAD_SHARD, 1.0).
-define(COST_DISRUPTION, 0.5).

%%

-spec new(list(R), list(M)) -> alloc(R, M).
new(Resources, Members) ->
    #{
        resources => maps:from_keys(Resources, unallocated),
        members => maps:from_keys(Members, [])
    }.

%% Idempotent.
add_member(Member, A = #{members := MemberM}) ->
    case MemberM of
        #{Member := _} ->
            A;
        #{} ->
            A#{members := MemberM#{Member => []}}
    end.

%% Idempotent.
remove_member(Member, A = #{resources := ResourceM, members := MemberM}) ->
    case MemberM of
        #{Member := MResources} ->
            A#{
                resources := lists:foldl(fun resm_clear/2, ResourceM, MResources),
                members := maps:remove(Member, MemberM)
            };
        #{} ->
            A
    end.

%% Idempotent.
occupy_resource(Resource, Member, A = #{resources := ResourceM, members := MemberM}) ->
    MemberKnown = maps:is_key(Member, MemberM),
    case MemberKnown andalso ResourceM of
        #{Resource := {Member}} ->
            A;
        #{Resource := {_}} ->
            {error, resource_occupied};
        #{Resource := unallocated} ->
            #{Member := MResources} = MemberM,
            A#{
                resources := resm_set(Resource, Member, ResourceM),
                members := MemberM#{Member := [Resource | MResources]}
            };
        #{} ->
            {error, resource_unknown};
        false ->
            {error, member_unknown}
    end.

%% Idempotent.
release_resource(Resource, Member, A = #{resources := ResourceM, members := MemberM}) ->
    MemberKnown = maps:is_key(Member, MemberM),
    case MemberKnown andalso ResourceM of
        #{Resource := {Member}} ->
            #{Member := MResources} = MemberM,
            A#{
                resources := resm_clear(Resource, ResourceM),
                members := MemberM#{Member := lists:delete(Resource, MResources)}
            };
        #{Resource := {_}} ->
            {error, resource_occupied};
        #{Resource := unallocated} ->
            A;
        #{} ->
            {error, resource_unknown};
        false ->
            {error, member_unknown}
    end.

-spec lookup_resource(R, alloc(R, M)) -> {M} | unallocated | {error, resource_unknown}.
lookup_resource(Resource, #{resources := ResourceM}) ->
    maps:get(Resource, ResourceM, {error, resource_unknown}).

-spec lookup_member(M, alloc(R, M)) -> [R] | {error, member_unknown}.
lookup_member(Member, #{members := MemberM}) ->
    maps:get(Member, MemberM, {error, member_unknown}).

-spec unallocated_resources(alloc(R, _)) -> list(R).
unallocated_resources(#{resources := ResourceM}) ->
    resm_unallocated_set(ResourceM).

-spec allocate([cost_estimate(R, M)], alloc(R, M)) -> [{R, M}].
allocate(CostEstimates, A = #{resources := ResourceM, members := MemberM}) ->
    case maps:size(MemberM) of
        N when N > 0 ->
            Unallocated = resm_unallocated_set(ResourceM),
            allocate_resources(CostEstimates, Unallocated, [], A);
        0 ->
            []
    end.

%% Allocate resources to members.
%% Resources are allocated one-by-one, each member is evaluated for each resource.
%% Quadratic complexity, good but non-optimal allocation.
allocate_resources(CostEstimates, [Resource | Rest], Acc0, A) ->
    Member = allocate_resource(CostEstimates, Resource, A),
    Acc = [{Resource, Member} | Acc0],
    allocate_resources(CostEstimates, Rest, Acc, occupy_resource(Resource, Member, A));
allocate_resources(_CostEstimates, [], Acc, _) ->
    Acc.

%% NOTE: Callers are responsible to make sure `MembersM` has one or more members.
allocate_resource(CostEstimates, Resource, #{members := MemberM}) ->
    MemberCosts = maps:fold(
        fun(M, _, Set) ->
            Cost = estimate_cost(CostEstimates, Resource, M, MemberM),
            ordsets:add_element({Cost, M}, Set)
        end,
        ordsets:new(),
        MemberM
    ),
    [{_CostMin, Member} | _] = MemberCosts,
    Member.

estimate_cost(CostEstimates, Resource, Member, MemberM) ->
    BaseCost = estimate_load_factor(Member, MemberM),
    lists:foldl(fun(CE, Cost) -> Cost + CE(Resource, Member) end, BaseCost, CostEstimates).

estimate_load_factor(Member, Members) ->
    length(maps:get(Member, Members, [])) * ?COST_LOAD_SHARD.

-spec rebalance([cost_estimate(R, M)], alloc(R, M)) -> [{R, _From :: M, _To :: M}].
rebalance(CostEstimates, A = #{resources := ResourceM, members := MemberM}) ->
    case maps:keys(MemberM) of
        Members = [_ | _] ->
            Resources = resm_resource_set(ResourceM),
            A0 = new(Resources, Members),
            rebalance_resources(Resources, A0, [], CostEstimates, A);
        [] ->
            []
    end.

%% Rebalance resources among members.
%% Unallocated resources are not included in the result.
%% Resources are evaluated one-by-one, rebalance is essentially a situation where
%% current allocation is more costly (including "disruption" costs) than blank state
%% reallocation.
%% Quadratic complexity, good but non-optimal allocation.
%% NOTE: Callers are responsible to make sure `A0` has one or more members.
rebalance_resources([Resource | Rest], A0, Acc0, CostEstimates, ACurrent) ->
    CostDisruption = estimate_disruption_fun(ACurrent),
    Member = allocate_resource([CostDisruption | CostEstimates], Resource, A0),
    A1 = occupy_resource(Resource, Member, A0),
    case lookup_resource(Resource, ACurrent) of
        {MCurrent} when MCurrent =/= Member ->
            Acc = [{Resource, MCurrent, Member} | Acc0];
        _ ->
            Acc = Acc0
    end,
    rebalance_resources(Rest, A1, Acc, CostEstimates, ACurrent);
rebalance_resources([], _, Acc, _CostEstimates, _DCurrent) ->
    Acc.

estimate_disruption_fun(A) ->
    fun(R, M) ->
        case lookup_resource(R, A) of
            {M} -> 0;
            {_} -> ?COST_DISRUPTION;
            unallocated -> 0
        end
    end.

resm_resource_set(ResourceM) ->
    maps:fold(fun(R, _, Set) -> ordsets:add_element(R, Set) end, ordsets:new(), ResourceM).

resm_unallocated_set(ResourceM) ->
    maps:fold(
        fun
            (R, unallocated, Set) -> ordsets:add_element(R, Set);
            (_, _, Set) -> Set
        end,
        ordsets:new(),
        ResourceM
    ).

resm_set(Resource, Member, ResourceM) ->
    ResourceM#{Resource := {Member}}.

resm_clear(Resource, ResourceM) ->
    ResourceM#{Resource := unallocated}.

%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

no_members_test() ->
    A = new([1, 2, 3], []),
    ?assertEqual(unallocated, lookup_resource(1, A)),
    ?assertEqual(unallocated, lookup_resource(2, A)),
    ?assertEqual({error, resource_unknown}, lookup_resource(4, A)),
    ?assertEqual([], allocate([], A)),
    ?assertEqual([], rebalance([], A)).

add_remove_member_test() ->
    A0 = new([1, 2, 3], []),
    A1 = add_member(m1, A0),
    Allocations = allocate([], A1),
    A2 = lists:foldl(fun apply_allocation/2, A1, Allocations),
    ?assertEqual({m1}, lookup_resource(1, A2)),
    ?assertEqual({m1}, lookup_resource(2, A2)),
    ?assertEqual({m1}, lookup_resource(3, A2)),
    A3 = remove_member(m1, A2),
    ?assertEqual(unallocated, lookup_resource(1, A3)),
    ?assertEqual(unallocated, lookup_resource(2, A3)).

even_allocation_test() ->
    Rs = lists:seq(1, 14),
    A0 = new(Rs, [ma, mb, mc]),
    Allocations = allocate([], A0),
    A1 = lists:foldl(fun apply_allocation/2, A0, Allocations),
    [?assertMatch({_}, lookup_resource(R, A1)) || R <- Rs],
    ?assertMatch([_, _, _, _, _], lookup_member(ma, A1)),
    ?assertMatch([_, _, _, _, _], lookup_member(mb, A1)),
    ?assertMatch([_, _, _, _], lookup_member(mc, A1)),
    ?assertEqual([], allocate([], A1)).

rebalance_test() ->
    Rs = lists:seq(1, 14),
    A0 = new(Rs, [ma]),
    Allocations = allocate([], A0),
    A1 = lists:foldl(fun apply_allocation/2, A0, Allocations),
    ?assertEqual(Rs, lists:sort(lookup_member(ma, A1))),
    A2 = add_member(mb, A1),
    A3 = add_member(mc, A2),
    %% Offload resources from MA to MB and MC:
    Rebalances1 = rebalance([], A3),
    A4 = lists:foldl(fun apply_rebalance/2, A3, Rebalances1),
    M1Rs = lookup_member(ma, A4),
    M2Rs = lookup_member(mb, A4),
    M3Rs = lookup_member(mc, A4),
    ?assertMatch([_, _, _, _, _], M1Rs),
    ?assertMatch([_, _, _, _, _], M2Rs),
    ?assertMatch([_, _, _, _], M3Rs),
    A5 = add_member(md, A4),
    Rebalances2 = rebalance([], A5),
    %% Minimal number of rebalances:
    ?assertMatch([_, _, _], Rebalances2),
    A6 = lists:foldl(fun apply_rebalance/2, A5, Rebalances2),
    %% First three members have only one shard release each:
    ?assertMatch([_], M1Rs -- lookup_member(ma, A6)),
    ?assertMatch([], lookup_member(ma, A6) -- M1Rs),
    ?assertMatch([_], M2Rs -- lookup_member(mb, A6)),
    ?assertMatch([], lookup_member(mb, A6) -- M2Rs),
    ?assertMatch([_], M3Rs -- lookup_member(mc, A6)),
    ?assertMatch([], lookup_member(mc, A6) -- M3Rs),
    ?assertMatch([_, _, _], lookup_member(md, A6)),
    %% Well-balanced now:
    ?assertEqual([], rebalance([], A6)).

apply_allocation({Resource, Member}, A) ->
    occupy_resource(Resource, Member, A).

apply_rebalance({Resource, MFrom, MTo}, A) ->
    occupy_resource(Resource, MTo, release_resource(Resource, MFrom, A)).

-endif.
