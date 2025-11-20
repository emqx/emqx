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
    free_resource/2,
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

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

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

%% Idempotent.
free_resource(Resource, A) ->
    case lookup_resource(Resource, A) of
        {Member} ->
            release_resource(Resource, Member, A);
        unallocated ->
            A
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
    [{_CostMin, Member} | _] = estimate_member_costs(CostEstimates, Resource, MemberM),
    Member.

estimate_member_costs(CostEstimates, Resource, MemberM) ->
    maps:fold(
        fun(M, _, Set) ->
            Cost = estimate_cost(CostEstimates, Resource, M, MemberM),
            ordsets:add_element({Cost, M}, Set)
        end,
        ordsets:new(),
        MemberM
    ).

estimate_cost(CostEstimates, Resource, Member, MemberM) ->
    BaseCost = estimate_load_factor(Member, MemberM),
    lists:foldl(fun(CE, Cost) -> Cost + CE(Resource, Member) end, BaseCost, CostEstimates).

estimate_load_factor(Member, Members) ->
    length(maps:get(Member, Members, [])) * ?COST_LOAD_SHARD.

-spec rebalance([cost_estimate(R, M)], alloc(R, M)) -> [{R, _From :: M, _To :: M}].
rebalance(CostEstimates, A = #{members := MemberM}) ->
    case maps:size(MemberM) of
        N when N > 0 ->
            ResourceCosts = estimate_resource_costs(CostEstimates, A),
            Resources = [R || {_Cost, R} <- ResourceCosts],
            rebalance_resources(Resources, [], CostEstimates, A);
        0 ->
            []
    end.

%% NOTE: Reverse order, starting from the most costly.
estimate_resource_costs(CostEstimates, #{resources := ResourceM, members := MemberM}) ->
    A0 = new(resm_resource_set(ResourceM), maps:keys(MemberM)),
    {ResourceCosts, _A} = maps:fold(
        fun
            (R, {M}, {Set, Acc}) ->
                Cost = estimate_cost(CostEstimates, R, M, maps:get(members, Acc)),
                {ordsets:add_element({-Cost, R}, Set), occupy_resource(R, M, Acc)};
            (R, _, {Set, Acc}) ->
                {ordsets:add_element({0, R}, Set), Acc}
        end,
        {ordsets:new(), A0},
        ResourceM
    ),
    ResourceCosts.

%% Rebalance resources among members.
%% Unallocated resources are not included in the result.
%% Resources are evaluated one-by-one, rebalance is essentially a situation where
%% current allocation is more costly (including "disruption" costs) than reallocation.
%% Quadratic complexity, good but non-optimal allocation.
%% NOTE: Callers are responsible to make sure `A0` has one or more members.
rebalance_resources([Resource | Rest], Acc0, CostEstimates, A0) ->
    CostDisruption = estimate_disruption_fun(A0),
    case lookup_resource(Resource, A0) of
        {MCurrent} ->
            A1 = release_resource(Resource, MCurrent, A0),
            Member = allocate_resource([CostDisruption | CostEstimates], Resource, A1),
            case Member of
                MCurrent ->
                    rebalance_resources(Rest, Acc0, CostEstimates, A0);
                _Different ->
                    Acc = [{Resource, MCurrent, Member} | Acc0],
                    A = occupy_resource(Resource, Member, A1),
                    rebalance_resources(Rest, Acc, CostEstimates, A)
            end;
        unallocated ->
            rebalance_resources(Rest, Acc0, CostEstimates, A0)
    end;
rebalance_resources([], Acc, _CostEstimates, _DCurrent) ->
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
    ?assertMatch([_, _, _, _, _], lookup_member(ma, A4)),
    ?assertMatch([_, _, _, _, _], lookup_member(mb, A4)),
    ?assertMatch([_, _, _, _], lookup_member(mc, A4)),
    A5 = add_member(md, A4),
    Rebalances2 = rebalance([], A5),
    %% Minimal number of rebalances:
    ?assertMatch([_, _, _], Rebalances2),
    A6 = lists:foldl(fun apply_rebalance/2, A5, Rebalances2),
    ?assertMatch([_, _, _], lookup_member(md, A6)),
    %% Well-balanced now:
    ?assertEqual([], rebalance([], A6)).

rebalance_prop_test() ->
    Opts = [{numtests, 2000}, {to_file, user}],
    ?assert(proper:quickcheck(p_rebalance(), Opts)).

p_rebalance() ->
    ?FORALL(
        A0,
        g_allocation(),
        ?FORALL(
            Rebalances,
            exactly(rebalance([], A0)),
            begin
                A1 = lists:foldl(fun apply_rebalance/2, A0, Rebalances),
                measure(
                    #{"NRebalances" => length(Rebalances), "Imbalance" => imbalance(A0)},
                    imbalance(A1) =< 1 andalso
                        length(pointless_rebalances(Rebalances)) =:= 0
                )
            end
        )
    ).

imbalance(#{members := MemberM}) ->
    MResourceN = [length(Rs) || Rs <- maps:values(MemberM)],
    case MResourceN of
        [] -> 0;
        _ -> lists:max(MResourceN) - lists:min(MResourceN)
    end.

pointless_rebalances(Rebalances) ->
    [
        Rebalance
     || Rebalance = {_R, _F1, T1} <- Rebalances,
        [] =/= [true || {_, F2, _T2} <- Rebalances, T1 =:= F2]
    ].

g_allocation() ->
    ?LET(
        Resources,
        ?SIZED(S, lists:seq(0, S * 2)),
        ?LET(
            Occupation,
            [{R, g_member()} || R <- Resources],
            lists:foldl(
                fun({R, M}, A) ->
                    A1 = free_resource(R, A),
                    A2 = add_member(M, A1),
                    occupy_resource(R, M, A2)
                end,
                new(Resources, []),
                Occupation
            )
        )
    ).

g_member() ->
    %% Skewed:
    frequency([{100 - C, list_to_atom([C])} || C <- lists:seq($A, $Z)]).

apply_allocation({Resource, Member}, A) ->
    occupy_resource(Resource, Member, A).

apply_rebalance({Resource, MFrom, MTo}, A) ->
    occupy_resource(Resource, MTo, release_resource(Resource, MFrom, A)).

measure(NamedSamples, Test) ->
    maps:fold(fun(Name, Sample, Acc) -> measure(Name, Sample, Acc) end, Test, NamedSamples).

-endif.
