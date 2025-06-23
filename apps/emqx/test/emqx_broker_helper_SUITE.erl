%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_broker_helper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                override_env => [{boot_modules, [broker]}]
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

t_lookup_subid(_) ->
    ?assertEqual(undefined, emqx_broker_helper:lookup_subid(self())),
    emqx_broker_helper:register_sub(self(), <<"clientid">>),
    ct:sleep(10),
    ?assertEqual(<<"clientid">>, emqx_broker_helper:lookup_subid(self())).

t_lookup_subpid(_) ->
    ?assertEqual(undefined, emqx_broker_helper:lookup_subpid(<<"clientid">>)),
    emqx_broker_helper:register_sub(self(), <<"clientid">>),
    ct:sleep(10),
    ?assertEqual(self(), emqx_broker_helper:lookup_subpid(<<"clientid">>)).

t_register_sub(_) ->
    ok = emqx_broker_helper:register_sub(self(), <<"clientid">>),
    ct:sleep(10),
    ok = emqx_broker_helper:register_sub(self(), <<"clientid">>),
    try emqx_broker_helper:register_sub(self(), <<"clientid2">>) of
        _ -> ct:fail(should_throw_error)
    catch
        error:Reason ->
            ?assertEqual(Reason, subid_conflict)
    end,
    ?assertEqual(self(), emqx_broker_helper:lookup_subpid(<<"clientid">>)).

t_shard_seq(_) ->
    TestTopic = atom_to_list(?FUNCTION_NAME),
    ?assertEqual([], ets:lookup(emqx_subseq, TestTopic)),
    emqx_broker_helper:create_seq(TestTopic),
    ?assertEqual([{TestTopic, 1}], ets:lookup(emqx_subseq, TestTopic)),
    emqx_broker_helper:reclaim_seq(TestTopic),
    ?assertEqual([], ets:lookup(emqx_subseq, TestTopic)).

t_uncovered_func(_) ->
    gen_server:call(emqx_broker_helper, test),
    gen_server:cast(emqx_broker_helper, test),
    emqx_broker_helper ! test.

t_shard_assignment_concurrent(_) ->
    Topic = <<"t/shard/assign">>,
    SCapacity = emqx_broker_helper:shard_capacity(),
    %% Run 15 concurrent workers
    NWorkers = 15,
    %% ...performing 1000 operations each
    NOperations = 1000,
    %% ...with assign-to-unassign ratio per each worker:
    Ratios = [{IWorker, floor(math:sqrt(IWorker))} || IWorker <- lists:seq(1, NWorkers)],
    %% ...which amounts to this ratio of live assignments in the end (lower bound):
    RLive = lists:sum([(NA - NU) / (NA + NU) || {NA, NU} <- Ratios]) / NWorkers,
    Worker = fun
        Worker(WRatio = {NA, NU}, N, Hist) when N < NOperations ->
            case N rem (NA + NU) of
                %% Operation is 'assign':
                X when X < NA ->
                    I = emqx_broker_helper:assign_sub_shard(Topic),
                    NHist = maps:update_with(I, fun(C) -> C + 1 end, 1, Hist);
                %% Operation is 'unassign':
                _ when map_size(Hist) > 0 ->
                    %% Pick shard to unassign pseudo-randomly:
                    R = N rem maps:size(Hist),
                    I = lists:nth(R + 1, maps:keys(Hist)),
                    _ = emqx_broker_helper:unassign_sub_shard(Topic, I),
                    NHist = Hist;
                %% Operation is 'unassign', but there's no shard to unassign:
                _ ->
                    NHist = Hist
            end,
            Worker(WRatio, N + 1, NHist);
        Worker(_WRatio, _, Hist) ->
            exit(Hist)
    end,
    %% Spawn workers:
    Workers = [erlang:spawn_monitor(fun() -> Worker(WRatio, 0, #{}) end) || WRatio <- Ratios],
    %% Collect histograms of #{IShard => NAssignments}:
    Hists = [
        receive
            {'DOWN', MRef, process, Pid, Hist} ->
                #{} = Hist
        end
     || {Pid, MRef} <- Workers
    ],
    %% Aggregate histograms:
    AccHist = lists:foldl(
        fun(Hist, Acc) -> maps:merge_with(fun(_, C, CAcc) -> C + CAcc end, Hist, Acc) end,
        #{},
        Hists
    ),
    %% At most `NWorkers` shards have ever been assigned:
    ?assertNotMatch(
        #{(NWorkers - 1) := _NAssignments},
        AccHist
    ),
    %% Shards are assigned sequentially:
    ?assertEqual(
        %% NOTE
        %% Assertion is non-strict, e.g. it allows [1, 3, 2] sequence of assignments.
        %% Strict one is much harder to construct while the benefit is insignificant.
        lists:seq(0, maps:size(AccHist) - 1),
        lists:sort(maps:keys(AccHist))
    ),
    %% Assigments should have no more than a "small tail" of non-optimal shard assigments,
    %% when compared against an optimal assigment:
    OptimalLastShard = floor(RLive * NWorkers * NOperations / SCapacity),
    ?assertMatch(
        NAssignments when NAssignments < SCapacity * 1.5,
        lists:sum([N || {Shard, N} <- maps:to_list(AccHist), Shard > OptimalLastShard]),
        AccHist
    ).

t_shard_unassignment_concurrent(_) ->
    Topic = <<"t/shard/unassign">>,
    %% Run 15 concurrent workers
    NWorkers = 15,
    %% ...performing 1000 operations each.
    NOperations = 1000,
    %% Which operation to perform at `N`th step.
    FOperation = fun(WI, N, Assignments) ->
        case N =< NOperations of
            true when N rem (WI + 1) > 0 ->
                assign;
            true when N rem (WI + 1) =:= 0 ->
                unassign;
            false when map_size(Assignments) > 0 ->
                unassign;
            false ->
                stop
        end
    end,
    Worker = fun Worker(WI, N, Assignments) ->
        case FOperation(WI, N, Assignments) of
            assign ->
                I = emqx_broker_helper:assign_sub_shard(Topic),
                NAssignments = maps:update_with(I, fun(C) -> C + 1 end, 1, Assignments),
                Worker(WI, N + 1, NAssignments);
            unassign when map_size(Assignments) > 0 ->
                %% Pick shard to unassign pseudo-randomly:
                I = lists:nth((N rem maps:size(Assignments)) + 1, maps:keys(Assignments)),
                _ = emqx_broker_helper:unassign_sub_shard(Topic, I),
                NAssignments = maps:filter(
                    fun(_, C) -> C > 0 end,
                    maps:update_with(I, fun(C) -> C - 1 end, Assignments)
                ),
                Worker(WI, N + 1, NAssignments);
            unassign ->
                Worker(WI, N + 1, Assignments);
            stop ->
                exit(Assignments)
        end
    end,
    %% Spawn workers:
    Workers = [erlang:spawn_monitor(fun() -> Worker(I, 1, #{}) end) || I <- lists:seq(1, NWorkers)],
    %% Collect finished workers:
    [
        receive
            {'DOWN', MRef, process, Pid, Hist} ->
                ?assertEqual(#{}, Hist)
        end
     || {Pid, MRef} <- Workers
    ],
    %% Only one shard should be there afterwards:
    ?assertEqual(
        [{0, 0}],
        emqx_broker_helper:assigned_sub_shards(Topic)
    ).
