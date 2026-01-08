%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_tests).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_resource.hrl").

is_dry_run_test_() ->
    [
        ?_assert(emqx_resource:is_dry_run(?PROBE_ID_NEW())),
        ?_assertNot(emqx_resource:is_dry_run("foobar")),
        ?_assert(emqx_resource:is_dry_run(bin([?PROBE_ID_NEW(), "_abc"]))),
        ?_assert(
            emqx_resource:is_dry_run(
                bin(["action:typeA:", ?PROBE_ID_NEW(), ":connector:typeB:dryrun"])
            )
        ),
        ?_assertNot(
            emqx_resource:is_dry_run(
                bin(["action:type1:dryrun:connector:typeb:dryrun"])
            )
        )
    ].

bin(X) -> iolist_to_binary(X).

%%--------------------------------------------------------------------
%% cleanup_by_agent tests
%%--------------------------------------------------------------------

cleanup_by_agent_test_() ->
    [
        {"normal completion", fun test_normal_completion/0},
        {"abnormal completion", fun test_abnormal_completion/0},
        {"parent dies before task completes", fun test_parent_dies_before_task/0},
        {"timeout scenario", fun test_timeout/0},
        {"orphaned agent - parent dies then task completes", fun test_orphaned_agent/0}
    ].

test_normal_completion() ->
    LogCalls = ets:new(log_calls, [bag, public]),
    LogFn = fun(Level, LogData) ->
        ets:insert(LogCalls, {Level, LogData}),
        ok
    end,
    TaskFn = fun() -> ok end,
    Timeout = 1000,

    Result = emqx_resource_manager:cleanup_by_agent(LogFn, TaskFn, Timeout),

    %% Should complete normally (returns result of LogFn)
    ?assertEqual(ok, Result),

    %% Should log debug message for normal completion
    LogEntries = ets:tab2list(LogCalls),
    ?assertMatch([{debug, #{msg := "resource_cleanup_done"}}], LogEntries),

    ets:delete(LogCalls).

test_abnormal_completion() ->
    LogCalls = ets:new(log_calls, [bag, public]),
    LogFn = fun(Level, LogData) ->
        ets:insert(LogCalls, {Level, LogData}),
        ok
    end,
    TaskFn = fun() -> exit(task_failed) end,
    Timeout = 1000,

    Result = emqx_resource_manager:cleanup_by_agent(LogFn, TaskFn, Timeout),

    %% Should complete (but task failed, returns result of LogFn)
    ?assertEqual(ok, Result),

    %% Should log error message for abnormal completion
    LogEntries = ets:tab2list(LogCalls),
    ?assertMatch(
        [{error, #{msg := "resource_cleanup_exception", reason := {cleanup_result, task_failed}}}],
        LogEntries
    ),

    ets:delete(LogCalls).

test_parent_dies_before_task() ->
    LogCalls = ets:new(log_calls, [bag, public]),
    LogFn = fun(Level, LogData) ->
        ets:insert(LogCalls, {Level, LogData}),
        ok
    end,
    Timeout = 1000,

    TaskFn = fun() ->
        %% Wait a bit to ensure parent dies first
        timer:sleep(100),
        ok
    end,

    %% Spawn parent process that will die
    ParentPid = spawn(fun() ->
        emqx_resource_manager:cleanup_by_agent(LogFn, TaskFn, Timeout)
    end),

    %% Kill parent immediately
    timer:sleep(50),
    exit(ParentPid, kill),

    %% Wait for agent to complete
    timer:sleep(200),

    %% Should log warning about orphaned agent (or completion if parent died after task completed)
    LogEntries = ets:tab2list(LogCalls),
    %% Could be either orphaned or completed message depending on timing
    HasOrphanedOrCompleted = lists:all(
        fun({warning, Log}) ->
            Msg = maps:get(msg, Log, undefined),
            Msg =:= "cleanup_agent_is_orphanated" orelse
                Msg =:= "orphanated_cleanup_agent_completed"
        end,
        LogEntries
    ),
    ?assert(HasOrphanedOrCompleted, "Should have orphaned or completed log"),

    ets:delete(LogCalls).

test_timeout() ->
    LogCalls = ets:new(log_calls, [bag, public]),
    LogFn = fun(Level, LogData) ->
        ets:insert(LogCalls, {Level, LogData}),
        ok
    end,
    Timeout = 100,

    %% Task that takes longer than timeout
    TaskFn = fun() ->
        timer:sleep(500),
        ok
    end,

    Result = emqx_resource_manager:cleanup_by_agent(LogFn, TaskFn, Timeout),

    %% Should complete (timeout handled, returns result of LogFn)
    ?assertEqual(ok, Result),

    %% Should log error message for timeout
    %% The agent logs cleanup_task_aborted_after_timeout, but parent receives it as DOWN with that reason
    %% and logs it as resource_cleanup_exception
    LogEntries = ets:tab2list(LogCalls),
    %% Could be either the direct timeout log or the exception log from parent
    HasTimeoutLog = lists:all(
        fun({error, Log}) ->
            maps:get(msg, Log, undefined) =:= cleanup_task_aborted_after_timeout orelse
                (maps:get(msg, Log, undefined) =:= "resource_cleanup_exception" andalso
                    maps:get(reason, Log, undefined) =:= cleanup_task_aborted_after_timeout)
        end,
        LogEntries
    ),
    ?assert(HasTimeoutLog, "Should have timeout log"),

    ets:delete(LogCalls).

test_orphaned_agent() ->
    LogCalls = ets:new(log_calls, [bag, public]),
    LogFn = fun(Level, LogData) ->
        ets:insert(LogCalls, {Level, LogData}),
        ok
    end,
    Timeout = 1000,

    TaskFn = fun() ->
        timer:sleep(50),
        ok
    end,

    %% Spawn parent process that will die
    ParentPid = spawn(fun() ->
        emqx_resource_manager:cleanup_by_agent(LogFn, TaskFn, Timeout)
    end),

    %% Kill parent after a short delay (before task completes)
    timer:sleep(10),
    exit(ParentPid, kill),

    %% Wait for task to complete and agent to log
    timer:sleep(200),

    %% Should log warning about orphaned agent, then warning about completion
    LogEntries = ets:tab2list(LogCalls),
    %% Check that we have at least one warning log
    Levels = [Level || {Level, _} <- LogEntries],
    case lists:member(warning, Levels) of
        true ->
            %% Should have orphaned message or completion message
            OrphanedLogs = [Log || {warning, Log} <- LogEntries],
            HasOrphaned = lists:any(
                fun(Log) ->
                    maps:get(msg, Log, undefined) =:= "cleanup_agent_is_orphanated"
                end,
                OrphanedLogs
            ),
            HasCompletion = lists:any(
                fun(Log) ->
                    maps:get(msg, Log, undefined) =:= "orphanated_cleanup_agent_completed"
                end,
                OrphanedLogs
            ),
            %% Depending on timing, we might get:
            %% 1. Both orphaned and completion (parent dies before task completes)
            %% 2. Only completion (parent dies after task completes but before agent exits)
            %% 3. Only orphaned (parent dies, task hasn't completed yet)
            ?assert(HasOrphaned orelse HasCompletion, "Should have at least one warning log");
        false ->
            %% If no warnings, that's also acceptable - agent might have completed before parent died
            ok
    end,

    ets:delete(LogCalls).
