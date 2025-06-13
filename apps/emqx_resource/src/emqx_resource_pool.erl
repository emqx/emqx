%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_pool).

-feature(maybe_expr, enable).

-export([
    start/3,
    stop/1,
    health_check_timeout/0,
    health_check_workers/2,
    health_check_workers/3,
    health_check_workers/4,
    health_check_workers_optimistic/3
]).

-include_lib("emqx/include/logger.hrl").
-include("emqx_resource.hrl").

-ifndef(TEST).
-define(HEALTH_CHECK_TIMEOUT, 15000).
-else.
%% make tests faster
-define(HEALTH_CHECK_TIMEOUT, 1000).
-endif.

start(Name, Mod, Options) ->
    case ecpool:start_sup_pool(Name, Mod, Options) of
        {ok, _} ->
            ?SLOG(info, #{msg => "start_ecpool_ok", pool_name => Name}, #{tag => ?TAG}),
            ok;
        {error, already_present} ->
            stop(Name),
            start(Name, Mod, Options);
        {error, {already_started, _Pid}} ->
            stop(Name),
            start(Name, Mod, Options);
        {error, Reason} ->
            NReason = parse_reason(Reason),
            IsDryRun = emqx_resource:is_dry_run(Name),
            ?SLOG(
                ?LOG_LEVEL(IsDryRun),
                #{
                    msg => "start_ecpool_error",
                    resource_id => Name,
                    reason => NReason
                },
                #{tag => ?TAG}
            ),
            {error, {start_pool_failed, Name, NReason}}
    end.

stop(Name) ->
    case ecpool:stop_sup_pool(Name) of
        ok ->
            ?SLOG(info, #{msg => "stop_ecpool_ok", pool_name => Name}, #{tag => ?TAG});
        {error, not_found} ->
            ok;
        {error, Reason} ->
            IsDryRun = emqx_resource:is_dry_run(Name),
            ?SLOG(
                ?LOG_LEVEL(IsDryRun),
                #{
                    msg => "stop_ecpool_failed",
                    resource_id => Name,
                    reason => Reason
                },
                #{tag => ?TAG}
            ),
            error({stop_pool_failed, Name, Reason})
    end.

health_check_timeout() ->
    ?HEALTH_CHECK_TIMEOUT.

health_check_workers(PoolName, CheckFunc) ->
    health_check_workers(PoolName, CheckFunc, ?HEALTH_CHECK_TIMEOUT, _Opts = #{}).

health_check_workers(PoolName, CheckFunc, Timeout) ->
    health_check_workers(PoolName, CheckFunc, Timeout, _Opts = #{}).

health_check_workers(PoolName, CheckFunc, Timeout, Opts) ->
    ReturnValues = maps:get(return_values, Opts, false),
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    DoPerWorker =
        fun(Worker) ->
            maybe
                {ok, Conn} ?= ecpool_worker:client(Worker),
                true ?= erlang:is_process_alive(Conn),
                try
                    ecpool_worker:exec(Worker, CheckFunc, Timeout)
                catch
                    exit:{timeout, _} ->
                        {error, timeout}
                end
            else
                false ->
                    {error, ecpool_worker_dead};
                Error ->
                    Error
            end
        end,
    Results =
        try
            {ok, emqx_utils:pmap(DoPerWorker, Workers, Timeout)}
        catch
            exit:timeout ->
                {error, timeout}
        end,
    case ReturnValues of
        true ->
            Results;
        false ->
            case Results of
                {ok, []} -> false;
                {ok, Rs = [_ | _]} -> lists:all(fun(St) -> St =:= true end, Rs);
                _ -> false
            end
    end.

-doc """
Calls each work serially and stops at the first successful response.  Useful for resources
that want to avoid doing too many requests, such as Kinesis Producer.

`CheckFn` should return `ok` if it's successful, `{halt, Res}` if the check should end
immediately with `Result`, `{error, Reason}` otherwise.
""".
health_check_workers_optimistic(PoolName, CheckFn, Timeout0) ->
    Start = now_ms(),
    Deadline = Start + Timeout0,
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    FoldFn =
        fun(Worker, LastError) ->
            maybe
                Timeout = Deadline - now_ms(),
                true ?= Timeout > 0 orelse {error, deadline},
                {ok, Conn} ?= ecpool_worker:client(Worker),
                true ?= is_process_alive(Conn),
                try ecpool_worker:exec(Worker, CheckFn, Timeout) of
                    ok ->
                        {halt, ok};
                    {halt, Result} ->
                        {halt, Result};
                    Error ->
                        {cont, Error}
                catch
                    exit:{timeout, _} ->
                        {cont, {error, timeout}};
                    exit:timeout ->
                        {cont, {error, timeout}}
                end
            else
                {error, deadline} -> {halt, LastError};
                _ -> {cont, LastError}
            end
        end,
    emqx_utils:foldl_while(FoldFn, {error, no_worker_alive}, Workers).

parse_reason({worker_start_failed, Reason}) ->
    Reason;
parse_reason({worker_exit, Reason}) ->
    Reason;
parse_reason(Reason) ->
    Reason.

now_ms() ->
    erlang:system_time(millisecond).
