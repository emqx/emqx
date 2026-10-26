%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_pg_maintenance).
-moduledoc """
Delete old messages per topic and expired subscriptions.
Only the first running node runs the cleanup. During membership changes,
two nodes can run it at the same time. The deletes are idempotent.
The worker uses its own connection, so cleanup never waits behind appends.
""".
-behaviour(gen_server).
-include_lib("emqx/include/logger.hrl").
-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(NEXT_BATCH_DELAY, 10).
-define(TOPIC_BATCH, 100).
-define(DELETE_BATCH, 1000).
-define(EXPIRE_INTERVAL, 3600000).

-doc "Start the cleanup worker.".
-spec start_link(emqx_omp_pg_config:config()) -> gen_server:start_ret().
start_link(Config) -> gen_server:start_link(?MODULE, Config, []).

init(#{retention := #{interval := Interval}} = Config) ->
    %% A connection failure must not stop the worker.
    process_flag(trap_exit, true),
    _ = erlang:send_after(Interval, self(), cleanup),
    _ = erlang:send_after(?EXPIRE_INTERVAL, self(), expire),
    {ok, #{config => Config, conn => undefined, last_topic => 0}}.

handle_call(_Request, _From, State) -> {reply, {error, unsupported}, State}.
handle_cast(_Request, State) -> {noreply, State}.

handle_info(cleanup, #{config := #{retention := #{interval := Interval}}} = State0) ->
    {Delay, State} =
        case is_leader() of
            true -> run(fun cleanup/2, State0, Interval);
            false -> {Interval, State0}
        end,
    _ = erlang:send_after(Delay, self(), cleanup),
    {noreply, State};
handle_info(expire, State0) ->
    {Delay, State} =
        case is_leader() of
            true -> run(fun expire/2, State0, ?EXPIRE_INTERVAL);
            false -> {?EXPIRE_INTERVAL, State0}
        end,
    _ = erlang:send_after(Delay, self(), expire),
    {noreply, State};
handle_info({'EXIT', Conn, _Reason}, #{conn := Conn} = State) ->
    {noreply, State#{conn := undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{conn := Conn}) ->
    close(Conn).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

is_leader() -> lists:min(emqx:running_nodes()) =:= node().

-doc "Run one cleanup step and reconnect after the configured interval if it fails.".
run(Fun, #{config := Config, conn := Conn0} = State0, Interval) ->
    case connection(Conn0, Config) of
        {ok, Conn} ->
            try
                Fun(Conn, State0#{conn := Conn})
            catch
                Class:Reason ->
                    failed({Class, Reason}),
                    close(Conn),
                    {Interval, State0#{conn := undefined, last_topic := 0}}
            end;
        {error, Reason} ->
            failed(Reason),
            {Interval, State0}
    end.

failed(Reason) ->
    ?SLOG(warning, #{msg => offline_messages_pg_cleanup_failed, reason => Reason}).

-doc "Reuse the current connection or report a failed reconnect without stopping cleanup.".
connection(undefined, Config) ->
    try emqx_omp_pg_store:connect(Config) of
        {ok, Conn} -> {ok, Conn};
        {error, Reason} -> {error, Reason}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end;
connection(Conn, _Config) ->
    {ok, Conn}.

close(undefined) ->
    ok;
close(Conn) ->
    unlink(Conn),
    _ = catch epgsql:close(Conn),
    ok.

-doc "Process one topic batch and schedule the next batch or retention cycle.".
cleanup(Conn, #{config := Config, last_topic := After} = State) ->
    #{namespace := Namespace, retention := #{interval := Interval}} = Config,
    {ok, Topics} = emqx_omp_pg_sql:topics(Conn, Namespace, After, ?TOPIC_BATCH),
    lists:foreach(fun({Id, Next}) -> trim(Conn, Id, Next, Config) end, Topics),
    case Topics of
        [_ | _] when length(Topics) =:= ?TOPIC_BATCH ->
            {Last, _} = lists:last(Topics),
            {?NEXT_BATCH_DELAY, State#{last_topic := Last}};
        _ ->
            {Interval, State#{last_topic := 0}}
    end.

-doc "Combine age, count and consumed offsets to select the prefix eligible for deletion.".
trim(Conn, Id, Next, Config) ->
    #{
        retention := #{max_age := MaxAge, max_messages := MaxMessages},
        new_subscription_start := Start
    } = Config,
    Oldest = erlang:system_time(millisecond) - MaxAge,
    {ok, AgeCutoff} = emqx_omp_pg_sql:age_cutoff(Conn, Id, Next, Oldest),
    %% With `latest`, new subscriptions start at the tip, so acknowledged messages are not needed.
    Cutoffs =
        case Start of
            latest ->
                {ok, Consumed} = emqx_omp_pg_sql:consumed_cutoff(Conn, Id, Next),
                [Consumed];
            earliest ->
                []
        end,
    Cutoff = lists:max([Next - MaxMessages, AgeCutoff | Cutoffs]),
    {ok, _} = emqx_omp_pg_sql:trim(Conn, Id, Cutoff, ?DELETE_BATCH),
    ok.

-doc "Delete expired subscriptions in batches until the current sweep is complete.".
expire(Conn, #{config := #{namespace := Namespace, subscription_expiry := Expiry}} = State) ->
    {ok, Removed} = emqx_omp_pg_sql:expire(Conn, Namespace, Expiry, ?DELETE_BATCH),
    case Removed >= ?DELETE_BATCH of
        true -> {?NEXT_BATCH_DELAY, State};
        false -> {?EXPIRE_INTERVAL, State}
    end.
