%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_promoter).

%% L2 promoter: drains the node-local intake queue in order, promotes each
%% batch into mria with one transaction (delivery + message rows only; the
%% per-device index is appended afterwards by the ETS index owner), deletes
%% the queue entries only after the commit and the append succeeded
%% (commit-then-dequeue), then emits one coalesced trigger broadcast per
%% (product, template) group so pull pools start claiming.

%% The mria commit is the durability point: once committed, a delivery
%% survives any single node crash (dual-core ram_copies) and a promoter or
%% owner crash only delays the derived ETS index append, which is either
%% retried or rebuilt from bcast_msg at owner takeover.
%%
%% A batch whose index append keeps failing is never dropped: mria has the
%% delivery row but the device has no index entry, so dropping it would
%% silently lose an accepted request. Instead the batch is retried a bounded
%% number of times in the worker and then handed back to the intake queue
%% with a doubling backoff, which releases the worker. A permanently
%% unreachable index shard therefore costs one deferred batch per affected
%% batch - not one pinned worker per affected batch, which is what stopped
%% the queue from draining and stalled the fanout in the field.

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

-define(BATCH_MAX_ENTRIES, 50).
%% 5000 keys (5 x bs1000 requests) per batch: large enough to amortize the
%% mnesia tx, small enough that the per-shard append (1250 rows) stays a
%% few-ms gen_server call. 20000 (20 requests) made one append hold the
%% shard for ~250ms, starving admit/claim (API 130 req/s, drain ~1k/s).
-define(BATCH_MAX_KEYS, 5000).
-define(DRAIN_BACKOFF_MS, 10).
-define(RETRY_BACKOFF_MS, 5).
-define(MAX_CONSECUTIVE_FAILURES, 10).
%% In-worker attempts on the SAME batch before it is handed back to the intake
%% queue with a backoff. 10 x ?RETRY_BACKOFF_MS (5ms) covers an owner takeover
%% or a transient shard-call timeout, which is what most append failures are.
-define(APPEND_RETRY_MAX, 10).

%% Drain workers per core: the single-promoter digest rate (~240 req/s at
%% bs=1000) capped the load-phase acceptance (intake queue filled, 429s);
%% N workers take disjoint batches from the intake queue (atomic ets:take)
%% and promote/append them in parallel. The per-device FIFO order across
%% batches becomes the shards' arrival order (within the concurrent
%% window), which is not a tested property; the durability contract is
%% unchanged (the mnesia commit is the durability point).
%%
%% The worker count follows schedulers_online (one per scheduler, matching
%% the other pools where delivery_pool_size=0 means one per scheduler)
%% instead of a hard-coded constant, so a bigger machine gets proportionally
%% more promotion parallelism without a rebuild. The gen_server itself is
%% worker 0, so the number of spawned siblings is one less.
%%
%% The workers poll (take + sleep ?DRAIN_BACKOFF_MS) instead of being woken by
%% the enqueue path, so acceptance never waits on the promoter and a promoter
%% crash cannot drop an accepted request. Two consequences are accepted on
%% purpose: an idle core wakes one process per scheduler every 10ms, and the
%% first promote after an idle period waits up to that same interval. A
%% notification-driven drain would remove both, but it changes the drain
%% protocol (the workers would have to be woken explicitly, and the enqueue
%% path would take a dependency on promoter liveness), so it is not a change to
%% make for the idle-wakeup count alone.
promoter_workers() ->
    max(1, erlang:system_info(schedulers_online)).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% The gen_server is worker 0; the linked siblings run the same loop.
    %% A worker crash kills the link and the supervisor restarts the whole
    %% promoter (simplest safe recovery).
    Workers = [spawn_link(fun() -> worker_loop(0) end) || _ <- lists:seq(2, promoter_workers())],
    erlang:send_after(0, self(), drain),
    {ok, #{failures => 0, pending => undefined, workers => Workers}}.

handle_info(drain, State = #{failures := Failures, pending := Pending}) ->
    %% The batch under retry lives in the state: with the atomic intake
    %% take a failed batch is no longer in the queue, so the retry MUST
    %% re-process the SAME batch (a fresh take would silently lose it).
    %% Waking deferred batches first also keeps them moving when the node has
    %% no other traffic at all.
    _ = emqx_bcast_intake:sweep_deferred(),
    Batch =
        case Pending of
            undefined -> emqx_bcast_intake:take_batch(?BATCH_MAX_ENTRIES, ?BATCH_MAX_KEYS);
            _ -> Pending
        end,
    case Batch of
        [] ->
            erlang:send_after(?DRAIN_BACKOFF_MS, self(), drain),
            {noreply, State#{failures => 0, pending => undefined}};
        _ ->
            case process_batch_guarded(Batch, Failures) of
                {done, F} ->
                    erlang:send_after(0, self(), drain),
                    {noreply, State#{failures => F, pending => undefined}};
                {retry, F} ->
                    erlang:send_after(?RETRY_BACKOFF_MS, self(), drain),
                    {noreply, State#{failures => F, pending => Batch}}
            end
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%% Worker drain loop (no mailbox): take a disjoint batch, promote, repeat.
%% A retry re-processes the SAME batch (see handle_info comment).
worker_loop(Failures) ->
    case emqx_bcast_intake:take_batch(?BATCH_MAX_ENTRIES, ?BATCH_MAX_KEYS) of
        [] ->
            _ = emqx_bcast_intake:sweep_deferred(),
            timer:sleep(?DRAIN_BACKOFF_MS),
            worker_loop(0);
        Batch ->
            worker_process(Batch, Failures)
    end.

worker_process(Batch, Failures) ->
    case process_batch_guarded(Batch, Failures) of
        {done, F} ->
            worker_loop(F);
        {retry, F} ->
            timer:sleep(?RETRY_BACKOFF_MS),
            worker_process(Batch, F)
    end.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.
handle_cast(_Msg, State) ->
    {noreply, State}.
terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Promotion pipeline
%%--------------------------------------------------------------------

%% A crash inside batch processing must not kill the worker: the intake
%% take is atomic (dequeue-then-process), so an uncaught crash would
%% silently lose the whole batch - and the worker link would take the
%% promoter gen_server (and its pending batch) down with it. Contain the
%% crash as a bounded retry of the SAME batch (idempotent:
%% already_promoted + append dedup); once the in-worker budget is spent the
%% batch goes back to the intake queue with a backoff (never dropped: the
%% crash can happen after the mria commit, and a dropped batch would leave
%% committed deliveries with no index entry and nothing to retry them).
process_batch_guarded(Batch, Failures) ->
    try process_batch(Batch, Failures) of
        Result ->
            Result
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "bcast_promoter_batch_crashed",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace,
                batch_size => length(Batch)
            }),
            emqx_bcast_metrics:qos1_promote_error(),
            case Failures >= ?MAX_CONSECUTIVE_FAILURES of
                true ->
                    defer_batch(
                        "bcast_promoter_crash_deferred",
                        [Entry || {_Seq, Entry} <- Batch],
                        {Error, Reason}
                    );
                false ->
                    {retry, Failures + 1}
            end
    end.

%% Entries dropped because their message was deleted after the request was
%% accepted. Nothing durable was written for them and no ledger counter was
%% touched, so only the admission reservation has to be released.
release_deleted_admits([]) ->
    ok;
release_deleted_admits(Entries) ->
    ?SLOG(info, #{
        msg => "bcast_promoter_dropped_deleted_message",
        entries => length(Entries)
    }),
    lists:foreach(
        fun(E) ->
            _ = emqx_bcast_index_owner:release_admit(
                maps:get(product_key, E), maps:get(devices, E)
            )
        end,
        Entries
    ).

process_batch(Batch, Failures) ->
    Entries = [Entry || {_Seq, Entry} <- Batch],
    case emqx_bcast_storage:promote_batch(Entries) of
        {ok, Results} ->
            Promoted = [
                Entry
             || {Result, Entry} <- lists:zip(Results, Entries), Result =:= ok
            ],
            AlreadyPromoted = [
                Entry
             || {Result, Entry} <- lists:zip(Results, Entries), Result =:= already_promoted
            ],
            %% Entries whose message was removed by Delete Message after the
            %% request was accepted. No delivery row and no index entry was
            %% written for them, so they must not be appended and must not
            %% count as wanted. Their admission reservation is released here -
            %% exactly like a dropped batch - or it would linger until the
            %% stale-reservation sweep.
            Deleted = [
                Entry
             || {Result, Entry} <- lists:zip(Results, Entries), Result =:= deleted
            ],
            ok = release_deleted_admits(Deleted),
            %% The batch was already dequeued by the atomic take; the append
            %% completes the promotion. On failure the worker retries the
            %% same batch (idempotent: already_promoted + append dedup).
            Appends = [
                {maps:get(product_key, E), DN, maps:get(delivery_id, E)}
             || E <- Promoted ++ AlreadyPromoted, DN <- maps:get(devices, E)
            ],
            case emqx_bcast_index_owner:append_batch(Appends) of
                ok ->
                    %% Trigger BEFORE counting, and count last: this branch is
                    %% the only place a batch is counted, and a raise from the
                    %% trigger lands in the crash guard, which retries the SAME
                    %% batch - counting first would count those deliveries a
                    %% second time. Nothing after the count can fail, so the
                    %% count happens exactly once per committed batch.
                    %%
                    %% already_promoted entries were committed by an earlier
                    %% run that failed before its append/trigger; re-trigger
                    %% them too so committed deliveries are always claimed.
                    ok = trigger_broadcast(Promoted ++ AlreadyPromoted),
                    %% Durable ledger base: count logical deliveries (one
                    %% per appended device) only after the mria commit AND
                    %% the per-device index append both succeeded. The
                    %% retry path re-enters here with already_promoted
                    %% entries, so every committed device is counted
                    %% exactly once (no double count, no leak).
                    emqx_bcast_metrics:qos1_wanted(length(Appends)),
                    {done, 0};
                {error, _Reason} = Error ->
                    %% Owner takeover, shard overload (call timeout), a shard
                    %% that is dormant or unreachable or any other append
                    %% failure: retry the SAME batch. The retry is idempotent
                    %% (already_promoted + append dedup), and with the atomic
                    %% intake take this is the only safety net against losing
                    %% committed-but-unindexed deliveries.
                    Attempts = Failures + 1,
                    ?SLOG(warning, #{
                        msg => "bcast_promoter_append_failed_retry",
                        result => Error,
                        batch_size => length(Batch),
                        attempts => Attempts,
                        defer_attempts => defer_attempts(Entries)
                    }),
                    case Attempts >= ?APPEND_RETRY_MAX of
                        false ->
                            {retry, Attempts};
                        true ->
                            %% Out of in-worker budget: hand the batch back to
                            %% the intake queue with a backoff and free this
                            %% worker. Dropping it is not an option (see the
                            %% module comment), and holding the worker is what
                            %% wedges the whole drain path once every worker
                            %% holds a permanently failing batch.
                            defer_batch("bcast_promoter_append_deferred", Entries, Error)
                    end
            end;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "bcast_promote_batch_failed",
                reason => Reason,
                batch_size => length(Batch)
            }),
            emqx_bcast_metrics:qos1_promote_error(),
            case Failures >= ?MAX_CONSECUTIVE_FAILURES of
                true ->
                    %% Out of in-worker budget for a promotion that keeps
                    %% aborting: give the batch back to the intake queue with
                    %% a backoff instead of dropping accepted requests.
                    defer_batch("bcast_promoter_promote_deferred", Entries, Reason);
                false ->
                    {retry, Failures + 1}
            end
    end.

%% Hand a batch the worker cannot finish back to the intake queue for a later
%% retry, and free the worker. Used for three faults that share the same
%% requirement - the accepted request must not be dropped:
%%   - the index append keeps failing (the batch IS committed in mria),
%%   - promotion keeps aborting (nothing durable yet, but the request was
%%     already acknowledged to the caller),
%%   - batch processing keeps crashing (it may have committed before crashing).
%% The admission reservation stays held throughout: the request is still in
%% flight, so the quota it consumes is too.
defer_batch(Msg, Entries, Reason) ->
    Count = emqx_bcast_intake:requeue(Entries),
    emqx_bcast_metrics:qos1_deferred(Count),
    ?SLOG(warning, #{
        msg => Msg,
        result => Reason,
        entries => Count,
        defer_attempts => defer_attempts(Entries),
        node => node()
    }),
    {done, 0}.

defer_attempts([Entry | _]) ->
    maps:get(defer_attempts, Entry, 0);
defer_attempts([]) ->
    0.

%% Coalesced trigger: group the promoted devices by (product, template) and
%% emit one broadcast per group instead of one per request, so the 5-node
%% fanout cost scales with batches, not with request rate.
trigger_broadcast(Promoted) ->
    Groups = lists:foldr(
        fun(E, Acc) ->
            Key = {maps:get(product_key, E), maps:get(topic_template, E)},
            case lists:keyfind(Key, 1, Acc) of
                {Key, Devices} ->
                    lists:keyreplace(Key, 1, Acc, {Key, Devices ++ maps:get(devices, E)});
                false ->
                    [{Key, maps:get(devices, E)} | Acc]
            end
        end,
        [],
        Promoted
    ),
    lists:foreach(
        fun({{PK, Tpl}, Devices}) ->
            _ = emqx_bcast_pull_server_pool:qos1_trigger(PK, Devices, Tpl)
        end,
        Groups
    ),
    ok.
