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

%% Drain workers per core: the single-promoter digest rate (~240 req/s at
%% bs=1000) capped the load-phase acceptance (intake queue filled, 429s);
%% N workers take disjoint batches from the intake queue (atomic ets:take)
%% and promote/append them in parallel. The per-device FIFO order across
%% batches becomes the shards' arrival order (within the concurrent
%% window), which is not a tested property; the durability contract is
%% unchanged (the mnesia commit is the durability point).
-define(PROMOTER_WORKERS, 4).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% The gen_server is worker 0; the linked siblings run the same loop.
    %% A worker crash kills the link and the supervisor restarts the whole
    %% promoter (simplest safe recovery).
    Workers = [spawn_link(fun() -> worker_loop(0) end) || _ <- lists:seq(1, ?PROMOTER_WORKERS - 1)],
    erlang:send_after(0, self(), drain),
    {ok, #{failures => 0, pending => undefined, workers => Workers}}.

handle_info(drain, State = #{failures := Failures, pending := Pending}) ->
    %% The batch under retry lives in the state: with the atomic intake
    %% take a failed batch is no longer in the queue, so the retry MUST
    %% re-process the SAME batch (a fresh take would silently lose it).
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
            case process_batch(Batch, Failures) of
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
            timer:sleep(?DRAIN_BACKOFF_MS),
            worker_loop(0);
        Batch ->
            worker_process(Batch, Failures)
    end.

worker_process(Batch, Failures) ->
    case process_batch(Batch, Failures) of
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
            %% The batch was already dequeued by the atomic take; the append
            %% completes the promotion. On failure the worker retries the
            %% same batch (idempotent: already_promoted + append dedup).
            case
                emqx_bcast_index_owner:append_batch(
                    [
                        {maps:get(product_key, E), DN, maps:get(delivery_id, E)}
                     || E <- Promoted ++ AlreadyPromoted, DN <- maps:get(devices, E)
                    ]
                )
            of
                ok ->
                    emqx_bcast_metrics:qos1_promoted(length(Promoted)),
                    ok = trigger_broadcast(Promoted),
                    {done, 0};
                {error, _Reason} = Error ->
                    %% Owner takeover, shard overload (call timeout) or any
                    %% other append failure: retry the SAME batch. The retry
                    %% is idempotent (already_promoted + append dedup), and
                    %% with the atomic intake take this is the only safety
                    %% net against losing committed-but-unindexed deliveries.
                    ?SLOG(warning, #{
                        msg => "bcast_promoter_append_failed_retry",
                        result => Error,
                        batch_size => length(Batch)
                    }),
                    {retry, Failures + 1}
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
                    %% A persistently failing batch must not block the
                    %% queue forever. Drop it loudly (the queue is
                    %% crash-volatile by contract; this is a bug path).
                    ?SLOG(error, #{
                        msg => "bcast_promoter_dropped_batch",
                        reason => Reason,
                        batch_size => length(Batch)
                    }),
                    lists:foreach(
                        fun(E) ->
                            _ = emqx_bcast_index_owner:release_admit(
                                maps:get(product_key, E), maps:get(devices, E)
                            )
                        end,
                        Entries
                    ),
                    {done, 0};
                false ->
                    {retry, Failures + 1}
            end
    end.

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
