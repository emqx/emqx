%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% This module implements async message sending, disk message queuing,
%%  and message batching using ReplayQ.

-module(emqx_resource_buffer_worker).

-include("emqx_resource.hrl").
-include("emqx_resource_errors.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_statem).

-export([
    start_link/3,
    sync_query/3,
    async_query/3,
    block/1,
    resume/1,
    flush_worker/1
]).

-export([
    simple_sync_query/2,
    simple_async_query/3
]).

-export([
    callback_mode/0,
    init/1,
    terminate/2,
    code_change/3
]).

-export([running/3, blocked/3]).

-export([queue_item_marshaller/1, estimate_size/1]).

-export([handle_async_reply/2, handle_async_batch_reply/2]).

-export([clear_disk_queue_dir/2]).

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-define(COLLECT_REQ_LIMIT, 1000).
-define(SEND_REQ(FROM, REQUEST), {'$send_req', FROM, REQUEST}).
-define(QUERY(FROM, REQUEST, SENT, EXPIRE_AT), {query, FROM, REQUEST, SENT, EXPIRE_AT}).
-define(SIMPLE_QUERY(REQUEST), ?QUERY(undefined, REQUEST, false, infinity)).
-define(REPLY(FROM, SENT, RESULT), {reply, FROM, SENT, RESULT}).
-define(INFLIGHT_ITEM(Ref, BatchOrQuery, IsRetriable, WorkerMRef),
    {Ref, BatchOrQuery, IsRetriable, WorkerMRef}
).
-define(ITEM_IDX, 2).
-define(RETRY_IDX, 3).
-define(WORKER_MREF_IDX, 4).

-type id() :: binary().
-type index() :: pos_integer().
-type expire_at() :: infinity | integer().
-type queue_query() :: ?QUERY(reply_fun(), request(), HasBeenSent :: boolean(), expire_at()).
-type request() :: term().
-type request_from() :: undefined | gen_statem:from().
-type state() :: blocked | running.
-type inflight_key() :: integer().
-type data() :: #{
    id := id(),
    index := index(),
    inflight_tid := ets:tid(),
    async_workers := #{pid() => reference()},
    batch_size := pos_integer(),
    batch_time := timer:time(),
    queue := replayq:q(),
    resume_interval := timer:time(),
    tref := undefined | timer:tref()
}.

callback_mode() -> [state_functions, state_enter].

start_link(Id, Index, Opts) ->
    gen_statem:start_link(?MODULE, {Id, Index, Opts}, []).

-spec sync_query(id(), request(), query_opts()) -> Result :: term().
sync_query(Id, Request, Opts0) ->
    Opts1 = ensure_timeout_query_opts(Opts0, sync),
    Opts = ensure_expire_at(Opts1),
    PickKey = maps:get(pick_key, Opts, self()),
    Timeout = maps:get(timeout, Opts),
    emqx_resource_metrics:matched_inc(Id),
    pick_call(Id, PickKey, {query, Request, Opts}, Timeout).

-spec async_query(id(), request(), query_opts()) -> Result :: term().
async_query(Id, Request, Opts0) ->
    Opts1 = ensure_timeout_query_opts(Opts0, async),
    Opts = ensure_expire_at(Opts1),
    PickKey = maps:get(pick_key, Opts, self()),
    emqx_resource_metrics:matched_inc(Id),
    pick_cast(Id, PickKey, {query, Request, Opts}).

%% simple query the resource without batching and queuing.
-spec simple_sync_query(id(), request()) -> term().
simple_sync_query(Id, Request) ->
    %% Note: since calling this function implies in bypassing the
    %% buffer workers, and each buffer worker index is used when
    %% collecting gauge metrics, we use this dummy index.  If this
    %% call ends up calling buffering functions, that's a bug and
    %% would mess up the metrics anyway.  `undefined' is ignored by
    %% `emqx_resource_metrics:*_shift/3'.
    Index = undefined,
    QueryOpts = simple_query_opts(),
    emqx_resource_metrics:matched_inc(Id),
    Ref = make_request_ref(),
    Result = call_query(sync, Id, Index, Ref, ?SIMPLE_QUERY(Request), QueryOpts),
    _ = handle_query_result(Id, Result, _HasBeenSent = false),
    Result.

%% simple async-query the resource without batching and queuing.
-spec simple_async_query(id(), request(), query_opts()) -> term().
simple_async_query(Id, Request, QueryOpts0) ->
    Index = undefined,
    QueryOpts = maps:merge(simple_query_opts(), QueryOpts0),
    emqx_resource_metrics:matched_inc(Id),
    Ref = make_request_ref(),
    Result = call_query(async, Id, Index, Ref, ?SIMPLE_QUERY(Request), QueryOpts),
    _ = handle_query_result(Id, Result, _HasBeenSent = false),
    Result.

simple_query_opts() ->
    ensure_expire_at(#{simple_query => true, timeout => infinity}).

-spec block(pid()) -> ok.
block(ServerRef) ->
    gen_statem:cast(ServerRef, block).

-spec resume(pid()) -> ok.
resume(ServerRef) ->
    gen_statem:cast(ServerRef, resume).

-spec flush_worker(pid()) -> ok.
flush_worker(ServerRef) ->
    gen_statem:cast(ServerRef, flush).

-spec init({id(), pos_integer(), map()}) -> gen_statem:init_result(state(), data()).
init({Id, Index, Opts}) ->
    process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Id, {Id, Index}),
    BatchSize = maps:get(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
    SegBytes0 = maps:get(queue_seg_bytes, Opts, ?DEFAULT_QUEUE_SEG_SIZE),
    TotalBytes = maps:get(max_queue_bytes, Opts, ?DEFAULT_QUEUE_SIZE),
    SegBytes = min(SegBytes0, TotalBytes),
    QueueOpts =
        #{
            dir => disk_queue_dir(Id, Index),
            marshaller => fun ?MODULE:queue_item_marshaller/1,
            max_total_bytes => TotalBytes,
            %% we don't want to retain the queue after
            %% resource restarts.
            offload => {true, volatile},
            seg_bytes => SegBytes,
            sizer => fun ?MODULE:estimate_size/1
        },
    Queue = replayq:open(QueueOpts),
    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Queue)),
    emqx_resource_metrics:inflight_set(Id, Index, 0),
    InflightWinSize = maps:get(async_inflight_window, Opts, ?DEFAULT_INFLIGHT),
    InflightTID = inflight_new(InflightWinSize, Id, Index),
    HealthCheckInterval = maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL),
    Data = #{
        id => Id,
        index => Index,
        inflight_tid => InflightTID,
        async_workers => #{},
        batch_size => BatchSize,
        batch_time => maps:get(batch_time, Opts, ?DEFAULT_BATCH_TIME),
        queue => Queue,
        resume_interval => maps:get(resume_interval, Opts, HealthCheckInterval),
        tref => undefined
    },
    ?tp(buffer_worker_init, #{id => Id, index => Index}),
    {ok, running, Data}.

running(enter, _, Data) ->
    ?tp(buffer_worker_enter_running, #{id => maps:get(id, Data)}),
    %% According to `gen_statem' laws, we mustn't call `maybe_flush'
    %% directly because it may decide to return `{next_state, blocked, _}',
    %% and that's an invalid response for a state enter call.
    %% Returning a next event from a state enter call is also
    %% prohibited.
    {keep_state, ensure_flush_timer(Data, 0)};
running(cast, resume, _St) ->
    keep_state_and_data;
running(cast, flush, Data) ->
    flush(Data);
running(cast, block, St) ->
    {next_state, blocked, St};
running(info, ?SEND_REQ(_ReplyTo, _Req) = Request0, Data) ->
    handle_query_requests(Request0, Data);
running(info, {flush, Ref}, St = #{tref := {_TRef, Ref}}) ->
    flush(St#{tref := undefined});
running(internal, flush, St) ->
    flush(St);
running(info, {flush, _Ref}, _St) ->
    keep_state_and_data;
running(info, {'DOWN', _MRef, process, Pid, Reason}, Data0 = #{async_workers := AsyncWorkers0}) when
    is_map_key(Pid, AsyncWorkers0)
->
    ?SLOG(info, #{msg => async_worker_died, state => running, reason => Reason}),
    handle_async_worker_down(Data0, Pid);
running(info, Info, _St) ->
    ?SLOG(error, #{msg => unexpected_msg, state => running, info => Info}),
    keep_state_and_data.

blocked(enter, _, #{resume_interval := ResumeT} = _St) ->
    ?tp(buffer_worker_enter_blocked, #{}),
    {keep_state_and_data, {state_timeout, ResumeT, unblock}};
blocked(cast, block, _St) ->
    keep_state_and_data;
blocked(cast, resume, St) ->
    resume_from_blocked(St);
blocked(cast, flush, Data) ->
    resume_from_blocked(Data);
blocked(state_timeout, unblock, St) ->
    resume_from_blocked(St);
blocked(info, ?SEND_REQ(_ReplyTo, _Req) = Request0, Data0) ->
    Data = collect_and_enqueue_query_requests(Request0, Data0),
    {keep_state, Data};
blocked(info, {flush, _Ref}, _Data) ->
    keep_state_and_data;
blocked(info, {'DOWN', _MRef, process, Pid, Reason}, Data0 = #{async_workers := AsyncWorkers0}) when
    is_map_key(Pid, AsyncWorkers0)
->
    ?SLOG(info, #{msg => async_worker_died, state => blocked, reason => Reason}),
    handle_async_worker_down(Data0, Pid);
blocked(info, Info, _Data) ->
    ?SLOG(error, #{msg => unexpected_msg, state => blocked, info => Info}),
    keep_state_and_data.

terminate(_Reason, #{id := Id, index := Index, queue := Q}) ->
    _ = replayq:close(Q),
    emqx_resource_metrics:inflight_set(Id, Index, 0),
    %% since we want volatile queues, this will be 0 after
    %% termination.
    emqx_resource_metrics:queuing_set(Id, Index, 0),
    gproc_pool:disconnect_worker(Id, {Id, Index}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%==============================================================================
-define(PICK(ID, KEY, PID, EXPR),
    try
        case gproc_pool:pick_worker(ID, KEY) of
            PID when is_pid(PID) ->
                EXPR;
            _ ->
                ?RESOURCE_ERROR(worker_not_created, "resource not created")
        end
    catch
        error:badarg ->
            ?RESOURCE_ERROR(worker_not_created, "resource not created");
        error:timeout ->
            ?RESOURCE_ERROR(timeout, "call resource timeout")
    end
).

pick_call(Id, Key, Query, Timeout) ->
    ?PICK(Id, Key, Pid, begin
        Caller = self(),
        MRef = erlang:monitor(process, Pid, [{alias, reply_demonitor}]),
        From = {Caller, MRef},
        ReplyTo = {fun gen_statem:reply/2, [From]},
        erlang:send(Pid, ?SEND_REQ(ReplyTo, Query)),
        receive
            {MRef, Response} ->
                erlang:demonitor(MRef, [flush]),
                Response;
            {'DOWN', MRef, process, Pid, Reason} ->
                error({worker_down, Reason})
        after Timeout ->
            erlang:demonitor(MRef, [flush]),
            receive
                {MRef, Response} ->
                    Response
            after 0 ->
                error(timeout)
            end
        end
    end).

pick_cast(Id, Key, Query) ->
    ?PICK(Id, Key, Pid, begin
        ReplyTo = undefined,
        erlang:send(Pid, ?SEND_REQ(ReplyTo, Query)),
        ok
    end).

resume_from_blocked(Data) ->
    ?tp(buffer_worker_resume_from_blocked_enter, #{}),
    #{
        id := Id,
        index := Index,
        inflight_tid := InflightTID
    } = Data,
    Now = now_(),
    case inflight_get_first_retriable(InflightTID, Now) of
        none ->
            case is_inflight_full(InflightTID) of
                true ->
                    {keep_state, Data};
                false ->
                    {next_state, running, Data}
            end;
        {expired, Ref, Batch} ->
            IsAcked = ack_inflight(InflightTID, Ref, Id, Index),
            IsAcked andalso emqx_resource_metrics:dropped_expired_inc(Id, length(Batch)),
            ?tp(buffer_worker_retry_expired, #{expired => Batch}),
            resume_from_blocked(Data);
        {single, Ref, Query} ->
            %% We retry msgs in inflight window sync, as if we send them
            %% async, they will be appended to the end of inflight window again.
            case is_inflight_full(InflightTID) of
                true ->
                    {keep_state, Data};
                false ->
                    retry_inflight_sync(Ref, Query, Data)
            end;
        {batch, Ref, NotExpired, Expired} ->
            update_inflight_item(InflightTID, Ref, NotExpired),
            NumExpired = length(Expired),
            emqx_resource_metrics:dropped_expired_inc(Id, NumExpired),
            NumExpired > 0 andalso ?tp(buffer_worker_retry_expired, #{expired => Expired}),
            %% We retry msgs in inflight window sync, as if we send them
            %% async, they will be appended to the end of inflight window again.
            case is_inflight_full(InflightTID) of
                true ->
                    {keep_state, Data};
                false ->
                    retry_inflight_sync(Ref, NotExpired, Data)
            end
    end.

retry_inflight_sync(Ref, QueryOrBatch, Data0) ->
    #{
        id := Id,
        inflight_tid := InflightTID,
        index := Index,
        resume_interval := ResumeT
    } = Data0,
    ?tp(buffer_worker_retry_inflight, #{query_or_batch => QueryOrBatch, ref => Ref}),
    QueryOpts = #{simple_query => false},
    Result = call_query(sync, Id, Index, Ref, QueryOrBatch, QueryOpts),
    ReplyResult =
        case QueryOrBatch of
            ?QUERY(ReplyTo, _, HasBeenSent, _ExpireAt) ->
                Reply = ?REPLY(ReplyTo, HasBeenSent, Result),
                reply_caller_defer_metrics(Id, Reply, QueryOpts);
            [?QUERY(_, _, _, _) | _] = Batch ->
                batch_reply_caller_defer_metrics(Id, Result, Batch, QueryOpts)
        end,
    case ReplyResult of
        %% Send failed because resource is down
        {nack, PostFn} ->
            PostFn(),
            ?tp(
                buffer_worker_retry_inflight_failed,
                #{
                    ref => Ref,
                    query_or_batch => QueryOrBatch
                }
            ),
            {keep_state, Data0, {state_timeout, ResumeT, unblock}};
        %% Send ok or failed but the resource is working
        {ack, PostFn} ->
            IsAcked = ack_inflight(InflightTID, Ref, Id, Index),
            %% we need to defer bumping the counters after
            %% `inflight_drop' to avoid the race condition when an
            %% inflight request might get completed concurrently with
            %% the retry, bumping them twice.  Since both inflight
            %% requests (repeated and original) have the safe `Ref',
            %% we bump the counter when removing it from the table.
            IsAcked andalso PostFn(),
            ?tp(
                buffer_worker_retry_inflight_succeeded,
                #{
                    ref => Ref,
                    query_or_batch => QueryOrBatch
                }
            ),
            resume_from_blocked(Data0)
    end.

%% Called during the `running' state only.
-spec handle_query_requests(?SEND_REQ(request_from(), request()), data()) ->
    gen_statem:event_handler_result(state(), data()).
handle_query_requests(Request0, Data0) ->
    Data = collect_and_enqueue_query_requests(Request0, Data0),
    maybe_flush(Data).

collect_and_enqueue_query_requests(Request0, Data0) ->
    #{
        id := Id,
        index := Index,
        queue := Q
    } = Data0,
    Requests = collect_requests([Request0], ?COLLECT_REQ_LIMIT),
    Queries =
        lists:map(
            fun
                (?SEND_REQ(undefined = _ReplyTo, {query, Req, Opts})) ->
                    ReplyFun = maps:get(async_reply_fun, Opts, undefined),
                    HasBeenSent = false,
                    ExpireAt = maps:get(expire_at, Opts),
                    ?QUERY(ReplyFun, Req, HasBeenSent, ExpireAt);
                (?SEND_REQ(ReplyTo, {query, Req, Opts})) ->
                    HasBeenSent = false,
                    ExpireAt = maps:get(expire_at, Opts),
                    ?QUERY(ReplyTo, Req, HasBeenSent, ExpireAt)
            end,
            Requests
        ),
    {Overflown, NewQ} = append_queue(Id, Index, Q, Queries),
    ok = reply_overflown(Overflown),
    Data0#{queue := NewQ}.

reply_overflown([]) ->
    ok;
reply_overflown([?QUERY(ReplyTo, _Req, _HasBeenSent, _ExpireAt) | More]) ->
    do_reply_caller(ReplyTo, {error, buffer_overflow}),
    reply_overflown(More).

do_reply_caller(undefined, _Result) ->
    ok;
do_reply_caller({F, Args}, {async_return, Result}) ->
    %% this is an early return to async caller, the retry
    %% decision has to be made by the caller
    do_reply_caller({F, Args}, Result);
do_reply_caller({F, Args}, Result) when is_function(F) ->
    _ = erlang:apply(F, Args ++ [Result]),
    ok.

maybe_flush(Data0) ->
    #{
        batch_size := BatchSize,
        queue := Q
    } = Data0,
    QueueCount = queue_count(Q),
    case QueueCount >= BatchSize of
        true ->
            flush(Data0);
        false ->
            {keep_state, ensure_flush_timer(Data0)}
    end.

%% Called during the `running' state only.
-spec flush(data()) -> gen_statem:event_handler_result(state(), data()).
flush(Data0) ->
    #{
        id := Id,
        index := Index,
        batch_size := BatchSize,
        inflight_tid := InflightTID,
        queue := Q0
    } = Data0,
    Data1 = cancel_flush_timer(Data0),
    case {queue_count(Q0), is_inflight_full(InflightTID)} of
        {0, _} ->
            {keep_state, Data1};
        {_, true} ->
            ?tp(buffer_worker_flush_but_inflight_full, #{}),
            Data2 = ensure_flush_timer(Data1),
            {keep_state, Data2};
        {_, false} ->
            ?tp(buffer_worker_flush_before_pop, #{}),
            {Q1, QAckRef, Batch} = replayq:pop(Q0, #{count_limit => BatchSize}),
            Data2 = Data1#{queue := Q1},
            ?tp(buffer_worker_flush_before_sieve_expired, #{}),
            Now = now_(),
            %% if the request has expired, the caller is no longer
            %% waiting for a response.
            case sieve_expired_requests(Batch, Now) of
                all_expired ->
                    ok = replayq:ack(Q1, QAckRef),
                    emqx_resource_metrics:dropped_expired_inc(Id, length(Batch)),
                    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
                    ?tp(buffer_worker_flush_all_expired, #{batch => Batch}),
                    flush(Data2);
                {NotExpired, Expired} ->
                    NumExpired = length(Expired),
                    emqx_resource_metrics:dropped_expired_inc(Id, NumExpired),
                    IsBatch = BatchSize =/= 1,
                    %% We *must* use the new queue, because we currently can't
                    %% `nack' a `pop'.
                    %% Maybe we could re-open the queue?
                    ?tp(
                        buffer_worker_flush_potentially_partial,
                        #{expired => Expired, not_expired => NotExpired}
                    ),
                    Ref = make_request_ref(),
                    do_flush(Data2, #{
                        new_queue => Q1,
                        is_batch => IsBatch,
                        batch => NotExpired,
                        ref => Ref,
                        ack_ref => QAckRef
                    })
            end
    end.

-spec do_flush(data(), #{
    is_batch := boolean(),
    batch := [queue_query()],
    ack_ref := replayq:ack_ref(),
    ref := inflight_key(),
    new_queue := replayq:q()
}) ->
    gen_statem:event_handler_result(state(), data()).
do_flush(
    Data0,
    #{
        is_batch := false,
        batch := Batch,
        ref := Ref,
        ack_ref := QAckRef,
        new_queue := Q1
    }
) ->
    #{
        id := Id,
        index := Index,
        inflight_tid := InflightTID
    } = Data0,
    %% unwrap when not batching (i.e., batch size == 1)
    [?QUERY(ReplyTo, _, HasBeenSent, _ExpireAt) = Request] = Batch,
    QueryOpts = #{inflight_tid => InflightTID, simple_query => false},
    Result = call_query(configured, Id, Index, Ref, Request, QueryOpts),
    Reply = ?REPLY(ReplyTo, HasBeenSent, Result),
    case reply_caller(Id, Reply, QueryOpts) of
        %% Failed; remove the request from the queue, as we cannot pop
        %% from it again, but we'll retry it using the inflight table.
        nack ->
            ok = replayq:ack(Q1, QAckRef),
            %% we set it atomically just below; a limitation of having
            %% to use tuples for atomic ets updates
            IsRetriable = true,
            WorkerMRef0 = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Request, IsRetriable, WorkerMRef0),
            %% we must append again to the table to ensure that the
            %% request will be retried (i.e., it might not have been
            %% inserted during `call_query' if the resource was down
            %% and/or if it was a sync request).
            inflight_append(InflightTID, InflightItem, Id, Index),
            mark_inflight_as_retriable(InflightTID, Ref),
            {Data1, WorkerMRef} = ensure_async_worker_monitored(Data0, Result),
            store_async_worker_reference(InflightTID, Ref, WorkerMRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            ?tp(
                buffer_worker_flush_nack,
                #{
                    ref => Ref,
                    is_retriable => IsRetriable,
                    batch_or_query => Request,
                    result => Result
                }
            ),
            {next_state, blocked, Data1};
        %% Success; just ack.
        ack ->
            ok = replayq:ack(Q1, QAckRef),
            %% Async requests are acked later when the async worker
            %% calls the corresponding callback function.  Also, we
            %% must ensure the async worker is being monitored for
            %% such requests.
            IsUnrecoverableError = is_unrecoverable_error(Result),
            case is_async_return(Result) of
                true when IsUnrecoverableError ->
                    ack_inflight(InflightTID, Ref, Id, Index);
                true ->
                    ok;
                false ->
                    ack_inflight(InflightTID, Ref, Id, Index)
            end,
            {Data1, WorkerMRef} = ensure_async_worker_monitored(Data0, Result),
            store_async_worker_reference(InflightTID, Ref, WorkerMRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            ?tp(
                buffer_worker_flush_ack,
                #{
                    batch_or_query => Request,
                    result => Result
                }
            ),
            case queue_count(Q1) > 0 of
                true ->
                    flush_worker(self());
                false ->
                    ok
            end,
            {keep_state, Data1}
    end;
do_flush(Data0, #{
    is_batch := true,
    batch := Batch,
    ref := Ref,
    ack_ref := QAckRef,
    new_queue := Q1
}) ->
    #{
        id := Id,
        index := Index,
        batch_size := BatchSize,
        inflight_tid := InflightTID
    } = Data0,
    QueryOpts = #{inflight_tid => InflightTID, simple_query => false},
    Result = call_query(configured, Id, Index, Ref, Batch, QueryOpts),
    case batch_reply_caller(Id, Result, Batch, QueryOpts) of
        %% Failed; remove the request from the queue, as we cannot pop
        %% from it again, but we'll retry it using the inflight table.
        nack ->
            ok = replayq:ack(Q1, QAckRef),
            %% we set it atomically just below; a limitation of having
            %% to use tuples for atomic ets updates
            IsRetriable = true,
            WorkerMRef0 = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Batch, IsRetriable, WorkerMRef0),
            %% we must append again to the table to ensure that the
            %% request will be retried (i.e., it might not have been
            %% inserted during `call_query' if the resource was down
            %% and/or if it was a sync request).
            inflight_append(InflightTID, InflightItem, Id, Index),
            mark_inflight_as_retriable(InflightTID, Ref),
            {Data1, WorkerMRef} = ensure_async_worker_monitored(Data0, Result),
            store_async_worker_reference(InflightTID, Ref, WorkerMRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            ?tp(
                buffer_worker_flush_nack,
                #{
                    ref => Ref,
                    is_retriable => IsRetriable,
                    batch_or_query => Batch,
                    result => Result
                }
            ),
            {next_state, blocked, Data1};
        %% Success; just ack.
        ack ->
            ok = replayq:ack(Q1, QAckRef),
            %% Async requests are acked later when the async worker
            %% calls the corresponding callback function.  Also, we
            %% must ensure the async worker is being monitored for
            %% such requests.
            IsUnrecoverableError = is_unrecoverable_error(Result),
            case is_async_return(Result) of
                true when IsUnrecoverableError ->
                    ack_inflight(InflightTID, Ref, Id, Index);
                true ->
                    ok;
                false ->
                    ack_inflight(InflightTID, Ref, Id, Index)
            end,
            {Data1, WorkerMRef} = ensure_async_worker_monitored(Data0, Result),
            store_async_worker_reference(InflightTID, Ref, WorkerMRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            ?tp(
                buffer_worker_flush_ack,
                #{
                    batch_or_query => Batch,
                    result => Result
                }
            ),
            CurrentCount = queue_count(Q1),
            Data2 =
                case {CurrentCount > 0, CurrentCount >= BatchSize} of
                    {false, _} ->
                        Data1;
                    {true, true} ->
                        flush_worker(self()),
                        Data1;
                    {true, false} ->
                        ensure_flush_timer(Data1)
                end,
            {keep_state, Data2}
    end.

batch_reply_caller(Id, BatchResult, Batch, QueryOpts) ->
    {ShouldBlock, PostFn} = batch_reply_caller_defer_metrics(Id, BatchResult, Batch, QueryOpts),
    PostFn(),
    ShouldBlock.

batch_reply_caller_defer_metrics(Id, BatchResult, Batch, QueryOpts) ->
    %% the `Mod:on_batch_query/3` returns a single result for a batch,
    %% so we need to expand
    Replies = lists:map(
        fun(?QUERY(FROM, _REQUEST, SENT, _EXPIRE_AT)) ->
            ?REPLY(FROM, SENT, BatchResult)
        end,
        Batch
    ),
    {ShouldAck, PostFns} =
        lists:foldl(
            fun(Reply, {_ShouldAck, PostFns}) ->
                {ShouldAck, PostFn} = reply_caller_defer_metrics(Id, Reply, QueryOpts),
                {ShouldAck, [PostFn | PostFns]}
            end,
            {ack, []},
            Replies
        ),
    PostFn = fun() -> lists:foreach(fun(F) -> F() end, PostFns) end,
    {ShouldAck, PostFn}.

reply_caller(Id, Reply, QueryOpts) ->
    {ShouldAck, PostFn} = reply_caller_defer_metrics(Id, Reply, QueryOpts),
    PostFn(),
    ShouldAck.

%% Should only reply to the caller when the decision is final (not
%% retriable).  See comment on `handle_query_result_pure'.
reply_caller_defer_metrics(Id, ?REPLY(undefined, HasBeenSent, Result), _QueryOpts) ->
    handle_query_result_pure(Id, Result, HasBeenSent);
reply_caller_defer_metrics(Id, ?REPLY(ReplyTo, HasBeenSent, Result), QueryOpts) ->
    IsSimpleQuery = maps:get(simple_query, QueryOpts, false),
    IsUnrecoverableError = is_unrecoverable_error(Result),
    {ShouldAck, PostFn} = handle_query_result_pure(Id, Result, HasBeenSent),
    case {ShouldAck, Result, IsUnrecoverableError, IsSimpleQuery} of
        {ack, {async_return, _}, true, _} ->
            ok = do_reply_caller(ReplyTo, Result);
        {ack, {async_return, _}, false, _} ->
            ok;
        {_, _, _, true} ->
            ok = do_reply_caller(ReplyTo, Result);
        {nack, _, _, _} ->
            ok;
        {ack, _, _, _} ->
            ok = do_reply_caller(ReplyTo, Result)
    end,
    {ShouldAck, PostFn}.

handle_query_result(Id, Result, HasBeenSent) ->
    {ShouldBlock, PostFn} = handle_query_result_pure(Id, Result, HasBeenSent),
    PostFn(),
    ShouldBlock.

%% We should always retry (nack), except when:
%%   * resource is not found
%%   * resource is stopped
%%   * the result is a success (or at least a delayed result)
%% We also retry even sync requests.  In that case, we shouldn't reply
%% the caller until one of those final results above happen.
handle_query_result_pure(_Id, ?RESOURCE_ERROR_M(exception, Msg), _HasBeenSent) ->
    PostFn = fun() ->
        ?SLOG(error, #{msg => resource_exception, info => Msg}),
        ok
    end,
    {nack, PostFn};
handle_query_result_pure(_Id, ?RESOURCE_ERROR_M(NotWorking, _), _HasBeenSent) when
    NotWorking == not_connected; NotWorking == blocked
->
    {nack, fun() -> ok end};
handle_query_result_pure(Id, ?RESOURCE_ERROR_M(not_found, Msg), _HasBeenSent) ->
    PostFn = fun() ->
        ?SLOG(error, #{id => Id, msg => resource_not_found, info => Msg}),
        emqx_resource_metrics:dropped_resource_not_found_inc(Id),
        ok
    end,
    {ack, PostFn};
handle_query_result_pure(Id, ?RESOURCE_ERROR_M(stopped, Msg), _HasBeenSent) ->
    PostFn = fun() ->
        ?SLOG(error, #{id => Id, msg => resource_stopped, info => Msg}),
        emqx_resource_metrics:dropped_resource_stopped_inc(Id),
        ok
    end,
    {ack, PostFn};
handle_query_result_pure(Id, ?RESOURCE_ERROR_M(Reason, _), _HasBeenSent) ->
    PostFn = fun() ->
        ?SLOG(error, #{id => Id, msg => other_resource_error, reason => Reason}),
        ok
    end,
    {nack, PostFn};
handle_query_result_pure(Id, {error, Reason} = Error, HasBeenSent) ->
    case is_unrecoverable_error(Error) of
        true ->
            PostFn =
                fun() ->
                    ?SLOG(error, #{id => Id, msg => unrecoverable_error, reason => Reason}),
                    inc_sent_failed(Id, HasBeenSent),
                    ok
                end,
            {ack, PostFn};
        false ->
            PostFn =
                fun() ->
                    ?SLOG(error, #{id => Id, msg => send_error, reason => Reason}),
                    ok
                end,
            {nack, PostFn}
    end;
handle_query_result_pure(Id, {async_return, Result}, HasBeenSent) ->
    handle_query_async_result_pure(Id, Result, HasBeenSent);
handle_query_result_pure(Id, Result, HasBeenSent) ->
    PostFn = fun() ->
        assert_ok_result(Result),
        inc_sent_success(Id, HasBeenSent),
        ok
    end,
    {ack, PostFn}.

handle_query_async_result_pure(Id, {error, Reason} = Error, HasBeenSent) ->
    case is_unrecoverable_error(Error) of
        true ->
            PostFn =
                fun() ->
                    ?SLOG(error, #{id => Id, msg => unrecoverable_error, reason => Reason}),
                    inc_sent_failed(Id, HasBeenSent),
                    ok
                end,
            {ack, PostFn};
        false ->
            PostFn = fun() ->
                ?SLOG(error, #{id => Id, msg => async_send_error, reason => Reason}),
                ok
            end,
            {nack, PostFn}
    end;
handle_query_async_result_pure(_Id, {ok, Pid}, _HasBeenSent) when is_pid(Pid) ->
    {ack, fun() -> ok end};
handle_query_async_result_pure(_Id, ok, _HasBeenSent) ->
    {ack, fun() -> ok end}.

handle_async_worker_down(Data0, Pid) ->
    #{async_workers := AsyncWorkers0} = Data0,
    {WorkerMRef, AsyncWorkers} = maps:take(Pid, AsyncWorkers0),
    Data = Data0#{async_workers := AsyncWorkers},
    mark_inflight_items_as_retriable(Data, WorkerMRef),
    {keep_state, Data}.

call_query(QM0, Id, Index, Ref, Query, QueryOpts) ->
    ?tp(call_query_enter, #{id => Id, query => Query}),
    case emqx_resource_manager:ets_lookup(Id) of
        {ok, _Group, #{status := stopped}} ->
            ?RESOURCE_ERROR(stopped, "resource stopped or disabled");
        {ok, _Group, Resource} ->
            QM =
                case QM0 =:= configured of
                    true -> maps:get(query_mode, Resource);
                    false -> QM0
                end,
            do_call_query(QM, Id, Index, Ref, Query, QueryOpts, Resource);
        {error, not_found} ->
            ?RESOURCE_ERROR(not_found, "resource not found")
    end.

do_call_query(QM, Id, Index, Ref, Query, #{is_buffer_supported := true} = QueryOpts, Resource) ->
    %% The connector supports buffer, send even in disconnected state
    #{mod := Mod, state := ResSt, callback_mode := CBM} = Resource,
    CallMode = call_mode(QM, CBM),
    apply_query_fun(CallMode, Mod, Id, Index, Ref, Query, ResSt, QueryOpts);
do_call_query(QM, Id, Index, Ref, Query, QueryOpts, #{status := connected} = Resource) ->
    %% when calling from the buffer worker or other simple queries,
    %% only apply the query fun when it's at connected status
    #{mod := Mod, state := ResSt, callback_mode := CBM} = Resource,
    CallMode = call_mode(QM, CBM),
    apply_query_fun(CallMode, Mod, Id, Index, Ref, Query, ResSt, QueryOpts);
do_call_query(_QM, _Id, _Index, _Ref, _Query, _QueryOpts, _Data) ->
    ?RESOURCE_ERROR(not_connected, "resource not connected").

-define(APPLY_RESOURCE(NAME, EXPR, REQ),
    try
        %% if the callback module (connector) wants to return an error that
        %% makes the current resource goes into the `blocked` state, it should
        %% return `{error, {recoverable_error, Reason}}`
        EXPR
    catch
        ERR:REASON:STACKTRACE ->
            ?RESOURCE_ERROR(exception, #{
                name => NAME,
                id => Id,
                request => REQ,
                error => {ERR, REASON},
                stacktrace => STACKTRACE
            })
    end
).

apply_query_fun(sync, Mod, Id, _Index, _Ref, ?QUERY(_, Request, _, _) = _Query, ResSt, _QueryOpts) ->
    ?tp(call_query, #{id => Id, mod => Mod, query => _Query, res_st => ResSt, call_mode => sync}),
    ?APPLY_RESOURCE(call_query, Mod:on_query(Id, Request, ResSt), Request);
apply_query_fun(async, Mod, Id, Index, Ref, ?QUERY(_, Request, _, _) = Query, ResSt, QueryOpts) ->
    ?tp(call_query_async, #{
        id => Id, mod => Mod, query => Query, res_st => ResSt, call_mode => async
    }),
    InflightTID = maps:get(inflight_tid, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_query_async,
        begin
            ReplyFun = fun ?MODULE:handle_async_reply/2,
            ReplyContext = #{
                buffer_worker => self(),
                resource_id => Id,
                worker_index => Index,
                inflight_tid => InflightTID,
                request_ref => Ref,
                query_opts => QueryOpts,
                query => minimize(Query)
            },
            IsRetriable = false,
            WorkerMRef = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Query, IsRetriable, WorkerMRef),
            ok = inflight_append(InflightTID, InflightItem, Id, Index),
            Result = Mod:on_query_async(Id, Request, {ReplyFun, [ReplyContext]}, ResSt),
            {async_return, Result}
        end,
        Request
    );
apply_query_fun(sync, Mod, Id, _Index, _Ref, [?QUERY(_, _, _, _) | _] = Batch, ResSt, _QueryOpts) ->
    ?tp(call_batch_query, #{
        id => Id, mod => Mod, batch => Batch, res_st => ResSt, call_mode => sync
    }),
    Requests = lists:map(fun(?QUERY(_ReplyTo, Request, _, _ExpireAt)) -> Request end, Batch),
    ?APPLY_RESOURCE(call_batch_query, Mod:on_batch_query(Id, Requests, ResSt), Batch);
apply_query_fun(async, Mod, Id, Index, Ref, [?QUERY(_, _, _, _) | _] = Batch, ResSt, QueryOpts) ->
    ?tp(call_batch_query_async, #{
        id => Id, mod => Mod, batch => Batch, res_st => ResSt, call_mode => async
    }),
    InflightTID = maps:get(inflight_tid, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_batch_query_async,
        begin
            ReplyFun = fun ?MODULE:handle_async_batch_reply/2,
            ReplyContext = #{
                buffer_worker => self(),
                resource_id => Id,
                worker_index => Index,
                inflight_tid => InflightTID,
                request_ref => Ref,
                query_opts => QueryOpts,
                batch => minimize(Batch)
            },
            Requests = lists:map(
                fun(?QUERY(_ReplyTo, Request, _, _ExpireAt)) -> Request end, Batch
            ),
            IsRetriable = false,
            WorkerMRef = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Batch, IsRetriable, WorkerMRef),
            ok = inflight_append(InflightTID, InflightItem, Id, Index),
            Result = Mod:on_batch_query_async(Id, Requests, {ReplyFun, [ReplyContext]}, ResSt),
            {async_return, Result}
        end,
        Batch
    ).

handle_async_reply(
    #{
        request_ref := Ref,
        inflight_tid := InflightTID,
        resource_id := Id,
        worker_index := Index,
        buffer_worker := Pid,
        query := ?QUERY(_, _, _, ExpireAt) = _Query
    } = ReplyContext,
    Result
) ->
    ?tp(
        handle_async_reply_enter,
        #{batch_or_query => [_Query], ref => Ref}
    ),
    Now = now_(),
    case is_expired(ExpireAt, Now) of
        true ->
            IsFullBefore = is_inflight_full(InflightTID),
            IsAcked = ack_inflight(InflightTID, Ref, Id, Index),
            IsAcked andalso emqx_resource_metrics:late_reply_inc(Id),
            IsFullBefore andalso ?MODULE:flush_worker(Pid),
            ?tp(handle_async_reply_expired, #{expired => [_Query]}),
            ok;
        false ->
            do_handle_async_reply(ReplyContext, Result)
    end.

do_handle_async_reply(
    #{
        query_opts := QueryOpts,
        resource_id := Id,
        request_ref := Ref,
        worker_index := Index,
        buffer_worker := Pid,
        inflight_tid := InflightTID,
        query := ?QUERY(ReplyTo, _, Sent, _ExpireAt) = _Query
    },
    Result
) ->
    %% NOTE: 'inflight' is the count of messages that were sent async
    %% but received no ACK, NOT the number of messages queued in the
    %% inflight window.
    {Action, PostFn} = reply_caller_defer_metrics(
        Id, ?REPLY(ReplyTo, Sent, Result), QueryOpts
    ),

    ?tp(handle_async_reply, #{
        action => Action,
        batch_or_query => [_Query],
        ref => Ref,
        result => Result
    }),

    case Action of
        nack ->
            %% Keep retrying.
            mark_inflight_as_retriable(InflightTID, Ref),
            ?MODULE:block(Pid);
        ack ->
            do_ack(InflightTID, Ref, Id, Index, PostFn, Pid, QueryOpts)
    end.

handle_async_batch_reply(
    #{
        buffer_worker := Pid,
        resource_id := Id,
        worker_index := Index,
        inflight_tid := InflightTID,
        request_ref := Ref,
        batch := Batch
    } = ReplyContext,
    Result
) ->
    ?tp(
        handle_async_reply_enter,
        #{batch_or_query => Batch, ref => Ref}
    ),
    Now = now_(),
    case sieve_expired_requests(Batch, Now) of
        all_expired ->
            IsFullBefore = is_inflight_full(InflightTID),
            IsAcked = ack_inflight(InflightTID, Ref, Id, Index),
            IsAcked andalso emqx_resource_metrics:late_reply_inc(Id),
            IsFullBefore andalso ?MODULE:flush_worker(Pid),
            ?tp(handle_async_reply_expired, #{expired => Batch}),
            ok;
        {NotExpired, Expired} ->
            NumExpired = length(Expired),
            emqx_resource_metrics:late_reply_inc(Id, NumExpired),
            NumExpired > 0 andalso
                ?tp(handle_async_reply_expired, #{expired => Expired}),
            do_handle_async_batch_reply(ReplyContext#{batch := NotExpired}, Result)
    end.

do_handle_async_batch_reply(
    #{
        buffer_worker := Pid,
        resource_id := Id,
        worker_index := Index,
        inflight_tid := InflightTID,
        request_ref := Ref,
        batch := Batch,
        query_opts := QueryOpts
    },
    Result
) ->
    {Action, PostFn} = batch_reply_caller_defer_metrics(Id, Result, Batch, QueryOpts),
    ?tp(handle_async_reply, #{
        action => Action,
        batch_or_query => Batch,
        ref => Ref,
        result => Result
    }),
    case Action of
        nack ->
            %% Keep retrying.
            mark_inflight_as_retriable(InflightTID, Ref),
            ?MODULE:block(Pid);
        ack ->
            do_ack(InflightTID, Ref, Id, Index, PostFn, Pid, QueryOpts)
    end.

do_ack(InflightTID, Ref, Id, Index, PostFn, WorkerPid, QueryOpts) ->
    IsFullBefore = is_inflight_full(InflightTID),
    IsKnownRef = ack_inflight(InflightTID, Ref, Id, Index),
    case maps:get(simple_query, QueryOpts, false) of
        true ->
            PostFn();
        false when IsKnownRef ->
            PostFn();
        false ->
            ok
    end,
    IsFullBefore andalso ?MODULE:flush_worker(WorkerPid),
    ok.

%%==============================================================================
%% operations for queue
queue_item_marshaller(Bin) when is_binary(Bin) ->
    binary_to_term(Bin);
queue_item_marshaller(Item) ->
    term_to_binary(Item).

estimate_size(QItem) ->
    erlang:external_size(QItem).

-spec append_queue(id(), index(), replayq:q(), [queue_query()]) ->
    {[queue_query()], replayq:q()}.
append_queue(Id, Index, Q, Queries) ->
    %% this assertion is to ensure that we never append a raw binary
    %% because the marshaller will get lost.
    false = is_binary(hd(Queries)),
    Q0 = replayq:append(Q, Queries),
    {Overflown, Q2} =
        case replayq:overflow(Q0) of
            OverflownBytes when OverflownBytes =< 0 ->
                {[], Q0};
            OverflownBytes ->
                PopOpts = #{bytes_limit => OverflownBytes, count_limit => 999999999},
                {Q1, QAckRef, Items2} = replayq:pop(Q0, PopOpts),
                ok = replayq:ack(Q1, QAckRef),
                Dropped = length(Items2),
                emqx_resource_metrics:dropped_queue_full_inc(Id, Dropped),
                ?SLOG(info, #{
                    msg => buffer_worker_overflow,
                    resource_id => Id,
                    worker_index => Index,
                    dropped => Dropped
                }),
                {Items2, Q1}
        end,
    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q2)),
    ?tp(
        buffer_worker_appended_to_queue,
        #{
            id => Id,
            items => Queries,
            queue_count => queue_count(Q2),
            overflown => length(Overflown)
        }
    ),
    {Overflown, Q2}.

%%==============================================================================
%% the inflight queue for async query
-define(MAX_SIZE_REF, max_size).
-define(SIZE_REF, size).
-define(INITIAL_TIME_REF, initial_time).
-define(INITIAL_MONOTONIC_TIME_REF, initial_monotonic_time).

%% NOTE
%% There are 4 metadata rows in an inflight table, keyed by atoms declared above. ☝
-define(INFLIGHT_META_ROWS, 4).

inflight_new(InfltWinSZ, Id, Index) ->
    TableId = ets:new(
        emqx_resource_buffer_worker_inflight_tab,
        [ordered_set, public, {write_concurrency, true}]
    ),
    inflight_append(TableId, {?MAX_SIZE_REF, InfltWinSZ}, Id, Index),
    %% we use this counter because we might deal with batches as
    %% elements.
    inflight_append(TableId, {?SIZE_REF, 0}, Id, Index),
    inflight_append(TableId, {?INITIAL_TIME_REF, erlang:system_time()}, Id, Index),
    inflight_append(
        TableId, {?INITIAL_MONOTONIC_TIME_REF, make_request_ref()}, Id, Index
    ),
    TableId.

-spec inflight_get_first_retriable(ets:tid(), integer()) ->
    none
    | {expired, inflight_key(), [queue_query()]}
    | {single, inflight_key(), queue_query()}
    | {batch, inflight_key(), _NotExpired :: [queue_query()], _Expired :: [queue_query()]}.
inflight_get_first_retriable(InflightTID, Now) ->
    MatchSpec =
        ets:fun2ms(
            fun(?INFLIGHT_ITEM(Ref, BatchOrQuery, IsRetriable, _WorkerMRef)) when
                IsRetriable =:= true
            ->
                {Ref, BatchOrQuery}
            end
        ),
    case ets:select(InflightTID, MatchSpec, _Limit = 1) of
        '$end_of_table' ->
            none;
        {[{Ref, Query = ?QUERY(_ReplyTo, _CoreReq, _HasBeenSent, ExpireAt)}], _Continuation} ->
            case is_expired(ExpireAt, Now) of
                true ->
                    {expired, Ref, [Query]};
                false ->
                    {single, Ref, Query}
            end;
        {[{Ref, Batch = [_ | _]}], _Continuation} ->
            %% batch is non-empty because we check that in
            %% `sieve_expired_requests'.
            case sieve_expired_requests(Batch, Now) of
                all_expired ->
                    {expired, Ref, Batch};
                {NotExpired, Expired} ->
                    {batch, Ref, NotExpired, Expired}
            end
    end.

is_inflight_full(undefined) ->
    false;
is_inflight_full(InflightTID) ->
    [{_, MaxSize}] = ets:lookup(InflightTID, ?MAX_SIZE_REF),
    %% we consider number of batches rather than number of messages
    %% because one batch request may hold several messages.
    Size = inflight_num_batches(InflightTID),
    Size >= MaxSize.

inflight_num_batches(InflightTID) ->
    case ets:info(InflightTID, size) of
        undefined -> 0;
        Size -> max(0, Size - ?INFLIGHT_META_ROWS)
    end.

inflight_num_msgs(InflightTID) ->
    [{_, Size}] = ets:lookup(InflightTID, ?SIZE_REF),
    Size.

inflight_append(undefined, _InflightItem, _Id, _Index) ->
    ok;
inflight_append(
    InflightTID,
    ?INFLIGHT_ITEM(Ref, [?QUERY(_, _, _, _) | _] = Batch0, IsRetriable, WorkerMRef),
    Id,
    Index
) ->
    Batch = mark_as_sent(Batch0),
    InflightItem = ?INFLIGHT_ITEM(Ref, Batch, IsRetriable, WorkerMRef),
    IsNew = ets:insert_new(InflightTID, InflightItem),
    BatchSize = length(Batch),
    IsNew andalso ets:update_counter(InflightTID, ?SIZE_REF, {2, BatchSize}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    ?tp(buffer_worker_appended_to_inflight, #{item => InflightItem, is_new => IsNew}),
    ok;
inflight_append(
    InflightTID,
    ?INFLIGHT_ITEM(
        Ref, ?QUERY(_ReplyTo, _Req, _HasBeenSent, _ExpireAt) = Query0, IsRetriable, WorkerMRef
    ),
    Id,
    Index
) ->
    Query = mark_as_sent(Query0),
    InflightItem = ?INFLIGHT_ITEM(Ref, Query, IsRetriable, WorkerMRef),
    IsNew = ets:insert_new(InflightTID, InflightItem),
    IsNew andalso ets:update_counter(InflightTID, ?SIZE_REF, {2, 1}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    ?tp(buffer_worker_appended_to_inflight, #{item => InflightItem, is_new => IsNew}),
    ok;
inflight_append(InflightTID, {Ref, Data}, _Id, _Index) ->
    ets:insert(InflightTID, {Ref, Data}),
    %% this is a metadata row being inserted; therefore, we don't bump
    %% the inflight metric.
    ok.

%% a request was already appended and originally not retriable, but an
%% error occurred and it is now retriable.
mark_inflight_as_retriable(undefined, _Ref) ->
    ok;
mark_inflight_as_retriable(InflightTID, Ref) ->
    _ = ets:update_element(InflightTID, Ref, {?RETRY_IDX, true}),
    ok.

%% Track each worker pid only once.
ensure_async_worker_monitored(
    Data0 = #{async_workers := AsyncWorkers}, {async_return, {ok, WorkerPid}} = _Result
) when
    is_pid(WorkerPid), is_map_key(WorkerPid, AsyncWorkers)
->
    WorkerMRef = maps:get(WorkerPid, AsyncWorkers),
    {Data0, WorkerMRef};
ensure_async_worker_monitored(
    Data0 = #{async_workers := AsyncWorkers0}, {async_return, {ok, WorkerPid}}
) when
    is_pid(WorkerPid)
->
    WorkerMRef = monitor(process, WorkerPid),
    AsyncWorkers = AsyncWorkers0#{WorkerPid => WorkerMRef},
    Data = Data0#{async_workers := AsyncWorkers},
    {Data, WorkerMRef};
ensure_async_worker_monitored(Data0, _Result) ->
    {Data0, undefined}.

store_async_worker_reference(undefined = _InflightTID, _Ref, _WorkerMRef) ->
    ok;
store_async_worker_reference(_InflightTID, _Ref, undefined = _WorkerRef) ->
    ok;
store_async_worker_reference(InflightTID, Ref, WorkerMRef) when
    is_reference(WorkerMRef)
->
    _ = ets:update_element(
        InflightTID, Ref, {?WORKER_MREF_IDX, WorkerMRef}
    ),
    ok.

ack_inflight(undefined, _Ref, _Id, _Index) ->
    false;
ack_inflight(InflightTID, Ref, Id, Index) ->
    Count =
        case ets:take(InflightTID, Ref) of
            [?INFLIGHT_ITEM(Ref, ?QUERY(_, _, _, _), _IsRetriable, _WorkerMRef)] ->
                1;
            [?INFLIGHT_ITEM(Ref, [?QUERY(_, _, _, _) | _] = Batch, _IsRetriable, _WorkerMRef)] ->
                length(Batch);
            _ ->
                0
        end,
    IsAcked = Count > 0,
    IsAcked andalso ets:update_counter(InflightTID, ?SIZE_REF, {2, -Count, 0, 0}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    IsAcked.

mark_inflight_items_as_retriable(Data, WorkerMRef) ->
    #{inflight_tid := InflightTID} = Data,
    IsRetriable = true,
    MatchSpec =
        ets:fun2ms(
            fun(?INFLIGHT_ITEM(Ref, BatchOrQuery, _IsRetriable, WorkerMRef0)) when
                WorkerMRef =:= WorkerMRef0
            ->
                ?INFLIGHT_ITEM(Ref, BatchOrQuery, IsRetriable, WorkerMRef0)
            end
        ),
    _NumAffected = ets:select_replace(InflightTID, MatchSpec),
    ?tp(buffer_worker_worker_down_update, #{num_affected => _NumAffected}),
    ok.

%% used to update a batch after dropping expired individual queries.
update_inflight_item(InflightTID, Ref, NewBatch) ->
    _ = ets:update_element(InflightTID, Ref, {?ITEM_IDX, NewBatch}),
    ?tp(buffer_worker_worker_update_inflight_item, #{ref => Ref}),
    ok.

%%==============================================================================

inc_sent_failed(Id, _HasBeenSent = true) ->
    emqx_resource_metrics:retried_failed_inc(Id);
inc_sent_failed(Id, _HasBeenSent) ->
    emqx_resource_metrics:failed_inc(Id).

inc_sent_success(Id, _HasBeenSent = true) ->
    emqx_resource_metrics:retried_success_inc(Id);
inc_sent_success(Id, _HasBeenSent) ->
    emqx_resource_metrics:success_inc(Id).

call_mode(sync, _) -> sync;
call_mode(async, always_sync) -> sync;
call_mode(async, async_if_possible) -> async.

assert_ok_result(ok) ->
    true;
assert_ok_result({async_return, R}) ->
    assert_ok_result(R);
assert_ok_result(R) when is_tuple(R) ->
    try
        ok = erlang:element(1, R)
    catch
        error:{badmatch, _} ->
            error({not_ok_result, R})
    end;
assert_ok_result(R) ->
    error({not_ok_result, R}).

queue_count(Q) ->
    replayq:count(Q).

disk_queue_dir(Id, Index) ->
    QDir0 = binary_to_list(Id) ++ ":" ++ integer_to_list(Index),
    QDir = filename:join([emqx:data_dir(), "bufs", node(), QDir0]),
    emqx_misc:safe_filename(QDir).

clear_disk_queue_dir(Id, Index) ->
    ReplayQDir = disk_queue_dir(Id, Index),
    case file:del_dir_r(ReplayQDir) of
        {error, enoent} ->
            ok;
        Res ->
            Res
    end.

ensure_flush_timer(Data = #{batch_time := T}) ->
    ensure_flush_timer(Data, T).

ensure_flush_timer(Data = #{tref := undefined}, T) ->
    Ref = make_ref(),
    TRef = erlang:send_after(T, self(), {flush, Ref}),
    Data#{tref => {TRef, Ref}};
ensure_flush_timer(Data, _T) ->
    Data.

cancel_flush_timer(St = #{tref := undefined}) ->
    St;
cancel_flush_timer(St = #{tref := {TRef, _Ref}}) ->
    _ = erlang:cancel_timer(TRef),
    St#{tref => undefined}.

-spec make_request_ref() -> inflight_key().
make_request_ref() ->
    now_().

collect_requests(Acc, Limit) ->
    Count = length(Acc),
    do_collect_requests(Acc, Count, Limit).

do_collect_requests(Acc, Count, Limit) when Count >= Limit ->
    lists:reverse(Acc);
do_collect_requests(Acc, Count, Limit) ->
    receive
        ?SEND_REQ(_ReplyTo, _Req) = Request ->
            do_collect_requests([Request | Acc], Count + 1, Limit)
    after 0 ->
        lists:reverse(Acc)
    end.

mark_as_sent(Batch) when is_list(Batch) ->
    lists:map(fun mark_as_sent/1, Batch);
mark_as_sent(?QUERY(ReplyTo, Req, _HasBeenSent, ExpireAt)) ->
    HasBeenSent = true,
    ?QUERY(ReplyTo, Req, HasBeenSent, ExpireAt).

is_unrecoverable_error({error, {unrecoverable_error, _}}) ->
    true;
is_unrecoverable_error({error, {recoverable_error, _}}) ->
    false;
is_unrecoverable_error({async_return, Result}) ->
    is_unrecoverable_error(Result);
is_unrecoverable_error({error, _}) ->
    %% TODO: delete this clause.
    %% Ideally all errors except for 'unrecoverable_error' should be
    %% retried, including DB schema errors.
    true;
is_unrecoverable_error(_) ->
    false.

is_async_return({async_return, _}) ->
    true;
is_async_return(_) ->
    false.

sieve_expired_requests(Batch, Now) ->
    {Expired, NotExpired} =
        lists:partition(
            fun(?QUERY(_ReplyTo, _CoreReq, _HasBeenSent, ExpireAt)) ->
                is_expired(ExpireAt, Now)
            end,
            Batch
        ),
    case {NotExpired, Expired} of
        {[], []} ->
            %% Should be impossible for batch_size >= 1.
            all_expired;
        {[], [_ | _]} ->
            all_expired;
        {[_ | _], _} ->
            {NotExpired, Expired}
    end.

-spec is_expired(infinity | integer(), integer()) -> boolean().
is_expired(infinity = _ExpireAt, _Now) ->
    false;
is_expired(ExpireAt, Now) ->
    Now > ExpireAt.

now_() ->
    erlang:monotonic_time(nanosecond).

-spec ensure_timeout_query_opts(query_opts(), sync | async) -> query_opts().
ensure_timeout_query_opts(#{timeout := _} = Opts, _SyncOrAsync) ->
    Opts;
ensure_timeout_query_opts(#{} = Opts0, sync) ->
    Opts0#{timeout => ?DEFAULT_REQUEST_TIMEOUT};
ensure_timeout_query_opts(#{} = Opts0, async) ->
    Opts0#{timeout => infinity}.

-spec ensure_expire_at(query_opts()) -> query_opts().
ensure_expire_at(#{expire_at := _} = Opts) ->
    Opts;
ensure_expire_at(#{timeout := infinity} = Opts) ->
    Opts#{expire_at => infinity};
ensure_expire_at(#{timeout := TimeoutMS} = Opts) ->
    TimeoutNS = erlang:convert_time_unit(TimeoutMS, millisecond, nanosecond),
    ExpireAt = now_() + TimeoutNS,
    Opts#{expire_at => ExpireAt}.

%% no need to keep the request for async reply handler
minimize(?QUERY(_, _, _, _) = Q) ->
    do_minimize(Q);
minimize(L) when is_list(L) ->
    lists:map(fun do_minimize/1, L).

-ifdef(TEST).
do_minimize(?QUERY(_ReplyTo, _Req, _Sent, _ExpireAt) = Query) -> Query.
-else.
do_minimize(?QUERY(ReplyTo, _Req, Sent, ExpireAt)) -> ?QUERY(ReplyTo, [], Sent, ExpireAt).
-endif.
