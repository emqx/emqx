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

-module(emqx_resource_worker).

-include("emqx_resource.hrl").
-include("emqx_resource_utils.hrl").
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

-export([reply_after_query/7, batch_reply_after_query/7]).

-export([clear_disk_queue_dir/2]).

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-define(COLLECT_REQ_LIMIT, 1000).
-define(SEND_REQ(FROM, REQUEST), {'$send_req', FROM, REQUEST}).
-define(QUERY(FROM, REQUEST, SENT), {query, FROM, REQUEST, SENT}).
-define(REPLY(FROM, REQUEST, SENT, RESULT), {reply, FROM, REQUEST, SENT, RESULT}).
-define(EXPAND(RESULT, BATCH), [
    ?REPLY(FROM, REQUEST, SENT, RESULT)
 || ?QUERY(FROM, REQUEST, SENT) <- BATCH
]).
-define(INFLIGHT_ITEM(Ref, BatchOrQuery, IsRetriable, WorkerMRef),
    {Ref, BatchOrQuery, IsRetriable, WorkerMRef}
).
-define(RETRY_IDX, 3).
-define(WORKER_MREF_IDX, 4).

-type id() :: binary().
-type index() :: pos_integer().
-type queue_query() :: ?QUERY(from(), request(), HasBeenSent :: boolean()).
-type request() :: term().
-type from() :: pid() | reply_fun() | request_from().
-type request_from() :: undefined | gen_statem:from().
-type state() :: blocked | running.
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
sync_query(Id, Request, Opts) ->
    PickKey = maps:get(pick_key, Opts, self()),
    Timeout = maps:get(timeout, Opts, timer:seconds(15)),
    emqx_resource_metrics:matched_inc(Id),
    pick_call(Id, PickKey, {query, Request, Opts}, Timeout).

-spec async_query(id(), request(), query_opts()) -> Result :: term().
async_query(Id, Request, Opts) ->
    PickKey = maps:get(pick_key, Opts, self()),
    emqx_resource_metrics:matched_inc(Id),
    pick_cast(Id, PickKey, {query, Request, Opts}).

%% simple query the resource without batching and queuing messages.
-spec simple_sync_query(id(), request()) -> Result :: term().
simple_sync_query(Id, Request) ->
    %% Note: since calling this function implies in bypassing the
    %% buffer workers, and each buffer worker index is used when
    %% collecting gauge metrics, we use this dummy index.  If this
    %% call ends up calling buffering functions, that's a bug and
    %% would mess up the metrics anyway.  `undefined' is ignored by
    %% `emqx_resource_metrics:*_shift/3'.
    Index = undefined,
    QueryOpts = #{perform_inflight_capacity_check => false},
    emqx_resource_metrics:matched_inc(Id),
    Ref = make_message_ref(),
    Result = call_query(sync, Id, Index, Ref, ?QUERY(self(), Request, false), QueryOpts),
    HasBeenSent = false,
    _ = handle_query_result(Id, Result, HasBeenSent),
    Result.

-spec simple_async_query(id(), request(), reply_fun()) -> Result :: term().
simple_async_query(Id, Request, ReplyFun) ->
    %% Note: since calling this function implies in bypassing the
    %% buffer workers, and each buffer worker index is used when
    %% collecting gauge metrics, we use this dummy index.  If this
    %% call ends up calling buffering functions, that's a bug and
    %% would mess up the metrics anyway.  `undefined' is ignored by
    %% `emqx_resource_metrics:*_shift/3'.
    Index = undefined,
    QueryOpts = #{perform_inflight_capacity_check => false},
    emqx_resource_metrics:matched_inc(Id),
    Ref = make_message_ref(),
    Result = call_query(async, Id, Index, Ref, ?QUERY(ReplyFun, Request, false), QueryOpts),
    HasBeenSent = false,
    _ = handle_query_result(Id, Result, HasBeenSent),
    Result.

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
    ?tp(resource_worker_init, #{id => Id, index => Index}),
    {ok, running, Data}.

running(enter, _, St) ->
    ?tp(resource_worker_enter_running, #{}),
    maybe_flush(St);
running(cast, resume, _St) ->
    keep_state_and_data;
running(cast, flush, Data) ->
    flush(Data);
running(cast, block, St) ->
    {next_state, blocked, St};
running(info, ?SEND_REQ(_From, _Req) = Request0, Data) ->
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
    ?tp(resource_worker_enter_blocked, #{}),
    {keep_state_and_data, {state_timeout, ResumeT, unblock}};
blocked(cast, block, _St) ->
    keep_state_and_data;
blocked(cast, resume, St) ->
    resume_from_blocked(St);
blocked(cast, flush, Data) ->
    resume_from_blocked(Data);
blocked(state_timeout, unblock, St) ->
    resume_from_blocked(St);
blocked(info, ?SEND_REQ(_ReqFrom, {query, _Request, _Opts}) = Request0, Data0) ->
    {_Queries, Data} = collect_and_enqueue_query_requests(Request0, Data0),
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
    replayq:close(Q),
    emqx_resource_metrics:inflight_set(Id, Index, 0),
    %% since we want volatile queues, this will be 0 after
    %% termination.
    emqx_resource_metrics:queuing_set(Id, Index, 0),
    gproc_pool:disconnect_worker(Id, {Id, Index}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%==============================================================================
-define(PICK(ID, KEY, EXPR),
    try gproc_pool:pick_worker(ID, KEY) of
        Pid when is_pid(Pid) ->
            EXPR;
        _ ->
            ?RESOURCE_ERROR(worker_not_created, "resource not created")
    catch
        error:badarg ->
            ?RESOURCE_ERROR(worker_not_created, "resource not created");
        exit:{timeout, _} ->
            ?RESOURCE_ERROR(timeout, "call resource timeout")
    end
).

pick_call(Id, Key, Query, Timeout) ->
    ?PICK(Id, Key, begin
        Caller = self(),
        MRef = erlang:monitor(process, Pid, [{alias, reply_demonitor}]),
        From = {Caller, MRef},
        erlang:send(Pid, ?SEND_REQ(From, Query)),
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
    ?PICK(Id, Key, begin
        From = undefined,
        erlang:send(Pid, ?SEND_REQ(From, Query)),
        ok
    end).

resume_from_blocked(Data) ->
    #{inflight_tid := InflightTID} = Data,
    case inflight_get_first_retriable(InflightTID) of
        none ->
            case is_inflight_full(InflightTID) of
                true ->
                    {keep_state, Data};
                false ->
                    {next_state, running, Data}
            end;
        {Ref, FirstQuery} ->
            %% We retry msgs in inflight window sync, as if we send them
            %% async, they will be appended to the end of inflight window again.
            case is_inflight_full(InflightTID) of
                true ->
                    {keep_state, Data};
                false ->
                    retry_inflight_sync(Ref, FirstQuery, Data)
            end
    end.

retry_inflight_sync(Ref, QueryOrBatch, Data0) ->
    #{
        id := Id,
        inflight_tid := InflightTID,
        index := Index,
        resume_interval := ResumeT
    } = Data0,
    ?tp(resource_worker_retry_inflight, #{query_or_batch => QueryOrBatch, ref => Ref}),
    QueryOpts = #{},
    Result = call_query(sync, Id, Index, Ref, QueryOrBatch, QueryOpts),
    ReplyResult =
        case QueryOrBatch of
            ?QUERY(From, CoreReq, HasBeenSent) ->
                Reply = ?REPLY(From, CoreReq, HasBeenSent, Result),
                reply_caller_defer_metrics(Id, Reply);
            [?QUERY(_, _, _) | _] = Batch ->
                batch_reply_caller_defer_metrics(Id, Result, Batch)
        end,
    case ReplyResult of
        %% Send failed because resource is down
        {nack, PostFn} ->
            PostFn(),
            ?tp(
                resource_worker_retry_inflight_failed,
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
                resource_worker_retry_inflight_succeeded,
                #{
                    ref => Ref,
                    query_or_batch => QueryOrBatch
                }
            ),
            resume_from_blocked(Data0)
    end.

%% Called during the `running' state only.
-spec handle_query_requests(?SEND_REQ(request_from(), request()), data()) -> data().
handle_query_requests(Request0, Data0) ->
    {_Queries, Data} = collect_and_enqueue_query_requests(Request0, Data0),
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
                (?SEND_REQ(undefined = _From, {query, Req, Opts})) ->
                    ReplyFun = maps:get(async_reply_fun, Opts, undefined),
                    HasBeenSent = false,
                    ?QUERY(ReplyFun, Req, HasBeenSent);
                (?SEND_REQ(From, {query, Req, _Opts})) ->
                    HasBeenSent = false,
                    ?QUERY(From, Req, HasBeenSent)
            end,
            Requests
        ),
    NewQ = append_queue(Id, Index, Q, Queries),
    Data = Data0#{queue := NewQ},
    {Queries, Data}.

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
        batch_size := BatchSize,
        inflight_tid := InflightTID,
        queue := Q0
    } = Data0,
    Data1 = cancel_flush_timer(Data0),
    case {queue_count(Q0), is_inflight_full(InflightTID)} of
        {0, _} ->
            {keep_state, Data1};
        {_, true} ->
            ?tp(resource_worker_flush_but_inflight_full, #{}),
            Data2 = ensure_flush_timer(Data1),
            {keep_state, Data2};
        {_, false} ->
            {Q1, QAckRef, Batch} = replayq:pop(Q0, #{count_limit => BatchSize}),
            IsBatch = BatchSize =/= 1,
            %% We *must* use the new queue, because we currently can't
            %% `nack' a `pop'.
            %% Maybe we could re-open the queue?
            Data2 = Data1#{queue := Q1},
            Ref = make_message_ref(),
            do_flush(Data2, #{
                new_queue => Q1,
                is_batch => IsBatch,
                batch => Batch,
                ref => Ref,
                ack_ref => QAckRef
            })
    end.

-spec do_flush(data(), #{
    is_batch := boolean(),
    batch := [?QUERY(from(), request(), boolean())],
    ack_ref := replayq:ack_ref()
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
    [?QUERY(From, CoreReq, HasBeenSent) = Request] = Batch,
    QueryOpts = #{inflight_tid => InflightTID},
    Result = call_query(configured, Id, Index, Ref, Request, QueryOpts),
    Reply = ?REPLY(From, CoreReq, HasBeenSent, Result),
    case reply_caller(Id, Reply) of
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
                resource_worker_flush_nack,
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
            is_async(Id) orelse ack_inflight(InflightTID, Ref, Id, Index),
            {Data1, WorkerMRef} = ensure_async_worker_monitored(Data0, Result),
            store_async_worker_reference(InflightTID, Ref, WorkerMRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            ?tp(
                resource_worker_flush_ack,
                #{
                    batch_or_query => Request,
                    result => Result
                }
            ),
            case queue_count(Q1) > 0 of
                true ->
                    {keep_state, Data1, [{next_event, internal, flush}]};
                false ->
                    {keep_state, Data1}
            end
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
    QueryOpts = #{inflight_tid => InflightTID},
    Result = call_query(configured, Id, Index, Ref, Batch, QueryOpts),
    case batch_reply_caller(Id, Result, Batch) of
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
                resource_worker_flush_nack,
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
            is_async(Id) orelse ack_inflight(InflightTID, Ref, Id, Index),
            {Data1, WorkerMRef} = ensure_async_worker_monitored(Data0, Result),
            store_async_worker_reference(InflightTID, Ref, WorkerMRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            ?tp(
                resource_worker_flush_ack,
                #{
                    batch_or_query => Batch,
                    result => Result
                }
            ),
            CurrentCount = queue_count(Q1),
            case {CurrentCount > 0, CurrentCount >= BatchSize} of
                {false, _} ->
                    {keep_state, Data1};
                {true, true} ->
                    {keep_state, Data1, [{next_event, internal, flush}]};
                {true, false} ->
                    Data2 = ensure_flush_timer(Data1),
                    {keep_state, Data2}
            end
    end.

batch_reply_caller(Id, BatchResult, Batch) ->
    {ShouldBlock, PostFn} = batch_reply_caller_defer_metrics(Id, BatchResult, Batch),
    PostFn(),
    ShouldBlock.

batch_reply_caller_defer_metrics(Id, BatchResult, Batch) ->
    {ShouldAck, PostFns} =
        lists:foldl(
            fun(Reply, {_ShouldAck, PostFns}) ->
                {ShouldAck, PostFn} = reply_caller_defer_metrics(Id, Reply),
                {ShouldAck, [PostFn | PostFns]}
            end,
            {ack, []},
            %% the `Mod:on_batch_query/3` returns a single result for a batch,
            %% so we need to expand
            ?EXPAND(BatchResult, Batch)
        ),
    PostFn = fun() -> lists:foreach(fun(F) -> F() end, PostFns) end,
    {ShouldAck, PostFn}.

reply_caller(Id, Reply) ->
    {ShouldAck, PostFn} = reply_caller_defer_metrics(Id, Reply),
    PostFn(),
    ShouldAck.

%% Should only reply to the caller when the decision is final (not
%% retriable).  See comment on `handle_query_result_pure'.
reply_caller_defer_metrics(Id, ?REPLY(undefined, _, HasBeenSent, Result)) ->
    handle_query_result_pure(Id, Result, HasBeenSent);
reply_caller_defer_metrics(Id, ?REPLY({ReplyFun, Args}, _, HasBeenSent, Result)) when
    is_function(ReplyFun)
->
    {ShouldAck, PostFn} = handle_query_result_pure(Id, Result, HasBeenSent),
    case {ShouldAck, Result} of
        {nack, _} ->
            ok;
        {ack, {async_return, _}} ->
            ok;
        {ack, _} ->
            apply(ReplyFun, Args ++ [Result]),
            ok
    end,
    {ShouldAck, PostFn};
reply_caller_defer_metrics(Id, ?REPLY(From, _, HasBeenSent, Result)) ->
    {ShouldAck, PostFn} = handle_query_result_pure(Id, Result, HasBeenSent),
    case {ShouldAck, Result} of
        {nack, _} ->
            ok;
        {ack, {async_return, _}} ->
            ok;
        {ack, _} ->
            gen_statem:reply(From, Result),
            ok
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
        %% inc_sent_failed(Id, HasBeenSent),
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
        %% emqx_resource_metrics:dropped_other_inc(Id),
        ok
    end,
    {nack, PostFn};
%% TODO: invert this logic: we should differentiate errors that are
%% irrecoverable; all others are deemed recoverable.
handle_query_result_pure(Id, {error, {recoverable_error, Reason}}, _HasBeenSent) ->
    %% the message will be queued in replayq or inflight window,
    %% i.e. the counter 'queuing' or 'dropped' will increase, so we pretend that we have not
    %% sent this message.
    PostFn = fun() ->
        ?SLOG(warning, #{id => Id, msg => recoverable_error, reason => Reason}),
        ok
    end,
    {nack, PostFn};
handle_query_result_pure(Id, {error, Reason}, _HasBeenSent) ->
    PostFn = fun() ->
        ?SLOG(error, #{id => Id, msg => send_error, reason => Reason}),
        ok
    end,
    {nack, PostFn};
handle_query_result_pure(Id, {async_return, {error, Msg}}, _HasBeenSent) ->
    PostFn = fun() ->
        ?SLOG(error, #{id => Id, msg => async_send_error, info => Msg}),
        ok
    end,
    {nack, PostFn};
handle_query_result_pure(_Id, {async_return, ok}, _HasBeenSent) ->
    {ack, fun() -> ok end};
handle_query_result_pure(_Id, {async_return, {ok, Pid}}, _HasBeenSent) when is_pid(Pid) ->
    {ack, fun() -> ok end};
handle_query_result_pure(Id, Result, HasBeenSent) ->
    PostFn = fun() ->
        assert_ok_result(Result),
        inc_sent_success(Id, HasBeenSent),
        ok
    end,
    {ack, PostFn}.

handle_async_worker_down(Data0, Pid) ->
    #{async_workers := AsyncWorkers0} = Data0,
    {WorkerMRef, AsyncWorkers} = maps:take(Pid, AsyncWorkers0),
    Data = Data0#{async_workers := AsyncWorkers},
    cancel_inflight_items(Data, WorkerMRef),
    {keep_state, Data}.

call_query(QM0, Id, Index, Ref, Query, QueryOpts) ->
    ?tp(call_query_enter, #{id => Id, query => Query}),
    case emqx_resource_manager:ets_lookup(Id) of
        {ok, _Group, #{mod := Mod, state := ResSt, status := connected} = Data} ->
            QM =
                case QM0 of
                    configured -> maps:get(query_mode, Data);
                    _ -> QM0
                end,
            CBM = maps:get(callback_mode, Data),
            CallMode = call_mode(QM, CBM),
            apply_query_fun(CallMode, Mod, Id, Index, Ref, Query, ResSt, QueryOpts);
        {ok, _Group, #{status := stopped}} ->
            ?RESOURCE_ERROR(stopped, "resource stopped or disabled");
        {ok, _Group, #{status := S}} when S == connecting; S == disconnected ->
            ?RESOURCE_ERROR(not_connected, "resource not connected");
        {error, not_found} ->
            ?RESOURCE_ERROR(not_found, "resource not found")
    end.

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

apply_query_fun(sync, Mod, Id, _Index, _Ref, ?QUERY(_, Request, _) = _Query, ResSt, _QueryOpts) ->
    ?tp(call_query, #{id => Id, mod => Mod, query => _Query, res_st => ResSt, call_mode => sync}),
    ?APPLY_RESOURCE(call_query, Mod:on_query(Id, Request, ResSt), Request);
apply_query_fun(async, Mod, Id, Index, Ref, ?QUERY(_, Request, _) = Query, ResSt, QueryOpts) ->
    ?tp(call_query_async, #{
        id => Id, mod => Mod, query => Query, res_st => ResSt, call_mode => async
    }),
    InflightTID = maps:get(inflight_tid, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_query_async,
        begin
            ReplyFun = fun ?MODULE:reply_after_query/7,
            Args = [self(), Id, Index, InflightTID, Ref, Query],
            IsRetriable = false,
            WorkerMRef = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Query, IsRetriable, WorkerMRef),
            ok = inflight_append(InflightTID, InflightItem, Id, Index),
            Result = Mod:on_query_async(Id, Request, {ReplyFun, Args}, ResSt),
            {async_return, Result}
        end,
        Request
    );
apply_query_fun(sync, Mod, Id, _Index, _Ref, [?QUERY(_, _, _) | _] = Batch, ResSt, _QueryOpts) ->
    ?tp(call_batch_query, #{
        id => Id, mod => Mod, batch => Batch, res_st => ResSt, call_mode => sync
    }),
    Requests = [Request || ?QUERY(_From, Request, _) <- Batch],
    ?APPLY_RESOURCE(call_batch_query, Mod:on_batch_query(Id, Requests, ResSt), Batch);
apply_query_fun(async, Mod, Id, Index, Ref, [?QUERY(_, _, _) | _] = Batch, ResSt, QueryOpts) ->
    ?tp(call_batch_query_async, #{
        id => Id, mod => Mod, batch => Batch, res_st => ResSt, call_mode => async
    }),
    InflightTID = maps:get(inflight_tid, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_batch_query_async,
        begin
            ReplyFun = fun ?MODULE:batch_reply_after_query/7,
            ReplyFunAndArgs = {ReplyFun, [self(), Id, Index, InflightTID, Ref, Batch]},
            Requests = [Request || ?QUERY(_From, Request, _) <- Batch],
            IsRetriable = false,
            WorkerMRef = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Batch, IsRetriable, WorkerMRef),
            ok = inflight_append(InflightTID, InflightItem, Id, Index),
            Result = Mod:on_batch_query_async(Id, Requests, ReplyFunAndArgs, ResSt),
            {async_return, Result}
        end,
        Batch
    ).

reply_after_query(Pid, Id, Index, InflightTID, Ref, ?QUERY(From, Request, HasBeenSent), Result) ->
    %% NOTE: 'inflight' is the count of messages that were sent async
    %% but received no ACK, NOT the number of messages queued in the
    %% inflight window.
    {Action, PostFn} = reply_caller_defer_metrics(Id, ?REPLY(From, Request, HasBeenSent, Result)),
    case Action of
        nack ->
            %% Keep retrying.
            ?tp(resource_worker_reply_after_query, #{
                action => Action,
                batch_or_query => ?QUERY(From, Request, HasBeenSent),
                ref => Ref,
                result => Result
            }),
            mark_inflight_as_retriable(InflightTID, Ref),
            ?MODULE:block(Pid);
        ack ->
            ?tp(resource_worker_reply_after_query, #{
                action => Action,
                batch_or_query => ?QUERY(From, Request, HasBeenSent),
                ref => Ref,
                result => Result
            }),
            IsFullBefore = is_inflight_full(InflightTID),
            IsAcked = ack_inflight(InflightTID, Ref, Id, Index),
            IsAcked andalso PostFn(),
            IsFullBefore andalso ?MODULE:flush_worker(Pid),
            ok
    end.

batch_reply_after_query(Pid, Id, Index, InflightTID, Ref, Batch, Result) ->
    %% NOTE: 'inflight' is the count of messages that were sent async
    %% but received no ACK, NOT the number of messages queued in the
    %% inflight window.
    {Action, PostFn} = batch_reply_caller_defer_metrics(Id, Result, Batch),
    case Action of
        nack ->
            %% Keep retrying.
            ?tp(resource_worker_reply_after_query, #{
                action => nack,
                batch_or_query => Batch,
                ref => Ref,
                result => Result
            }),
            mark_inflight_as_retriable(InflightTID, Ref),
            ?MODULE:block(Pid);
        ack ->
            ?tp(resource_worker_reply_after_query, #{
                action => ack,
                batch_or_query => Batch,
                ref => Ref,
                result => Result
            }),
            IsFullBefore = is_inflight_full(InflightTID),
            IsAcked = ack_inflight(InflightTID, Ref, Id, Index),
            IsAcked andalso PostFn(),
            IsFullBefore andalso ?MODULE:flush_worker(Pid),
            ok
    end.

%%==============================================================================
%% operations for queue
queue_item_marshaller(Bin) when is_binary(Bin) ->
    binary_to_term(Bin);
queue_item_marshaller(Item) ->
    term_to_binary(Item).

estimate_size(QItem) ->
    erlang:external_size(QItem).

-spec append_queue(id(), index(), replayq:q(), [queue_query()]) -> replayq:q().
append_queue(Id, Index, Q, Queries) when not is_binary(Q) ->
    %% we must not append a raw binary because the marshaller will get
    %% lost.
    Q0 = replayq:append(Q, Queries),
    Q2 =
        case replayq:overflow(Q0) of
            Overflow when Overflow =< 0 ->
                Q0;
            Overflow ->
                PopOpts = #{bytes_limit => Overflow, count_limit => 999999999},
                {Q1, QAckRef, Items2} = replayq:pop(Q0, PopOpts),
                ok = replayq:ack(Q1, QAckRef),
                Dropped = length(Items2),
                emqx_resource_metrics:dropped_queue_full_inc(Id),
                ?SLOG(error, #{msg => drop_query, reason => queue_full, dropped => Dropped}),
                Q1
        end,
    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q2)),
    ?tp(
        resource_worker_appended_to_queue,
        #{
            id => Id,
            items => Queries,
            queue_count => queue_count(Q2)
        }
    ),
    Q2.

%%==============================================================================
%% the inflight queue for async query
-define(MAX_SIZE_REF, max_size).
-define(SIZE_REF, size).
-define(INITIAL_TIME_REF, initial_time).
-define(INITIAL_MONOTONIC_TIME_REF, initial_monotonic_time).

inflight_new(InfltWinSZ, Id, Index) ->
    TableId = ets:new(
        emqx_resource_worker_inflight_tab,
        [ordered_set, public, {write_concurrency, true}]
    ),
    inflight_append(TableId, {?MAX_SIZE_REF, InfltWinSZ}, Id, Index),
    %% we use this counter because we might deal with batches as
    %% elements.
    inflight_append(TableId, {?SIZE_REF, 0}, Id, Index),
    inflight_append(TableId, {?INITIAL_TIME_REF, erlang:system_time()}, Id, Index),
    inflight_append(
        TableId, {?INITIAL_MONOTONIC_TIME_REF, erlang:monotonic_time(nanosecond)}, Id, Index
    ),
    TableId.

-spec inflight_get_first_retriable(ets:tid()) ->
    none | {integer(), [?QUERY(_, _, _)] | ?QUERY(_, _, _)}.
inflight_get_first_retriable(InflightTID) ->
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
        {[{Ref, BatchOrQuery}], _Continuation} ->
            {Ref, BatchOrQuery}
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
    %% Note: we subtract 2 because there're 2 metadata rows that hold
    %% the maximum size value and the number of messages.
    MetadataRowCount = 2,
    case ets:info(InflightTID, size) of
        undefined -> 0;
        Size -> max(0, Size - MetadataRowCount)
    end.

inflight_num_msgs(InflightTID) ->
    [{_, Size}] = ets:lookup(InflightTID, ?SIZE_REF),
    Size.

inflight_append(undefined, _InflightItem, _Id, _Index) ->
    ok;
inflight_append(
    InflightTID,
    ?INFLIGHT_ITEM(Ref, [?QUERY(_, _, _) | _] = Batch0, IsRetriable, WorkerMRef),
    Id,
    Index
) ->
    Batch = mark_as_sent(Batch0),
    InflightItem = ?INFLIGHT_ITEM(Ref, Batch, IsRetriable, WorkerMRef),
    IsNew = ets:insert_new(InflightTID, InflightItem),
    BatchSize = length(Batch),
    IsNew andalso ets:update_counter(InflightTID, ?SIZE_REF, {2, BatchSize}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    ?tp(resource_worker_appended_to_inflight, #{item => InflightItem, is_new => IsNew}),
    ok;
inflight_append(
    InflightTID,
    ?INFLIGHT_ITEM(Ref, ?QUERY(_From, _Req, _HasBeenSent) = Query0, IsRetriable, WorkerMRef),
    Id,
    Index
) ->
    Query = mark_as_sent(Query0),
    InflightItem = ?INFLIGHT_ITEM(Ref, Query, IsRetriable, WorkerMRef),
    IsNew = ets:insert_new(InflightTID, InflightItem),
    IsNew andalso ets:update_counter(InflightTID, ?SIZE_REF, {2, 1}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    ?tp(resource_worker_appended_to_inflight, #{item => InflightItem, is_new => IsNew}),
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
            [?INFLIGHT_ITEM(Ref, ?QUERY(_, _, _), _IsRetriable, _WorkerMRef)] ->
                1;
            [?INFLIGHT_ITEM(Ref, [?QUERY(_, _, _) | _] = Batch, _IsRetriable, _WorkerMRef)] ->
                length(Batch);
            _ ->
                0
        end,
    IsAcked = Count > 0,
    IsAcked andalso ets:update_counter(InflightTID, ?SIZE_REF, {2, -Count, 0, 0}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    IsAcked.

cancel_inflight_items(Data, WorkerMRef) ->
    #{inflight_tid := InflightTID} = Data,
    MatchSpec =
        ets:fun2ms(
            fun(?INFLIGHT_ITEM(Ref, _BatchOrQuery, _IsRetriable, WorkerMRef0)) when
                WorkerMRef =:= WorkerMRef0
            ->
                Ref
            end
        ),
    Refs = ets:select(InflightTID, MatchSpec),
    lists:foreach(fun(Ref) -> do_cancel_inflight_item(Data, Ref) end, Refs).

do_cancel_inflight_item(Data, Ref) ->
    #{id := Id, index := Index, inflight_tid := InflightTID} = Data,
    {Count, Batch} =
        case ets:take(InflightTID, Ref) of
            [?INFLIGHT_ITEM(Ref, ?QUERY(_, _, _) = Query, _IsRetriable, _WorkerMRef)] ->
                {1, [Query]};
            [?INFLIGHT_ITEM(Ref, [?QUERY(_, _, _) | _] = Batch0, _IsRetriable, _WorkerMRef)] ->
                {length(Batch0), Batch0};
            _ ->
                {0, []}
        end,
    IsAcked = Count > 0,
    IsAcked andalso ets:update_counter(InflightTID, ?SIZE_REF, {2, -Count, 0, 0}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    Result = {error, interrupted},
    _ = batch_reply_caller(Id, Result, Batch),
    ?tp(resource_worker_cancelled_inflight, #{ref => Ref}),
    ok.

%%==============================================================================

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
    QDir0 = binary_to_list(Id) ++ "_" ++ integer_to_list(Index),
    QDir = sanitize_file_path(QDir0),
    filename:join([emqx:data_dir(), "bufs", node(), QDir]).

sanitize_file_path(Filepath) ->
    iolist_to_binary(string:replace(Filepath, ":", "_", all)).

clear_disk_queue_dir(Id, Index) ->
    ReplayQDir = disk_queue_dir(Id, Index),
    case file:del_dir_r(ReplayQDir) of
        {error, enoent} ->
            ok;
        Res ->
            Res
    end.

ensure_flush_timer(Data = #{tref := undefined, batch_time := T}) ->
    Ref = make_ref(),
    TRef = erlang:send_after(T, self(), {flush, Ref}),
    Data#{tref => {TRef, Ref}};
ensure_flush_timer(Data) ->
    Data.

cancel_flush_timer(St = #{tref := undefined}) ->
    St;
cancel_flush_timer(St = #{tref := {TRef, _Ref}}) ->
    _ = erlang:cancel_timer(TRef),
    St#{tref => undefined}.

make_message_ref() ->
    erlang:monotonic_time(nanosecond).

collect_requests(Acc, Limit) ->
    Count = length(Acc),
    do_collect_requests(Acc, Count, Limit).

do_collect_requests(Acc, Count, Limit) when Count >= Limit ->
    lists:reverse(Acc);
do_collect_requests(Acc, Count, Limit) ->
    receive
        ?SEND_REQ(_From, _Req) = Request ->
            do_collect_requests([Request | Acc], Count + 1, Limit)
    after 0 ->
        lists:reverse(Acc)
    end.

mark_as_sent(Batch) when is_list(Batch) ->
    lists:map(fun mark_as_sent/1, Batch);
mark_as_sent(?QUERY(From, Req, _)) ->
    HasBeenSent = true,
    ?QUERY(From, Req, HasBeenSent).

is_async(ResourceId) ->
    case emqx_resource_manager:ets_lookup(ResourceId) of
        {ok, _Group, #{query_mode := QM, callback_mode := CM}} ->
            call_mode(QM, CM) =:= async;
        _ ->
            false
    end.
