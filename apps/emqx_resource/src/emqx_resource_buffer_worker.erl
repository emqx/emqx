%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_resource_runtime.hrl").
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
    simple_sync_query/3,
    simple_async_query/3,
    simple_sync_internal_buffer_query/3
]).

-export([
    callback_mode/0,
    init/1,
    terminate/2,
    code_change/3
]).

-export([running/3, blocked/3]).

-export([queue_item_marshaller/1, estimate_size/1]).

-export([
    handle_async_reply/2, handle_async_batch_reply/2, reply_call/2, reply_call_internal_buffer/3
]).

-export([clear_disk_queue_dir/2]).

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-define(COLLECT_REQ_LIMIT, 1000).
-define(SEND_REQ(FROM, REQUEST), {'$send_req', FROM, REQUEST}).
-define(QUERY(FROM, REQUEST, SENT, EXPIRE_AT, TRACE_CTX),
    {query, FROM, REQUEST, SENT, EXPIRE_AT, TRACE_CTX}
).
-define(SIMPLE_QUERY(FROM, REQUEST, TRACE_CTX), ?QUERY(FROM, REQUEST, false, infinity, TRACE_CTX)).
-define(REPLY(FROM, SENT, RESULT, TRACE_CTX), {reply, FROM, SENT, RESULT, TRACE_CTX}).
-define(INFLIGHT_ITEM(Ref, BatchOrQuery, IsRetriable, AsyncWorkerMRef),
    {Ref, BatchOrQuery, IsRetriable, AsyncWorkerMRef}
).
-define(ITEM_IDX, 2).
-define(RETRY_IDX, 3).
-define(WORKER_MREF_IDX, 4).

-type id() :: binary().
-type index() :: pos_integer().
-type expire_at() :: infinity | integer().
-type trace_context() :: map() | undefined.
-type queue_query() :: ?QUERY(
    reply_fun(), request(), HasBeenSent :: boolean(), expire_at(), TraceCtx :: trace_context()
).
-type request() :: term().
-type request_from() :: undefined | gen_statem:from().
-type timeout_ms() :: emqx_schema:timeout_duration_ms().
-type request_ttl() :: emqx_schema:timeout_duration_ms().
-type health_check_interval() :: pos_integer().
-type state() :: blocked | running.
-type inflight_key() :: integer().
-type counters() :: #{
    dropped_expired => non_neg_integer(),
    dropped_queue_full => non_neg_integer(),
    dropped_resource_not_found => non_neg_integer(),
    dropped_resource_stopped => non_neg_integer(),
    success => non_neg_integer(),
    failed => non_neg_integer(),
    retried_success => non_neg_integer(),
    retried_failed => non_neg_integer()
}.
-type inflight_table() :: ets:tid().
-type data() :: #{
    id := id(),
    index := index(),
    inflight_tid := inflight_table(),
    async_workers := #{pid() => reference()},
    batch_size := pos_integer(),
    batch_time := timeout_ms(),
    counters := counters(),
    metrics_flush_interval := timeout_ms(),
    queue := replayq:q(),
    resume_interval := timeout_ms(),
    tref := undefined | {reference(), reference()},
    metrics_tref := undefined | {reference(), reference()}
}.

-define(NO_INFLIGHT, []).
-define(NO_INDEX, []).
-define(NO_REQ_REF, []).
-define(NO_BUFFER_WORKER, []).
-define(IS_BYPASS(Mode),
    (Mode =:= simple_sync_internal_buffer orelse Mode =:= simple_async_internal_buffer)
).

callback_mode() -> [state_functions, state_enter].

start_link(Id, Index, Opts) ->
    gen_statem:start_link(?MODULE, {Id, Index, Opts}, []).

-spec sync_query(id(), request(), query_opts()) -> Result :: term().
sync_query(Id, Request, Opts0) ->
    ?tp(sync_query, #{id => Id, request => Request, query_opts => Opts0}),
    Opts1 = ensure_timeout_query_opts(Opts0, sync),
    Opts = ensure_expire_at(Opts1),
    PickKey = maps:get(pick_key, Opts, self()),
    Timeout = maps:get(timeout, Opts),
    emqx_resource_metrics:matched_inc(Id),
    pick_call(Id, PickKey, {query, Request, Opts}, Timeout).

-spec async_query(id(), request(), query_opts()) -> Result :: term().
async_query(Id, Request, Opts0) ->
    ?tp(async_query, #{id => Id, request => Request, query_opts => Opts0}),
    Opts1 = ensure_timeout_query_opts(Opts0, async),
    Opts = ensure_expire_at(Opts1),
    PickKey = maps:get(pick_key, Opts, self()),
    emqx_resource_metrics:matched_inc(Id),
    pick_cast(Id, PickKey, {query, Request, Opts}).

%% simple query the resource without batching and queuing.
-spec simple_sync_query(id(), request()) -> term().
simple_sync_query(Id, Request) ->
    simple_sync_query(Id, Request, #{}).

-spec simple_sync_query(id(), request(), query_opts()) -> term().
simple_sync_query(Id, Request, QueryOpts0) ->
    %% Note: since calling this function implies in bypassing the
    %% buffer workers, and each buffer worker index is used when
    %% collecting gauge metrics, we use this dummy index.  If this
    %% call ends up calling buffering functions, that's a bug and
    %% would mess up the metrics anyway.  `undefined' is ignored by
    %% `emqx_resource_metrics:*_shift/3'.
    ?tp(simple_sync_query, #{id => Id, request => Request}),
    QueryOpts = maps:merge(simple_sync_query_opts(), QueryOpts0),
    emqx_resource_metrics:matched_inc(Id),
    ReplyTo = maps:get(reply_to, QueryOpts0, undefined),
    TraceCtx = maps:get(trace_ctx, QueryOpts0, undefined),
    Result = call_query(
        force_sync, Id, ?NO_INDEX, ?NO_REQ_REF, ?SIMPLE_QUERY(ReplyTo, Request, TraceCtx), QueryOpts
    ),
    _ = handle_query_result(Id, Result, _HasBeenSent = false),
    Result.

%% simple async-query the resource without batching and queuing.
-spec simple_async_query(id(), request(), query_opts()) -> term().
simple_async_query(Id, Request, QueryOpts0) ->
    ?tp(simple_async_query, #{id => Id, request => Request, query_opts => QueryOpts0}),
    QueryOpts = maps:merge(simple_async_query_opts(), QueryOpts0),
    emqx_resource_metrics:matched_inc(Id),
    ReplyTo = maps:get(reply_to, QueryOpts0, undefined),
    TraceCtx = maps:get(trace_ctx, QueryOpts0, undefined),
    Result = call_query(
        async_if_possible,
        Id,
        ?NO_INDEX,
        ?NO_REQ_REF,
        ?SIMPLE_QUERY(ReplyTo, Request, TraceCtx),
        QueryOpts
    ),
    _ = handle_query_result(Id, Result, _HasBeenSent = false),
    Result.

%% This is a hack to handle cases where the underlying connector has internal buffering
%% (e.g.: Kafka and Pulsar producers).  Since the message may be inernally retried at a
%% later time, we can't bump metrics immediatelly if the return value is not a success
%% (e.g.: if the call timed out, but the message was enqueued nevertheless).
-spec simple_sync_internal_buffer_query(id(), request(), query_opts()) -> term().
simple_sync_internal_buffer_query(Id, Request, QueryOpts0) ->
    ?tp(simple_sync_internal_buffer_query, #{id => Id, request => Request, query_opts => QueryOpts0}),
    ReplyAlias = alias([reply]),
    try
        MaybeReplyTo = maps:get(reply_to, QueryOpts0, undefined),
        QueryOpts1 = QueryOpts0#{
            reply_to => {fun ?MODULE:reply_call_internal_buffer/3, [ReplyAlias, MaybeReplyTo]}
        },
        QueryOpts = #{timeout := Timeout} = maps:merge(simple_sync_query_opts(), QueryOpts1),
        case simple_async_query(Id, Request, QueryOpts) of
            {error, _} = Error ->
                ?tp("resource_simple_sync_internal_buffer_query_error", #{
                    id => Id, request => Request
                }),
                Error;
            {async_return, {error, _} = Error} ->
                Error;
            {async_return, {ok, _Pid}} ->
                %% TODO: monitor pid
                receive
                    {ReplyAlias, Response} ->
                        Response
                after Timeout ->
                    _ = unalias(ReplyAlias),
                    receive
                        {ReplyAlias, Response} ->
                            Response
                    after 0 ->
                        ?tp("resource_simple_sync_internal_buffer_query_timeout", #{
                            id => Id, request => Request
                        }),
                        {error, timeout}
                    end
                end
        end
    after
        _ = unalias(ReplyAlias),
        receive
            {ReplyAlias, _Response} ->
                %% stale response
                ok
        after 0 ->
            ok
        end
    end.

simple_sync_query_opts() ->
    %% Default `resource_opts.resource_ttl' is 45 seconds, so we use the same default
    %% value here to match that.  No need to set `expire_at', since there's no queuing
    %% involved.
    #{simple_query => true, timeout => 45_000}.

simple_async_query_opts() ->
    %% No need to define `expire_at' nor `timeout', since there's no queuing involved in
    %% simple queries.
    #{simple_query => true}.

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
    QueueOpts = replayq_opts(Id, Index, Opts),
    Queue = replayq:open(QueueOpts),
    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Queue)),
    emqx_resource_metrics:queuing_bytes_set(Id, Index, queue_bytes(Queue)),
    emqx_resource_metrics:inflight_set(Id, Index, 0),
    InflightWinSize = maps:get(inflight_window, Opts, ?DEFAULT_INFLIGHT),
    InflightTID = inflight_new(InflightWinSize),
    HealthCheckInterval = maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL),
    RequestTTL = maps:get(request_ttl, Opts, ?DEFAULT_REQUEST_TTL),
    BatchTime0 = maps:get(batch_time, Opts, ?DEFAULT_BATCH_TIME),
    BatchTime = adjust_batch_time(Id, RequestTTL, BatchTime0),
    DefaultResumeInterval = default_resume_interval(RequestTTL, HealthCheckInterval),
    ResumeInterval = maps:get(resume_interval, Opts, DefaultResumeInterval),
    MetricsFlushInterval = maps:get(metrics_flush_interval, Opts, ?DEFAULT_METRICS_FLUSH_INTERVAL),
    Data0 = #{
        id => Id,
        index => Index,
        inflight_tid => InflightTID,
        async_workers => #{},
        batch_size => BatchSize,
        batch_time => BatchTime,
        counters => #{},
        metrics_flush_interval => MetricsFlushInterval,
        queue => Queue,
        resume_interval => ResumeInterval,
        tref => undefined,
        metrics_tref => undefined
    },
    Data = ensure_metrics_flush_timer(Data0),
    ?tp(buffer_worker_init, #{id => Id, index => Index, queue_opts => QueueOpts}),
    {ok, running, Data}.

running(enter, _, #{tref := _Tref} = Data) ->
    ?tp(buffer_worker_enter_running, #{id => maps:get(id, Data), tref => _Tref}),
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
running(info, {flush, Ref}, Data = #{tref := {_TRef, Ref}}) ->
    flush(Data#{tref := undefined});
running(info, {flush, _Ref}, _Data) ->
    ?tp(discarded_stale_flush, #{}),
    keep_state_and_data;
running(info, {flush_metrics, Ref}, Data0 = #{metrics_tref := {_TRef, Ref}}) ->
    Data = flush_metrics(Data0#{metrics_tref := undefined}),
    {keep_state, Data};
running(info, {flush_metrics, _Ref}, _Data) ->
    keep_state_and_data;
running(info, {'DOWN', _MRef, process, Pid, Reason}, Data0 = #{async_workers := AsyncWorkers0}) when
    is_map_key(Pid, AsyncWorkers0)
->
    ?SLOG(info, #{msg => "async_worker_died", state => running, reason => Reason}, #{tag => ?TAG}),
    handle_async_worker_down(Data0, Pid);
running(info, Info, _St) ->
    ?SLOG(error, #{msg => "unexpected_msg", state => running, info => Info}, #{tag => ?TAG}),
    keep_state_and_data.

blocked(enter, _, #{resume_interval := ResumeT} = St0) ->
    ?tp(buffer_worker_enter_blocked, #{buffer_worker => self()}),
    %% discard the old timer, new timer will be started when entering running state again
    St = cancel_flush_timer(St0),
    {keep_state, St, {state_timeout, ResumeT, unblock}};
blocked(cast, block, _St) ->
    keep_state_and_data;
blocked(cast, resume, St) ->
    resume_from_blocked(St);
blocked(cast, flush, St) ->
    resume_from_blocked(St);
blocked(state_timeout, unblock, St) ->
    resume_from_blocked(St);
blocked(info, ?SEND_REQ(_ReplyTo, _Req) = Request0, Data0) ->
    Data = collect_and_enqueue_query_requests(Request0, Data0),
    {keep_state, Data};
blocked(info, {flush, _Ref}, _Data) ->
    %% ignore stale timer
    keep_state_and_data;
blocked(info, {flush_metrics, Ref}, Data0 = #{metrics_tref := {_TRef, Ref}}) ->
    Data = flush_metrics(Data0#{metrics_tref := undefined}),
    {keep_state, Data};
blocked(info, {flush_metrics, _Ref}, _Data) ->
    keep_state_and_data;
blocked(info, {'DOWN', _MRef, process, Pid, Reason}, Data0 = #{async_workers := AsyncWorkers0}) when
    is_map_key(Pid, AsyncWorkers0)
->
    ?SLOG(info, #{msg => "async_worker_died", state => blocked, reason => Reason}, #{tag => ?TAG}),
    handle_async_worker_down(Data0, Pid);
blocked(info, Info, _Data) ->
    ?SLOG(error, #{msg => "unexpected_msg", state => blocked, info => Info}, #{tag => ?TAG}),
    keep_state_and_data.

terminate(_Reason, #{id := Id, index := Index, queue := Q}) ->
    _ = replayq:close(Q),
    emqx_resource_metrics:inflight_set(Id, Index, 0),
    %% since we want volatile queues, this will be 0 after
    %% termination.
    emqx_resource_metrics:queuing_set(Id, Index, 0),
    emqx_resource_metrics:queuing_bytes_set(Id, Index, 0),
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

pick_call(Id, Key, Query = {_, _, QueryOpts}, Timeout) ->
    ?PICK(Id, Key, Pid, begin
        MRef = erlang:monitor(process, Pid, [{alias, reply_demonitor}]),
        ReplyTo = {fun ?MODULE:reply_call/2, [MRef]},
        erlang:send(Pid, ?SEND_REQ(ReplyTo, Query)),
        receive
            {MRef, Response} ->
                erlang:demonitor(MRef, [flush]),
                maybe_reply_to(Response, QueryOpts);
            {'DOWN', MRef, process, Pid, Reason} ->
                error({worker_down, Reason})
        after Timeout ->
            erlang:demonitor(MRef, [flush]),
            receive
                {MRef, Response} ->
                    maybe_reply_to(Response, QueryOpts)
            after 0 ->
                error(timeout)
            end
        end
    end).

pick_cast(Id, Key, Query = {query, _Request, QueryOpts}) ->
    ?PICK(Id, Key, Pid, begin
        ReplyTo = maps:get(reply_to, QueryOpts, undefined),
        erlang:send(Pid, ?SEND_REQ(ReplyTo, Query)),
        ok
    end).

resume_from_blocked(Data) ->
    ?tp(buffer_worker_resume_from_blocked_enter, #{}),
    #{inflight_tid := InflightTID} = Data,
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
            BufferWorkerPid = self(),
            IsAcked = ack_inflight(InflightTID, Ref, BufferWorkerPid),
            Counters =
                case IsAcked of
                    true -> #{dropped_expired => length(Batch)};
                    false -> #{}
                end,
            batch_reply_dropped(Batch, {error, request_expired}),
            NData = aggregate_counters(Data, Counters),
            ?tp(buffer_worker_retry_expired, #{expired => Batch}),
            resume_from_blocked(NData);
        {single, Ref, Query} ->
            %% We retry msgs in inflight window sync, as if we send them
            %% async, they will be appended to the end of inflight window again.
            retry_inflight_sync(Ref, Query, Data);
        {batch, Ref, NotExpired, []} ->
            retry_inflight_sync(Ref, NotExpired, Data);
        {batch, Ref, NotExpired, Expired} ->
            NumExpired = length(Expired),
            ok = update_inflight_item(InflightTID, Ref, NotExpired, NumExpired),
            batch_reply_dropped(Expired, {error, request_expired}),
            NData = aggregate_counters(Data, #{dropped_expired => NumExpired}),
            ?tp(buffer_worker_retry_expired, #{expired => Expired}),
            %% We retry msgs in inflight window sync, as if we send them
            %% async, they will be appended to the end of inflight window again.
            retry_inflight_sync(Ref, NotExpired, NData)
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
    Result = call_query(force_sync, Id, Index, Ref, QueryOrBatch, QueryOpts),
    {ShouldAck, PostFn, DeltaCounters} =
        case QueryOrBatch of
            ?QUERY(ReplyTo, _, HasBeenSent, _ExpireAt, TraceCtx) ->
                Reply = ?REPLY(ReplyTo, HasBeenSent, Result, TraceCtx),
                reply_caller_defer_metrics(Id, Reply, QueryOpts);
            [?QUERY(_, _, _, _, _) | _] = Batch ->
                batch_reply_caller_defer_metrics(Id, Result, Batch, QueryOpts)
        end,
    Data1 = aggregate_counters(Data0, DeltaCounters),
    case ShouldAck of
        %% Send failed because resource is down
        nack ->
            PostFn(),
            ?tp(
                buffer_worker_retry_inflight_failed,
                #{
                    ref => Ref,
                    query_or_batch => QueryOrBatch,
                    result => Result
                }
            ),
            {keep_state, Data1, {state_timeout, ResumeT, unblock}};
        %% Send ok or failed but the resource is working
        ack ->
            BufferWorkerPid = self(),
            IsAcked = ack_inflight(InflightTID, Ref, BufferWorkerPid),
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
            resume_from_blocked(Data1)
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
                    TraceCtx = maps:get(trace_ctx, Opts, undefined),
                    ?QUERY(ReplyFun, Req, HasBeenSent, ExpireAt, TraceCtx);
                (?SEND_REQ(ReplyTo, {query, Req, Opts})) ->
                    HasBeenSent = false,
                    ExpireAt = maps:get(expire_at, Opts),
                    TraceCtx = maps:get(trace_ctx, Opts, undefined),
                    ?QUERY(ReplyTo, Req, HasBeenSent, ExpireAt, TraceCtx)
            end,
            Requests
        ),
    {Overflown, NewQ, DeltaCounters} = append_queue(Id, Index, Q, Queries),
    ok = reply_overflown(Overflown),
    aggregate_counters(Data0#{queue := NewQ}, DeltaCounters).

reply_overflown([]) ->
    ok;
reply_overflown([?QUERY(ReplyTo, _Req, _HasBeenSent, _ExpireAt, _TraceCtx) | More]) ->
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
    ok;
do_reply_caller({F, Args, _Context}, Result) when is_function(F) ->
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
        batch_size := BatchSize,
        inflight_tid := InflightTID,
        queue := Q0
    } = Data0,
    Data1 = cancel_flush_timer(Data0),
    CurrentCount = queue_count(Q0),
    IsFull = is_inflight_full(InflightTID),
    ?tp_ignore_side_effects_in_prod(buffer_worker_flush, #{
        queued => CurrentCount,
        is_inflight_full => IsFull,
        inflight => inflight_count(InflightTID)
    }),
    case {CurrentCount, IsFull} of
        {0, _} ->
            ?tp_ignore_side_effects_in_prod(buffer_worker_queue_drained, #{
                inflight => inflight_count(InflightTID)
            }),
            {keep_state, Data1};
        {_, true} ->
            ?tp(buffer_worker_flush_but_inflight_full, #{}),
            {keep_state, Data1};
        {_, false} ->
            ?tp(buffer_worker_flush_before_pop, #{}),
            PopOpts = #{
                count_limit => BatchSize,
                stop_before => {fun stop_batching/2, initial_state}
            },
            {Q1, QAckRef, Batch} = replayq:pop(Q0, PopOpts),
            Data2 = Data1#{queue := Q1},
            ?tp(buffer_worker_flush_before_sieve_expired, #{}),
            Now = now_(),
            %% if the request has expired, the caller is no longer
            %% waiting for a response.
            case sieve_expired_requests(Batch, Now) of
                {[], _AllExpired} ->
                    ok = replayq:ack(Q1, QAckRef),
                    NumExpired = length(Batch),
                    batch_reply_dropped(Batch, {error, request_expired}),
                    Data3 = aggregate_counters(Data2, #{dropped_expired => NumExpired}),
                    ?tp(buffer_worker_flush_all_expired, #{batch => Batch}),
                    flush(Data3);
                {NotExpired, Expired} ->
                    NumExpired = length(Expired),
                    batch_reply_dropped(Expired, {error, request_expired}),
                    Data3 = aggregate_counters(Data2, #{dropped_expired => NumExpired}),
                    IsBatch = (BatchSize > 1),
                    %% We *must* use the new queue, because we currently can't
                    %% `nack' a `pop'.
                    %% Maybe we could re-open the queue?
                    ?tp(
                        buffer_worker_flush_potentially_partial,
                        #{expired => Expired, not_expired => NotExpired}
                    ),
                    Ref = make_request_ref(),
                    do_flush(Data3, #{
                        is_batch => IsBatch,
                        batch => NotExpired,
                        ref => Ref,
                        ack_ref => QAckRef
                    })
            end
    end.

stop_batching(Query, initial_state) ->
    get_stop_flag(Query);
stop_batching(Query, PrevStopFlag) ->
    case get_stop_flag(Query) =:= PrevStopFlag of
        true ->
            PrevStopFlag;
        false ->
            %% We stop beceause we don't want a batch with mixed values for the
            %% stop_action_after_render option
            true
    end.

get_stop_flag(?QUERY(_, _, _, _, #{stop_action_after_render := true})) ->
    stop_action_after_render;
get_stop_flag(_) ->
    no_stop_action_after_render.

-spec do_flush(data(), #{
    is_batch := boolean(),
    batch := [queue_query()],
    ack_ref := replayq:ack_ref(),
    ref := inflight_key()
}) ->
    gen_statem:event_handler_result(state(), data()).
do_flush(
    #{queue := Q1} = Data0,
    #{
        is_batch := false,
        batch := Batch,
        ref := Ref,
        ack_ref := QAckRef
    }
) ->
    #{
        id := Id,
        index := Index,
        inflight_tid := InflightTID
    } = Data0,
    %% unwrap when not batching (i.e., batch size == 1)
    [?QUERY(ReplyTo, _, HasBeenSent, _ExpireAt, TraceCtx) = Request] = Batch,
    QueryOpts = #{inflight_tid => InflightTID, simple_query => false},
    Result = call_query(async_if_possible, Id, Index, Ref, Request, QueryOpts),
    Reply = ?REPLY(ReplyTo, HasBeenSent, Result, TraceCtx),
    {ShouldAck, DeltaCounters} = reply_caller(Id, Reply, QueryOpts),
    Data1 = aggregate_counters(Data0, DeltaCounters),
    case ShouldAck of
        %% Failed; remove the request from the queue, as we cannot pop
        %% from it again, but we'll retry it using the inflight table.
        nack ->
            ok = replayq:ack(Q1, QAckRef),
            %% we set it atomically just below; a limitation of having
            %% to use tuples for atomic ets updates
            IsRetriable = true,
            AsyncWorkerMRef0 = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Request, IsRetriable, AsyncWorkerMRef0),
            %% we must append again to the table to ensure that the
            %% request will be retried (i.e., it might not have been
            %% inserted during `call_query' if the resource was down
            %% and/or if it was a sync request).
            inflight_append(InflightTID, InflightItem),
            mark_inflight_as_retriable(InflightTID, Ref),
            {Data2, AsyncWorkerMRef} = ensure_async_worker_monitored(Data1, Result),
            store_async_worker_reference(InflightTID, Ref, AsyncWorkerMRef),
            ?tp(
                buffer_worker_flush_nack,
                #{
                    ref => Ref,
                    is_retriable => IsRetriable,
                    batch_or_query => Request,
                    result => Result
                }
            ),
            {next_state, blocked, Data2};
        %% Success; just ack.
        ack ->
            ok = replayq:ack(Q1, QAckRef),
            %% Async requests are acked later when the async worker
            %% calls the corresponding callback function.  Also, we
            %% must ensure the async worker is being monitored for
            %% such requests.
            IsUnrecoverableError = is_unrecoverable_error(Result),
            BufferWorkerPid = self(),
            case is_async_return(Result) of
                true when IsUnrecoverableError ->
                    ack_inflight(InflightTID, Ref, BufferWorkerPid);
                true ->
                    ok;
                false ->
                    ack_inflight(InflightTID, Ref, BufferWorkerPid)
            end,
            {Data2, AsyncWorkerMRef} = ensure_async_worker_monitored(Data1, Result),
            store_async_worker_reference(InflightTID, Ref, AsyncWorkerMRef),
            ?tp(
                buffer_worker_flush_ack,
                #{
                    batch_or_query => Request,
                    result => Result
                }
            ),
            CurrentCount = queue_count(Q1),
            case CurrentCount > 0 of
                true ->
                    ?tp(buffer_worker_flush_ack_reflush, #{
                        batch_or_query => Request, result => Result, queue_count => CurrentCount
                    }),
                    flush_worker(self());
                false ->
                    ?tp_ignore_side_effects_in_prod(buffer_worker_queue_drained, #{
                        inflight => inflight_count(InflightTID)
                    }),
                    ok
            end,
            {keep_state, Data2}
    end;
do_flush(#{queue := Q1} = Data0, #{
    is_batch := true,
    batch := Batch,
    ref := Ref,
    ack_ref := QAckRef
}) ->
    #{
        id := Id,
        index := Index,
        batch_size := BatchSize,
        inflight_tid := InflightTID
    } = Data0,
    QueryOpts = #{inflight_tid => InflightTID, simple_query => false},
    Result = call_query(async_if_possible, Id, Index, Ref, Batch, QueryOpts),
    {ShouldAck, DeltaCounters} = batch_reply_caller(Id, Result, Batch, QueryOpts),
    Data1 = aggregate_counters(Data0, DeltaCounters),
    case ShouldAck of
        %% Failed; remove the request from the queue, as we cannot pop
        %% from it again, but we'll retry it using the inflight table.
        nack ->
            ok = replayq:ack(Q1, QAckRef),
            %% we set it atomically just below; a limitation of having
            %% to use tuples for atomic ets updates
            IsRetriable = true,
            AsyncWorkerMRef0 = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Batch, IsRetriable, AsyncWorkerMRef0),
            %% we must append again to the table to ensure that the
            %% request will be retried (i.e., it might not have been
            %% inserted during `call_query' if the resource was down
            %% and/or if it was a sync request).
            inflight_append(InflightTID, InflightItem),
            mark_inflight_as_retriable(InflightTID, Ref),
            {Data2, AsyncWorkerMRef} = ensure_async_worker_monitored(Data1, Result),
            store_async_worker_reference(InflightTID, Ref, AsyncWorkerMRef),
            ?tp(
                buffer_worker_flush_nack,
                #{
                    ref => Ref,
                    is_retriable => IsRetriable,
                    batch_or_query => Batch,
                    result => Result
                }
            ),
            {next_state, blocked, Data2};
        %% Success; just ack.
        ack ->
            ok = replayq:ack(Q1, QAckRef),
            %% Async requests are acked later when the async worker
            %% calls the corresponding callback function.  Also, we
            %% must ensure the async worker is being monitored for
            %% such requests.
            IsUnrecoverableError = is_unrecoverable_error(Result),
            BufferWorkerPid = self(),
            case is_async_return(Result) of
                true when IsUnrecoverableError ->
                    ack_inflight(InflightTID, Ref, BufferWorkerPid);
                true ->
                    ok;
                false ->
                    ack_inflight(InflightTID, Ref, BufferWorkerPid)
            end,
            {Data2, AsyncWorkerMRef} = ensure_async_worker_monitored(Data1, Result),
            store_async_worker_reference(InflightTID, Ref, AsyncWorkerMRef),
            CurrentCount = queue_count(Q1),
            ?tp(
                buffer_worker_flush_ack,
                #{
                    batch_or_query => Batch,
                    result => Result,
                    queue_count => CurrentCount
                }
            ),
            Data3 =
                case {CurrentCount > 0, CurrentCount >= BatchSize} of
                    {false, _} ->
                        ?tp_ignore_side_effects_in_prod(buffer_worker_queue_drained, #{
                            inflight => inflight_count(InflightTID)
                        }),
                        Data2;
                    {true, true} ->
                        ?tp(buffer_worker_flush_ack_reflush, #{
                            batch_or_query => Batch,
                            result => Result,
                            queue_count => CurrentCount,
                            batch_size => BatchSize
                        }),
                        flush_worker(self()),
                        Data2;
                    {true, false} ->
                        ensure_flush_timer(Data2)
                end,
            {keep_state, Data3}
    end.

batch_reply_caller(Id, BatchResult, Batch, QueryOpts) ->
    IsSimpleQuery = is_simple_query(QueryOpts),
    {ShouldBlock, PostFn, DeltaCounters} =
        batch_reply_caller_defer_metrics(Id, BatchResult, Batch, IsSimpleQuery),
    PostFn(),
    {ShouldBlock, DeltaCounters}.

batch_reply_caller_defer_metrics(Id, BatchResult, Batch, IsSimpleQuery) ->
    Replies = expand_batch_reply(BatchResult, Batch),
    {ShouldAck, PostFns, Counters} =
        lists:foldl(
            fun(Reply, {_ShouldAck, PostFns, OldCounters}) ->
                %% _ShouldAck should be the same as ShouldAck starting from the second reply
                {ShouldAck, PostFn, DeltaCounters} = reply_caller_defer_metrics(
                    Id, Reply, IsSimpleQuery
                ),
                {ShouldAck, [PostFn | PostFns], merge_counters(OldCounters, DeltaCounters)}
            end,
            {ack, [], #{}},
            Replies
        ),
    PostFn = fun() -> lists:foreach(fun(F) -> F() end, lists:reverse(PostFns)) end,
    {ShouldAck, PostFn, Counters}.

expand_batch_reply(BatchResults, Batch) when is_list(BatchResults) ->
    lists:map(
        fun({?QUERY(FROM, _REQUEST, SENT, _EXPIRE_AT, TraceCtx), Result}) ->
            ?REPLY(FROM, SENT, Result, TraceCtx)
        end,
        lists:zip(Batch, BatchResults)
    );
expand_batch_reply(BatchResult, Batch) ->
    lists:map(
        fun(?QUERY(FROM, _REQUEST, SENT, _EXPIRE_AT, TraceCtx)) ->
            ?REPLY(FROM, SENT, BatchResult, TraceCtx)
        end,
        Batch
    ).

reply_caller(Id, Reply, QueryOpts) ->
    {ShouldAck, PostFn, DeltaCounters} = reply_caller_defer_metrics(Id, Reply, QueryOpts),
    PostFn(),
    {ShouldAck, DeltaCounters}.

%% Should only reply to the caller when the decision is final (not
%% retriable).  See comment on `handle_query_result_pure'.
reply_caller_defer_metrics(Id, ?REPLY(undefined, HasBeenSent, Result, TraceCtx), _IsSimpleQuery) ->
    handle_query_result_pure(Id, Result, HasBeenSent, TraceCtx);
reply_caller_defer_metrics(Id, ?REPLY(ReplyTo, HasBeenSent, Result, TraceCtx), IsSimpleQuery) ->
    IsUnrecoverableError = is_unrecoverable_error(Result),
    {ShouldAck, PostFn, DeltaCounters} = handle_query_result_pure(
        Id, Result, HasBeenSent, TraceCtx
    ),
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
    {ShouldAck, PostFn, DeltaCounters}.

%% This is basically used only by rule actions.  To avoid rule action metrics from
%% becoming inconsistent when we drop messages, we need a way to signal rule engine that
%% this action has reached a conclusion.
-spec reply_dropped(reply_fun(), {error, late_reply | request_expired}) -> ok.
reply_dropped(_ReplyTo = {Fn, Args, #{reply_dropped := true}}, Result) when
    is_function(Fn), is_list(Args)
->
    %% We want to avoid bumping metrics inside the buffer worker, since it's costly.
    emqx_pool:async_submit(Fn, Args ++ [Result]),
    ok;
reply_dropped(_ReplyTo, _Result) ->
    ok.

-spec batch_reply_dropped([queue_query()], {error, late_reply | request_expired}) -> ok.
batch_reply_dropped(Batch, Result) ->
    lists:foreach(
        fun(?QUERY(ReplyTo, _CoreReq, _HasBeenSent, _ExpireAt, _TraceCtx)) ->
            reply_dropped(ReplyTo, Result)
        end,
        Batch
    ).

%% This is only called by `simple_{,a}sync_query', so we can bump the
%% counters here.
handle_query_result(Id, Result, HasBeenSent) ->
    {ShouldBlock, PostFn, DeltaCounters} = handle_query_result_pure(Id, Result, HasBeenSent, #{}),
    PostFn(),
    bump_counters(Id, DeltaCounters),
    ShouldBlock.

%% We should always retry (nack), except when:
%%   * resource is not found
%%   * resource is stopped
%%   * the result is a success (or at least a delayed result)
%% We also retry even sync requests.  In that case, we shouldn't reply
%% the caller until one of those final results above happen.
-spec handle_query_result_pure(id(), term(), HasBeenSent :: boolean(), TraceCTX :: map()) ->
    {ack | nack, function(), counters()}.
handle_query_result_pure(_Id, ?RESOURCE_ERROR_M(exception, Msg), _HasBeenSent, TraceCTX) ->
    PostFn = fun() ->
        ?TRACE(
            error,
            "ERROR",
            "resource_exception",
            (trace_ctx_map(TraceCTX))#{info => emqx_utils:redact(Msg)}
        ),
        ok
    end,
    {nack, PostFn, #{}};
handle_query_result_pure(_Id, ?RESOURCE_ERROR_M(NotWorking, _), _HasBeenSent, _TraceCTX) when
    NotWorking == not_connected; NotWorking == blocked
->
    {nack, fun() -> ok end, #{}};
handle_query_result_pure(Id, ?RESOURCE_ERROR_M(not_found, Msg), _HasBeenSent, TraceCTX) ->
    PostFn = fun() ->
        ?TRACE(
            error,
            "ERROR",
            "resource_not_found",
            (trace_ctx_map(TraceCTX))#{id => Id, info => Msg}
        ),
        ok
    end,
    {ack, PostFn, #{dropped_resource_not_found => 1}};
handle_query_result_pure(Id, ?RESOURCE_ERROR_M(stopped, Msg), _HasBeenSent, TraceCTX) ->
    PostFn = fun() ->
        ?TRACE(error, "ERROR", "resource_stopped", (trace_ctx_map(TraceCTX))#{id => Id, info => Msg}),
        ok
    end,
    {ack, PostFn, #{dropped_resource_stopped => 1}};
handle_query_result_pure(Id, ?RESOURCE_ERROR_M(Reason, _), _HasBeenSent, TraceCTX) ->
    PostFn = fun() ->
        ?TRACE(error, "ERROR", "other_resource_error", (trace_ctx_map(TraceCTX))#{
            id => Id, reason => Reason
        }),
        ok
    end,
    {nack, PostFn, #{}};
handle_query_result_pure(Id, {error, Reason} = Error, HasBeenSent, TraceCTX) ->
    case is_unrecoverable_error(Error) of
        true ->
            PostFn =
                fun() ->
                    ?SLOG_THROTTLE(
                        error,
                        Id,
                        #{
                            resource_id => Id,
                            msg => unrecoverable_resource_error,
                            reason => Reason
                        },
                        #{tag => ?TAG}
                    ),
                    ok
                end,
            Counters =
                case HasBeenSent of
                    true -> #{retried_failed => 1};
                    false -> #{failed => 1}
                end,
            {ack, PostFn, Counters};
        false ->
            PostFn =
                fun() ->
                    ?TRACE(error, "ERROR", "send_error", (trace_ctx_map(TraceCTX))#{
                        id => Id, reason => Reason
                    }),
                    ok
                end,
            {nack, PostFn, #{}}
    end;
handle_query_result_pure(Id, {async_return, Result}, HasBeenSent, TraceCTX) ->
    handle_query_async_result_pure(Id, Result, HasBeenSent, TraceCTX);
handle_query_result_pure(_Id, Result, HasBeenSent, _TraceCTX) ->
    PostFn = fun() ->
        assert_ok_result(Result),
        ok
    end,
    Counters =
        case HasBeenSent of
            true -> #{retried_success => 1};
            false -> #{success => 1}
        end,
    {ack, PostFn, Counters}.

-spec handle_query_async_result_pure(id(), term(), HasBeenSent :: boolean(), map()) ->
    {ack | nack, function(), counters()}.
handle_query_async_result_pure(Id, {error, Reason} = Error, HasBeenSent, TraceCTX) ->
    case is_unrecoverable_error(Error) of
        true ->
            PostFn =
                fun() ->
                    ?SLOG_THROTTLE(
                        error,
                        Id,
                        #{
                            resource_id => Id,
                            msg => unrecoverable_resource_error,
                            reason => Reason
                        },
                        #{tag => ?TAG}
                    ),
                    ok
                end,
            Counters =
                case HasBeenSent of
                    true -> #{retried_failed => 1};
                    false -> #{failed => 1}
                end,
            {ack, PostFn, Counters};
        false ->
            PostFn = fun() ->
                ?TRACE(error, "ERROR", "async_send_error", (trace_ctx_map(TraceCTX))#{
                    id => Id, reason => Reason
                }),
                ok
            end,
            {nack, PostFn, #{}}
    end;
handle_query_async_result_pure(_Id, {ok, Pid}, _HasBeenSent, _TraceCTX) when is_pid(Pid) ->
    {ack, fun() -> ok end, #{}};
handle_query_async_result_pure(_Id, ok, _HasBeenSent, _TraceCTX) ->
    {ack, fun() -> ok end, #{}};
handle_query_async_result_pure(Id, Results, HasBeenSent, TraceCTX) when is_list(Results) ->
    All = fun(L) ->
        case L of
            {ok, Pid} -> is_pid(Pid);
            _ -> false
        end
    end,
    case lists:all(All, Results) of
        true ->
            {ack, fun() -> ok end, #{}};
        false ->
            PostFn = fun() ->
                ?TRACE(
                    error,
                    "ERROR",
                    "async_batch_send_error",
                    (trace_ctx_map(TraceCTX))#{
                        id => Id,
                        reason => Results,
                        has_been_sent => HasBeenSent
                    }
                ),
                ok
            end,
            {nack, PostFn, #{}}
    end.

trace_ctx_map(undefined) ->
    #{};
trace_ctx_map(Map) ->
    Map.

-spec aggregate_counters(data(), counters()) -> data().
aggregate_counters(Data = #{counters := OldCounters}, DeltaCounters) ->
    Counters = merge_counters(OldCounters, DeltaCounters),
    Data#{counters := Counters}.

-spec merge_counters(counters(), counters()) -> counters().
merge_counters(OldCounters, DeltaCounters) ->
    maps:fold(
        fun(Metric, Val, Acc) ->
            maps:update_with(Metric, fun(X) -> X + Val end, Val, Acc)
        end,
        OldCounters,
        DeltaCounters
    ).

-spec flush_metrics(data()) -> data().
flush_metrics(Data = #{id := Id, counters := Counters}) ->
    bump_counters(Id, Counters),
    set_gauges(Data),
    log_expired_message_count(Data),
    ensure_metrics_flush_timer(Data#{counters := #{}}).

-spec ensure_metrics_flush_timer(data()) -> data().
ensure_metrics_flush_timer(Data = #{metrics_tref := undefined, metrics_flush_interval := T}) ->
    Ref = make_ref(),
    TRef = erlang:send_after(T, self(), {flush_metrics, Ref}),
    Data#{metrics_tref := {TRef, Ref}}.

-spec bump_counters(id(), counters()) -> ok.
bump_counters(Id, Counters) ->
    Iter = maps:iterator(Counters),
    do_bump_counters(Iter, Id).

do_bump_counters(Iter, Id) ->
    case maps:next(Iter) of
        {Key, Val, NIter} ->
            do_bump_counters1(Key, Val, Id),
            do_bump_counters(NIter, Id);
        none ->
            ok
    end.

do_bump_counters1(dropped_expired, Val, Id) ->
    emqx_resource_metrics:dropped_expired_inc(Id, Val);
do_bump_counters1(dropped_queue_full, Val, Id) ->
    emqx_resource_metrics:dropped_queue_full_inc(Id, Val);
do_bump_counters1(failed, Val, Id) ->
    emqx_resource_metrics:failed_inc(Id, Val);
do_bump_counters1(retried_failed, Val, Id) ->
    emqx_resource_metrics:retried_failed_inc(Id, Val);
do_bump_counters1(success, Val, Id) ->
    emqx_resource_metrics:success_inc(Id, Val);
do_bump_counters1(retried_success, Val, Id) ->
    emqx_resource_metrics:retried_success_inc(Id, Val);
do_bump_counters1(dropped_resource_not_found, Val, Id) ->
    emqx_resource_metrics:dropped_resource_not_found_inc(Id, Val);
do_bump_counters1(dropped_resource_stopped, Val, Id) ->
    emqx_resource_metrics:dropped_resource_stopped_inc(Id, Val).

-spec log_expired_message_count(data()) -> ok.
log_expired_message_count(_Data = #{id := Id, index := Index, counters := Counters}) ->
    ExpiredCount = maps:get(dropped_expired, Counters, 0),
    case ExpiredCount > 0 of
        false ->
            ok;
        true ->
            ?SLOG(
                info,
                #{
                    msg => "buffer_worker_dropped_expired_messages",
                    resource_id => Id,
                    worker_index => Index,
                    expired_count => ExpiredCount
                },
                #{tag => ?TAG}
            ),
            ok
    end.

-spec set_gauges(data()) -> ok.
set_gauges(_Data = #{id := Id, index := Index, queue := Q, inflight_tid := InflightTID}) ->
    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q)),
    emqx_resource_metrics:queuing_bytes_set(Id, Index, queue_bytes(Q)),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    ok.

handle_async_worker_down(Data0, Pid) ->
    #{async_workers := AsyncWorkers0} = Data0,
    {AsyncWorkerMRef, AsyncWorkers} = maps:take(Pid, AsyncWorkers0),
    Data = Data0#{async_workers := AsyncWorkers},
    mark_inflight_items_as_retriable(Data, AsyncWorkerMRef),
    {next_state, blocked, Data}.

-spec call_query(force_sync | async_if_possible, _, _, _, _, _) -> _.
call_query(QM, Id, Index, Ref, Query, QueryOpts) ->
    ?tp(call_query_enter, #{id => Id, query => Query, query_mode => QM}),
    case emqx_resource_cache:get_runtime(Id) of
        %% This seems to be the only place where the `rm_status_stopped' status matters,
        %% to distinguish from the `disconnected' status.
        {ok, #rt{st_err = #{status := ?rm_status_stopped}}} ->
            ?RESOURCE_ERROR(stopped, "resource stopped or disabled");
        {ok, #rt{st_err = #{status := ?status_connecting, error := unhealthy_target}}} ->
            {error, {unrecoverable_error, unhealthy_target}};
        {ok, #rt{st_err = #{status := Status}, cb = Resource, channel_status = ChanSt}} ->
            IsAlwaysSend = is_always_send(QueryOpts, Resource),
            case Status =:= ?status_connected orelse IsAlwaysSend of
                true ->
                    call_query2(QM, Id, Index, Ref, Query, QueryOpts, Resource, ChanSt);
                false ->
                    ?RESOURCE_ERROR(not_connected, "resource not connected")
            end;
        {error, not_found} ->
            ?RESOURCE_ERROR(not_found, "resource not found")
    end.

call_query2(QM, Id, Index, Ref, Query, QueryOpts, Resource, ChanSt) ->
    PrevLoggerProcessMetadata = logger:get_process_metadata(),
    try
        set_rule_id_trace_meta_data(Query),
        do_call_query(QM, Id, Index, Ref, Query, QueryOpts, Resource, ChanSt)
    after
        reset_logger_process_metadata(PrevLoggerProcessMetadata)
    end.

set_rule_id_trace_meta_data(Requests) when is_list(Requests) ->
    %% Get the rule ids from requests
    RuleIDs = lists:foldl(fun collect_rule_id/2, #{}, Requests),
    ClientIDs = lists:foldl(fun collect_client_id/2, #{}, Requests),
    RuleTriggerTimes0 = lists:foldl(fun collect_rule_trigger_times/2, [], Requests),
    RuleTriggerTimes = lists:flatten(RuleTriggerTimes0),
    TraceMetadata =
        case Requests of
            %% We know that the batch is not mixed since we prevent this by
            %% using a stop_after function in the replayq:pop call
            [?QUERY(_, _, _, _, #{stop_action_after_render := true}) | _] ->
                #{
                    rule_ids => RuleIDs,
                    client_ids => ClientIDs,
                    rule_trigger_ts => RuleTriggerTimes,
                    stop_action_after_render => true
                };
            [?QUERY(_, _, _, _, _TraceCTX) | _] ->
                #{
                    rule_ids => RuleIDs,
                    client_ids => ClientIDs,
                    rule_trigger_ts => RuleTriggerTimes
                }
        end,
    logger:update_process_metadata(TraceMetadata),
    ok;
set_rule_id_trace_meta_data(Request) ->
    set_rule_id_trace_meta_data([Request]),
    ok.

reset_logger_process_metadata(undefined = _PrevProcessMetadata) ->
    logger:unset_process_metadata();
reset_logger_process_metadata(PrevProcessMetadata) ->
    logger:set_process_metadata(PrevProcessMetadata).

collect_rule_id(?QUERY(_, _, _, _, #{rule_id := RuleId}), Acc) ->
    Acc#{RuleId => true};
collect_rule_id(?QUERY(_, _, _, _, _), Acc) ->
    Acc.

collect_client_id(?QUERY(_, _, _, _, #{clientid := ClientId}), Acc) ->
    Acc#{ClientId => true};
collect_client_id(?QUERY(_, _, _, _, _), Acc) ->
    Acc.

collect_rule_trigger_times(?QUERY(_, _, _, _, #{rule_trigger_ts := Time}), Acc) ->
    [Time | Acc];
collect_rule_trigger_times(?QUERY(_, _, _, _, _), Acc) ->
    Acc.

%% action:kafka_producer:myproducer1:connector:kafka_producer:mykakfaclient1
extract_connector_id(Id) when is_binary(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [
            _ChannelGlobalType,
            _ChannelSubType,
            _ChannelName,
            <<"connector">>,
            ConnectorType,
            ConnectorName
        ] ->
            <<"connector:", ConnectorType/binary, ":", ConnectorName/binary>>;
        _ ->
            Id
    end.

%% Check if channel is installed in the connector state.
%% There is no need to query the conncector if the channel is not
%% installed as the query will fail anyway.
pre_query_channel_check(Id, {Id, _} = _Request, ChanSt, IsSimpleQuery) ->
    case is_channel_apt_for_queries(ChanSt) of
        true ->
            ok;
        false ->
            error_if_channel_is_not_installed(Id, IsSimpleQuery)
    end;
pre_query_channel_check(_Id, _Request, _ChanSt, _IsSimpleQuery) ->
    %% Not a per-channel request
    ok.

error_if_channel_is_not_installed(Id, IsSimpleQuery) ->
    %% Fail with a recoverable error if the channel is not installed and there are buffer
    %% workers involved so that the operation can be retried.  Otherwise, this is
    %% unrecoverable.  It is emqx_resource_manager's responsibility to ensure that the
    %% channel installation is retried.
    Msg = iolist_to_binary(io_lib:format("channel: \"~s\" not operational", [Id])),
    ErrorType =
        case IsSimpleQuery of
            true ->
                unrecoverable_error;
            false ->
                recoverable_error
        end,
    {error, {ErrorType, Msg}}.

is_always_send(#{query_mode := M}, _) when ?IS_BYPASS(M) ->
    %% The query overrides the query mode of the resource, send even in disconnected state
    ?tp(simple_query_override, #{query_mode => M}),
    ?tp(simple_query_enter, #{}),
    true;
is_always_send(_, #{query_mode := M}) when ?IS_BYPASS(M) ->
    %% The connector supports buffer, send even in disconnected state
    ?tp(simple_query_enter, #{}),
    true;
is_always_send(_, _) ->
    false.

do_call_query(QM, Id, Index, Ref, Query, QueryOpts, Resource, ChanSt) ->
    #{mod := Mod, state := ResSt, callback_mode := CBM} = Resource,
    CallMode = call_mode(QM, CBM),
    apply_query_fun(CallMode, Mod, Id, Index, Ref, Query, ResSt, ChanSt, QueryOpts).

-define(APPLY_RESOURCE(NAME, EXPR, REQ),
    try
        %% if the callback module (connector) wants to return an error that
        %% makes the current resource goes into the `blocked` state, it should
        %% return `{error, {recoverable_error, Reason}}`
        EXPR
    catch
        %% For convenience and to make the code in the callbacks cleaner an
        %% error exception with the two following formats are translated to the
        %% corresponding return values. The receiver of the return values
        %% recognizes these special return formats and use them to decided if a
        %% request should be retried.
        error:{unrecoverable_error, Msg} ->
            {error, {unrecoverable_error, Msg}};
        error:{recoverable_error, Msg} ->
            {error, {recoverable_error, Msg}};
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

apply_query_fun(
    sync,
    Mod,
    Id,
    _Index,
    _Ref,
    ?QUERY(_, Request, _, _, _TraceCtx) = _Query,
    ResSt,
    ChanSt,
    QueryOpts
) ->
    ?tp(call_query, #{id => Id, mod => Mod, query => _Query, res_st => ResSt, call_mode => sync}),
    maybe_reply_to(
        ?APPLY_RESOURCE(
            call_query,
            begin
                case pre_query_channel_check(Id, Request, ChanSt, is_simple_query(QueryOpts)) of
                    ok ->
                        Mod:on_query(extract_connector_id(Id), Request, ResSt);
                    Error ->
                        Error
                end
            end,
            Request
        ),
        QueryOpts
    );
apply_query_fun(
    async,
    Mod,
    Id,
    Index,
    Ref,
    ?QUERY(_, Request, _, _, _TraceCtx) = Query,
    ResSt,
    ChanSt,
    QueryOpts
) ->
    ?tp(call_query_async, #{
        id => Id, mod => Mod, query => Query, res_st => ResSt, call_mode => async
    }),
    InflightTID = maps:get(inflight_tid, QueryOpts, ?NO_INFLIGHT),
    ?APPLY_RESOURCE(
        call_query_async,
        begin
            ReplyFun = fun ?MODULE:handle_async_reply/2,
            IsSimpleQuery = is_simple_query(QueryOpts),
            ReplyContext = #{
                buffer_worker => buffer_worker(InflightTID),
                resource_id => Id,
                worker_index => Index,
                inflight_tid => InflightTID,
                request_ref => Ref,
                simple_query => IsSimpleQuery,
                min_query => minimize(Query)
            },
            IsRetriable = false,
            AsyncWorkerMRef = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Query, IsRetriable, AsyncWorkerMRef),
            ok = inflight_append(InflightTID, InflightItem),
            case pre_query_channel_check(Id, Request, ChanSt, IsSimpleQuery) of
                ok ->
                    case
                        Mod:on_query_async(
                            extract_connector_id(Id), Request, {ReplyFun, [ReplyContext]}, ResSt
                        )
                    of
                        {error, _} = Error when IsSimpleQuery ->
                            %% If this callback returns error, we assume it won't reply
                            %% anything else and won't retry.
                            maybe_reply_to(Error, QueryOpts),
                            Error;
                        Result ->
                            {async_return, Result}
                    end;
                Error ->
                    maybe_reply_to(Error, QueryOpts)
            end
        end,
        Request
    );
apply_query_fun(
    sync,
    Mod,
    Id,
    _Index,
    _Ref,
    [?QUERY(_, FirstRequest, _, _, _) | _] = Batch,
    ResSt,
    ChanSt,
    QueryOpts
) ->
    ?tp(call_batch_query, #{
        id => Id, mod => Mod, batch => Batch, res_st => ResSt, call_mode => sync
    }),
    Requests = lists:map(
        fun(?QUERY(_ReplyTo, Request, _, _ExpireAt, _TraceCtx)) -> Request end, Batch
    ),
    maybe_reply_to(
        ?APPLY_RESOURCE(
            call_batch_query,
            begin
                case
                    pre_query_channel_check(Id, FirstRequest, ChanSt, is_simple_query(QueryOpts))
                of
                    ok ->
                        Mod:on_batch_query(extract_connector_id(Id), Requests, ResSt);
                    Error ->
                        Error
                end
            end,
            Batch
        ),
        QueryOpts
    );
apply_query_fun(
    async,
    Mod,
    Id,
    Index,
    Ref,
    [?QUERY(_, FirstRequest, _, _, _) | _] = Batch,
    ResSt,
    ChanSt,
    QueryOpts
) ->
    ?tp(call_batch_query_async, #{
        id => Id, mod => Mod, batch => Batch, res_st => ResSt, call_mode => async
    }),
    InflightTID = maps:get(inflight_tid, QueryOpts),
    ?APPLY_RESOURCE(
        call_batch_query_async,
        begin
            ReplyFun = fun ?MODULE:handle_async_batch_reply/2,
            IsSimpleQuery = is_simple_query(QueryOpts),
            ReplyContext = #{
                buffer_worker => buffer_worker(InflightTID),
                resource_id => Id,
                worker_index => Index,
                inflight_tid => InflightTID,
                request_ref => Ref,
                simple_query => IsSimpleQuery,
                min_batch => minimize(Batch)
            },
            Requests = lists:map(
                fun(?QUERY(_ReplyTo, Request, _, _ExpireAt, _TraceCtx)) -> Request end, Batch
            ),
            IsRetriable = false,
            AsyncWorkerMRef = undefined,
            InflightItem = ?INFLIGHT_ITEM(Ref, Batch, IsRetriable, AsyncWorkerMRef),
            ok = inflight_append(InflightTID, InflightItem),
            case pre_query_channel_check(Id, FirstRequest, ChanSt, IsSimpleQuery) of
                ok ->
                    case
                        Mod:on_batch_query_async(
                            extract_connector_id(Id), Requests, {ReplyFun, [ReplyContext]}, ResSt
                        )
                    of
                        {error, _} = Error when IsSimpleQuery ->
                            %% If this callback returns error, we assume it won't reply
                            %% anything else and won't retry.
                            maybe_reply_to(Error, QueryOpts),
                            Error;
                        Result ->
                            {async_return, Result}
                    end;
                Error ->
                    maybe_reply_to(Error, QueryOpts)
            end
        end,
        Batch
    ).

maybe_reply_to(Result, #{reply_to := ReplyTo}) ->
    do_reply_caller(ReplyTo, Result),
    Result;
maybe_reply_to(Result, _) ->
    Result.

handle_async_reply(
    #{
        request_ref := Ref,
        inflight_tid := InflightTID,
        simple_query := IsSimpleQuery
    } = ReplyContext,
    Result
) ->
    case maybe_handle_unknown_async_reply(InflightTID, Ref, IsSimpleQuery) of
        discard ->
            ok;
        continue ->
            handle_async_reply1(ReplyContext, Result)
    end.

handle_async_reply1(
    #{
        request_ref := Ref,
        inflight_tid := InflightTID,
        resource_id := Id,
        buffer_worker := BufferWorkerPid,
        min_query := ?QUERY(ReplyTo, _, _, ExpireAt, _TraceCtx) = _Query
    } = ReplyContext,
    Result
) ->
    ?tp(
        handle_async_reply_enter,
        #{batch_or_query => [_Query], ref => Ref, result => Result}
    ),
    Now = now_(),
    case is_expired(ExpireAt, Now) of
        true ->
            IsAcked = ack_inflight(InflightTID, Ref, BufferWorkerPid),
            %% evaluate metrics call here since we're not inside
            %% buffer worker
            IsAcked andalso
                begin
                    emqx_resource_metrics:late_reply_inc(Id),
                    reply_dropped(ReplyTo, {error, late_reply})
                end,
            ?tp(handle_async_reply_expired, #{expired => [_Query]}),
            ok;
        false ->
            do_handle_async_reply(ReplyContext, Result)
    end.

do_handle_async_reply(
    #{
        simple_query := IsSimpleQuery,
        resource_id := Id,
        request_ref := Ref,
        buffer_worker := BufferWorkerPid,
        inflight_tid := InflightTID,
        min_query := ?QUERY(ReplyTo, _, Sent, _ExpireAt, TraceCtx) = _Query
    },
    Result
) ->
    %% NOTE: 'inflight' is the count of messages that were sent async
    %% but received no ACK, NOT the number of messages queued in the
    %% inflight window.
    {Action, PostFn, DeltaCounters} = reply_caller_defer_metrics(
        Id, ?REPLY(ReplyTo, Sent, Result, TraceCtx), IsSimpleQuery
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
            ok = mark_inflight_as_retriable(InflightTID, Ref),
            ok = ?MODULE:block(BufferWorkerPid),
            blocked;
        ack ->
            ok = do_async_ack(
                InflightTID, Ref, Id, PostFn, BufferWorkerPid, DeltaCounters, IsSimpleQuery
            )
    end.

handle_async_batch_reply(
    #{
        inflight_tid := InflightTID,
        request_ref := Ref,
        simple_query := IsSimpleQuery
    } = ReplyContext,
    Result
) ->
    case maybe_handle_unknown_async_reply(InflightTID, Ref, IsSimpleQuery) of
        discard ->
            ok;
        continue ->
            handle_async_batch_reply1(ReplyContext, Result)
    end.

handle_async_batch_reply1(
    #{
        inflight_tid := InflightTID,
        request_ref := Ref,
        min_batch := Batch
    } = ReplyContext,
    Result
) ->
    ?tp(
        handle_async_reply_enter,
        #{batch_or_query => Batch, ref => Ref, result => Result}
    ),
    Now = now_(),
    case sieve_expired_requests(Batch, Now) of
        {_NotExpired, []} ->
            %% this is the critical code path,
            %% we try not to do ets:lookup in this case
            %% because the batch can be quite big
            do_handle_async_batch_reply(ReplyContext, Result);
        {_NotExpired, _Expired} ->
            %% at least one is expired
            %% the batch from reply context is minimized, so it cannot be used
            %% to update the inflight items, hence discard Batch and lookup the RealBatch
            ?tp(handle_async_reply_expired, #{expired => _Expired}),
            handle_async_batch_reply2(ets:lookup(InflightTID, Ref), ReplyContext, Result, Now)
    end.

handle_async_batch_reply2([], _, _, _) ->
    %% this usually should never happen unless the async callback is being evaluated concurrently
    ok;
handle_async_batch_reply2([Inflight], ReplyContext, Results0, Now) ->
    ?INFLIGHT_ITEM(_, RealBatch, _IsRetriable, _AsyncWorkerMRef) = Inflight,
    #{
        resource_id := Id,
        buffer_worker := BufferWorkerPid,
        inflight_tid := InflightTID,
        request_ref := Ref,
        min_batch := Batch
    } = ReplyContext,
    %% All batch items share the same HasBeenSent flag
    %% So we just take the original flag from the ReplyContext batch
    %% and put it back to the batch found in inflight table
    %% which must have already been set to `false`
    [?QUERY(_ReplyTo, _, HasBeenSent, _ExpireAt, _TraceCtx) | _] = Batch,
    {RealNotExpired0, RealExpired, Results} =
        sieve_expired_requests_with_results(RealBatch, Now, Results0),
    RealNotExpired =
        lists:map(
            fun(?QUERY(ReplyTo, CoreReq, _HasBeenSent, ExpireAt, TraceCtx)) ->
                ?QUERY(ReplyTo, CoreReq, HasBeenSent, ExpireAt, TraceCtx)
            end,
            RealNotExpired0
        ),
    NumExpired = length(RealExpired),
    %% evalutate metrics call here since we're not inside buffer
    %% worker
    emqx_resource_metrics:late_reply_inc(Id, NumExpired),
    batch_reply_dropped(RealExpired, {error, late_reply}),
    case RealNotExpired of
        [] ->
            %% all expired, no need to update back the inflight batch
            _ = ack_inflight(InflightTID, Ref, BufferWorkerPid),
            ok;
        _ ->
            %% some queries are not expired, put them back to the inflight batch
            %% so it can be either acked now or retried later
            ok = update_inflight_item(InflightTID, Ref, RealNotExpired, NumExpired),
            ?tp_ignore_side_effects_in_prod(
                handle_async_reply_partially_expired,
                #{
                    inflight_count => inflight_count(InflightTID),
                    num_inflight_messages => inflight_num_msgs(InflightTID)
                }
            ),
            do_handle_async_batch_reply(ReplyContext#{min_batch := RealNotExpired}, Results)
    end.

do_handle_async_batch_reply(
    #{
        buffer_worker := BufferWorkerPid,
        resource_id := Id,
        inflight_tid := InflightTID,
        request_ref := Ref,
        min_batch := Batch,
        simple_query := IsSimpleQuery
    },
    Result
) ->
    {Action, PostFn, DeltaCounters} = batch_reply_caller_defer_metrics(
        Id, Result, Batch, IsSimpleQuery
    ),
    ?tp(handle_async_reply, #{
        action => Action,
        batch_or_query => Batch,
        ref => Ref,
        result => Result
    }),
    case Action of
        nack ->
            %% Keep retrying.
            ok = mark_inflight_as_retriable(InflightTID, Ref),
            ok = ?MODULE:block(BufferWorkerPid),
            blocked;
        ack ->
            ok = do_async_ack(
                InflightTID, Ref, Id, PostFn, BufferWorkerPid, DeltaCounters, IsSimpleQuery
            )
    end.

do_async_ack(InflightTID, Ref, Id, PostFn, BufferWorkerPid, DeltaCounters, IsSimpleQuery) ->
    IsKnownRef = ack_inflight(InflightTID, Ref, BufferWorkerPid),
    case IsSimpleQuery of
        true ->
            PostFn(),
            bump_counters(Id, DeltaCounters);
        false when IsKnownRef ->
            PostFn(),
            bump_counters(Id, DeltaCounters);
        false ->
            ok
    end,
    ok.

%% check if the async reply is valid.
%% e.g. if a connector evaluates the callback more than once:
%% 1. If the request was previously deleted from inflight table due to
%%    either succeeded previously or expired, this function logs a
%%    warning message and returns 'discard' instruction.
%% 2. If the request was previously failed and now pending on a retry,
%%    then this function will return 'continue' as there is no way to
%%    tell if this reply is stae or not.
maybe_handle_unknown_async_reply(?NO_INFLIGHT, _Ref, true) ->
    continue;
maybe_handle_unknown_async_reply(InflightTID, Ref, _) ->
    try ets:member(InflightTID, Ref) of
        true ->
            continue;
        false ->
            ?tp(
                warning,
                unknown_async_reply_discarded,
                #{inflight_key => Ref}
            ),
            discard
    catch
        error:badarg ->
            %% shutdown ?
            discard
    end.

%%==============================================================================
%% operations for queue
queue_item_marshaller(Bin) when is_binary(Bin) ->
    binary_to_term(Bin);
queue_item_marshaller(Item) ->
    term_to_binary(Item).

estimate_size(QItem) ->
    erlang:external_size(QItem).

-spec append_queue(id(), index(), replayq:q(), [queue_query()]) ->
    {[queue_query()], replayq:q(), counters()}.
append_queue(Id, Index, Q, Queries) ->
    %% this assertion is to ensure that we never append a raw binary
    %% because the marshaller will get lost.
    false = is_binary(hd(Queries)),
    Q0 = replayq:append(Q, Queries),
    {Overflown, Q2, DeltaCounters} =
        case replayq:overflow(Q0) of
            OverflownBytes when OverflownBytes =< 0 ->
                {[], Q0, #{}};
            OverflownBytes ->
                PopOpts = #{bytes_limit => OverflownBytes, count_limit => 999999999},
                {Q1, QAckRef, Items2} = replayq:pop(Q0, PopOpts),
                ok = replayq:ack(Q1, QAckRef),
                Dropped = length(Items2),
                Counters = #{dropped_queue_full => Dropped},
                ?SLOG_THROTTLE(
                    warning,
                    #{
                        msg => data_bridge_buffer_overflow,
                        resource_id => Id,
                        worker_index => Index,
                        dropped => Dropped
                    },
                    #{tag => ?TAG}
                ),
                {Items2, Q1, Counters}
        end,
    ?tp(
        buffer_worker_appended_to_queue,
        #{
            id => Id,
            items => Queries,
            queue_count => queue_count(Q2),
            overflown => length(Overflown)
        }
    ),
    {Overflown, Q2, DeltaCounters}.

%%==============================================================================
%% the inflight queue for async query
-define(MAX_SIZE_REF, max_size).
-define(SIZE_REF, size).
-define(BATCH_COUNT_REF, batch_count).
-define(INITIAL_TIME_REF, initial_time).
-define(INITIAL_MONOTONIC_TIME_REF, initial_monotonic_time).

inflight_new(InfltWinSZ) ->
    TableId = ets:new(
        emqx_resource_buffer_worker_inflight_tab,
        [ordered_set, public, {write_concurrency, true}]
    ),
    inflight_append(TableId, {?MAX_SIZE_REF, InfltWinSZ}),
    %% we use this counter because we might deal with batches as
    %% elements.
    inflight_append(TableId, {?SIZE_REF, 0}),
    inflight_append(TableId, {?BATCH_COUNT_REF, 0}),
    inflight_append(TableId, {?INITIAL_TIME_REF, erlang:system_time()}),
    inflight_append(TableId, {?INITIAL_MONOTONIC_TIME_REF, make_request_ref()}),
    TableId.

-spec inflight_get_first_retriable(ets:tid(), integer()) ->
    none
    | {expired, inflight_key(), [queue_query()]}
    | {single, inflight_key(), queue_query()}
    | {batch, inflight_key(), _NotExpired :: [queue_query()], _Expired :: [queue_query()]}.
inflight_get_first_retriable(InflightTID, Now) ->
    MatchSpec =
        ets:fun2ms(
            fun(?INFLIGHT_ITEM(Ref, BatchOrQuery, IsRetriable, _AsyncWorkerMRef)) when
                IsRetriable =:= true
            ->
                {Ref, BatchOrQuery}
            end
        ),
    case ets:select(InflightTID, MatchSpec, _Limit = 1) of
        '$end_of_table' ->
            none;
        {
            [{Ref, Query = ?QUERY(_ReplyTo, _CoreReq, _HasBeenSent, ExpireAt, _TraceCtx)}],
            _Continuation
        } ->
            case is_expired(ExpireAt, Now) of
                true ->
                    {expired, Ref, [Query]};
                false ->
                    {single, Ref, Query}
            end;
        {[{Ref, Batch = [_ | _]}], _Continuation} ->
            case sieve_expired_requests(Batch, Now) of
                {[], _AllExpired} ->
                    {expired, Ref, Batch};
                {NotExpired, Expired} ->
                    {batch, Ref, NotExpired, Expired}
            end
    end.

is_inflight_full(?NO_INFLIGHT) ->
    false;
is_inflight_full(InflightTID) ->
    [{_, MaxSize}] = ets:lookup(InflightTID, ?MAX_SIZE_REF),
    %% we consider number of batches rather than number of messages
    %% because one batch request may hold several messages.
    Size = inflight_count(InflightTID),
    Size >= MaxSize.

inflight_count(InflightTID) ->
    emqx_utils_ets:lookup_value(InflightTID, ?BATCH_COUNT_REF, 0).

inflight_num_msgs(InflightTID) ->
    [{_, Size}] = ets:lookup(InflightTID, ?SIZE_REF),
    Size.

inflight_append(?NO_INFLIGHT, _InflightItem) ->
    ok;
inflight_append(
    InflightTID,
    ?INFLIGHT_ITEM(Ref, [?QUERY(_, _, _, _, _) | _] = Batch0, IsRetriable, AsyncWorkerMRef)
) ->
    Batch = mark_as_sent(Batch0),
    InflightItem = ?INFLIGHT_ITEM(Ref, Batch, IsRetriable, AsyncWorkerMRef),
    IsNew = ets:insert_new(InflightTID, InflightItem),
    BatchSize = length(Batch),
    IsNew andalso inc_inflight(InflightTID, BatchSize),
    ?tp(buffer_worker_appended_to_inflight, #{item => InflightItem, is_new => IsNew}),
    ok;
inflight_append(
    InflightTID,
    ?INFLIGHT_ITEM(
        Ref,
        ?QUERY(_ReplyTo, _Req, _HasBeenSent, _ExpireAt, _TraceCtx) = Query0,
        IsRetriable,
        AsyncWorkerMRef
    )
) ->
    Query = mark_as_sent(Query0),
    InflightItem = ?INFLIGHT_ITEM(Ref, Query, IsRetriable, AsyncWorkerMRef),
    IsNew = ets:insert_new(InflightTID, InflightItem),
    IsNew andalso inc_inflight(InflightTID, 1),
    ?tp(buffer_worker_appended_to_inflight, #{item => InflightItem, is_new => IsNew}),
    ok;
inflight_append(InflightTID, {Ref, Data}) ->
    ets:insert(InflightTID, {Ref, Data}),
    %% this is a metadata row being inserted; therefore, we don't bump
    %% the inflight metric.
    ok.

%% a request was already appended and originally not retriable, but an
%% error occurred and it is now retriable.
mark_inflight_as_retriable(?NO_INFLIGHT, _Ref) ->
    ok;
mark_inflight_as_retriable(InflightTID, Ref) ->
    _ = ets:update_element(InflightTID, Ref, {?RETRY_IDX, true}),
    %% the old worker's DOWN should not affect this inflight any more
    _ = ets:update_element(InflightTID, Ref, {?WORKER_MREF_IDX, erased}),
    ok.

%% Track each worker pid only once.
ensure_async_worker_monitored(
    Data0 = #{async_workers := AsyncWorkers}, {async_return, {ok, AsyncWorkerPid}} = _Result
) when
    is_pid(AsyncWorkerPid), is_map_key(AsyncWorkerPid, AsyncWorkers)
->
    AsyncWorkerMRef = maps:get(AsyncWorkerPid, AsyncWorkers),
    {Data0, AsyncWorkerMRef};
ensure_async_worker_monitored(
    Data0 = #{async_workers := AsyncWorkers0}, {async_return, {ok, AsyncWorkerPid}}
) when
    is_pid(AsyncWorkerPid)
->
    AsyncWorkerMRef = monitor(process, AsyncWorkerPid),
    AsyncWorkers = AsyncWorkers0#{AsyncWorkerPid => AsyncWorkerMRef},
    Data = Data0#{async_workers := AsyncWorkers},
    {Data, AsyncWorkerMRef};
ensure_async_worker_monitored(Data0, _Result) ->
    {Data0, undefined}.

-spec store_async_worker_reference(inflight_table(), inflight_key(), undefined | reference()) ->
    ok.
store_async_worker_reference(_InflightTID, _Ref, undefined) ->
    ok;
store_async_worker_reference(InflightTID, Ref, AsyncWorkerMRef) ->
    is_integer(Ref) orelse error(#{unexpected_ref => Ref}),
    _ = ets:update_element(
        InflightTID, Ref, {?WORKER_MREF_IDX, AsyncWorkerMRef}
    ),
    ok.

ack_inflight(?NO_INFLIGHT, _Ref, _BufferWorkerPid) ->
    false;
ack_inflight(InflightTID, Ref, BufferWorkerPid) ->
    {Count, Removed} =
        case ets:take(InflightTID, Ref) of
            [?INFLIGHT_ITEM(Ref, ?QUERY(_, _, _, _, _), _IsRetriable, _AsyncWorkerMRef)] ->
                {1, true};
            [
                ?INFLIGHT_ITEM(
                    Ref, [?QUERY(_, _, _, _, _) | _] = Batch, _IsRetriable, _AsyncWorkerMRef
                )
            ] ->
                {length(Batch), true};
            [] ->
                {0, false}
        end,
    FlushCheck = dec_inflight_remove(InflightTID, Count, Removed),
    case FlushCheck of
        no_flush -> ok;
        flush -> ?MODULE:flush_worker(BufferWorkerPid)
    end,
    IsKnownRef = (Count > 0),
    IsKnownRef.

mark_inflight_items_as_retriable(Data, AsyncWorkerMRef) ->
    #{inflight_tid := InflightTID} = Data,
    IsRetriable = true,
    MatchSpec =
        ets:fun2ms(
            fun(?INFLIGHT_ITEM(Ref, BatchOrQuery, _IsRetriable, AsyncWorkerMRef0)) when
                AsyncWorkerMRef =:= AsyncWorkerMRef0
            ->
                ?INFLIGHT_ITEM(Ref, BatchOrQuery, IsRetriable, AsyncWorkerMRef0)
            end
        ),
    _NumAffected = ets:select_replace(InflightTID, MatchSpec),
    ?tp(buffer_worker_async_agent_down, #{num_affected => _NumAffected, buffer_worker => self()}),
    ok.

%% used to update a batch after dropping expired individual queries.
update_inflight_item(InflightTID, Ref, NewBatch, NumExpired) ->
    _ = ets:update_element(InflightTID, Ref, {?ITEM_IDX, NewBatch}),
    ok = dec_inflight_update(InflightTID, NumExpired).

inc_inflight(InflightTID, Count) ->
    _ = ets:update_counter(InflightTID, ?SIZE_REF, {2, Count}),
    _ = ets:update_counter(InflightTID, ?BATCH_COUNT_REF, {2, 1}),
    ok.

-spec dec_inflight_remove(?NO_INFLIGHT | ets:tid(), non_neg_integer(), Removed :: boolean()) ->
    no_flush | flush.
dec_inflight_remove(_InflightTID, _Count = 0, _Removed = false) ->
    no_flush;
dec_inflight_remove(InflightTID, _Count = 0, _Removed = true) ->
    NewValue = ets:update_counter(InflightTID, ?BATCH_COUNT_REF, {2, -1, 0, 0}),
    MaxValue = emqx_utils_ets:lookup_value(InflightTID, ?MAX_SIZE_REF, 0),
    %% if the new value is Max - 1, it means that we've just made room
    %% in the inflight table, so we should poke the buffer worker to
    %% make it continue flushing.
    case NewValue =:= MaxValue - 1 of
        true -> flush;
        false -> no_flush
    end;
dec_inflight_remove(InflightTID, Count, _Removed = true) when Count > 0 ->
    %% If Count > 0, it must have been removed
    NewValue = ets:update_counter(InflightTID, ?BATCH_COUNT_REF, {2, -1, 0, 0}),
    _ = ets:update_counter(InflightTID, ?SIZE_REF, {2, -Count, 0, 0}),
    MaxValue = emqx_utils_ets:lookup_value(InflightTID, ?MAX_SIZE_REF, 0),
    %% if the new value is Max - 1, it means that we've just made room
    %% in the inflight table, so we should poke the buffer worker to
    %% make it continue flushing.
    case NewValue =:= MaxValue - 1 of
        true -> flush;
        false -> no_flush
    end.

dec_inflight_update(_InflightTID, _Count = 0) ->
    ok;
dec_inflight_update(InflightTID, Count) when Count > 0 ->
    _ = ets:update_counter(InflightTID, ?SIZE_REF, {2, -Count, 0, 0}),
    ok.

%%==============================================================================

call_mode(force_sync, _) -> sync;
call_mode(async_if_possible, always_sync) -> sync;
call_mode(async_if_possible, async_if_possible) -> async.

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

queue_bytes(Q) ->
    replayq:bytes(Q).

disk_queue_dir(Id, Index) ->
    QDir0 = binary_to_list(Id) ++ ":" ++ integer_to_list(Index),
    QDir = filename:join([emqx:data_dir(), "bufs", node(), QDir0]),
    emqx_utils:safe_filename(QDir).

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

ensure_flush_timer(Data = #{tref := undefined}, 0) ->
    %% if the batch_time is 0, we don't need to start a timer, which
    %% can be costly at high rates.
    Ref = make_ref(),
    self() ! {flush, Ref},
    Data#{tref => {Ref, Ref}};
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
mark_as_sent(?QUERY(ReplyTo, Req, _HasBeenSent, ExpireAt, TraceCtx)) ->
    HasBeenSent = true,
    ?QUERY(ReplyTo, Req, HasBeenSent, ExpireAt, TraceCtx).

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
    lists:partition(
        fun(?QUERY(_ReplyTo, _CoreReq, _HasBeenSent, ExpireAt, _TraceCtx)) ->
            not is_expired(ExpireAt, Now)
        end,
        Batch
    ).

sieve_expired_requests_with_results(Batch, Now, Results) when is_list(Results) ->
    %% individual results; we need to drop those that match expired queries
    {RevNotExpiredBatch, RevNotExpiredResults, ExpiredBatch} =
        lists:foldl(
            fun(
                {?QUERY(_ReplyTo, _CoreReq, _HasBeenSent, ExpireAt, _TraceCtx) = Query, Result},
                {NotExpAcc, ResAcc, ExpAcc}
            ) ->
                case not is_expired(ExpireAt, Now) of
                    true ->
                        {[Query | NotExpAcc], [Result | ResAcc], ExpAcc};
                    false ->
                        {NotExpAcc, ResAcc, [Query | ExpAcc]}
                end
            end,
            {[], [], []},
            lists:zip(Batch, Results)
        ),
    {lists:reverse(RevNotExpiredBatch), lists:reverse(RevNotExpiredResults), ExpiredBatch};
sieve_expired_requests_with_results(Batch, Now, Result) ->
    %% one result for the whole batch, we just pass it along and
    %% `batch_reply_caller_defer_metrics' will expand it
    {NotExpiredBatch, ExpiredBatch} = sieve_expired_requests(Batch, Now),
    {NotExpiredBatch, ExpiredBatch, Result}.

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
    Opts0#{timeout => ?DEFAULT_REQUEST_TTL};
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
minimize(?QUERY(_, _, _, _, _) = Q) ->
    do_minimize(Q);
minimize(L) when is_list(L) ->
    lists:map(fun do_minimize/1, L).

-ifdef(TEST).
do_minimize(?QUERY(_ReplyTo, _Req, _Sent, _ExpireAt, _TraceCtx) = Query) -> Query.
-else.
do_minimize(?QUERY(ReplyTo, _Req, Sent, ExpireAt, TraceCtx)) ->
    ?QUERY(ReplyTo, [], Sent, ExpireAt, TraceCtx).
-endif.

%% To avoid message loss due to misconfigurations, we adjust
%% `batch_time' based on `request_ttl'.  If `batch_time' >
%% `request_ttl', all requests will timeout before being sent if
%% the message rate is low.  Even worse if `pool_size' is high.
%% We cap `batch_time' at `request_ttl div 2' as a rule of thumb.
adjust_batch_time(_Id, _RequestTTL = infinity, BatchTime0) ->
    BatchTime0;
adjust_batch_time(Id, RequestTTL, BatchTime0) ->
    BatchTime = max(0, min(BatchTime0, RequestTTL div 2)),
    case BatchTime =:= BatchTime0 of
        false ->
            ?SLOG(
                info,
                #{
                    resource_id => Id,
                    msg => "adjusting_buffer_worker_batch_time",
                    new_batch_time => BatchTime
                },
                #{tag => ?TAG}
            );
        true ->
            ok
    end,
    BatchTime.

replayq_opts(Id, Index, Opts) ->
    BufferMode = maps:get(buffer_mode, Opts, memory_only),
    TotalBytes = maps:get(max_buffer_bytes, Opts, ?DEFAULT_BUFFER_BYTES),
    case BufferMode of
        memory_only ->
            #{
                mem_only => true,
                marshaller => fun ?MODULE:queue_item_marshaller/1,
                max_total_bytes => TotalBytes,
                sizer => fun ?MODULE:estimate_size/1
            };
        volatile_offload ->
            SegBytes0 = maps:get(buffer_seg_bytes, Opts, TotalBytes),
            SegBytes = min(SegBytes0, TotalBytes),
            #{
                dir => disk_queue_dir(Id, Index),
                marshaller => fun ?MODULE:queue_item_marshaller/1,
                max_total_bytes => TotalBytes,
                %% we don't want to retain the queue after
                %% resource restarts.
                offload => {true, volatile},
                seg_bytes => SegBytes,
                sizer => fun ?MODULE:estimate_size/1
            }
    end.

%% The request timeout should be greater than the resume interval, as
%% it defines how often the buffer worker tries to unblock. If request
%% timeout is <= resume interval and the buffer worker is ever
%% blocked, than all queued requests will basically fail without being
%% attempted.
-spec default_resume_interval(request_ttl(), health_check_interval()) -> timeout_ms().
default_resume_interval(_RequestTTL = infinity, HealthCheckInterval) ->
    max(1, HealthCheckInterval);
default_resume_interval(RequestTTL, HealthCheckInterval) ->
    max(1, min(HealthCheckInterval, RequestTTL div 3)).

-spec reply_call(reference(), term()) -> ok.
reply_call(Alias, Response) ->
    %% Since we use a reference created with `{alias,
    %% reply_demonitor}', after we `demonitor' it in case of a
    %% timeout, we won't send any more messages that the caller is not
    %% expecting anymore.  Using `gen_statem:reply({pid(),
    %% reference()}, _)' would still send a late reply even after the
    %% demonitor.
    erlang:send(Alias, {Alias, Response}),
    ok.

%% Used by `simple_sync_internal_buffer_query' to reply and chain existing `reply_to'
%% callbacks.
reply_call_internal_buffer(ReplyAlias, MaybeReplyTo, Response) ->
    ?tp("reply_call_internal_buffer", #{}),
    ?MODULE:reply_call(ReplyAlias, Response),
    do_reply_caller(MaybeReplyTo, Response).

%% record buffer worker only when it's a request initiated from buffer worker.
buffer_worker(?NO_INFLIGHT) ->
    %% simple_sync_internal_buffer_query
    ?NO_BUFFER_WORKER;
buffer_worker(_Tid) ->
    self().

is_simple_query(#{simple_query := Bool}) ->
    Bool.

is_channel_apt_for_queries(?status_connected) ->
    true;
is_channel_apt_for_queries(?status_connecting) ->
    true;
is_channel_apt_for_queries(_) ->
    false.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
adjust_batch_time_test_() ->
    %% just for logging
    Id = some_id,
    [
        {"batch time smaller than request_time/2",
            ?_assertEqual(
                100,
                adjust_batch_time(Id, 500, 100)
            )},
        {"batch time equal to request_time/2",
            ?_assertEqual(
                100,
                adjust_batch_time(Id, 200, 100)
            )},
        {"batch time greater than request_time/2",
            ?_assertEqual(
                50,
                adjust_batch_time(Id, 100, 100)
            )},
        {"batch time smaller than request_time/2 (request_time = infinity)",
            ?_assertEqual(
                100,
                adjust_batch_time(Id, infinity, 100)
            )}
    ].
-endif.
