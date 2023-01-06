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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_statem).

-export([
    start_link/3,
    sync_query/3,
    async_query/3,
    block/1,
    resume/1
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

-define(Q_ITEM(REQUEST), {q_item, REQUEST}).

-define(COLLECT_REQ_LIMIT, 1000).
-define(SEND_REQ(FROM, REQUEST), {'$send_req', FROM, REQUEST}).
-define(QUERY(FROM, REQUEST, SENT), {query, FROM, REQUEST, SENT}).
-define(REPLY(FROM, REQUEST, SENT, RESULT), {reply, FROM, REQUEST, SENT, RESULT}).
-define(EXPAND(RESULT, BATCH), [
    ?REPLY(FROM, REQUEST, SENT, RESULT)
 || ?QUERY(FROM, REQUEST, SENT) <- BATCH
]).

-type id() :: binary().
-type index() :: pos_integer().
-type queue_query() :: ?QUERY(from(), request(), HasBeenSent :: boolean()).
-type request() :: term().
-type from() :: pid() | reply_fun() | request_from().
-type request_from() :: undefined | gen_statem:from().
-type state() :: blocked | running.
-type data() :: #{
    id => id(),
    index => index(),
    inflight_tid => ets:tid(),
    batch_size => pos_integer(),
    batch_time => timer:time(),
    queue => replayq:q(),
    resume_interval => timer:time(),
    tref => undefined | timer:tref()
}.

callback_mode() -> [state_functions, state_enter].

start_link(Id, Index, Opts) ->
    gen_statem:start_link(?MODULE, {Id, Index, Opts}, []).

-spec sync_query(id(), request(), query_opts()) -> Result :: term().
sync_query(Id, Request, Opts) ->
    PickKey = maps:get(pick_key, Opts, self()),
    Timeout = maps:get(timeout, Opts, infinity),
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
    QueryOpts = #{},
    emqx_resource_metrics:matched_inc(Id),
    Result = call_query(sync, Id, Index, ?QUERY(self(), Request, false), QueryOpts),
    _ = handle_query_result(Id, Result, false, false),
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
    QueryOpts = #{},
    emqx_resource_metrics:matched_inc(Id),
    Result = call_query(async, Id, Index, ?QUERY(ReplyFun, Request, false), QueryOpts),
    _ = handle_query_result(Id, Result, false, false),
    Result.

-spec block(pid()) -> ok.
block(ServerRef) ->
    gen_statem:cast(ServerRef, block).

-spec resume(pid()) -> ok.
resume(ServerRef) ->
    gen_statem:cast(ServerRef, resume).

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
            offload => true,
            seg_bytes => SegBytes,
            sizer => fun ?MODULE:estimate_size/1
        },
    Queue = replayq:open(QueueOpts),
    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Queue)),
    emqx_resource_metrics:inflight_set(Id, Index, 0),
    InfltWinSZ = maps:get(async_inflight_window, Opts, ?DEFAULT_INFLIGHT),
    InflightTID = inflight_new(InfltWinSZ, Id, Index),
    HCItvl = maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL),
    St = #{
        id => Id,
        index => Index,
        inflight_tid => InflightTID,
        batch_size => BatchSize,
        batch_time => maps:get(batch_time, Opts, ?DEFAULT_BATCH_TIME),
        queue => Queue,
        resume_interval => maps:get(resume_interval, Opts, HCItvl),
        tref => undefined
    },
    ?tp(resource_worker_init, #{id => Id, index => Index}),
    {ok, blocked, St, {next_event, cast, resume}}.

running(enter, _, St) ->
    ?tp(resource_worker_enter_running, #{}),
    maybe_flush(St);
running(cast, resume, _St) ->
    keep_state_and_data;
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
running(info, Info, _St) ->
    ?SLOG(error, #{msg => unexpected_msg, state => running, info => Info}),
    keep_state_and_data.

blocked(enter, _, #{resume_interval := ResumeT} = _St) ->
    ?tp(resource_worker_enter_blocked, #{}),
    {keep_state_and_data, {state_timeout, ResumeT, resume}};
blocked(cast, block, _St) ->
    keep_state_and_data;
blocked(cast, resume, St) ->
    do_resume(St);
blocked(state_timeout, resume, St) ->
    do_resume(St);
blocked(info, ?SEND_REQ(ReqFrom, {query, Request, Opts}), Data0) ->
    #{
        id := Id,
        index := Index,
        queue := Q
    } = Data0,
    From =
        case ReqFrom of
            undefined -> maps:get(async_reply_fun, Opts, undefined);
            From1 -> From1
        end,
    Error = ?RESOURCE_ERROR(blocked, "resource is blocked"),
    HasBeenSent = false,
    _ = reply_caller(Id, ?REPLY(From, Request, HasBeenSent, Error)),
    NewQ = append_queue(Id, Index, Q, [?QUERY(From, Request, HasBeenSent)]),
    Data = Data0#{queue := NewQ},
    {keep_state, Data};
blocked(info, {flush, _Ref}, _Data) ->
    keep_state_and_data;
blocked(info, Info, _Data) ->
    ?SLOG(error, #{msg => unexpected_msg, state => blocked, info => Info}),
    keep_state_and_data.

terminate(_Reason, #{id := Id, index := Index, queue := Q}) ->
    emqx_resource_metrics:inflight_set(Id, Index, 0),
    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q)),
    gproc_pool:disconnect_worker(Id, {Id, Index}),
    replayq:close(Q),
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

do_resume(#{id := Id, inflight_tid := InflightTID} = Data) ->
    case inflight_get_first(InflightTID) of
        empty ->
            retry_queue(Data);
        {Ref, FirstQuery} ->
            %% We retry msgs in inflight window sync, as if we send them
            %% async, they will be appended to the end of inflight window again.
            retry_inflight_sync(Id, Ref, FirstQuery, InflightTID, Data)
    end.

retry_queue(
    #{
        queue := Q0,
        id := Id,
        index := Index,
        batch_size := 1,
        inflight_tid := InflightTID,
        resume_interval := ResumeT
    } = Data0
) ->
    %% no batching
    case get_first_n_from_queue(Q0, 1) of
        empty ->
            {next_state, running, Data0};
        {Q1, QAckRef, [?QUERY(_, Request, HasBeenSent) = Query]} ->
            QueryOpts = #{inflight_name => InflightTID},
            Result = call_query(configured, Id, Index, Query, QueryOpts),
            Reply = ?REPLY(undefined, Request, HasBeenSent, Result),
            case reply_caller(Id, Reply) of
                true ->
                    {keep_state, Data0, {state_timeout, ResumeT, resume}};
                false ->
                    ok = replayq:ack(Q1, QAckRef),
                    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
                    Data = Data0#{queue := Q1},
                    retry_queue(Data)
            end
    end;
retry_queue(
    #{
        queue := Q,
        id := Id,
        index := Index,
        batch_size := BatchSize,
        inflight_tid := InflightTID,
        resume_interval := ResumeT
    } = Data0
) ->
    %% batching
    case get_first_n_from_queue(Q, BatchSize) of
        empty ->
            {next_state, running, Data0};
        {Q1, QAckRef, Batch0} ->
            QueryOpts = #{inflight_name => InflightTID},
            Result = call_query(configured, Id, Index, Batch0, QueryOpts),
            %% The caller has been replied with ?RESOURCE_ERROR(blocked, _) before saving into the queue,
            %% we now change the 'from' field to 'undefined' so it will not reply the caller again.
            Batch = [
                ?QUERY(undefined, Request, HasBeenSent0)
             || ?QUERY(_, Request, HasBeenSent0) <- Batch0
            ],
            case batch_reply_caller(Id, Result, Batch) of
                true ->
                    ?tp(resource_worker_retry_queue_batch_failed, #{batch => Batch}),
                    {keep_state, Data0, {state_timeout, ResumeT, resume}};
                false ->
                    ?tp(resource_worker_retry_queue_batch_succeeded, #{batch => Batch}),
                    ok = replayq:ack(Q1, QAckRef),
                    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
                    Data = Data0#{queue := Q1},
                    retry_queue(Data)
            end
    end.

retry_inflight_sync(
    Id,
    Ref,
    QueryOrBatch,
    InflightTID,
    #{index := Index, resume_interval := ResumeT} = Data0
) ->
    QueryOpts = #{},
    %% if we are retrying an inflight query, it has been sent
    HasBeenSent = true,
    Result = call_query(sync, Id, Index, QueryOrBatch, QueryOpts),
    BlockWorker = false,
    case handle_query_result(Id, Result, HasBeenSent, BlockWorker) of
        %% Send failed because resource is down
        true ->
            {keep_state, Data0, {state_timeout, ResumeT, resume}};
        %% Send ok or failed but the resource is working
        false ->
            inflight_drop(InflightTID, Ref, Id, Index),
            do_resume(Data0)
    end.

%% Called during the `running' state only.
-spec handle_query_requests(?SEND_REQ(request_from(), request()), data()) -> data().
handle_query_requests(Request0, Data0) ->
    #{
        id := Id,
        index := Index,
        queue := Q
    } = Data0,
    Requests = collect_requests([Request0], ?COLLECT_REQ_LIMIT),
    QueueItems =
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
    NewQ = append_queue(Id, Index, Q, QueueItems),
    Data = Data0#{queue := NewQ},
    maybe_flush(Data).

maybe_flush(Data) ->
    #{
        batch_size := BatchSize,
        queue := Q
    } = Data,
    QueueCount = queue_count(Q),
    case QueueCount >= BatchSize of
        true ->
            flush(Data);
        false ->
            {keep_state, ensure_flush_timer(Data)}
    end.

%% Called during the `running' state only.
-spec flush(data()) -> gen_statem:event_handler_result(state(), data()).
flush(Data0) ->
    #{
        batch_size := BatchSize,
        queue := Q0
    } = Data0,
    case replayq:count(Q0) of
        0 ->
            Data = cancel_flush_timer(Data0),
            {keep_state, Data};
        _ ->
            {Q1, QAckRef, Batch0} = replayq:pop(Q0, #{count_limit => BatchSize}),
            Batch = [Item || ?Q_ITEM(Item) <- Batch0],
            IsBatch = BatchSize =/= 1,
            %% We *must* use the new queue, because we currently can't
            %% `nack' a `pop'.
            %% Maybe we could re-open the queue?
            Data1 = Data0#{queue := Q1},
            do_flush(Data1, #{
                new_queue => Q1,
                is_batch => IsBatch,
                batch => Batch,
                ack_ref => QAckRef
            })
    end.

-spec do_flush(data(), #{
    is_batch := boolean(),
    batch := [?QUERY(from(), request(), boolean())],
    ack_ref := replayq:ack_ref()
}) ->
    gen_statem:event_handler_result(state(), data()).
do_flush(Data0, #{is_batch := false, batch := Batch, ack_ref := QAckRef, new_queue := Q1}) ->
    #{
        id := Id,
        index := Index,
        inflight_tid := InflightTID
    } = Data0,
    %% unwrap when not batching (i.e., batch size == 1)
    [?QUERY(From, CoreReq, HasBeenSent) = Request] = Batch,
    QueryOpts = #{inflight_name => InflightTID},
    Result = call_query(configured, Id, Index, Request, QueryOpts),
    IsAsync = is_async(Id),
    Data1 = cancel_flush_timer(Data0),
    Reply = ?REPLY(From, CoreReq, HasBeenSent, Result),
    case {reply_caller(Id, Reply), IsAsync} of
        %% failed and is not async; keep the request in the queue to
        %% be retried
        {true, false} ->
            %% Note: currently, we cannot safely pop an item from
            %% `replayq', keep the old reference to the queue and
            %% later try to append new items to the old ref: by
            %% popping an item, we may cause the side effect of
            %% closing an open segment and opening a new one, and the
            %% later `append' with the old file descriptor will fail
            %% with `einval' because it has been closed.  So we are
            %% forced to re-append the item, changing the order of
            %% requests...
            ok = replayq:ack(Q1, QAckRef),
            SentBatch = mark_as_sent(Batch),
            Q2 = append_queue(Id, Index, Q1, SentBatch),
            Data2 = Data1#{queue := Q2},
            {next_state, blocked, Data2};
        %% failed and is async; remove the request from the queue, as
        %% it is already in inflight table
        {true, true} ->
            ok = replayq:ack(Q1, QAckRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            {next_state, blocked, Data1};
        %% success; just ack
        {false, _} ->
            ok = replayq:ack(Q1, QAckRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            case replayq:count(Q1) > 0 of
                true ->
                    {keep_state, Data1, [{next_event, internal, flush}]};
                false ->
                    {keep_state, Data1}
            end
    end;
do_flush(Data0, #{is_batch := true, batch := Batch, ack_ref := QAckRef, new_queue := Q1}) ->
    #{
        id := Id,
        index := Index,
        batch_size := BatchSize,
        inflight_tid := InflightTID
    } = Data0,
    QueryOpts = #{inflight_name => InflightTID},
    Result = call_query(configured, Id, Index, Batch, QueryOpts),
    IsAsync = is_async(Id),
    Data1 = cancel_flush_timer(Data0),
    case {batch_reply_caller(Id, Result, Batch), IsAsync} of
        %% failed and is not async; keep the request in the queue to
        %% be retried
        {true, false} ->
            %% Note: currently, we cannot safely pop an item from
            %% `replayq', keep the old reference to the queue and
            %% later try to append new items to the old ref: by
            %% popping an item, we may cause the side effect of
            %% closing an open segment and opening a new one, and the
            %% later `append' with the old file descriptor will fail
            %% with `einval' because it has been closed.  So we are
            %% forced to re-append the item, changing the order of
            %% requests...
            ok = replayq:ack(Q1, QAckRef),
            SentBatch = mark_as_sent(Batch),
            Q2 = append_queue(Id, Index, Q1, SentBatch),
            Data2 = Data1#{queue := Q2},
            {next_state, blocked, Data2};
        %% failed and is async; remove the request from the queue, as
        %% it is already in inflight table
        {true, true} ->
            ok = replayq:ack(Q1, QAckRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            {next_state, blocked, Data1};
        %% success; just ack
        {false, _} ->
            ok = replayq:ack(Q1, QAckRef),
            emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q1)),
            CurrentCount = replayq:count(Q1),
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
    lists:foldl(
        fun(Reply, BlockWorker) ->
            reply_caller(Id, Reply, BlockWorker)
        end,
        false,
        %% the `Mod:on_batch_query/3` returns a single result for a batch,
        %% so we need to expand
        ?EXPAND(BatchResult, Batch)
    ).

reply_caller(Id, Reply) ->
    BlockWorker = false,
    reply_caller(Id, Reply, BlockWorker).

reply_caller(Id, ?REPLY(undefined, _, HasBeenSent, Result), BlockWorker) ->
    handle_query_result(Id, Result, HasBeenSent, BlockWorker);
reply_caller(Id, ?REPLY({ReplyFun, Args}, _, HasBeenSent, Result), BlockWorker) when
    is_function(ReplyFun)
->
    _ =
        case Result of
            {async_return, _} -> no_reply_for_now;
            _ -> apply(ReplyFun, Args ++ [Result])
        end,
    handle_query_result(Id, Result, HasBeenSent, BlockWorker);
reply_caller(Id, ?REPLY(From, _, HasBeenSent, Result), BlockWorker) ->
    gen_statem:reply(From, Result),
    handle_query_result(Id, Result, HasBeenSent, BlockWorker).

handle_query_result(Id, ?RESOURCE_ERROR_M(exception, Msg), HasBeenSent, BlockWorker) ->
    ?SLOG(error, #{msg => resource_exception, info => Msg}),
    inc_sent_failed(Id, HasBeenSent),
    BlockWorker;
handle_query_result(_Id, ?RESOURCE_ERROR_M(NotWorking, _), _HasBeenSent, _) when
    NotWorking == not_connected; NotWorking == blocked
->
    true;
handle_query_result(Id, ?RESOURCE_ERROR_M(not_found, Msg), _HasBeenSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => resource_not_found, info => Msg}),
    emqx_resource_metrics:dropped_resource_not_found_inc(Id),
    BlockWorker;
handle_query_result(Id, ?RESOURCE_ERROR_M(stopped, Msg), _HasBeenSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => resource_stopped, info => Msg}),
    emqx_resource_metrics:dropped_resource_stopped_inc(Id),
    BlockWorker;
handle_query_result(Id, ?RESOURCE_ERROR_M(Reason, _), _HasBeenSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => other_resource_error, reason => Reason}),
    emqx_resource_metrics:dropped_other_inc(Id),
    BlockWorker;
handle_query_result(Id, {error, {recoverable_error, Reason}}, _HasBeenSent, _BlockWorker) ->
    %% the message will be queued in replayq or inflight window,
    %% i.e. the counter 'queuing' or 'dropped' will increase, so we pretend that we have not
    %% sent this message.
    ?SLOG(warning, #{id => Id, msg => recoverable_error, reason => Reason}),
    true;
handle_query_result(Id, {error, Reason}, HasBeenSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => send_error, reason => Reason}),
    inc_sent_failed(Id, HasBeenSent),
    BlockWorker;
handle_query_result(_Id, {async_return, inflight_full}, _HasBeenSent, _BlockWorker) ->
    true;
handle_query_result(Id, {async_return, {error, Msg}}, HasBeenSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => async_send_error, info => Msg}),
    inc_sent_failed(Id, HasBeenSent),
    BlockWorker;
handle_query_result(_Id, {async_return, ok}, _HasBeenSent, BlockWorker) ->
    BlockWorker;
handle_query_result(Id, Result, HasBeenSent, BlockWorker) ->
    assert_ok_result(Result),
    inc_sent_success(Id, HasBeenSent),
    BlockWorker.

call_query(QM0, Id, Index, Query, QueryOpts) ->
    ?tp(call_query_enter, #{id => Id, query => Query}),
    case emqx_resource_manager:ets_lookup(Id) of
        {ok, _Group, #{mod := Mod, state := ResSt, status := connected} = Data} ->
            QM =
                case QM0 of
                    configured -> maps:get(query_mode, Data);
                    _ -> QM0
                end,
            CM = maps:get(callback_mode, Data),
            apply_query_fun(call_mode(QM, CM), Mod, Id, Index, Query, ResSt, QueryOpts);
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

apply_query_fun(sync, Mod, Id, _Index, ?QUERY(_, Request, _) = _Query, ResSt, _QueryOpts) ->
    ?tp(call_query, #{id => Id, mod => Mod, query => _Query, res_st => ResSt}),
    ?APPLY_RESOURCE(call_query, Mod:on_query(Id, Request, ResSt), Request);
apply_query_fun(async, Mod, Id, Index, ?QUERY(_, Request, _) = Query, ResSt, QueryOpts) ->
    ?tp(call_query_async, #{id => Id, mod => Mod, query => Query, res_st => ResSt}),
    InflightTID = maps:get(inflight_name, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_query_async,
        case is_inflight_full(InflightTID) of
            true ->
                {async_return, inflight_full};
            false ->
                ReplyFun = fun ?MODULE:reply_after_query/7,
                Ref = make_message_ref(),
                Args = [self(), Id, Index, InflightTID, Ref, Query],
                ok = inflight_append(InflightTID, Ref, Query, Id, Index),
                Result = Mod:on_query_async(Id, Request, {ReplyFun, Args}, ResSt),
                {async_return, Result}
        end,
        Request
    );
apply_query_fun(sync, Mod, Id, _Index, [?QUERY(_, _, _) | _] = Batch, ResSt, _QueryOpts) ->
    ?tp(call_batch_query, #{id => Id, mod => Mod, batch => Batch, res_st => ResSt}),
    Requests = [Request || ?QUERY(_From, Request, _) <- Batch],
    ?APPLY_RESOURCE(call_batch_query, Mod:on_batch_query(Id, Requests, ResSt), Batch);
apply_query_fun(async, Mod, Id, Index, [?QUERY(_, _, _) | _] = Batch, ResSt, QueryOpts) ->
    ?tp(call_batch_query_async, #{id => Id, mod => Mod, batch => Batch, res_st => ResSt}),
    InflightTID = maps:get(inflight_name, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_batch_query_async,
        case is_inflight_full(InflightTID) of
            true ->
                {async_return, inflight_full};
            false ->
                ReplyFun = fun ?MODULE:batch_reply_after_query/7,
                Ref = make_message_ref(),
                ReplyFunAndArgs = {ReplyFun, [self(), Id, Index, InflightTID, Ref, Batch]},
                Requests = [Request || ?QUERY(_From, Request, _) <- Batch],
                ok = inflight_append(InflightTID, Ref, Batch, Id, Index),
                Result = Mod:on_batch_query_async(Id, Requests, ReplyFunAndArgs, ResSt),
                {async_return, Result}
        end,
        Batch
    ).

reply_after_query(Pid, Id, Index, InflightTID, Ref, ?QUERY(From, Request, HasBeenSent), Result) ->
    %% NOTE: 'inflight' is the count of messages that were sent async
    %% but received no ACK, NOT the number of messages queued in the
    %% inflight window.
    case reply_caller(Id, ?REPLY(From, Request, HasBeenSent, Result)) of
        true ->
            ?MODULE:block(Pid);
        false ->
            drop_inflight_and_resume(Pid, InflightTID, Ref, Id, Index)
    end.

batch_reply_after_query(Pid, Id, Index, InflightTID, Ref, Batch, Result) ->
    %% NOTE: 'inflight' is the count of messages that were sent async
    %% but received no ACK, NOT the number of messages queued in the
    %% inflight window.
    case batch_reply_caller(Id, Result, Batch) of
        true ->
            ?MODULE:block(Pid);
        false ->
            drop_inflight_and_resume(Pid, InflightTID, Ref, Id, Index)
    end.

drop_inflight_and_resume(Pid, InflightTID, Ref, Id, Index) ->
    case is_inflight_full(InflightTID) of
        true ->
            inflight_drop(InflightTID, Ref, Id, Index),
            ?MODULE:resume(Pid);
        false ->
            inflight_drop(InflightTID, Ref, Id, Index)
    end.

%%==============================================================================
%% operations for queue
queue_item_marshaller(?Q_ITEM(_) = I) ->
    term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
    binary_to_term(Bin).

estimate_size(QItem) ->
    size(queue_item_marshaller(QItem)).

-spec append_queue(id(), index(), replayq:q(), [queue_query()]) -> replayq:q().
append_queue(Id, Index, Q, Queries) ->
    Q2 =
        case replayq:overflow(Q) of
            Overflow when Overflow =< 0 ->
                Q;
            Overflow ->
                PopOpts = #{bytes_limit => Overflow, count_limit => 999999999},
                {Q1, QAckRef, Items2} = replayq:pop(Q, PopOpts),
                ok = replayq:ack(Q1, QAckRef),
                Dropped = length(Items2),
                emqx_resource_metrics:dropped_queue_full_inc(Id),
                ?SLOG(error, #{msg => drop_query, reason => queue_full, dropped => Dropped}),
                Q1
        end,
    Items = [?Q_ITEM(X) || X <- Queries],
    Q3 = replayq:append(Q2, Items),
    emqx_resource_metrics:queuing_set(Id, Index, replayq:count(Q3)),
    ?tp(resource_worker_appended_to_queue, #{id => Id, items => Queries}),
    Q3.

-spec get_first_n_from_queue(replayq:q(), pos_integer()) ->
    empty | {replayq:q(), replayq:ack_ref(), [?Q_ITEM(?QUERY(_From, _Request, _HasBeenSent))]}.
get_first_n_from_queue(Q, N) ->
    case replayq:count(Q) of
        0 ->
            empty;
        _ ->
            {NewQ, QAckRef, Items} = replayq:pop(Q, #{count_limit => N}),
            Queries = [X || ?Q_ITEM(X) <- Items],
            {NewQ, QAckRef, Queries}
    end.

%%==============================================================================
%% the inflight queue for async query
-define(MAX_SIZE_REF, -1).
-define(SIZE_REF, -2).
inflight_new(InfltWinSZ, Id, Index) ->
    TableId = ets:new(
        emqx_resource_worker_inflight_tab,
        [ordered_set, public, {write_concurrency, true}]
    ),
    inflight_append(TableId, ?MAX_SIZE_REF, {max_size, InfltWinSZ}, Id, Index),
    %% we use this counter because we might deal with batches as
    %% elements.
    inflight_append(TableId, ?SIZE_REF, 0, Id, Index),
    TableId.

inflight_get_first(InflightTID) ->
    case ets:next(InflightTID, ?MAX_SIZE_REF) of
        '$end_of_table' ->
            empty;
        Ref ->
            case ets:lookup(InflightTID, Ref) of
                [Object] ->
                    Object;
                [] ->
                    %% it might have been dropped
                    inflight_get_first(InflightTID)
            end
    end.

is_inflight_full(undefined) ->
    false;
is_inflight_full(InflightTID) ->
    [{_, {max_size, MaxSize}}] = ets:lookup(InflightTID, ?MAX_SIZE_REF),
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

inflight_append(undefined, _Ref, _Query, _Id, _Index) ->
    ok;
inflight_append(InflightTID, Ref, [?QUERY(_, _, _) | _] = Batch0, Id, Index) ->
    Batch = mark_as_sent(Batch0),
    ets:insert(InflightTID, {Ref, Batch}),
    BatchSize = length(Batch),
    ets:update_counter(InflightTID, ?SIZE_REF, {2, BatchSize}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    ok;
inflight_append(InflightTID, Ref, ?QUERY(_From, _Req, _HasBeenSent) = Query0, Id, Index) ->
    Query = mark_as_sent(Query0),
    ets:insert(InflightTID, {Ref, Query}),
    ets:update_counter(InflightTID, ?SIZE_REF, {2, 1}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
    ok;
inflight_append(InflightTID, Ref, Data, _Id, _Index) ->
    ets:insert(InflightTID, {Ref, Data}),
    %% this is a metadata row being inserted; therefore, we don't bump
    %% the inflight metric.
    ok.

inflight_drop(undefined, _, _Id, _Index) ->
    ok;
inflight_drop(InflightTID, Ref, Id, Index) ->
    Count =
        case ets:take(InflightTID, Ref) of
            [{Ref, ?QUERY(_, _, _)}] -> 1;
            [{Ref, [?QUERY(_, _, _) | _] = Batch}] -> length(Batch);
            _ -> 0
        end,
    Count > 0 andalso ets:update_counter(InflightTID, ?SIZE_REF, {2, -Count, 0, 0}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_num_msgs(InflightTID)),
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
    QDir = binary_to_list(Id) ++ ":" ++ integer_to_list(Index),
    filename:join([emqx:data_dir(), "resource_worker", node(), QDir]).

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
    erlang:unique_integer([monotonic, positive]).

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
