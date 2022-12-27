%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    block/2,
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
-type query() :: {query, request(), query_opts()}.
-type request() :: term().
-type from() :: pid() | reply_fun() | request_from().
-type request_from() :: undefined | gen_statem:from().
-type state() :: blocked | running.
-type data() :: #{
    id => id(),
    index => pos_integer(),
    name => atom(),
    %% enable_batch => boolean(),
    batch_size => pos_integer(),
    batch_time => timer:time(),
    %% queue => undefined | replayq:q(),
    queue => replayq:q(),
    resume_interval => timer:time(),
    %% acc => [?QUERY(_, _, _)],
    %% acc_left => non_neg_integer(),
    tref => undefined | timer:tref()
}.

-callback batcher_flush(Acc :: [{from(), request()}], CbState :: term()) ->
    {{from(), result()}, NewCbState :: term()}.

callback_mode() -> [state_functions, state_enter].

start_link(Id, Index, Opts) ->
    gen_statem:start_link({local, name(Id, Index)}, ?MODULE, {Id, Index, Opts}, []).

-spec sync_query(id(), request(), query_opts()) -> Result :: term().
sync_query(Id, Request, Opts) ->
    PickKey = maps:get(pick_key, Opts, self()),
    Timeout = maps:get(timeout, Opts, infinity),
    pick_call(Id, PickKey, {query, Request, Opts}, Timeout).

-spec async_query(id(), request(), query_opts()) -> Result :: term().
async_query(Id, Request, Opts) ->
    PickKey = maps:get(pick_key, Opts, self()),
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
    Result = call_query(sync, Id, Index, ?QUERY(self(), Request, false), #{}),
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
    Result = call_query(async, Id, Index, ?QUERY(ReplyFun, Request, false), #{}),
    _ = handle_query_result(Id, Result, false, false),
    Result.

-spec block(pid() | atom()) -> ok.
block(ServerRef) ->
    gen_statem:cast(ServerRef, block).

-spec block(pid() | atom(), [query()]) -> ok.
block(ServerRef, Query) ->
    gen_statem:cast(ServerRef, {block, Query}).

-spec resume(pid() | atom()) -> ok.
resume(ServerRef) ->
    gen_statem:cast(ServerRef, resume).

-spec init({id(), pos_integer(), map()}) -> gen_statem:init_result(state(), data()).
init({Id, Index, Opts}) ->
    process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Id, {Id, Index}),
    Name = name(Id, Index),
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
    ok = inflight_new(Name, InfltWinSZ, Id, Index),
    HCItvl = maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL),
    St = #{
        id => Id,
        index => Index,
        name => Name,
        batch_size => BatchSize,
        batch_time => maps:get(batch_time, Opts, ?DEFAULT_BATCH_TIME),
        queue => Queue,
        resume_interval => maps:get(resume_interval, Opts, HCItvl),
        tref => undefined
    },
    {ok, blocked, St, {next_event, cast, resume}}.

running(enter, _, St) ->
    flush(St);
running(cast, resume, _St) ->
    keep_state_and_data;
running(cast, block, St) ->
    {next_state, blocked, St};
running(
    cast, {block, [?QUERY(_, _, _) | _] = Batch}, #{id := Id, index := Index, queue := Q} = St
) when
    is_list(Batch)
->
    Q1 = append_queue(Id, Index, Q, [?Q_ITEM(Query) || Query <- Batch]),
    {next_state, blocked, St#{queue := Q1}};
running(info, ?SEND_REQ(_From, _Req) = Request0, Data) ->
    handle_query_requests(Request0, Data);
running(info, {flush, Ref}, St = #{tref := {_TRef, Ref}}) ->
    flush(St#{tref := undefined});
running(internal, flush, St) ->
    flush(St);
running(info, {flush, _Ref}, _St) ->
    keep_state_and_data;
running(info, Info, _St) ->
    ?SLOG(error, #{msg => unexpected_msg, info => Info}),
    keep_state_and_data.

blocked(enter, _, #{resume_interval := ResumeT} = _St) ->
    {keep_state_and_data, {state_timeout, ResumeT, resume}};
blocked(cast, block, _St) ->
    keep_state_and_data;
blocked(
    cast, {block, [?QUERY(_, _, _) | _] = Batch}, #{id := Id, index := Index, queue := Q} = St
) when
    is_list(Batch)
->
    Q1 = append_queue(Id, Index, Q, [?Q_ITEM(Query) || Query <- Batch]),
    {keep_state, St#{queue := Q1}};
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
    NewQ = append_queue(Id, Index, Q, [?Q_ITEM(?QUERY(From, Request, HasBeenSent))]),
    Data = Data0#{queue := NewQ},
    {keep_state, Data}.

terminate(_Reason, #{id := Id, index := Index, queue := Q}) ->
    emqx_resource_metrics:inflight_set(Id, Index, 0),
    emqx_resource_metrics:queuing_set(Id, Index, queue_count(Q)),
    gproc_pool:disconnect_worker(Id, {Id, Index}).

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
%% ?PICK(Id, Key, gen_statem:call(Pid, Query, {clean_timeout, Timeout})).

pick_cast(Id, Key, Query) ->
    ?PICK(Id, Key, begin
        From = undefined,
        erlang:send(Pid, ?SEND_REQ(From, Query)),
        ok
    end).
%% ?PICK(Id, Key, gen_statem:cast(Pid, Query)).

do_resume(#{id := Id, name := Name} = St) ->
    case inflight_get_first(Name) of
        empty ->
            retry_queue(St);
        {Ref, FirstQuery} ->
            %% We retry msgs in inflight window sync, as if we send them
            %% async, they will be appended to the end of inflight window again.
            retry_inflight_sync(Id, Ref, FirstQuery, Name, St)
    end.

retry_queue(
    #{
        queue := Q,
        id := Id,
        index := Index,
        batch_size := BatchSize,
        resume_interval := ResumeT
    } = St
) ->
    case get_first_n_from_queue(Q, BatchSize) of
        [] ->
            {next_state, running, St};
        Batch0 ->
            QueryOpts = #{inflight_name => maps:get(name, St)},
            Result = call_query(configured, Id, Index, Batch0, QueryOpts),
            %% The caller has been replied with ?RESOURCE_ERROR(blocked, _) before saving into the queue,
            %% we now change the 'from' field to 'undefined' so it will not reply the caller again.
            Batch = [
                ?QUERY(undefined, Request, HasBeenSent)
             || ?QUERY(_, Request, HasBeenSent) <- Batch0
            ],
            case batch_reply_caller(Id, Result, Batch) of
                true ->
                    {keep_state, St, {state_timeout, ResumeT, resume}};
                false ->
                    retry_queue(St#{queue := drop_first_n_from_queue(Q, length(Batch), Id, Index)})
            end
    end.

retry_inflight_sync(
    Id,
    Ref,
    ?QUERY(_, _, HasSent) = Query,
    Name,
    #{index := Index, resume_interval := ResumeT} = St0
) ->
    Result = call_query(sync, Id, Index, Query, #{}),
    case handle_query_result(Id, Result, HasSent, false) of
        %% Send failed because resource down
        true ->
            {keep_state, St0, {state_timeout, ResumeT, resume}};
        %% Send ok or failed but the resource is working
        false ->
            inflight_drop(Name, Ref, Id, Index),
            do_resume(St0)
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
                    ?Q_ITEM(?QUERY(ReplyFun, Req, HasBeenSent));
                (?SEND_REQ(From, {query, Req, _Opts})) ->
                    HasBeenSent = false,
                    ?Q_ITEM(?QUERY(From, Req, HasBeenSent))
            end,
            Requests
        ),
    NewQ = append_queue(Id, Index, Q, QueueItems),
    emqx_resource_metrics:queuing_set(Id, Index, queue_count(NewQ)),
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
            keep_state_and_data;
        _ ->
            {Q1, QAckRef, Batch0} = replayq:pop(Q0, #{count_limit => BatchSize}),
            Batch = [Item || ?Q_ITEM(Item) <- Batch0],
            Data1 = Data0#{queue := Q1},
            IsBatch = BatchSize =/= 1,
            do_flush(Data1, #{
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
do_flush(Data0, #{is_batch := true, batch := Batch, ack_ref := QAckRef}) ->
    #{
        id := Id,
        index := Index,
        name := Name,
        queue := Q0
    } = Data0,
    QueryOpts = #{inflight_name => Name},
    Result = call_query(configured, Id, Index, Batch, QueryOpts),
    Data1 = cancel_flush_timer(Data0),
    case batch_reply_caller(Id, Result, Batch) of
        true ->
            %% call failed and will retry later; we re-append them to
            %% the queue to give chance to other batches to be tried
            %% before this failed one is eventually retried.
            ok = replayq:ack(Q0, QAckRef),
            Q1 = append_queue(Id, Index, Q0, [?Q_ITEM(Query) || Query <- Batch]),
            Data = Data1#{queue := Q1},
            {next_state, blocked, Data};
        false ->
            ok = replayq:ack(Q0, QAckRef),
            case replayq:count(Q0) > 0 of
                true ->
                    {keep_state, Data1, [{next_event, internal, flush}]};
                false ->
                    {keep_state, Data1}
            end
    end;
do_flush(Data0, #{is_batch := false, batch := Batch, ack_ref := QAckRef}) ->
    #{
        id := Id,
        index := Index,
        name := Name,
        queue := Q0
    } = Data0,
    %% unwrap when not batching (i.e., batch size == 1)
    [?QUERY(From, CoreReq, _HasBeenSent) = Request] = Batch,
    %% [Request = {query, CoreReq, _Opts}] = Batch,
    QueryOpts = #{inflight_name => Name},
    Result = call_query(configured, Id, Index, Request, QueryOpts),
    Data2 = cancel_flush_timer(Data0),
    HasBeenSent = false,
    case reply_caller(Id, ?REPLY(From, CoreReq, HasBeenSent, Result)) of
        true ->
            %% call failed and will retry later; we re-append them to
            %% the queue to give chance to other batches to be tried
            %% before this failed one is eventually retried.
            ok = replayq:ack(Q0, QAckRef),
            Q1 = append_queue(Id, Index, Q0, [?Q_ITEM(Query) || Query <- Batch]),
            Data = Data2#{queue := Q1},
            {next_state, blocked, Data};
        false ->
            ok = replayq:ack(Q0, QAckRef),
            case replayq:count(Q0) > 0 of
                true ->
                    {keep_state, Data2, [{next_event, internal, flush}]};
                false ->
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

reply_caller(Id, ?REPLY(undefined, _, HasSent, Result), BlockWorker) ->
    handle_query_result(Id, Result, HasSent, BlockWorker);
reply_caller(Id, ?REPLY({ReplyFun, Args}, _, HasSent, Result), BlockWorker) when
    is_function(ReplyFun)
->
    _ =
        case Result of
            {async_return, _} -> no_reply_for_now;
            _ -> apply(ReplyFun, Args ++ [Result])
        end,
    handle_query_result(Id, Result, HasSent, BlockWorker);
reply_caller(Id, ?REPLY(From, _, HasSent, Result), BlockWorker) ->
    gen_statem:reply(From, Result),
    handle_query_result(Id, Result, HasSent, BlockWorker).

handle_query_result(Id, ?RESOURCE_ERROR_M(exception, Msg), HasSent, BlockWorker) ->
    ?SLOG(error, #{msg => resource_exception, info => Msg}),
    inc_sent_failed(Id, HasSent),
    BlockWorker;
handle_query_result(_Id, ?RESOURCE_ERROR_M(NotWorking, _), _HasSent, _) when
    NotWorking == not_connected; NotWorking == blocked
->
    true;
handle_query_result(Id, ?RESOURCE_ERROR_M(not_found, Msg), _HasSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => resource_not_found, info => Msg}),
    emqx_resource_metrics:dropped_resource_not_found_inc(Id),
    BlockWorker;
handle_query_result(Id, ?RESOURCE_ERROR_M(stopped, Msg), _HasSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => resource_stopped, info => Msg}),
    emqx_resource_metrics:dropped_resource_stopped_inc(Id),
    BlockWorker;
handle_query_result(Id, ?RESOURCE_ERROR_M(Reason, _), _HasSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => other_resource_error, reason => Reason}),
    emqx_resource_metrics:dropped_other_inc(Id),
    BlockWorker;
handle_query_result(Id, {error, {recoverable_error, Reason}}, _HasSent, _BlockWorker) ->
    %% the message will be queued in replayq or inflight window,
    %% i.e. the counter 'queuing' or 'dropped' will increase, so we pretend that we have not
    %% sent this message.
    ?SLOG(warning, #{id => Id, msg => recoverable_error, reason => Reason}),
    true;
handle_query_result(Id, {error, Reason}, HasSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => send_error, reason => Reason}),
    inc_sent_failed(Id, HasSent),
    BlockWorker;
handle_query_result(_Id, {async_return, inflight_full}, _HasSent, _BlockWorker) ->
    true;
handle_query_result(Id, {async_return, {error, Msg}}, HasSent, BlockWorker) ->
    ?SLOG(error, #{id => Id, msg => async_send_error, info => Msg}),
    inc_sent_failed(Id, HasSent),
    BlockWorker;
handle_query_result(_Id, {async_return, ok}, _HasSent, BlockWorker) ->
    BlockWorker;
handle_query_result(Id, Result, HasSent, BlockWorker) ->
    assert_ok_result(Result),
    inc_sent_success(Id, HasSent),
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
            emqx_resource_metrics:matched_inc(Id),
            apply_query_fun(call_mode(QM, CM), Mod, Id, Index, Query, ResSt, QueryOpts);
        {ok, _Group, #{status := stopped}} ->
            emqx_resource_metrics:matched_inc(Id),
            ?RESOURCE_ERROR(stopped, "resource stopped or disabled");
        {ok, _Group, #{status := S}} when S == connecting; S == disconnected ->
            emqx_resource_metrics:matched_inc(Id),
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
    Name = maps:get(inflight_name, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_query_async,
        case inflight_is_full(Name) of
            true ->
                {async_return, inflight_full};
            false ->
                ReplyFun = fun ?MODULE:reply_after_query/7,
                Ref = make_message_ref(),
                Args = [self(), Id, Index, Name, Ref, Query],
                ok = inflight_append(Name, Ref, Query, Id, Index),
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
    Name = maps:get(inflight_name, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_batch_query_async,
        case inflight_is_full(Name) of
            true ->
                {async_return, inflight_full};
            false ->
                ReplyFun = fun ?MODULE:batch_reply_after_query/7,
                Ref = make_message_ref(),
                ReplyFunAndArgs = {ReplyFun, [self(), Id, Index, Name, Ref, Batch]},
                Requests = [Request || ?QUERY(_From, Request, _) <- Batch],
                ok = inflight_append(Name, Ref, Batch, Id, Index),
                Result = Mod:on_batch_query_async(Id, Requests, ReplyFunAndArgs, ResSt),
                {async_return, Result}
        end,
        Batch
    ).

reply_after_query(Pid, Id, Index, Name, Ref, ?QUERY(From, Request, HasSent), Result) ->
    %% NOTE: 'inflight' is the count of messages that were sent async
    %% but received no ACK, NOT the number of messages queued in the
    %% inflight window.
    case reply_caller(Id, ?REPLY(From, Request, HasSent, Result)) of
        true ->
            ?MODULE:block(Pid);
        false ->
            drop_inflight_and_resume(Pid, Name, Ref, Id, Index)
    end.

batch_reply_after_query(Pid, Id, Index, Name, Ref, Batch, Result) ->
    %% NOTE: 'inflight' is the count of messages that were sent async
    %% but received no ACK, NOT the number of messages queued in the
    %% inflight window.
    case batch_reply_caller(Id, Result, Batch) of
        true ->
            ?MODULE:block(Pid);
        false ->
            drop_inflight_and_resume(Pid, Name, Ref, Id, Index)
    end.

drop_inflight_and_resume(Pid, Name, Ref, Id, Index) ->
    case inflight_is_full(Name) of
        true ->
            inflight_drop(Name, Ref, Id, Index),
            ?MODULE:resume(Pid);
        false ->
            inflight_drop(Name, Ref, Id, Index)
    end.

%%==============================================================================
%% operations for queue
queue_item_marshaller(?Q_ITEM(_) = I) ->
    term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
    binary_to_term(Bin).

estimate_size(QItem) ->
    size(queue_item_marshaller(QItem)).

append_queue(Id, Index, Q, Items) ->
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
    Q3 = replayq:append(Q2, Items),
    emqx_resource_metrics:queuing_set(Id, Index, replayq:count(Q3)),
    Q3.

get_first_n_from_queue(Q, N) ->
    get_first_n_from_queue(Q, N, []).

get_first_n_from_queue(_Q, 0, Acc) ->
    lists:reverse(Acc);
get_first_n_from_queue(Q, N, Acc) when N > 0 ->
    case replayq:peek(Q) of
        empty -> Acc;
        ?Q_ITEM(Query) -> get_first_n_from_queue(Q, N - 1, [Query | Acc])
    end.

drop_first_n_from_queue(Q, 0, _Id, _Index) ->
    Q;
drop_first_n_from_queue(Q, N, Id, Index) when N > 0 ->
    drop_first_n_from_queue(drop_head(Q, Id, Index), N - 1, Id, Index).

drop_head(Q, Id, Index) ->
    {NewQ, AckRef, _} = replayq:pop(Q, #{count_limit => 1}),
    ok = replayq:ack(NewQ, AckRef),
    emqx_resource_metrics:queuing_set(Id, Index, replayq:count(NewQ)),
    NewQ.

%%==============================================================================
%% the inflight queue for async query
-define(SIZE_REF, -1).
inflight_new(Name, InfltWinSZ, Id, Index) ->
    _ = ets:new(Name, [named_table, ordered_set, public, {write_concurrency, true}]),
    inflight_append(Name, ?SIZE_REF, {max_size, InfltWinSZ}, Id, Index),
    ok.

inflight_get_first(Name) ->
    case ets:next(Name, ?SIZE_REF) of
        '$end_of_table' ->
            empty;
        Ref ->
            case ets:lookup(Name, Ref) of
                [Object] ->
                    Object;
                [] ->
                    %% it might have been dropped
                    inflight_get_first(Name)
            end
    end.

inflight_is_full(undefined) ->
    false;
inflight_is_full(Name) ->
    [{_, {max_size, MaxSize}}] = ets:lookup(Name, ?SIZE_REF),
    Size = inflight_size(Name),
    Size >= MaxSize.

inflight_size(Name) ->
    %% Note: we subtract 1 because there's a metadata row that hold
    %% the maximum size value.
    MetadataRowCount = 1,
    case ets:info(Name, size) of
        undefined -> 0;
        Size -> max(0, Size - MetadataRowCount)
    end.

inflight_append(undefined, _Ref, _Query, _Id, _Index) ->
    ok;
inflight_append(Name, Ref, [?QUERY(_, _, _) | _] = Batch, Id, Index) ->
    ets:insert(Name, {Ref, [?QUERY(From, Req, true) || ?QUERY(From, Req, _) <- Batch]}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_size(Name)),
    ok;
inflight_append(Name, Ref, ?QUERY(From, Req, _), Id, Index) ->
    ets:insert(Name, {Ref, ?QUERY(From, Req, true)}),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_size(Name)),
    ok;
inflight_append(Name, Ref, Data, _Id, _Index) ->
    ets:insert(Name, {Ref, Data}),
    %% this is a metadata row being inserted; therefore, we don't bump
    %% the inflight metric.
    ok.

inflight_drop(undefined, _, _Id, _Index) ->
    ok;
inflight_drop(Name, Ref, Id, Index) ->
    ets:delete(Name, Ref),
    emqx_resource_metrics:inflight_set(Id, Index, inflight_size(Name)),
    ok.

%%==============================================================================

inc_sent_failed(Id, _HasSent = true) ->
    emqx_resource_metrics:retried_failed_inc(Id);
inc_sent_failed(Id, _HasSent) ->
    emqx_resource_metrics:failed_inc(Id).

inc_sent_success(Id, _HasSent = true) ->
    emqx_resource_metrics:retried_success_inc(Id);
inc_sent_success(Id, _HasSent) ->
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

-spec name(id(), integer()) -> atom().
name(Id, Index) ->
    Mod = atom_to_list(?MODULE),
    Id1 = binary_to_list(Id),
    Index1 = integer_to_list(Index),
    list_to_atom(lists:concat([Mod, ":", Id1, ":", Index1])).

disk_queue_dir(Id, Index) ->
    QDir = binary_to_list(Id) ++ ":" ++ integer_to_list(Index),
    filename:join([emqx:data_dir(), "resource_worker", node(), QDir]).

ensure_flush_timer(St = #{tref := undefined, batch_time := T}) ->
    Ref = make_ref(),
    TRef = erlang:send_after(T, self(), {flush, Ref}),
    St#{tref => {TRef, Ref}};
ensure_flush_timer(St) ->
    St.

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
