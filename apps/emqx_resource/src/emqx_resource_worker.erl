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

-export([reply_after_query/6, batch_reply_after_query/6]).

-define(Q_ITEM(REQUEST), {q_item, REQUEST}).

-define(QUERY(FROM, REQUEST, SENT), {query, FROM, REQUEST, SENT}).
-define(REPLY(FROM, REQUEST, SENT, RESULT), {reply, FROM, REQUEST, SENT, RESULT}).
-define(EXPAND(RESULT, BATCH), [
    ?REPLY(FROM, REQUEST, SENT, RESULT)
 || ?QUERY(FROM, REQUEST, SENT) <- BATCH
]).

-type id() :: binary().
-type query() :: {query, from(), request()}.
-type request() :: term().
-type from() :: pid() | reply_fun().

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
    Result = call_query(sync, Id, ?QUERY(self(), Request, false), #{}),
    _ = handle_query_result(Id, Result, false, false),
    Result.

-spec simple_async_query(id(), request(), reply_fun()) -> Result :: term().
simple_async_query(Id, Request, ReplyFun) ->
    Result = call_query(async, Id, ?QUERY(ReplyFun, Request, false), #{}),
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

init({Id, Index, Opts}) ->
    process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Id, {Id, Index}),
    Name = name(Id, Index),
    BatchSize = maps:get(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
    Queue =
        case maps:get(enable_queue, Opts, false) of
            true ->
                replayq:open(#{
                    dir => disk_queue_dir(Id, Index),
                    seg_bytes => maps:get(queue_seg_bytes, Opts, ?DEFAULT_QUEUE_SEG_SIZE),
                    max_total_bytes => maps:get(max_queue_bytes, Opts, ?DEFAULT_QUEUE_SIZE),
                    sizer => fun ?MODULE:estimate_size/1,
                    marshaller => fun ?MODULE:queue_item_marshaller/1
                });
            false ->
                undefined
        end,
    emqx_resource_metrics:queuing_change(Id, queue_count(Queue)),
    InfltWinSZ = maps:get(async_inflight_window, Opts, ?DEFAULT_INFLIGHT),
    ok = inflight_new(Name, InfltWinSZ),
    HCItvl = maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL),
    St = #{
        id => Id,
        index => Index,
        name => Name,
        enable_batch => maps:get(enable_batch, Opts, false),
        batch_size => BatchSize,
        batch_time => maps:get(batch_time, Opts, ?DEFAULT_BATCH_TIME),
        queue => Queue,
        resume_interval => maps:get(resume_interval, Opts, HCItvl),
        acc => [],
        acc_left => BatchSize,
        tref => undefined
    },
    {ok, blocked, St, {next_event, cast, resume}}.

running(enter, _, _St) ->
    keep_state_and_data;
running(cast, resume, _St) ->
    keep_state_and_data;
running(cast, block, St) ->
    {next_state, blocked, St};
running(cast, {block, [?QUERY(_, _, _) | _] = Batch}, #{id := Id, queue := Q} = St) when
    is_list(Batch)
->
    Q1 = maybe_append_queue(Id, Q, [?Q_ITEM(Query) || Query <- Batch]),
    {next_state, blocked, St#{queue := Q1}};
running({call, From}, {query, Request, _Opts}, St) ->
    query_or_acc(From, Request, St);
running(cast, {query, Request, Opts}, St) ->
    ReplayFun = maps:get(async_reply_fun, Opts, undefined),
    query_or_acc(ReplayFun, Request, St);
running(info, {flush, Ref}, St = #{tref := {_TRef, Ref}}) ->
    flush(St#{tref := undefined});
running(info, {flush, _Ref}, _St) ->
    keep_state_and_data;
running(info, Info, _St) ->
    ?SLOG(error, #{msg => unexpected_msg, info => Info}),
    keep_state_and_data.

blocked(enter, _, #{resume_interval := ResumeT} = _St) ->
    {keep_state_and_data, {state_timeout, ResumeT, resume}};
blocked(cast, block, _St) ->
    keep_state_and_data;
blocked(cast, {block, [?QUERY(_, _, _) | _] = Batch}, #{id := Id, queue := Q} = St) when
    is_list(Batch)
->
    Q1 = maybe_append_queue(Id, Q, [?Q_ITEM(Query) || Query <- Batch]),
    {keep_state, St#{queue := Q1}};
blocked(cast, resume, St) ->
    do_resume(St);
blocked(state_timeout, resume, St) ->
    do_resume(St);
blocked({call, From}, {query, Request, _Opts}, #{id := Id, queue := Q} = St) ->
    Error = ?RESOURCE_ERROR(blocked, "resource is blocked"),
    _ = reply_caller(Id, ?REPLY(From, Request, false, Error)),
    {keep_state, St#{queue := maybe_append_queue(Id, Q, [?Q_ITEM(?QUERY(From, Request, false))])}};
blocked(cast, {query, Request, Opts}, #{id := Id, queue := Q} = St) ->
    ReplayFun = maps:get(async_reply_fun, Opts, undefined),
    Error = ?RESOURCE_ERROR(blocked, "resource is blocked"),
    _ = reply_caller(Id, ?REPLY(ReplayFun, Request, false, Error)),
    {keep_state, St#{
        queue := maybe_append_queue(Id, Q, [?Q_ITEM(?QUERY(ReplayFun, Request, false))])
    }}.

terminate(_Reason, #{id := Id, index := Index}) ->
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
    ?PICK(Id, Key, gen_statem:call(Pid, Query, {clean_timeout, Timeout})).

pick_cast(Id, Key, Query) ->
    ?PICK(Id, Key, gen_statem:cast(Pid, Query)).

do_resume(#{id := Id, name := Name} = St) ->
    case inflight_get_first(Name) of
        empty ->
            retry_queue(St);
        {Ref, FirstQuery} ->
            %% We retry msgs in inflight window sync, as if we send them
            %% async, they will be appended to the end of inflight window again.
            retry_inflight_sync(Id, Ref, FirstQuery, Name, St)
    end.

retry_queue(#{queue := undefined} = St) ->
    {next_state, running, St};
retry_queue(#{queue := Q, id := Id, enable_batch := false, resume_interval := ResumeT} = St) ->
    case get_first_n_from_queue(Q, 1) of
        [] ->
            {next_state, running, St};
        [?QUERY(_, Request, HasSent) = Query] ->
            QueryOpts = #{inflight_name => maps:get(name, St)},
            Result = call_query(configured, Id, Query, QueryOpts),
            case reply_caller(Id, ?REPLY(undefined, Request, HasSent, Result)) of
                true ->
                    {keep_state, St, {state_timeout, ResumeT, resume}};
                false ->
                    retry_queue(St#{queue := drop_head(Q, Id)})
            end
    end;
retry_queue(
    #{
        queue := Q,
        id := Id,
        enable_batch := true,
        batch_size := BatchSize,
        resume_interval := ResumeT
    } = St
) ->
    case get_first_n_from_queue(Q, BatchSize) of
        [] ->
            {next_state, running, St};
        Batch0 ->
            QueryOpts = #{inflight_name => maps:get(name, St)},
            Result = call_query(configured, Id, Batch0, QueryOpts),
            %% The caller has been replied with ?RESOURCE_ERROR(blocked, _) before saving into the queue,
            %% we now change the 'from' field to 'undefined' so it will not reply the caller again.
            Batch = [?QUERY(undefined, Request, HasSent) || ?QUERY(_, Request, HasSent) <- Batch0],
            case batch_reply_caller(Id, Result, Batch) of
                true ->
                    {keep_state, St, {state_timeout, ResumeT, resume}};
                false ->
                    retry_queue(St#{queue := drop_first_n_from_queue(Q, length(Batch), Id)})
            end
    end.

retry_inflight_sync(
    Id, Ref, ?QUERY(_, _, HasSent) = Query, Name, #{resume_interval := ResumeT} = St0
) ->
    Result = call_query(sync, Id, Query, #{}),
    case handle_query_result(Id, Result, HasSent, false) of
        %% Send failed because resource down
        true ->
            {keep_state, St0, {state_timeout, ResumeT, resume}};
        %% Send ok or failed but the resource is working
        false ->
            inflight_drop(Name, Ref),
            do_resume(St0)
    end.

query_or_acc(From, Request, #{enable_batch := true, acc := Acc, acc_left := Left, id := Id} = St0) ->
    Acc1 = [?QUERY(From, Request, false) | Acc],
    emqx_resource_metrics:batching_change(Id, 1),
    St = St0#{acc := Acc1, acc_left := Left - 1},
    case Left =< 1 of
        true -> flush(St);
        false -> {keep_state, ensure_flush_timer(St)}
    end;
query_or_acc(From, Request, #{enable_batch := false, queue := Q, id := Id} = St) ->
    QueryOpts = #{
        inflight_name => maps:get(name, St)
    },
    Result = call_query(configured, Id, ?QUERY(From, Request, false), QueryOpts),
    case reply_caller(Id, ?REPLY(From, Request, false, Result)) of
        true ->
            Query = ?QUERY(From, Request, false),
            {next_state, blocked, St#{queue := maybe_append_queue(Id, Q, [?Q_ITEM(Query)])}};
        false ->
            {keep_state, St}
    end.

flush(#{acc := []} = St) ->
    {keep_state, St};
flush(
    #{
        id := Id,
        acc := Batch0,
        batch_size := Size,
        queue := Q0
    } = St
) ->
    Batch = lists:reverse(Batch0),
    QueryOpts = #{
        inflight_name => maps:get(name, St)
    },
    emqx_resource_metrics:batching_change(Id, -length(Batch)),
    Result = call_query(configured, Id, Batch, QueryOpts),
    St1 = cancel_flush_timer(St#{acc_left := Size, acc := []}),
    case batch_reply_caller(Id, Result, Batch) of
        true ->
            Q1 = maybe_append_queue(Id, Q0, [?Q_ITEM(Query) || Query <- Batch]),
            {next_state, blocked, St1#{queue := Q1}};
        false ->
            {keep_state, St1}
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
    reply_caller(Id, Reply, false).

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

call_query(QM0, Id, Query, QueryOpts) ->
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
            apply_query_fun(call_mode(QM, CM), Mod, Id, Query, ResSt, QueryOpts);
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

apply_query_fun(sync, Mod, Id, ?QUERY(_, Request, _) = _Query, ResSt, _QueryOpts) ->
    ?tp(call_query, #{id => Id, mod => Mod, query => _Query, res_st => ResSt}),
    ?APPLY_RESOURCE(call_query, Mod:on_query(Id, Request, ResSt), Request);
apply_query_fun(async, Mod, Id, ?QUERY(_, Request, _) = Query, ResSt, QueryOpts) ->
    ?tp(call_query_async, #{id => Id, mod => Mod, query => Query, res_st => ResSt}),
    Name = maps:get(inflight_name, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_query_async,
        case inflight_is_full(Name) of
            true ->
                {async_return, inflight_full};
            false ->
                ok = emqx_resource_metrics:inflight_change(Id, 1),
                ReplyFun = fun ?MODULE:reply_after_query/6,
                Ref = make_message_ref(),
                Args = [self(), Id, Name, Ref, Query],
                ok = inflight_append(Name, Ref, Query),
                Result = Mod:on_query_async(Id, Request, {ReplyFun, Args}, ResSt),
                {async_return, Result}
        end,
        Request
    );
apply_query_fun(sync, Mod, Id, [?QUERY(_, _, _) | _] = Batch, ResSt, _QueryOpts) ->
    ?tp(call_batch_query, #{id => Id, mod => Mod, batch => Batch, res_st => ResSt}),
    Requests = [Request || ?QUERY(_From, Request, _) <- Batch],
    ?APPLY_RESOURCE(call_batch_query, Mod:on_batch_query(Id, Requests, ResSt), Batch);
apply_query_fun(async, Mod, Id, [?QUERY(_, _, _) | _] = Batch, ResSt, QueryOpts) ->
    ?tp(call_batch_query_async, #{id => Id, mod => Mod, batch => Batch, res_st => ResSt}),
    Name = maps:get(inflight_name, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        call_batch_query_async,
        case inflight_is_full(Name) of
            true ->
                {async_return, inflight_full};
            false ->
                BatchLen = length(Batch),
                ok = emqx_resource_metrics:inflight_change(Id, BatchLen),
                ReplyFun = fun ?MODULE:batch_reply_after_query/6,
                Ref = make_message_ref(),
                Args = {ReplyFun, [self(), Id, Name, Ref, Batch]},
                Requests = [Request || ?QUERY(_From, Request, _) <- Batch],
                ok = inflight_append(Name, Ref, Batch),
                Result = Mod:on_batch_query_async(Id, Requests, Args, ResSt),
                {async_return, Result}
        end,
        Batch
    ).

reply_after_query(Pid, Id, Name, Ref, ?QUERY(From, Request, HasSent), Result) ->
    %% NOTE: 'inflight' is message count that sent async but no ACK received,
    %%        NOT the message number ququed in the inflight window.
    emqx_resource_metrics:inflight_change(Id, -1),
    case reply_caller(Id, ?REPLY(From, Request, HasSent, Result)) of
        true ->
            %% we marked these messages are 'queuing' although they are actually
            %% keeped in inflight window, not replayq
            emqx_resource_metrics:queuing_change(Id, 1),
            ?MODULE:block(Pid);
        false ->
            drop_inflight_and_resume(Pid, Name, Ref)
    end.

batch_reply_after_query(Pid, Id, Name, Ref, Batch, Result) ->
    %% NOTE: 'inflight' is message count that sent async but no ACK received,
    %%        NOT the message number ququed in the inflight window.
    BatchLen = length(Batch),
    emqx_resource_metrics:inflight_change(Id, -BatchLen),
    case batch_reply_caller(Id, Result, Batch) of
        true ->
            %% we marked these messages are 'queuing' although they are actually
            %% kept in inflight window, not replayq
            emqx_resource_metrics:queuing_change(Id, BatchLen),
            ?MODULE:block(Pid);
        false ->
            drop_inflight_and_resume(Pid, Name, Ref)
    end.

drop_inflight_and_resume(Pid, Name, Ref) ->
    case inflight_is_full(Name) of
        true ->
            inflight_drop(Name, Ref),
            ?MODULE:resume(Pid);
        false ->
            inflight_drop(Name, Ref)
    end.

%%==============================================================================
%% operations for queue
queue_item_marshaller(?Q_ITEM(_) = I) ->
    term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
    binary_to_term(Bin).

estimate_size(QItem) ->
    size(queue_item_marshaller(QItem)).

maybe_append_queue(Id, undefined, _Items) ->
    emqx_resource_metrics:dropped_queue_not_enabled_inc(Id),
    undefined;
maybe_append_queue(Id, Q, Items) ->
    Q2 =
        case replayq:overflow(Q) of
            Overflow when Overflow =< 0 ->
                Q;
            Overflow ->
                PopOpts = #{bytes_limit => Overflow, count_limit => 999999999},
                {Q1, QAckRef, Items2} = replayq:pop(Q, PopOpts),
                ok = replayq:ack(Q1, QAckRef),
                Dropped = length(Items2),
                emqx_resource_metrics:queuing_change(Id, -Dropped),
                emqx_resource_metrics:dropped_queue_full_inc(Id),
                ?SLOG(error, #{msg => drop_query, reason => queue_full, dropped => Dropped}),
                Q1
        end,
    emqx_resource_metrics:queuing_change(Id, 1),
    replayq:append(Q2, Items).

get_first_n_from_queue(Q, N) ->
    get_first_n_from_queue(Q, N, []).

get_first_n_from_queue(_Q, 0, Acc) ->
    lists:reverse(Acc);
get_first_n_from_queue(Q, N, Acc) when N > 0 ->
    case replayq:peek(Q) of
        empty -> Acc;
        ?Q_ITEM(Query) -> get_first_n_from_queue(Q, N - 1, [Query | Acc])
    end.

drop_first_n_from_queue(Q, 0, _Id) ->
    Q;
drop_first_n_from_queue(Q, N, Id) when N > 0 ->
    drop_first_n_from_queue(drop_head(Q, Id), N - 1, Id).

drop_head(Q, Id) ->
    {Q1, AckRef, _} = replayq:pop(Q, #{count_limit => 1}),
    ok = replayq:ack(Q1, AckRef),
    emqx_resource_metrics:queuing_change(Id, -1),
    Q1.

%%==============================================================================
%% the inflight queue for async query
-define(SIZE_REF, -1).
inflight_new(Name, InfltWinSZ) ->
    _ = ets:new(Name, [named_table, ordered_set, public, {write_concurrency, true}]),
    inflight_append(Name, ?SIZE_REF, {max_size, InfltWinSZ}),
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
    case ets:info(Name, size) of
        Size when Size > MaxSize -> true;
        _ -> false
    end.

inflight_append(undefined, _Ref, _Query) ->
    ok;
inflight_append(Name, Ref, [?QUERY(_, _, _) | _] = Batch) ->
    ets:insert(Name, {Ref, [?QUERY(From, Req, true) || ?QUERY(From, Req, _) <- Batch]}),
    ok;
inflight_append(Name, Ref, ?QUERY(From, Req, _)) ->
    ets:insert(Name, {Ref, ?QUERY(From, Req, true)}),
    ok;
inflight_append(Name, Ref, Data) ->
    ets:insert(Name, {Ref, Data}),
    ok.

inflight_drop(undefined, _) ->
    ok;
inflight_drop(Name, Ref) ->
    ets:delete(Name, Ref),
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

queue_count(undefined) ->
    0;
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
