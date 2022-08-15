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
    query/3,
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

-define(QUERY(FROM, REQUEST), {query, FROM, REQUEST}).
-define(REPLY(FROM, REQUEST, RESULT), {reply, FROM, REQUEST, RESULT}).
-define(EXPAND(RESULT, BATCH), [?REPLY(FROM, REQUEST, RESULT) || ?QUERY(FROM, REQUEST) <- BATCH]).

-type id() :: binary().
-type query() :: {query, from(), request()}.
-type request() :: term().
-type from() :: pid() | reply_fun().

-callback batcher_flush(Acc :: [{from(), request()}], CbState :: term()) ->
    {{from(), result()}, NewCbState :: term()}.

callback_mode() -> [state_functions, state_enter].

start_link(Id, Index, Opts) ->
    gen_statem:start_link({local, name(Id, Index)}, ?MODULE, {Id, Index, Opts}, []).

-spec query(id(), request(), query_opts()) -> Result :: term().
query(Id, Request, Opts) ->
    PickKey = maps:get(pick_key, Opts, self()),
    Timeout = maps:get(timeout, Opts, infinity),
    pick_call(Id, PickKey, {query, Request, Opts}, Timeout).

%% simple query the resource without batching and queuing messages.
-spec simple_sync_query(id(), request()) -> Result :: term().
simple_sync_query(Id, Request) ->
    Result = call_query(sync, Id, ?QUERY(self(), Request), #{}),
    _ = handle_query_result(Id, Result, false),
    Result.

-spec simple_async_query(id(), request(), reply_fun()) -> Result :: term().
simple_async_query(Id, Request, ReplyFun) ->
    Result = call_query(async, Id, ?QUERY(ReplyFun, Request), #{}),
    _ = handle_query_result(Id, Result, false),
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
                    seg_bytes => maps:get(queue_max_bytes, Opts, ?DEFAULT_QUEUE_SIZE),
                    sizer => fun ?MODULE:estimate_size/1,
                    marshaller => fun ?MODULE:queue_item_marshaller/1
                });
            false ->
                undefined
        end,
    ok = inflight_new(Name),
    St = #{
        id => Id,
        index => Index,
        name => Name,
        %% query_mode = dynamic | sync | async
        %% TODO:
        %%  dynamic mode is async mode when things are going well, but becomes sync mode
        %%  if the resource worker is overloaded
        query_mode => maps:get(query_mode, Opts, sync),
        async_inflight_window => maps:get(async_inflight_window, Opts, ?DEFAULT_INFLIGHT),
        enable_batch => maps:get(enable_batch, Opts, false),
        batch_size => BatchSize,
        batch_time => maps:get(batch_time, Opts, ?DEFAULT_BATCH_TIME),
        queue => Queue,
        resume_interval => maps:get(health_check_interval, Opts, ?HEALTHCHECK_INTERVAL),
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
    {next_state, block, St};
running(cast, {block, [?QUERY(_, _) | _] = Batch}, #{queue := Q} = St) when is_list(Batch) ->
    Q1 = maybe_append_queue(Q, [?Q_ITEM(Query) || Query <- Batch]),
    {next_state, block, St#{queue := Q1}};
running({call, From0}, {query, Request, Opts}, #{query_mode := QM} = St) ->
    From = maybe_quick_return(QM, From0, maps:get(async_reply_fun, Opts, undefined)),
    query_or_acc(From, Request, St);
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
blocked(cast, {block, [?QUERY(_, _) | _] = Batch}, #{queue := Q} = St) when is_list(Batch) ->
    Q1 = maybe_append_queue(Q, [?Q_ITEM(Query) || Query <- Batch]),
    {keep_state, St#{queue := Q1}};
blocked(cast, resume, St) ->
    do_resume(St);
blocked(state_timeout, resume, St) ->
    do_resume(St);
blocked({call, From0}, {query, Request, Opts}, #{id := Id, queue := Q, query_mode := QM} = St) ->
    From = maybe_quick_return(QM, From0, maps:get(async_reply_fun, Opts, undefined)),
    Error = ?RESOURCE_ERROR(blocked, "resource is blocked"),
    _ = reply_caller(Id, ?REPLY(From, Request, Error)),
    {keep_state, St#{queue := maybe_append_queue(Q, [?Q_ITEM(?QUERY(From, Request))])}}.

terminate(_Reason, #{id := Id, index := Index}) ->
    gproc_pool:disconnect_worker(Id, {Id, Index}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

queue_item_marshaller(?Q_ITEM(_) = I) ->
    term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
    binary_to_term(Bin).

estimate_size(QItem) ->
    size(queue_item_marshaller(QItem)).

%%==============================================================================
maybe_quick_return(sync, From, _ReplyFun) ->
    From;
maybe_quick_return(async, From, ReplyFun) ->
    gen_statem:reply(From, ok),
    ReplyFun.

pick_call(Id, Key, Query, Timeout) ->
    try gproc_pool:pick_worker(Id, Key) of
        Pid when is_pid(Pid) ->
            gen_statem:call(Pid, Query, {clean_timeout, Timeout});
        _ ->
            ?RESOURCE_ERROR(not_created, "resource not found")
    catch
        error:badarg ->
            ?RESOURCE_ERROR(not_created, "resource not found");
        exit:{timeout, _} ->
            ?RESOURCE_ERROR(timeout, "call resource timeout")
    end.

do_resume(#{queue := Q, id := Id, name := Name} = St) ->
    case inflight_get_first(Name) of
        empty ->
            retry_first_from_queue(Q, Id, St);
        {Ref, FirstQuery} ->
            retry_first_sync(Id, FirstQuery, Name, Ref, undefined, St)
    end.

retry_first_from_queue(undefined, _Id, St) ->
    {next_state, running, St};
retry_first_from_queue(Q, Id, St) ->
    case replayq:peek(Q) of
        empty ->
            {next_state, running, St};
        ?Q_ITEM(FirstQuery) ->
            retry_first_sync(Id, FirstQuery, undefined, undefined, Q, St)
    end.

retry_first_sync(Id, FirstQuery, Name, Ref, Q, #{resume_interval := ResumeT} = St0) ->
    Result = call_query(sync, Id, FirstQuery, #{}),
    case handle_query_result(Id, Result, false) of
        %% Send failed because resource down
        true ->
            {keep_state, St0, {state_timeout, ResumeT, resume}};
        %% Send ok or failed but the resource is working
        false ->
            %% We Send 'resume' to the end of the mailbox to give the worker
            %% a chance to process 'query' requests.
            St =
                case Q of
                    undefined ->
                        inflight_drop(Name, Ref),
                        St0;
                    _ ->
                        St0#{queue => drop_head(Q)}
                end,
            {keep_state, St, {state_timeout, 0, resume}}
    end.

drop_head(Q) ->
    {Q1, AckRef, _} = replayq:pop(Q, #{count_limit => 1}),
    ok = replayq:ack(Q1, AckRef),
    Q1.

query_or_acc(From, Request, #{enable_batch := true, acc := Acc, acc_left := Left} = St0) ->
    Acc1 = [?QUERY(From, Request) | Acc],
    St = St0#{acc := Acc1, acc_left := Left - 1},
    case Left =< 1 of
        true -> flush(St);
        false -> {keep_state, ensure_flush_timer(St)}
    end;
query_or_acc(From, Request, #{enable_batch := false, queue := Q, id := Id, query_mode := QM} = St) ->
    QueryOpts = #{
        inflight_name => maps:get(name, St),
        inflight_window => maps:get(async_inflight_window, St)
    },
    case send_query(QM, From, Request, Id, QueryOpts) of
        true ->
            Query = ?QUERY(From, Request),
            {next_state, blocked, St#{queue := maybe_append_queue(Q, [?Q_ITEM(Query)])}};
        false ->
            {keep_state, St}
    end.

send_query(QM, From, Request, Id, QueryOpts) ->
    Result = call_query(QM, Id, ?QUERY(From, Request), QueryOpts),
    reply_caller(Id, ?REPLY(From, Request, Result)).

flush(#{acc := []} = St) ->
    {keep_state, St};
flush(
    #{
        id := Id,
        acc := Batch,
        batch_size := Size,
        queue := Q0,
        query_mode := QM
    } = St
) ->
    QueryOpts = #{
        inflight_name => maps:get(name, St),
        inflight_window => maps:get(async_inflight_window, St)
    },
    Result = call_query(QM, Id, Batch, QueryOpts),
    St1 = cancel_flush_timer(St#{acc_left := Size, acc := []}),
    case batch_reply_caller(Id, Result, Batch) of
        true ->
            Q1 = maybe_append_queue(Q0, [?Q_ITEM(Query) || Query <- Batch]),
            {next_state, blocked, St1#{queue := Q1}};
        false ->
            {keep_state, St1}
    end.

maybe_append_queue(undefined, _Items) -> undefined;
maybe_append_queue(Q, Items) -> replayq:append(Q, Items).

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

reply_caller(Id, ?REPLY(undefined, _, Result), BlockWorker) ->
    handle_query_result(Id, Result, BlockWorker);
reply_caller(Id, ?REPLY({ReplyFun, Args}, _, Result), BlockWorker) when is_function(ReplyFun) ->
    _ =
        case Result of
            {async_return, _} -> ok;
            _ -> apply(ReplyFun, Args ++ [Result])
        end,
    handle_query_result(Id, Result, BlockWorker);
reply_caller(Id, ?REPLY(From, _, Result), BlockWorker) ->
    gen_statem:reply(From, Result),
    handle_query_result(Id, Result, BlockWorker).

handle_query_result(Id, ?RESOURCE_ERROR_M(exception, _), BlockWorker) ->
    emqx_metrics_worker:inc(?RES_METRICS, Id, exception),
    BlockWorker;
handle_query_result(_Id, ?RESOURCE_ERROR_M(NotWorking, _), _) when
    NotWorking == not_connected; NotWorking == blocked
->
    true;
handle_query_result(_Id, ?RESOURCE_ERROR_M(_, _), BlockWorker) ->
    BlockWorker;
handle_query_result(Id, {error, _}, BlockWorker) ->
    emqx_metrics_worker:inc(?RES_METRICS, Id, failed),
    BlockWorker;
handle_query_result(Id, {resource_down, _}, _BlockWorker) ->
    emqx_metrics_worker:inc(?RES_METRICS, Id, resource_down),
    true;
handle_query_result(_Id, {async_return, inflight_full}, _BlockWorker) ->
    true;
handle_query_result(_Id, {async_return, {resource_down, _}}, _BlockWorker) ->
    true;
handle_query_result(_Id, {async_return, ok}, BlockWorker) ->
    BlockWorker;
handle_query_result(Id, Result, BlockWorker) ->
    assert_ok_result(Result),
    emqx_metrics_worker:inc(?RES_METRICS, Id, success),
    BlockWorker.

call_query(QM, Id, Query, QueryOpts) ->
    case emqx_resource_manager:ets_lookup(Id) of
        {ok, _Group, #{callback_mode := CM, mod := Mod, state := ResSt, status := connected}} ->
            apply_query_fun(call_mode(QM, CM), Mod, Id, Query, ResSt, QueryOpts);
        {ok, _Group, #{status := stopped}} ->
            ?RESOURCE_ERROR(stopped, "resource stopped or disabled");
        {ok, _Group, #{status := S}} when S == connecting; S == disconnected ->
            ?RESOURCE_ERROR(not_connected, "resource not connected");
        {error, not_found} ->
            ?RESOURCE_ERROR(not_found, "resource not found")
    end.

-define(APPLY_RESOURCE(EXPR, REQ),
    try
        %% if the callback module (connector) wants to return an error that
        %% makes the current resource goes into the `error` state, it should
        %% return `{resource_down, Reason}`
        EXPR
    catch
        ERR:REASON:STACKTRACE ->
            MSG = io_lib:format(
                "call query failed, func: ~s, id: ~s, error: ~0p, Request: ~0p",
                [??EXPR, Id, {ERR, REASON, STACKTRACE}, REQ],
                [{chars_limit, 1024}]
            ),
            ?RESOURCE_ERROR(exception, MSG)
    end
).

apply_query_fun(sync, Mod, Id, ?QUERY(_, Request) = _Query, ResSt, _QueryOpts) ->
    ?tp(call_query, #{id => Id, mod => Mod, query => _Query, res_st => ResSt}),
    ok = emqx_metrics_worker:inc(?RES_METRICS, Id, matched),
    ?APPLY_RESOURCE(Mod:on_query(Id, Request, ResSt), Request);
apply_query_fun(async, Mod, Id, ?QUERY(_, Request) = Query, ResSt, QueryOpts) ->
    ?tp(call_query_async, #{id => Id, mod => Mod, query => Query, res_st => ResSt}),
    Name = maps:get(inflight_name, QueryOpts, undefined),
    WinSize = maps:get(inflight_window, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        case inflight_is_full(Name, WinSize) of
            true ->
                ?tp(inflight_full, #{id => Id, wind_size => WinSize}),
                {async_return, inflight_full};
            false ->
                ok = emqx_metrics_worker:inc(?RES_METRICS, Id, matched),
                ReplyFun = fun ?MODULE:reply_after_query/6,
                Ref = make_message_ref(),
                Args = [self(), Id, Name, Ref, Query],
                ok = inflight_append(Name, Ref, Query),
                Result = Mod:on_query_async(Id, Request, {ReplyFun, Args}, ResSt),
                {async_return, Result}
        end,
        Request
    );
apply_query_fun(sync, Mod, Id, [?QUERY(_, _) | _] = Batch, ResSt, _QueryOpts) ->
    ?tp(call_batch_query, #{id => Id, mod => Mod, batch => Batch, res_st => ResSt}),
    Requests = [Request || ?QUERY(_From, Request) <- Batch],
    ok = emqx_metrics_worker:inc(?RES_METRICS, Id, matched, length(Batch)),
    ?APPLY_RESOURCE(Mod:on_batch_query(Id, Requests, ResSt), Batch);
apply_query_fun(async, Mod, Id, [?QUERY(_, _) | _] = Batch, ResSt, QueryOpts) ->
    ?tp(call_batch_query_async, #{id => Id, mod => Mod, batch => Batch, res_st => ResSt}),
    Name = maps:get(inflight_name, QueryOpts, undefined),
    WinSize = maps:get(inflight_window, QueryOpts, undefined),
    ?APPLY_RESOURCE(
        case inflight_is_full(Name, WinSize) of
            true ->
                ?tp(inflight_full, #{id => Id, wind_size => WinSize}),
                {async_return, inflight_full};
            false ->
                ok = emqx_metrics_worker:inc(?RES_METRICS, Id, matched, length(Batch)),
                ReplyFun = fun ?MODULE:batch_reply_after_query/6,
                Ref = make_message_ref(),
                Args = {ReplyFun, [self(), Id, Name, Ref, Batch]},
                Requests = [Request || ?QUERY(_From, Request) <- Batch],
                ok = inflight_append(Name, Ref, Batch),
                Result = Mod:on_batch_query_async(Id, Requests, Args, ResSt),
                {async_return, Result}
        end,
        Batch
    ).

reply_after_query(Pid, Id, Name, Ref, ?QUERY(From, Request), Result) ->
    case reply_caller(Id, ?REPLY(From, Request, Result)) of
        true -> ?MODULE:block(Pid);
        false -> inflight_drop(Name, Ref)
    end.

batch_reply_after_query(Pid, Id, Name, Ref, Batch, Result) ->
    case batch_reply_caller(Id, Result, Batch) of
        true -> ?MODULE:block(Pid);
        false -> inflight_drop(Name, Ref)
    end.
%%==============================================================================
%% the inflight queue for async query

inflight_new(Name) ->
    _ = ets:new(Name, [named_table, ordered_set, public, {write_concurrency, true}]),
    ok.

inflight_get_first(Name) ->
    case ets:first(Name) of
        '$end_of_table' ->
            empty;
        Ref ->
            case ets:lookup(Name, Ref) of
                [Object] -> Object;
                [] -> inflight_get_first(Name)
            end
    end.

inflight_is_full(undefined, _) ->
    false;
inflight_is_full(Name, MaxSize) ->
    case ets:info(Name, size) of
        Size when Size >= MaxSize -> true;
        _ -> false
    end.

inflight_append(undefined, _Ref, _Query) ->
    ok;
inflight_append(Name, Ref, Query) ->
    ets:insert(Name, {Ref, Query}),
    ok.

inflight_drop(undefined, _) ->
    ok;
inflight_drop(Name, Ref) ->
    ets:delete(Name, Ref),
    ok.

%%==============================================================================
call_mode(sync, _) -> sync;
call_mode(async, always_sync) -> sync;
call_mode(async, async_if_possible) -> async.

assert_ok_result(ok) ->
    true;
assert_ok_result({async_return, R}) ->
    assert_ok_result(R);
assert_ok_result(R) when is_tuple(R) ->
    ok = erlang:element(1, R);
assert_ok_result(R) ->
    error({not_ok_result, R}).

-spec name(id(), integer()) -> atom().
name(Id, Index) ->
    Mod = atom_to_list(?MODULE),
    Id1 = binary_to_list(Id),
    Index1 = integer_to_list(Index),
    list_to_atom(lists:concat([Mod, ":", Id1, ":", Index1])).

disk_queue_dir(Id, Index) ->
    filename:join([node(), emqx:data_dir(), Id, "queue:" ++ integer_to_list(Index)]).

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
