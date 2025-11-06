%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_dispatcher).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/2,
    dispatch/1,
    next/1,
    %% RPC target (v2)
    wait_dispatch_complete/1,
    worker/0
]).

%% Hooks
-export([
    on_message_acked/2,
    on_message_delivered/2,
    on_client_handle_info/3,
    on_client_timeout/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Internal exports (only for mocking)
-export([retry_ttl/0, max_queue_size/0]).

%% This module is `emqx_retainer` companion
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%%===================================================================
%%% Type definitions
%%%===================================================================

-define(POOL, ?DISPATCHER_POOL).
-define(DELAY_MS, 100).
-define(MAX_WAITING_FOR_RETRY, 1_000).
-ifdef(TEST).
-define(REINDEX_DISPATCH_WAIT, 3_000).
-else.
-define(REINDEX_DISPATCH_WAIT, 30_000).
-endif.

-define(BATCH_QUEUE_PD_KEY, {?MODULE, batch_queue}).
-define(TRY_NEXT_NUDGED_PD_KEY, {?MODULE, try_next_nudged}).
-define(TRY_NEXT_BATCH_TIMER_PD_KEY, {?MODULE, try_next_batch_timer}).

-record(delay, {
    msgs :: [message()],
    limiter :: limiter()
}).
-record(done, {limiter}).
-record(more, {limiter}).
-record(retry, {
    enqueued_at :: time_ms(),
    started_at :: time_ms(),
    msgs :: [message()],
    pid :: pid(),
    topic :: topic(),
    cursor :: undefined | cursor()
}).
-record(dispatch_cursor, {
    started_at :: time_ms(),
    incarnation :: undefined | emqx_retainer:index_incarnation(),
    topic :: topic(),
    rest :: [message()],
    cursor :: undefined | cursor()
}).

-type time_ms() :: integer().
-type cursor() :: emqx_retainer:cursor().
-type message() :: emqx_types:message().
-type limiter() :: emqx_limiter_client:t().
-type topic() :: emqx_types:topic().

-type delay() :: #delay{}.
-type more() :: #more{}.
-type done() :: #done{}.

-type state() :: _TODO.

%%%===================================================================
%%% API
%%%===================================================================

-spec dispatch(topic()) -> ok.
dispatch(Topic) ->
    dispatch(Topic, self()).

-spec dispatch(topic(), pid()) -> ok.
dispatch(Topic, Pid) ->
    cast({dispatch, Pid, Topic}).

next(DispatchCursor) ->
    Pid = self(),
    cast({next, Pid, DispatchCursor}).

%% RPC target (v2)
-spec wait_dispatch_complete(timeout()) -> ok.
wait_dispatch_complete(Timeout) ->
    Workers = gproc_pool:active_workers(?POOL),
    emqx_utils:pforeach(
        fun({_, Pid}) ->
            ok = gen_server:call(Pid, wait_dispatch_complete, Timeout)
        end,
        Workers
    ).

-spec worker() -> pid().
worker() ->
    gproc_pool:pick_worker(?POOL, self()).

-spec start_link(atom(), pos_integer()) -> {ok, pid()}.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%%%===================================================================
%%% Hooks
%%%
%%% # Interaction between channel and dispatcher process
%%%
%%% 1) Client subscribes to topic, which iniates the dispatch with the subscribed topic
%%%    filter (`session.subscribed` hook).

%%% 2) Dispatcher process then starts iterating over the topic filter.  It then sends
%%%    chunks of messages along with a dispatch cursor to keep track of progress as a
%%%    `{retained_batch, Topic, Delivers, DispatchCursor}` message to the channel process.
%%%
%%% 3) The channel will trigger the `client.handle_info` hook, which will prompt the
%%%    processing of the batch.  We check if there is room in the session's inflight
%%%    window.  If there is, we return from the hook the delivers that fit said window.
%%%    Any messages from the batch that do not fit the inflight window are stored in the
%%%    channel's process dictionary (CPD).  If multiple `{retained_batch, _, _, _}`
%%%    messages are received, they are also stored in the CPD in a FIFO queue.
%%%
%%%    * Note [Retainer Try Next Nudge]: If, at this point, there are retained batches
%%%    still to be delivered but there's no inflight room, the channel is nudged by a
%%%    timed message (`try_next_retained_batch`) that will make it check again if there's
%%%    room later.
%%%
%%%    * This only happens if the channel state is `connected`.  Otherwise, the batches
%%%      are dropped.
%%%
%%% 4) Eventually, the channel will deliver and/or ack messages, triggering the
%%%    `message.delivered` and `message.acked` hooks, respectively.  On those hooks, we
%%%    check the CPD to see if there are batches still waiting to become delivers.  If
%%%    there are, we simply output those messages that fit the infligth window as in the
%%%    previous step.  If the batch became empty, then we asynchronously ask the dispatch
%%%    process for the next batch by sending it a message.  The dispatcher process will
%%%    then continue iterating the cursor and repeat the process from above.
%%%
%%%    * Note [Retainer Try Next Nudge] applies here too.
%%%
%%% 5) After enough iterations, the dispatcher process will simply no longer send any
%%%    messages to the channel, and the channel will drain its batches.
%%%
%%% If the dispatcher hits the dispatch rate limit, it'll enqueue the channel's request in
%%% a FIFO queue and schedule a timer to trigger a retry at a later time.
%%%
%%% Iteration requests that started long ago enough (configurable by
%%% `retainer.dispatch_retry_ttl`) will be silently dropped.
%%%
%%% The maximum number of concurrent iteration requests is limited (currently hard-coded
%%% at 1_000).
%%%
%%%===================================================================

on_message_acked(_ClientInfo, Msg) ->
    HookContext = emqx_hooks:context('message.acked'),
    try
        maybe_nudge_next_retained_batch(Msg, HookContext)
    after
        clear_nudged_flag()
    end.

on_message_delivered(_ClientInfo, Msg) ->
    HookContext = emqx_hooks:context('message.delivered'),
    try
        maybe_nudge_next_retained_batch(Msg, HookContext)
    after
        clear_nudged_flag()
    end.

on_client_handle_info({retained_batch, Topic, Delivers0, DispatchCursor}, HookContext, Acc0) ->
    ChanInfoFn = maps:get(chan_info_fn, HookContext),
    case ChanInfoFn(conn_state) of
        connected ->
            ?tp("channel_received_retained_batch", #{
                topic => Topic, delivers => Delivers0, cursor => DispatchCursor
            }),
            push_retained_batch(Topic, Delivers0, DispatchCursor),
            case try_pop_next_retained_batch(HookContext) of
                {ok, Delivers} ->
                    Acc = append_to_channel_info_delivers(Acc0, Delivers),
                    {ok, Acc};
                _ ->
                    %% blocked, done, requested
                    %% In any of these cases, we'll eventually retry after messages are delivered.
                    ok
            end;
        _ ->
            %% Not connected; simply discard, since we'll have to start over when (re)connected.
            clear_retained_batches(),
            ok
    end;
on_client_handle_info(try_next_retained_batch, HookContext, Acc0) ->
    ChanInfoFn = maps:get(chan_info_fn, HookContext),
    case ChanInfoFn(conn_state) of
        connected ->
            handle_try_next_retained_batch(HookContext, info, Acc0);
        _ ->
            %% Not connected, so no need to keep iterating.
            clear_retained_batches(),
            ok
    end;
on_client_handle_info(_Info, _HookContext, _Acc) ->
    ok.

on_client_timeout(_TRef, try_next_retained_batch, Acc0) ->
    HookContext = emqx_hooks:context('client.timeout'),
    clear_try_next_retained_batch_timer(),
    handle_try_next_retained_batch(HookContext, timeout, Acc0);
on_client_timeout(_TRef, _Msg, _Acc) ->
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Pool, Id]) ->
    erlang:process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    Limiter = get_limiter(),
    State = #{
        id => Id,
        pool => Pool,
        limiter => Limiter,
        %% For retrying dispatches when rate limited
        retry_tref => undefined,
        %% For replying callers waiting for dispatch to be complete, for reindex
        %% consistency.
        wait_dispatch_complete_tref => undefined,
        %% Last known incarnation number.  Used with reindex.
        incarnation => current_index_incarnation(),
        %% Cursors waiting to be retried.
        waiting_for_retry => queue:new(),
        %% Callers waiting for dispatchers older than current incarnation to
        %% complete, keyed on greatest incarnation number they are waiting on to be
        %% empty.  Used by reindex.
        waiting_for_dispatch => []
    },
    {ok, State}.

handle_call(wait_dispatch_complete, From, State0) ->
    #{waiting_for_dispatch := Waiting0} = State0,
    Waiting = add_caller(Waiting0, From),
    %% We bump the incarnation only after replying to the callers, so that requests with
    %% the now old incarnation are still served.
    State1 = State0#{waiting_for_dispatch := Waiting},
    State = ensure_wait_dispatch_complete_timer(State1),
    {noreply, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({dispatch, Pid, Topic}, State) ->
    handle_initial_dispatch(Pid, Topic, State);
handle_cast({next, Pid, DispatchCursor}, State) ->
    #{incarnation := CurrentIncarnation} = State,
    ReqIncarnation = DispatchCursor#dispatch_cursor.incarnation,
    IsStateReq = is_integer(ReqIncarnation) andalso CurrentIncarnation > ReqIncarnation,
    case IsStateReq of
        true ->
            %% Just drop stale request.
            {noreply, State};
        false ->
            handle_next(DispatchCursor, Pid, State)
    end;
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(retry, State0) ->
    State = State0#{retry_tref := undefined},
    handle_retry(State);
handle_info(wait_dispatch_complete_done, State0) ->
    #{waiting_for_dispatch := Waiting} = State0,
    notify_callers(Waiting),
    %% We now bump the incarnation.  Any further stale requests will now be dropped.
    State = State0#{
        wait_dispatch_complete_tref := undefined,
        incarnation := current_index_incarnation(),
        waiting_for_dispatch := []
    },
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

%%%===================================================================
%%% Internal exports (only for mocking)
%%%===================================================================

retry_ttl() ->
    emqx_conf:get([retainer, dispatch_retry_ttl], timer:minutes(10)).

max_queue_size() ->
    ?MAX_WAITING_FOR_RETRY.

%%%===================================================================
%%% Internal functions (dispatcher process)
%%%===================================================================

cast(Msg) ->
    gen_server:cast(worker(), Msg).

-spec handle_initial_dispatch(pid(), topic(), state()) -> {noreply, state()}.
handle_initial_dispatch(Pid, Topic, State0) ->
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    BackendState = emqx_retainer:backend_state(Context),
    case emqx_topic:wildcard(Topic) of
        true ->
            {ok, Messages, Cursor} = Mod:match_messages(BackendState, Topic, undefined),
            Incarnation = cursor_index_incarnation(Cursor),
            DispatchCursor = #dispatch_cursor{
                started_at = system_now_ms(),
                incarnation = Incarnation,
                topic = Topic,
                rest = Messages,
                cursor = Cursor
            },
            handle_next(DispatchCursor, Pid, State0);
        false ->
            {ok, Messages} = Mod:read_message(BackendState, Topic),
            DispatchCursor = #dispatch_cursor{
                started_at = system_now_ms(),
                incarnation = undefined,
                topic = Topic,
                rest = Messages,
                cursor = undefined
            },
            handle_next(DispatchCursor, Pid, State0)
    end.

handle_retry(State0) ->
    #{waiting_for_retry := ToRetry0} = State0,
    case queue:out(ToRetry0) of
        {empty, ToRetry} ->
            State = State0#{waiting_for_retry := ToRetry},
            {noreply, State};
        {{value, Retry}, ToRetry} ->
            State = State0#{waiting_for_retry := ToRetry},
            NowMS = now_ms(),
            case NowMS - Retry#retry.enqueued_at > ?MODULE:retry_ttl() of
                true ->
                    %% Drop old request.
                    %% Log? Inc some metric?
                    ?tp("retainer_retry_request_dropped_due_to_ttl", #{}),
                    handle_retry(State);
                false ->
                    Incarnation = cursor_index_incarnation(Retry#retry.cursor),
                    DispatchCursor = #dispatch_cursor{
                        started_at = Retry#retry.started_at,
                        incarnation = Incarnation,
                        topic = Retry#retry.topic,
                        rest = Retry#retry.msgs,
                        cursor = Retry#retry.cursor
                    },
                    handle_next(DispatchCursor, Retry#retry.pid, State)
            end
    end.

handle_next(DispatchCursor0, Pid, State0) ->
    #dispatch_cursor{
        started_at = StartedAt,
        topic = Topic,
        rest = RestMsgs,
        cursor = Cursor
    } = DispatchCursor0,
    #{limiter := Limiter0} = State0,
    case RestMsgs of
        [] ->
            State = handle_dispatch_next_cursor(Pid, Topic, Cursor, StartedAt, Limiter0, State0),
            {noreply, State};
        [_ | _] ->
            State = handle_dispatch_next_chunk(
                RestMsgs, Pid, Topic, Cursor, StartedAt, Limiter0, State0
            ),
            {noreply, State}
    end.

handle_dispatch_next_chunk(Messages0, Pid, Topic, Cursor0, StartedAt, Limiter0, State0) ->
    case deliver(Messages0, Pid, Topic, Cursor0, StartedAt, Limiter0) of
        #more{limiter = Limiter1} ->
            ?tp("retainer_deliver_more", #{topic => Topic, cursor => Cursor0}),
            State0#{limiter := Limiter1};
        #done{limiter = Limiter1} ->
            ?tp("retainer_deliver_done", #{topic => Topic, cursor => Cursor0}),
            ok = delete_cursor(Cursor0),
            State0#{limiter := Limiter1};
        #delay{msgs = Messages, limiter = Limiter1} ->
            ?tp("retainer_deliver_delay", #{topic => Topic, cursor => Cursor0}),
            #{waiting_for_retry := ToRetry0} = State0,
            Retry = #retry{
                enqueued_at = now_ms(),
                started_at = StartedAt,
                msgs = Messages,
                pid = Pid,
                topic = Topic,
                cursor = Cursor0
            },
            ToRetry = enqueue_retry(Retry, ToRetry0),
            State = State0#{
                waiting_for_retry := ToRetry,
                limiter := Limiter1
            },
            ensure_delayed_retry(State, ?DELAY_MS)
    end.

enqueue_retry(Retry, ToRetry0) ->
    ToRetry2 =
        case queue:len(ToRetry0) >= ?MODULE:max_queue_size() of
            true ->
                ?tp("retainer_retry_request_dropped_due_to_overflow", #{}),
                {{value, Dropped}, ToRetry1} = queue:out(ToRetry0),
                #retry{pid = Pid} = Dropped,
                Report = #{
                    msg => "retainer_retry_request_dropped_due_to_overflow",
                    topic => Dropped#retry.topic
                },
                Pid ! {log, warning, Report},
                ToRetry1;
            false ->
                ToRetry0
        end,
    queue:in(Retry, ToRetry2).

handle_dispatch_next_cursor(_Pid, _Topic, undefined = _Cursor, _StartedAt, Limiter, State0) ->
    State0#{limiter := Limiter};
handle_dispatch_next_cursor(Pid, Topic, Cursor0, StartedAt, Limiter0, State0) ->
    {ok, Msgs, Cursor} = match_next(Topic, Cursor0),
    handle_dispatch_next_chunk(Msgs, Pid, Topic, Cursor, StartedAt, Limiter0, State0).

match_next(_Topic, undefined) ->
    {ok, [], undefined};
match_next(Topic, Cursor) ->
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    Mod:match_messages(State, Topic, Cursor).

delete_cursor(undefined) ->
    ok;
delete_cursor(Cursor) ->
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    BackendState = emqx_retainer:backend_state(Context),
    ok = Mod:delete_cursor(BackendState, Cursor).

current_index_incarnation() ->
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    Mod:current_index_incarnation(State).

cursor_index_incarnation(undefined) ->
    undefined;
cursor_index_incarnation(Cursor) ->
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    BackendState = emqx_retainer:backend_state(Context),
    Mod:cursor_index_incarnation(BackendState, Cursor).

add_caller(Callers0, From) ->
    [From | Callers0].

notify_callers(Callers0) ->
    lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Callers0).

-spec deliver([emqx_types:message()], pid(), topic(), cursor(), time_ms(), limiter()) ->
    more() | done() | delay().
deliver(Messages, Pid, Topic, Cursor, StartedAt, Limiter) ->
    case erlang:is_process_alive(Pid) of
        false ->
            ?tp(debug, retainer_dispatcher_no_receiver, #{topic => Topic}),
            #done{limiter = Limiter};
        _ ->
            %% todo: cap at max process mailbox size to avoid killing; but each zone might
            %% have different max mailbox sizes...
            %% will have to be part of the dispatch request from the channel itself...
            BatchSize = emqx_conf:get([retainer, flow_control, batch_deliver_number], 0),
            NMessages = filter_delivery(Messages, StartedAt),
            case BatchSize of
                0 ->
                    deliver_in_batches(NMessages, all, Pid, Topic, Cursor, StartedAt, Limiter);
                _ ->
                    deliver_in_batches(NMessages, BatchSize, Pid, Topic, Cursor, StartedAt, Limiter)
            end
    end.

deliver_in_batches([], _BatchSize, _Pid, _Topic, _Cursor, _StartedAt, Limiter) ->
    #done{limiter = Limiter};
deliver_in_batches(Msgs, BatchSize, Pid, Topic, Cursor, StartedAt, Limiter0) ->
    {BatchActualSize, Batch, RestMsgs} = take(BatchSize, Msgs),
    case emqx_limiter_client:try_consume(Limiter0, BatchActualSize) of
        {true, Limiter1} ->
            ok = deliver_to_client(Batch, Pid, Topic, Cursor, StartedAt, RestMsgs),
            #more{limiter = Limiter1};
        {false, Limiter1, Reason} ->
            ?tp(retained_dispatch_failed_for_rate_exceeded_limit, #{}),
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => retained_dispatch_failed_due_to_quota_exceeded,
                    reason => Reason,
                    topic => Topic,
                    attempted_count => BatchActualSize
                },
                #{tag => "RETAINER"}
            ),
            #delay{limiter = Limiter1, msgs = Msgs}
    end.

deliver_to_client(Msgs, Pid, Topic, Cursor, StartedAt, RestMsgs) ->
    Delivers = lists:map(fun(Msg) -> {deliver, Topic, Msg} end, Msgs),
    DispatchCursor = #dispatch_cursor{
        started_at = StartedAt,
        incarnation = cursor_index_incarnation(Cursor),
        topic = Topic,
        cursor = Cursor,
        rest = RestMsgs
    },
    Pid ! {retained_batch, Topic, Delivers, DispatchCursor},
    ok.

filter_delivery(Messages0, StartedAt) ->
    Messages = lists:filter(fun check_clientid_banned/1, Messages0),
    %% Do not deliver messages that are 5 s or more newer than when we started iterating.
    Deadline = StartedAt + 5_000,
    lists:filter(fun(Msg) -> is_published_before_deadline(Msg, Deadline) end, Messages).

check_clientid_banned(Msg) ->
    case emqx_banned:check_clientid(Msg#message.from) of
        false ->
            true;
        true ->
            ?tp(debug, ignore_retained_message_due_to_banned, #{
                reason => publisher_client_banned,
                clientid => Msg#message.from
            }),
            false
    end.

is_published_before_deadline(#message{timestamp = Ts}, Deadline) when is_integer(Ts) ->
    Ts =< Deadline;
is_published_before_deadline(_Msg, _Deadline) ->
    %% poisoned message?
    true.

take(all, List) ->
    {length(List), List, []};
take(N, List) ->
    take(N, List, 0, []).

take(0, List, Count, Acc) ->
    {Count, lists:reverse(Acc), List};
take(_N, [], Count, Acc) ->
    {Count, lists:reverse(Acc), []};
take(N, [H | T], Count, Acc) ->
    take(N - 1, T, Count + 1, [H | Acc]).

get_limiter() ->
    LimiterId = {?RETAINER_LIMITER_GROUP, ?DISPATCHER_LIMITER_NAME},
    emqx_limiter:connect(LimiterId).

-spec ensure_delayed_retry(state(), timeout()) -> state().
ensure_delayed_retry(State0, Timeout) ->
    TRef = delay_retry(Timeout),
    State0#{retry_tref := TRef}.

-spec ensure_wait_dispatch_complete_timer(state()) -> state().
ensure_wait_dispatch_complete_timer(#{wait_dispatch_complete_tref := undefined} = State0) ->
    TRef = erlang:send_after(?REINDEX_DISPATCH_WAIT, self(), wait_dispatch_complete_done),
    State0#{wait_dispatch_complete_tref := TRef};
ensure_wait_dispatch_complete_timer(State) ->
    State.

delay_retry(Timeout) ->
    erlang:send_after(Timeout, self(), retry).

now_ms() ->
    erlang:monotonic_time(millisecond).

system_now_ms() ->
    erlang:system_time(millisecond).

%%%===================================================================
%%% Internal functions (channel hook handling; runs inside channel process)
%%%===================================================================

append_to_channel_info_delivers(Acc0, Delivers) ->
    maps:update_with(
        deliver,
        fun(Ds) -> Ds ++ Delivers end,
        Delivers,
        Acc0
    ).

handle_try_next_retained_batch(HookContext, AccType, Acc0) ->
    case has_available_session_inflight_room(HookContext) of
        false ->
            ?tp("channel_retained_batch_blocked", #{where => try_next}),
            ensure_try_next_retained_batch_timer(),
            ok;
        {true, AvailableRoom} ->
            case pop_next_retained_batch1(AvailableRoom) of
                done ->
                    ok;
                requested ->
                    ok;
                {ok, Delivers} when AccType == info ->
                    Acc = append_to_channel_info_delivers(Acc0, Delivers),
                    {ok, Acc};
                {ok, Delivers} when AccType == timeout ->
                    Acc = Acc0 ++ Delivers,
                    {ok, Acc}
            end
    end.

clear_try_next_retained_batch_timer() ->
    _ = erase(?TRY_NEXT_BATCH_TIMER_PD_KEY),
    ok.

ensure_try_next_retained_batch_timer() ->
    TryNextBatchIntervalMS = 200,
    case get(?TRY_NEXT_BATCH_TIMER_PD_KEY) of
        undefined ->
            TRef = emqx_utils:start_timer(TryNextBatchIntervalMS, try_next_retained_batch),
            _ = put(?TRY_NEXT_BATCH_TIMER_PD_KEY, TRef),
            ok;
        TRef when is_reference(TRef) ->
            ok
    end.

pop_next_retained_batch1(AvailableRoom) ->
    case pop_next_retained_batch() of
        undefined ->
            done;
        #{topic := _Topic, delivers := [], dispatch_cursor := DispatchCursor} ->
            ?tp("channel_retained_ask_more", #{cursor => DispatchCursor}),
            %% No more delivers; time to ask for more.
            next(DispatchCursor),
            requested;
        #{topic := Topic, delivers := Delivers0, dispatch_cursor := DispatchCursor} ->
            ?tp("channel_retained_push_delivers", #{cursor => DispatchCursor}),
            ToTake =
                case AvailableRoom of
                    infinity -> all;
                    N when is_integer(N) -> N
                end,
            {Delivers, RestDelivers} = take_delivers(ToTake, Delivers0),
            push_retained_batch(Topic, RestDelivers, DispatchCursor),
            {ok, Delivers}
    end.

push_retained_batch(Topic, Delivers, DispatchCursor) ->
    Q1 =
        case get(?BATCH_QUEUE_PD_KEY) of
            undefined -> queue:new();
            Q0 -> Q0
        end,
    RetainedBatch = #{topic => Topic, delivers => Delivers, dispatch_cursor => DispatchCursor},
    Q2 = queue:in(RetainedBatch, Q1),
    put(?BATCH_QUEUE_PD_KEY, Q2),
    ok.

clear_retained_batches() ->
    _ = put(?BATCH_QUEUE_PD_KEY, queue:new()),
    ok.

has_buffered_retained_batch() ->
    case get(?BATCH_QUEUE_PD_KEY) of
        undefined ->
            false;
        Q ->
            queue:len(Q) > 0
    end.

pop_next_retained_batch() ->
    case get(?BATCH_QUEUE_PD_KEY) of
        undefined ->
            undefined;
        Q0 ->
            case queue:out(Q0) of
                {empty, Q1} ->
                    _ = put(?BATCH_QUEUE_PD_KEY, Q1),
                    undefined;
                {{value, RetainedBatch}, Q1} ->
                    _ = put(?BATCH_QUEUE_PD_KEY, Q1),
                    RetainedBatch
            end
    end.

try_pop_next_retained_batch(HookContext) ->
    case has_available_session_inflight_room(HookContext) of
        false ->
            ?tp("channel_retained_batch_blocked", #{where => pop_next}),
            blocked;
        {true, AvailableRoom} ->
            pop_next_retained_batch1(AvailableRoom)
    end.

has_available_session_inflight_room(HookContext) ->
    SessionInfoFn = maps:get(session_info_fn, HookContext),
    InflightMax = SessionInfoFn(inflight_max),
    InflightCnt = SessionInfoFn(inflight_cnt),
    case InflightMax > 0 andalso InflightCnt >= InflightMax of
        true ->
            false;
        false when InflightMax == 0 ->
            {true, infinity};
        false ->
            {true, InflightMax - InflightCnt}
    end.

take_delivers(all, Delivers) ->
    {Delivers, []};
take_delivers(N, Delivers0) ->
    do_take_delivers(N, Delivers0, []).

do_take_delivers(0, RestDelivers, Acc) ->
    {lists:reverse(Acc), RestDelivers};
do_take_delivers(_N, [] = RestDelivers, Acc) ->
    {lists:reverse(Acc), RestDelivers};
do_take_delivers(N, [Deliver | RestDelivers], Acc) ->
    do_take_delivers(N - 1, RestDelivers, [Deliver | Acc]).

maybe_nudge_next_retained_batch(#message{}, HookContext) ->
    case has_buffered_retained_batch() of
        false ->
            ok;
        true ->
            case has_available_session_inflight_room(HookContext) of
                false ->
                    ?tp("channel_retained_batch_blocked", #{where => after_ack}),
                    ok;
                {true, _} ->
                    ?tp("channel_retained_try_next_batch", #{}),
                    ensure_nudged(),
                    ok
            end
    end.

ensure_nudged() ->
    case get(?TRY_NEXT_NUDGED_PD_KEY) of
        undefined ->
            _ = self() ! try_next_retained_batch,
            _ = put(?TRY_NEXT_NUDGED_PD_KEY, true),
            ok;
        true ->
            ok
    end.

clear_nudged_flag() ->
    _ = erase(?TRY_NEXT_NUDGED_PD_KEY),
    ok.
