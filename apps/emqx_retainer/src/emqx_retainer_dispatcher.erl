%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_dispatcher).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/2,
    dispatch/1,
    wait_dispatch_complete/1,
    worker/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% This module is `emqx_retainer` companion
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%%===================================================================
%%% Type definitions
%%%===================================================================

-define(POOL, ?DISPATCHER_POOL).
-define(DELAY_MS, 100).

-record(delay, {
    msgs :: [message()],
    cursor :: undefined | emqx_retainer:cursor(),
    limiter :: limiter()
}).
-record(at_once, {
    topic :: topic(),
    pid :: pid(),
    msgs :: [message()]
}).
-record(with_cursor, {
    topic :: topic(),
    pid :: pid(),
    msgs :: [message()],
    cursor :: undefined | emqx_retainer:cursor()
}).

-type message() :: emqx_types:message().
-type limiter() :: emqx_limiter_client:t().
-type topic() :: emqx_types:topic().
-type continuation() :: #at_once{} | #with_cursor{}.
-type delay() :: #delay{}.

-type state() :: #{
    id := integer(),
    pool := atom(),
    limiter := emqx_limiter_client:t(),
    current := undefined | continuation(),
    dispatch_tref := undefined | reference(),
    dispatch_requests := queue:queue({pid(), topic()}),
    waiting_for_dispatch_complete := [gen_server:from()]
}.

%%%===================================================================
%%% API
%%%===================================================================

-spec dispatch(topic()) -> ok.
dispatch(Topic) ->
    dispatch(Topic, self()).

-spec dispatch(topic(), pid()) -> ok.
dispatch(Topic, Pid) ->
    cast({dispatch, Pid, Topic}).

-spec wait_dispatch_complete(timeout()) -> ok.
wait_dispatch_complete(Timeout) ->
    Workers = gproc_pool:active_workers(?POOL),
    lists:foreach(
        fun({_, Pid}) ->
            ok = gen_server:call(Pid, ?FUNCTION_NAME, Timeout)
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
%%% gen_server callbacks
%%%===================================================================

-spec init(_) -> {ok, state()}.
init([Pool, Id]) ->
    erlang:process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    Limiter = get_limiter(),
    State = #{
        id => Id,
        pool => Pool,
        limiter => Limiter,
        current => undefined,
        dispatch_tref => undefined,
        dispatch_requests => queue:new(),
        waiting_for_dispatch_complete => []
    },
    {ok, State}.

handle_call(wait_dispatch_complete, From, State0) ->
    case State0 of
        #{current := undefined} ->
            {reply, ok, State0};
        #{waiting_for_dispatch_complete := Waiting0} ->
            Waiting = [From | Waiting0],
            State = State0#{waiting_for_dispatch_complete := Waiting},
            {noreply, State}
    end;
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({dispatch, Pid, Topic}, State0) ->
    State = handle_dispatch_request(Pid, Topic, State0),
    {noreply, State};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(dispatch, State0) ->
    State1 = clear_dispatch_timer(State0),
    State = handle_dispatch(State1),
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

cast(Msg) ->
    gen_server:cast(worker(), Msg).

-spec handle_dispatch_request(pid(), topic(), state()) -> state().
handle_dispatch_request(Pid, Topic, State0) ->
    #{dispatch_requests := Reqs0} = State0,
    Reqs = queue:in({Pid, Topic}, Reqs0),
    State = State0#{dispatch_requests := Reqs},
    case State of
        #{current := undefined} ->
            ensure_dispatch_now(State);
        #{current := _} ->
            State
    end.

-spec init_dispatch(pid(), topic(), state()) -> state().
init_dispatch(Pid, Topic, State0) ->
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    BackendState = emqx_retainer:backend_state(Context),
    case emqx_topic:wildcard(Topic) of
        true ->
            {ok, Messages, Cursor} = Mod:match_messages(BackendState, Topic, undefined),
            Current = #with_cursor{
                pid = Pid,
                topic = Topic,
                msgs = Messages,
                cursor = Cursor
            },
            State = State0#{current := Current},
            ensure_dispatch_now(State);
        false ->
            {ok, Messages} = Mod:read_message(BackendState, Topic),
            Current = #at_once{
                pid = Pid,
                topic = Topic,
                msgs = Messages
            },
            State = State0#{current := Current},
            ensure_dispatch_now(State)
    end.

-spec handle_dispatch(state()) -> state().
handle_dispatch(
    #{current := #at_once{pid = Pid, topic = Topic, msgs = Messages0} = Current0} = State0
) ->
    #{limiter := Limiter0} = State0,
    case dispatch_at_once(Messages0, Pid, Topic, Limiter0) of
        {ok, Limiter} ->
            State1 = State0#{limiter := Limiter, current := undefined},
            prepare_next_dispatch(State1);
        #delay{msgs = Messages, limiter = Limiter} ->
            State1 = State0#{
                limiter := Limiter,
                current := Current0#at_once{msgs = Messages}
            },
            ensure_delayed_dispatch(State1, ?DELAY_MS)
    end;
handle_dispatch(
    #{
        current := #with_cursor{pid = Pid, topic = Topic, cursor = Cursor0, msgs = Messages0} =
            Current0
    } = State0
) ->
    #{limiter := Limiter0} = State0,
    Context = emqx_retainer:context(),
    case dispatch_with_cursor(Context, Messages0, Cursor0, Pid, Topic, Limiter0) of
        {ok, Limiter} ->
            State1 = State0#{limiter := Limiter, current := undefined},
            prepare_next_dispatch(State1);
        #delay{msgs = Messages, cursor = Cursor, limiter = Limiter} ->
            State1 = State0#{
                limiter := Limiter,
                current := Current0#with_cursor{msgs = Messages, cursor = Cursor}
            },
            ensure_delayed_dispatch(State1, ?DELAY_MS)
    end;
handle_dispatch(#{current := undefined} = State0) ->
    prepare_next_dispatch(State0).

dispatch_at_once(Messages, Pid, Topic, Limiter0) ->
    case deliver(Messages, Pid, Topic, Limiter0) of
        {ok, Limiter1} ->
            {ok, Limiter1};
        #delay{} = Delay ->
            Delay;
        no_receiver ->
            {ok, Limiter0}
    end.

dispatch_with_cursor(Context, [], Cursor, _Pid, _Topic, Limiter) ->
    ok = delete_cursor(Context, Cursor),
    {ok, Limiter};
dispatch_with_cursor(Context, Messages0, Cursor0, Pid, Topic, Limiter0) ->
    case deliver(Messages0, Pid, Topic, Limiter0) of
        {ok, Limiter1} ->
            {ok, Messages1, Cursor1} = match_next(Context, Topic, Cursor0),
            dispatch_with_cursor(Context, Messages1, Cursor1, Pid, Topic, Limiter1);
        #delay{} = Delay ->
            Delay#delay{cursor = Cursor0};
        no_receiver ->
            ok = delete_cursor(Context, Cursor0),
            {ok, Limiter0}
    end.

match_next(_Context, _Topic, undefined) ->
    {ok, [], undefined};
match_next(Context, Topic, Cursor) ->
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    Mod:match_messages(State, Topic, Cursor).

delete_cursor(_Context, undefined) ->
    ok;
delete_cursor(Context, Cursor) ->
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    Mod:delete_cursor(State, Cursor).

-spec deliver([emqx_types:message()], pid(), topic(), limiter()) ->
    no_receiver | {ok, limiter()} | delay().
deliver(Messages, Pid, Topic, Limiter) ->
    case erlang:is_process_alive(Pid) of
        false ->
            ?tp(debug, retainer_dispatcher_no_receiver, #{topic => Topic}),
            no_receiver;
        _ ->
            %% todo: cap at max process mailbox size to avoid killing; but each zone might
            %% have different max mailbox sizes...
            BatchSize = emqx_conf:get([retainer, flow_control, batch_deliver_number], 0),
            NMessages = filter_delivery(Messages, Topic),
            case BatchSize of
                0 ->
                    deliver_in_batches(NMessages, all, Pid, Topic, Limiter);
                _ ->
                    deliver_in_batches(NMessages, BatchSize, Pid, Topic, Limiter)
            end
    end.

deliver_in_batches([], _BatchSize, _Pid, _Topic, Limiter) ->
    {ok, Limiter};
deliver_in_batches(Msgs, BatchSize, Pid, Topic, Limiter0) ->
    {BatchActualSize, Batch, RestMsgs} = take(BatchSize, Msgs),
    case emqx_limiter_client:try_consume(Limiter0, BatchActualSize) of
        {true, Limiter1} ->
            ok = deliver_to_client(Batch, Pid, Topic),
            deliver_in_batches(RestMsgs, BatchSize, Pid, Topic, Limiter1);
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

deliver_to_client([Msg | Rest], Pid, Topic) ->
    Pid ! {deliver, Topic, Msg},
    deliver_to_client(Rest, Pid, Topic);
deliver_to_client([], _, _) ->
    ok.

filter_delivery(Messages, _Topic) ->
    lists:filter(fun check_clientid_banned/1, Messages).

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

-spec notify_waiting(state()) -> state().
notify_waiting(State0) ->
    #{waiting_for_dispatch_complete := Waiting} = State0,
    lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Waiting),
    State0#{waiting_for_dispatch_complete := []}.

clear_dispatch_timer(#{dispatch_tref := undefined} = State0) ->
    State0;
clear_dispatch_timer(#{dispatch_tref := TRef} = State0) ->
    _ = erlang:cancel_timer(TRef),
    State0#{dispatch_tref := undefined}.

-spec prepare_next_dispatch(state()) -> state().
prepare_next_dispatch(State0) ->
    #{dispatch_requests := Reqs0} = State0,
    case queue:out(Reqs0) of
        {empty, Reqs} ->
            State1 = State0#{dispatch_requests := Reqs},
            notify_waiting(State1);
        {{value, {Pid, Topic}}, Reqs} ->
            State1 = State0#{dispatch_requests := Reqs},
            init_dispatch(Pid, Topic, State1)
    end.

-spec ensure_dispatch_now(state()) -> state().
ensure_dispatch_now(#{dispatch_tref := undefined} = State0) ->
    TRef = dispatch_now(),
    State0#{dispatch_tref := TRef};
ensure_dispatch_now(#{dispatch_tref := _} = State0) ->
    State0.

-spec ensure_delayed_dispatch(state(), timeout()) -> state().
ensure_delayed_dispatch(State0, Timeout) ->
    TRef = delay_dispatch(Timeout),
    State0#{dispatch_tref := TRef}.

dispatch_now() ->
    delay_dispatch(_Timeout = 0).

delay_dispatch(Timeout) ->
    erlang:send_after(Timeout, self(), dispatch).
