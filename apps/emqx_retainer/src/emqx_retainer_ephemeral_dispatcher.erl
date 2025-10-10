%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_retainer_ephemeral_dispatcher).

-behaviour(gen_server).

%% API
-export([
    start/3
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% This module is `emqx_retainer` companion
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-define(at_once, at_once).
-define(clientid, clientid).
-define(continue, continue).
-define(delay, delay).
-define(dispatch, dispatch).
-define(limiter, limiter).
-define(mref, mref).
-define(parent, parent).
-define(start_dispatch, start_dispatch).
-define(stop, stop).
-define(topic, topic).
-define(undefined, undefined).
-define(with_cursor, with_cursor).

-define(DELAY(MSGS, CURSOR, LIMITER), {?delay, MSGS, CURSOR, LIMITER}).
-define(DELAY_DELIVER(MSGS, LIMITER), {?delay, MSGS, undefined, LIMITER}).

-define(DELAY_MS, 100).

-type limiter() :: emqx_limiter_client:t().
-type message() :: emqx_types:message().
-type topic() :: emqx_types:topic().

-type continuation() ::
    {?at_once, [message()]}
    | {?with_cursor, [message()], emqx_retainer:cursor()}.

-type state() :: #{
    ?clientid := emqx_types:clientid(),
    ?continue := ?undefined | continuation(),
    ?limiter := limiter(),
    ?mref := reference(),
    ?parent := pid(),
    ?topic := emqx_types:topic()
}.

-type delay() :: ?DELAY(_RestMessages, _Cursor, limiter()).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start(Topic, Clientid, Parent) ->
    Opts = #{?topic => Topic, ?clientid => Clientid, ?parent => Parent},
    gen_server:start(?MODULE, Opts, []).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

-spec init(_) -> {ok, state(), {continue, ?start_dispatch}}.
init(Opts) ->
    #{
        ?topic := Topic,
        ?clientid := Clientid,
        ?parent := Parent
    } = Opts,
    MRef = monitor(process, Parent),
    State = #{
        ?clientid => Clientid,
        ?continue => ?undefined,
        ?limiter => get_limiter(),
        ?mref => MRef,
        ?parent => Parent,
        ?topic => Topic
    },
    {ok, State, {continue, ?start_dispatch}}.

handle_continue(?start_dispatch, State0) ->
    handle_start_dispatch(State0).

terminate(_Reason, _State) ->
    ok.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, _, _}, #{?mref := MRef} = State) ->
    #{?clientid := Clientid} = State,
    ?tp(debug, "retainer_dispatcher_parent_died_while_dispatching", #{clientid => Clientid}),
    {stop, normal, State};
handle_info(?dispatch, State0) ->
    handle_dispatch(State0);
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

handle_start_dispatch(State0) ->
    #{?topic := Topic} = State0,
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    BackendState = emqx_retainer:backend_state(Context),
    State =
        case emqx_topic:wildcard(Topic) of
            true ->
                {ok, Messages, Cursor} = Mod:match_messages(BackendState, Topic, undefined),
                State0#{?continue := {?with_cursor, Messages, Cursor}};
            false ->
                {ok, Messages} = Mod:read_message(BackendState, Topic),
                State0#{?continue := {?at_once, Messages}}
        end,
    dispatch_now(),
    {noreply, State}.

handle_dispatch(#{?continue := {?at_once, Messages0}} = State0) ->
    #{
        ?limiter := Limiter0,
        ?parent := Pid,
        ?topic := Topic
    } = State0,
    case dispatch_at_once(Messages0, Pid, Topic, Limiter0) of
        ?stop ->
            {stop, normal, State0};
        ?DELAY(Messages, _NoCursor, Limiter) ->
            State = State0#{
                ?limiter := Limiter,
                ?continue := {?at_once, Messages}
            },
            delay_dispatch(?DELAY_MS),
            {noreply, State}
    end;
handle_dispatch(#{?continue := {?with_cursor, Messages0, Cursor0}} = State0) ->
    #{
        ?limiter := Limiter0,
        ?parent := Pid,
        ?topic := Topic
    } = State0,
    Context = emqx_retainer:context(),
    case dispatch_with_cursor(Context, Messages0, Cursor0, Pid, Topic, Limiter0) of
        ?stop ->
            {stop, normal, State0};
        ?DELAY(Messages, Cursor, Limiter) ->
            State = State0#{
                ?limiter := Limiter,
                ?continue := {?with_cursor, Messages, Cursor}
            },
            delay_dispatch(?DELAY_MS),
            {noreply, State}
    end.

-spec dispatch_at_once([message()], pid(), topic(), limiter()) ->
    ?stop | delay().
dispatch_at_once(Messages, Pid, Topic, Limiter0) ->
    case deliver(Messages, Pid, Topic, Limiter0) of
        {ok, _FinalLimiter} ->
            ?stop;
        ?stop ->
            ?stop;
        ?DELAY(_Messages, _Cursor, _Limiter) = Delay ->
            Delay
    end.

-spec dispatch_with_cursor(
    _Context, [message()], emqx_retainer:cursor(), pid(), topic(), limiter()
) ->
    ?stop | delay().
dispatch_with_cursor(Context, [], Cursor, _Pid, _Topic, _Limiter) ->
    ok = delete_cursor(Context, Cursor),
    ?stop;
dispatch_with_cursor(Context, Messages0, Cursor0, Pid, Topic, Limiter0) ->
    case deliver(Messages0, Pid, Topic, Limiter0) of
        {ok, Limiter1} ->
            {ok, Messages1, Cursor1} = match_next(Context, Topic, Cursor0),
            dispatch_with_cursor(Context, Messages1, Cursor1, Pid, Topic, Limiter1);
        ?DELAY(Messages, _, Limiter1) ->
            ?DELAY(Messages, Cursor0, Limiter1);
        ?stop ->
            ok = delete_cursor(Context, Cursor0),
            ?stop
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

-spec deliver([message()], pid(), topic(), limiter()) ->
    {ok, limiter()} | ?stop | delay().
deliver(Messages, Pid, Topic, Limiter) ->
    case erlang:is_process_alive(Pid) of
        false ->
            %% Parent died
            ?tp(debug, retainer_dispatcher_no_receiver, #{topic => Topic}),
            ?stop;
        _ ->
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
                    msg => retained_dispatch_failed_for_rate_exceeded_limit,
                    reason => Reason,
                    topic => Topic,
                    dropped_count => BatchActualSize
                },
                #{tag => "RETAINER"}
            ),
            ?DELAY_DELIVER(Msgs, Limiter1)
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

dispatch_now() ->
    delay_dispatch(_Timeout = 0).

delay_dispatch(Timeout) ->
    _ = erlang:send_after(Timeout, self(), ?dispatch),
    ok.
