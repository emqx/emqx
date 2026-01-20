%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_retainer_extsub_handler).

-behaviour(emqx_extsub_handler).

%% API
-export([]).

%% `emqx_extsub_handler' API
-export([
    handle_subscribe/4,
    handle_delivered/4,
    handle_info/3,
    handle_pre_terminate/3
]).

%% This module is `emqx_retainer` companion
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-define(init, init).
-define(done, done).
-define(cursor(CURSOR), {cursor, CURSOR}).
-define(no_cursor, undefined).
-define(no_wildcard, no_wildcard).

-define(MIN_RATE_LIMIT_DELAY_MS, 300).
-define(MAX_RATE_LIMIT_DELAY_MS, 10_000).
-define(DEFAULT_BATCH_SIZE, 1_000).

-record(h, {
    send_fn,
    send_after_fn,
    topic_filter,
    context,
    mod,
    state,
    limiter,
    cursor,
    nudge_enqueued = false,
    times_throttled = 0,
    taking_over = false,
    delivered = 0
}).

%% Events
-record(next, {n}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `emqx_extsub_handler' API
%%------------------------------------------------------------------------------

handle_subscribe(_SubscribeType, _SubscribeCtx, _Handler, #share{}) ->
    ignore;
handle_subscribe(SubscribeType, SubscribeCtx, Handler, TopicFilter) ->
    IsNew =
        case {SubscribeType, SubscribeCtx} of
            {resume, _} ->
                false;
            {subscribe, #{subopts := #{is_new := false}}} ->
                false;
            {subscribe, #{subopts := #{}}} ->
                true
        end,
    RH =
        case SubscribeCtx of
            #{subopts := #{subopts := #{rh := RH0}}} ->
                %% DS session
                RH0;
            #{subopts := #{rh := RH0}} ->
                %% In-memory session
                RH0
        end,
    case RH == 0 orelse (RH == 1 andalso IsNew) of
        true ->
            subscribe(Handler, SubscribeCtx, TopicFilter);
        false ->
            ignore
    end.

handle_delivered(Handler0, #{desired_message_count := DesiredMsgCount}, _Msg, _Ack) ->
    Handler = Handler0#h{delivered = Handler0#h.delivered + 1},
    case DesiredMsgCount > 0 of
        true ->
            enqueue_nudge(Handler, batch_read_num(), now);
        false ->
            Handler
    end.

handle_info(Handler0, _InfoCtx, #next{n = N}) ->
    Handler = Handler0#h{nudge_enqueued = false},
    handle_next(Handler, N);
handle_info(Handler, _InfoCtx, _Info) ->
    {ok, Handler}.

handle_pre_terminate(Handler0, _PreTerminateCtx, _TopicFiltersToSubOpts) ->
    Handler = Handler0#h{taking_over = true},
    {ok, Handler, #{delivered => Handler#h.delivered}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

subscribe(undefined = _Handler, SubscribeCtx, TopicFilter) ->
    #{send_after := SendAfterFn, send := SendFn} = SubscribeCtx,
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    Limiter = get_limiter(),
    Handler0 = #h{
        send_fn = SendFn,
        send_after_fn = SendAfterFn,
        topic_filter = TopicFilter,
        context = Context,
        mod = Mod,
        state = State,
        limiter = Limiter,
        cursor = ?init
    },
    Handler = enqueue_nudge(Handler0, batch_read_num(), now),
    {ok, Handler};
subscribe(#h{} = Handler, _SubscribeCtx, _TopicFilter) ->
    {ok, Handler}.

handle_next(#h{taking_over = true} = Handler0, _N) ->
    %% Stop iterating if takeover is underway.
    {ok, Handler0};
handle_next(#h{cursor = ?done} = Handler0, _N) ->
    %% Impossible?
    {ok, Handler0};
handle_next(#h{cursor = ?init} = Handler0, N) ->
    #h{topic_filter = TopicFilter} = Handler0,
    case emqx_topic:wildcard(TopicFilter) of
        true ->
            Handler = Handler0#h{cursor = ?cursor(?no_cursor)},
            handle_next(Handler, N);
        false ->
            Handler = Handler0#h{cursor = ?no_wildcard},
            handle_next(Handler, N)
    end;
handle_next(#h{cursor = ?no_wildcard} = Handler0, _N) ->
    case try_consume(Handler0, 1) of
        {ok, Handler, Messages} ->
            {ok, Handler, Messages};
        {error, Handler1} ->
            Handler = enqueue_nudge(Handler1, 1, delay),
            {ok, Handler}
    end;
handle_next(#h{cursor = ?cursor(_)} = Handler0, N) ->
    case try_consume(Handler0, N) of
        {ok, Handler1, Messages} ->
            Handler =
                case Handler1#h.cursor of
                    ?cursor(_) ->
                        enqueue_nudge(Handler1, batch_read_num(), now);
                    _ ->
                        Handler1
                end,
            {ok, Handler, Messages};
        {error, Handler1} ->
            Handler = enqueue_nudge(Handler1, batch_read_num(), delay),
            {ok, Handler}
    end.

filter_delivery(Messages) ->
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

get_limiter() ->
    LimiterId = {?RETAINER_LIMITER_GROUP, ?DISPATCHER_LIMITER_NAME},
    emqx_limiter:connect(LimiterId).

next_cursor(?no_cursor = Cursor, _NumFetchedMessages, Handler) ->
    #h{mod = Mod, state = State} = Handler,
    _ = Mod:delete_cursor(State, Cursor),
    ?done;
next_cursor(?no_wildcard, _NumFetchedMessages, _Handler) ->
    ?done;
next_cursor(Cursor, 0 = _NumFetchedMessages, Handler) ->
    #h{mod = Mod, state = State} = Handler,
    _ = Mod:delete_cursor(State, Cursor),
    ?done;
next_cursor(Cursor, _NumFetchedMessages, _Handler) ->
    ?cursor(Cursor).

try_consume(#h{} = Handler0, N0) when is_integer(N0) ->
    #h{
        topic_filter = TopicFilter,
        limiter = Limiter0,
        times_throttled = TimesThrottled0
    } = Handler0,
    case try_consume_at_most(Limiter0, N0) of
        {ok, N, Limiter1} ->
            {Messages0, InnerCursor} = do_fetch(Handler0, N),
            NumFetchedMessages = length(Messages0),
            Surplus = max(0, N - NumFetchedMessages),
            Limiter = emqx_limiter_client:put_back(Limiter1, Surplus),
            Messages = filter_delivery(Messages0),
            Cursor = next_cursor(InnerCursor, NumFetchedMessages, Handler0),
            Handler = Handler0#h{limiter = Limiter, cursor = Cursor, times_throttled = 0},
            {ok, Handler, Messages};
        {error, N, Limiter1, Reason} ->
            ?tp(retained_fetch_rate_limit_exceeded, #{topic => TopicFilter}),
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => retained_fetch_rate_limit_exceeded,
                    reason => Reason,
                    topic => TopicFilter,
                    initially_attempted_count => N0,
                    attempted_to_fetch_count => N
                },
                #{tag => "RETAINER"}
            ),
            Handler = Handler0#h{limiter = Limiter1, times_throttled = TimesThrottled0 + 1},
            {error, Handler}
    end.

try_consume_at_most(Limiter, N) ->
    MinBatch = min_batch_backoff(N),
    do_try_consume_at_most(Limiter, N, MinBatch).

do_try_consume_at_most(Limiter0, CurrN, MinBatch) ->
    case emqx_limiter_client:try_consume(Limiter0, CurrN) of
        {true, Limiter1} ->
            {ok, CurrN, Limiter1};
        {false, Limiter1, _Reason} when CurrN > MinBatch ->
            N1 = CurrN div 2,
            do_try_consume_at_most(Limiter1, N1, MinBatch);
        {false, Limiter1, Reason} ->
            {error, CurrN, Limiter1, Reason}
    end.

%% When progressively reducing batch size after hitting rate limit, we want a minimum
%% batch size to avoid several clients fetching messages one-by-one when retainer is
%% busy.  We take it to be 20 % of requested size.  This is only a simple heuristic.
%%
%% Note that we must take the rate limiter into account, otherwise a large enough read
%% number and a low enough rate limit might otherwise lead to a situation where the
%% minimum batch size would be larger than the maximum capacity of the limiter.
min_batch_backoff(N) ->
    case emqx_config:get([retainer, flow_control, batch_deliver_limiter]) of
        infinity ->
            max(1, floor(0.2 * N));
        {Capacity, _Interval} ->
            min(Capacity, max(1, floor(0.2 * N)))
    end.

do_fetch(#h{cursor = ?no_wildcard} = Handler, _N) ->
    #h{
        topic_filter = TopicFilter,
        mod = Mod,
        state = State
    } = Handler,
    %% TODO: how could this fail?
    {ok, Messages0} = Mod:read_message(State, TopicFilter),
    {Messages0, ?no_wildcard};
do_fetch(#h{cursor = ?cursor(Cursor0)} = Handler, N) ->
    #h{
        topic_filter = TopicFilter,
        mod = Mod,
        state = State
    } = Handler,
    Opts0 = #{batch_read_number => N},
    Opts =
        case Cursor0 == ?no_cursor andalso emqx_extsub:pop_context(?MODULE, TopicFilter) of
            {ok, #{delivered := Skip}} when Skip > 0 ->
                Opts0#{skip => Skip};
            _ ->
                Opts0
        end,
    %% TODO: how could this fail?
    {ok, Messages0, Cursor} = Mod:match_messages(State, TopicFilter, Cursor0, Opts),
    {Messages0, Cursor}.

enqueue_nudge(#h{nudge_enqueued = true} = Handler0, _N, _Delay) ->
    Handler0;
enqueue_nudge(#h{} = Handler0, N, now) ->
    #h{send_fn = SendFn} = Handler0,
    SendFn(#next{n = N}),
    Handler0#h{nudge_enqueued = true};
enqueue_nudge(#h{} = Handler0, N, delay) ->
    #h{send_after_fn = SendAfterFn, times_throttled = TimesThrottled} = Handler0,
    Delay = min(
        ?MAX_RATE_LIMIT_DELAY_MS, max(?MIN_RATE_LIMIT_DELAY_MS, (1 bsl TimesThrottled) * 100)
    ),
    SendAfterFn(Delay, #next{n = N}),
    Handler0#h{nudge_enqueued = true}.

batch_read_num() ->
    case emqx_retainer:batch_read_number() of
        all_remaining ->
            ?DEFAULT_BATCH_SIZE;
        N when is_integer(N) ->
            N
    end.
