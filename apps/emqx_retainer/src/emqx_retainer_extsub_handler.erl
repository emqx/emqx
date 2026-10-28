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
    handle_unsubscribe/4,
    handle_delivered/4,
    handle_info/3,
    handle_terminate/1,
    handle_save_subopts/3
]).

%% This module is `emqx_retainer` companion
-elvis([{elvis_style, no_invalid_dynamic_calls, disable}]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-define(done, done).
-define(cursor(CURSOR), {cursor, CURSOR}).
-define(no_cursor, undefined).
-define(no_wildcard, no_wildcard).

-define(DISPATCHER_RETRY_DELAY_MS, 300).
-define(HEADER_SUB_TOPIC, sub_topic).

-record(h, {
    message_target :: emqx_extsub_handler:message_target(),
    topic_filter,
    context,
    mod,
    state,
    cursor,
    dispatcher,
    monitor,
    outstanding = false,
    nudge_enqueued = false,
    fetched = 0
}).

%% Events
-record(next, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `emqx_extsub_handler' API
%%------------------------------------------------------------------------------

handle_subscribe(_SubscribeType, _SubscribeCtx, _Handler, #share{}) ->
    ignore;
handle_subscribe(
    _SubscribeType, #{clientinfo := #{protocol := Protocol}}, _Handler, _TopicFilter
) when
    Protocol =/= mqtt
->
    ignore;
handle_subscribe(resume = _SubscribeType, _SubscribeCtx, _Handler, _TopicFilter) ->
    %% Note: the implementation prior to using `emqx_extsub` did **not** resume iteration
    %% of retained messages when resuming a session.  The previous implementation without
    %% `extsub` did not hook into `session.resumed`, hence iteration stopped when
    %% resuming/taking over.  For now, we adopt the same behavior.  Resuming iteration
    %% would thus be an improvement.
    ignore;
handle_subscribe(subscribe = _SubscribeType, SubscribeCtx, Handler, TopicFilter) ->
    IsNew =
        case SubscribeCtx of
            #{subopts := #{is_new := false}} ->
                false;
            #{subopts := #{}} ->
                true
        end,
    #{subopts := #{rh := RH}} = SubscribeCtx,
    case RH == 0 orelse (RH == 1 andalso IsNew) of
        true ->
            subscribe(Handler, SubscribeCtx, TopicFilter);
        false ->
            ignore
    end.

handle_unsubscribe(_UnsubscribeType, _UnsubscribeCtx, Handler, _TopicFilter) ->
    stop(Handler).

handle_terminate(Handler) ->
    demonitor_dispatcher(Handler).

handle_delivered(Handler, AckCtx, _Msg, _Ack) ->
    #{
        desired_message_count := DesiredMsgCount,
        delivering_count := DeliveringCount,
        unacked_count := UnackedCount
    } = AckCtx,
    case Handler#h.cursor of
        ?done when UnackedCount == 0 andalso DeliveringCount == 0 ->
            {destroy, [Handler#h.topic_filter]};
        _ when DesiredMsgCount > 0 ->
            {ok, request_next(Handler)};
        _ ->
            {ok, Handler}
    end.

handle_info(#h{outstanding = true} = Handler0, InfoCtx, {retained_batch, Messages0, Cursor0}) ->
    Cursor =
        case Cursor0 of
            done -> ?done;
            _ -> ?cursor(Cursor0)
        end,
    Handler1 = Handler0#h{outstanding = false, cursor = Cursor},
    Messages = enrich_messages(Handler1#h.topic_filter, filter_delivery(Messages0)),
    Handler2 = Handler1#h{fetched = Handler1#h.fetched + length(Messages)},
    #{desired_message_count := Demand, delivering_count := Delivering} = InfoCtx,
    case Cursor of
        ?done ->
            ok = demonitor_dispatcher(Handler2),
            Handler = Handler2#h{dispatcher = undefined, monitor = undefined},
            case Messages == [] andalso Delivering == 0 of
                true -> {destroy, [Handler#h.topic_filter]};
                false -> {ok, Handler, Messages}
            end;
        _ when Demand > 0 ->
            {ok, request_next(Handler2), Messages};
        _ ->
            {ok, Handler2, Messages}
    end;
handle_info(#h{monitor = Ref} = Handler0, _InfoCtx, {'DOWN', Ref, process, _Pid, _Reason}) ->
    Handler = Handler0#h{dispatcher = undefined, monitor = undefined, outstanding = false},
    case Handler0#h.outstanding of
        true -> {ok, enqueue_nudge(Handler, delay)};
        false -> {ok, Handler}
    end;
handle_info(Handler0, InfoCtx, #next{}) ->
    Handler = Handler0#h{nudge_enqueued = false},
    handle_next(Handler, InfoCtx);
handle_info(Handler, _InfoCtx, _Info) ->
    {ok, Handler}.

handle_save_subopts(Handler0, _Context, _SubOpts) ->
    %% Make the handler stop, since take over/disconnect persistence is ongoing.
    Handler = stop(Handler0),
    {ok, Handler}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

subscribe(undefined = _Handler, SubscribeCtx, TopicFilter) ->
    #{message_target := MessageTarget} = SubscribeCtx,
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    Handler0 = #h{
        message_target = MessageTarget,
        topic_filter = TopicFilter,
        context = Context,
        mod = Mod,
        state = State,
        cursor = ?no_wildcard,
        fetched = 0
    },
    Handler =
        case need_dispatcher(TopicFilter) of
            true -> request_next(Handler0#h{cursor = ?cursor(?no_cursor)});
            false -> enqueue_nudge(Handler0, now)
        end,
    {ok, Handler};
subscribe(#h{} = Handler, _SubscribeCtx, _TopicFilter) ->
    {ok, Handler}.

need_dispatcher(Topic) ->
    emqx_topic:wildcard(Topic) orelse
        emqx_config:get([retainer, flow_control, batch_deliver_limiter]) =/= infinity.

handle_next(#h{cursor = ?done} = Handler0, _InfoCtx) ->
    %% Impossible?
    {ok, Handler0};
handle_next(#h{cursor = ?no_wildcard} = Handler0, _InfoCtx) ->
    #h{topic_filter = Topic, mod = Mod, state = State} = Handler0,
    {ok, Messages0} = Mod:read_message(State, Topic),
    Messages = enrich_messages(Topic, filter_delivery(Messages0)),
    Handler = Handler0#h{cursor = ?done, fetched = length(Messages)},
    case Messages of
        [] -> {destroy, [Topic]};
        _ -> {ok, Handler, Messages}
    end;
handle_next(#h{cursor = ?cursor(_)} = Handler, _InfoCtx) ->
    {ok, request_next(Handler)}.

request_next(#h{outstanding = true} = Handler) ->
    Handler;
request_next(#h{cursor = ?done} = Handler) ->
    Handler;
request_next(#h{cursor = ?cursor(Cursor)} = Handler0) ->
    case ensure_dispatcher(Handler0) of
        #h{dispatcher = undefined} = Handler ->
            enqueue_nudge(Handler, delay);
        #h{dispatcher = Worker, message_target = Target, topic_filter = Topic} = Handler ->
            emqx_retainer_dispatcher:fetch(Worker, Target, Topic, Cursor),
            Handler#h{outstanding = true}
    end;
request_next(Handler) ->
    enqueue_nudge(Handler, now).

ensure_dispatcher(#h{dispatcher = undefined, message_target = Target} = Handler) ->
    try emqx_retainer_dispatcher:worker() of
        Worker when is_pid(Worker) ->
            Ref = emqx_extsub_handler:monitor(Target, Worker),
            Handler#h{dispatcher = Worker, monitor = Ref};
        false ->
            Handler
    catch
        error:badarg -> Handler
    end;
ensure_dispatcher(Handler) ->
    Handler.

demonitor_dispatcher(#h{monitor = undefined}) ->
    ok;
demonitor_dispatcher(#h{message_target = Target, monitor = Ref}) ->
    emqx_extsub_handler:demonitor(Target, Ref).

stop(Handler) ->
    ok = demonitor_dispatcher(Handler),
    Handler#h{cursor = ?done, dispatcher = undefined, monitor = undefined, outstanding = false}.

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

enrich_messages(TopicFilter, Messages) ->
    [emqx_message:set_header(?HEADER_SUB_TOPIC, TopicFilter, Msg) || Msg <- Messages].

enqueue_nudge(#h{nudge_enqueued = true} = Handler0, _Delay) ->
    Handler0;
enqueue_nudge(#h{} = Handler0, now) ->
    #h{message_target = MessageTarget} = Handler0,
    emqx_extsub_handler:send(MessageTarget, #next{}),
    Handler0#h{nudge_enqueued = true};
enqueue_nudge(#h{} = Handler0, delay) ->
    #h{message_target = MessageTarget} = Handler0,
    emqx_extsub_handler:send_after(?DISPATCHER_RETRY_DELAY_MS, MessageTarget, #next{}),
    Handler0#h{nudge_enqueued = true}.
