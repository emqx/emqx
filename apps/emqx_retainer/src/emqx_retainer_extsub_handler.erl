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
    %% The topic filter split with `emqx_topic:words/1'
    words :: emqx_types:words(),
    %% The exact-topic message read at subscribe time.
    messages = [],
    cursor,
    dispatcher,
    monitor,
    %% Reference of the dispatcher request that waits for a reply
    request_ref :: undefined | reference(),
    nudge_enqueued = false
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
    case {Handler, need_retained(SubscribeCtx)} of
        {undefined, true} ->
            start(SubscribeCtx, TopicFilter);
        {undefined, false} ->
            ignore;
        {#h{}, true} ->
            %% Send all retained messages again for the new SUBSCRIBE.
            _ = stop(Handler),
            case start(SubscribeCtx, TopicFilter) of
                ignore -> destroy;
                Result -> Result
            end;
        {#h{}, false} ->
            %% Continue the delivery that an earlier SUBSCRIBE started.
            ignore
    end.

handle_unsubscribe(_UnsubscribeType, _UnsubscribeCtx, Handler, _TopicFilter) ->
    stop(Handler).

handle_terminate(Handler) ->
    demonitor_dispatcher(Handler).

handle_delivered(Handler, AckCtx, _Msg, _Ack) ->
    #{
        desired_message_count := DesiredMsgCount,
        delivering_count := DeliveringCount
    } = AckCtx,
    case Handler#h.cursor of
        ?done when DeliveringCount == 0 ->
            {destroy, Handler, [Handler#h.topic_filter]};
        _ when DesiredMsgCount > 0 ->
            {ok, request_next(Handler)};
        _ ->
            {ok, Handler}
    end.

handle_info(#h{request_ref = Ref} = Handler0, InfoCtx, {retained_batch, Ref, Messages0, Cursor0}) ->
    Cursor =
        case Cursor0 of
            done -> ?done;
            _ -> ?cursor(Cursor0)
        end,
    Handler1 = Handler0#h{request_ref = undefined, cursor = Cursor},
    Messages = enrich_messages(Handler1#h.topic_filter, filter_delivery(Messages0)),
    #{desired_message_count := Demand, delivering_count := Delivering} = InfoCtx,
    case Cursor of
        ?done ->
            ok = demonitor_dispatcher(Handler1),
            Handler = Handler1#h{dispatcher = undefined, monitor = undefined},
            case Messages == [] andalso Delivering == 0 of
                true -> {destroy, Handler, [Handler#h.topic_filter]};
                false -> {ok, Handler, Messages}
            end;
        _ when Delivering + length(Messages) < Demand ->
            {ok, request_next(Handler1), Messages};
        _ ->
            {ok, Handler1, Messages}
    end;
handle_info(#h{monitor = Ref} = Handler0, _InfoCtx, {'DOWN', Ref, process, _Pid, _Reason}) ->
    Handler = Handler0#h{dispatcher = undefined, monitor = undefined, request_ref = undefined},
    case Handler0#h.request_ref of
        undefined -> {ok, Handler};
        _ -> {ok, enqueue_nudge(Handler, delay)}
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

%% Retain Handling 0 sends retained messages on each SUBSCRIBE, 1 only for a new subscription.
need_retained(#{subopts := #{rh := RH} = SubOpts}) ->
    RH == 0 orelse (RH == 1 andalso maps:get(is_new, SubOpts, true)).

start(SubscribeCtx, TopicFilter) ->
    #{message_target := MessageTarget} = SubscribeCtx,
    Words = emqx_topic:words(TopicFilter),
    Handler0 = #h{
        message_target = MessageTarget,
        topic_filter = TopicFilter,
        words = Words,
        cursor = ?no_wildcard
    },
    case need_dispatcher(Words) of
        true ->
            {ok, request_next(Handler0#h{cursor = ?cursor(?no_cursor)})};
        false ->
            case read_message(TopicFilter) of
                [] -> ignore;
                Messages -> {ok, enqueue_nudge(Handler0#h{messages = Messages}, now)}
            end
    end.

need_dispatcher(Words) ->
    emqx_topic:wildcard(Words) orelse
        emqx_config:get([retainer, flow_control, batch_deliver_limiter]) =/= infinity.

handle_next(#h{cursor = ?done} = Handler0, _InfoCtx) ->
    %% Impossible?
    {ok, Handler0};
handle_next(#h{cursor = ?no_wildcard, messages = Messages} = Handler, _InfoCtx) ->
    {ok, Handler#h{cursor = ?done, messages = []}, Messages};
handle_next(#h{cursor = ?cursor(_), topic_filter = Topic} = Handler, _InfoCtx) ->
    case emqx_retainer:context() of
        undefined ->
            %% The retainer is stopped, so no dispatcher will serve the request.
            {destroy, Handler, [Topic]};
        _ ->
            {ok, request_next(Handler)}
    end.

%% Read an exact topic in the channel. A stopped retainer returns no messages.
read_message(Topic) ->
    case emqx_retainer:context() of
        undefined ->
            [];
        Context ->
            Mod = emqx_retainer:backend_module(Context),
            {ok, Messages} = Mod:read_message(emqx_retainer:backend_state(Context), Topic),
            enrich_messages(Topic, filter_delivery(Messages))
    end.

request_next(#h{request_ref = Ref} = Handler) when is_reference(Ref) ->
    Handler;
request_next(#h{cursor = ?done} = Handler) ->
    Handler;
request_next(#h{cursor = ?cursor(Cursor)} = Handler0) ->
    case ensure_dispatcher(Handler0) of
        #h{dispatcher = undefined} = Handler ->
            enqueue_nudge(Handler, delay);
        #h{dispatcher = Worker, message_target = Target, topic_filter = Topic, words = Words} =
                Handler ->
            Ref = make_ref(),
            emqx_retainer_dispatcher:fetch(Worker, Target, Ref, Topic, Words, Cursor),
            Handler#h{request_ref = Ref}
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
    Handler#h{cursor = ?done, dispatcher = undefined, monitor = undefined, request_ref = undefined}.

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
