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

-define(done, done).
-define(cursor(CURSOR), {cursor, CURSOR}).

-define(DISPATCHER_RETRY_DELAY_MS, 300).
-define(HEADER_SUB_TOPIC, sub_topic).

-record(h, {
    message_target :: emqx_extsub_handler:message_target(),
    topic_filter,
    %% The topic filter split with `emqx_topic:words/1'
    words :: emqx_types:words(),
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

%% `emqx_retainer_channel' starts each retained read in the channel and creates the handler
%% only with a handover. The handover data is the cursor of the read: `{cursor, Cursor}'
%% or `done'.
handle_subscribe(handover, #{handover := Cursor} = SubscribeCtx, undefined, TopicFilter) ->
    {ok, start(SubscribeCtx, TopicFilter, Cursor)};
handle_subscribe(subscribe, #{subopts := SubOpts}, #h{}, _TopicFilter) ->
    case emqx_retainer_channel:need_retained(SubOpts) of
        true ->
            %% `emqx_retainer_channel' already started a new read for this SUBSCRIBE.
            destroy;
        false ->
            %% Continue the delivery that an earlier SUBSCRIBE started.
            ignore
    end;
handle_subscribe(_SubscribeType, _SubscribeCtx, _Handler, _TopicFilter) ->
    %% Note: the implementation prior to using `emqx_extsub` did **not** resume iteration
    %% of retained messages when resuming a session, so `resume' is ignored.
    ignore.

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

handle_info(
    #h{request_ref = Ref} = Handler0,
    InfoCtx,
    #retained_batch{request_ref = Ref, messages = Messages0, cursor = Cursor0}
) ->
    Cursor =
        case Cursor0 of
            done -> ?done;
            _ -> ?cursor(Cursor0)
        end,
    Handler1 = Handler0#h{request_ref = undefined, cursor = Cursor},
    Messages = enrich_messages(
        Handler1#h.topic_filter, emqx_retainer_channel:filter_delivery(Messages0)
    ),
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
        _ -> {ok, enqueue_nudge(Handler)}
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

start(SubscribeCtx, TopicFilter, Cursor) ->
    #{message_target := MessageTarget} = SubscribeCtx,
    Handler = #h{
        message_target = MessageTarget,
        topic_filter = TopicFilter,
        words = emqx_topic:words(TopicFilter),
        cursor = Cursor
    },
    request_next(Handler).

handle_next(#h{cursor = ?done} = Handler0, _InfoCtx) ->
    %% Impossible?
    {ok, Handler0};
handle_next(#h{cursor = ?cursor(_), topic_filter = Topic} = Handler, _InfoCtx) ->
    case emqx_retainer:context() of
        undefined ->
            %% The retainer is stopped, so no dispatcher will serve the request.
            {destroy, Handler, [Topic]};
        _ ->
            {ok, request_next(Handler)}
    end.

request_next(#h{request_ref = Ref} = Handler) when is_reference(Ref) ->
    Handler;
request_next(#h{cursor = ?done} = Handler) ->
    Handler;
request_next(#h{cursor = ?cursor(Cursor)} = Handler0) ->
    case ensure_dispatcher(Handler0) of
        #h{dispatcher = undefined} = Handler ->
            enqueue_nudge(Handler);
        #h{dispatcher = Worker, message_target = Target, topic_filter = Topic, words = Words} =
                Handler ->
            Ref = make_ref(),
            emqx_retainer_dispatcher:fetch(Worker, Target, Ref, Topic, Words, Cursor),
            Handler#h{request_ref = Ref}
    end.

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

enrich_messages(TopicFilter, Messages) ->
    [emqx_message:set_header(?HEADER_SUB_TOPIC, TopicFilter, Msg) || Msg <- Messages].

enqueue_nudge(#h{nudge_enqueued = true} = Handler0) ->
    Handler0;
enqueue_nudge(#h{} = Handler0) ->
    #h{message_target = MessageTarget} = Handler0,
    emqx_extsub_handler:send_after(?DISPATCHER_RETRY_DELAY_MS, MessageTarget, #next{}),
    Handler0#h{nudge_enqueued = true}.
