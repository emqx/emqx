%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub).

-moduledoc """
This module is responsible for delivering messages to the MQTT channels from the external sources,
other than regular MQTT messaging.
The module:
* obtains messages from handlers (`emqx_extsub_handler` implementations),
* buffers them,
* delivers them to the client through the inflight (bypassing mqueue).
""".

-include_lib("emqx/include/emqx_hooks.hrl").
-include("emqx_extsub_internal.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([register_hooks/0, unregister_hooks/0]).

-export([
    on_session_created/2,
    on_session_subscribed/3,
    on_session_unsubscribed/3,
    on_session_resumed/2,
    on_session_disconnected/2,
    on_delivery_completed/2,
    on_message_delivered/2,
    on_message_nack/2,
    on_client_handle_info/3
]).

-define(ST_PD_KEY, extsub_st).
-define(CAN_RECEIVE_ACKS_PD_KEY, extsub_can_receive_acks).

-record(st, {
    registry :: emqx_extsub_handler_registry:t(),
    buffer :: emqx_extsub_buffer:t(),
    deliver_retry_tref :: reference() | undefined,
    unacked :: #{emqx_extsub_buffer:seq_id() => true}
}).

-record(extsub_info, {
    handler_ref :: emqx_extsub_types:handler_ref(),
    seq_id :: emqx_extsub_buffer:seq_id(),
    original_qos :: emqx_types:qos()
}).

-spec register_hooks() -> ok.
register_hooks() ->
    ok = emqx_hooks:add('delivery.completed', {?MODULE, on_delivery_completed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('message.delivered', {?MODULE, on_message_delivered, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.created', {?MODULE, on_session_created, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.disconnected', {?MODULE, on_session_disconnected, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('message.nack', {?MODULE, on_message_nack, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_LOWEST).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    emqx_hooks:del('message.delivered', {?MODULE, on_message_delivered}),
    emqx_hooks:del('session.created', {?MODULE, on_session_created}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    emqx_hooks:del('session.disconnected', {?MODULE, on_session_disconnected}),
    emqx_hooks:del('message.nack', {?MODULE, on_message_nack}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}).

%%--------------------------------------------------------------------
%% Hooks callbacks
%%--------------------------------------------------------------------

on_message_delivered(_ClientInfo, Msg) ->
    ?tp_debug(extsub_message_delivered, #{message => Msg}),
    emqx_extsub_metrics:inc(delivered_messages),
    case emqx_message:qos(Msg) =:= ?QOS_0 orelse (not can_receive_acks()) of
        true ->
            ok = on_delivered(Msg, undefined);
        false ->
            ok
    end,
    {ok, Msg}.

on_delivery_completed(Msg, Info) ->
    ?tp_debug(extsub_delivery_completed, #{message => Msg, info => Info}),
    ReasonCode = maps:get(reason_code, Info, ?RC_SUCCESS),
    ok = on_delivered(Msg, ReasonCode).

on_delivered(Msg, ReasonCode) ->
    ?tp_debug(extsub_on_delivered, #{message => Msg, reason_code => ReasonCode}),
    with_msg_handler(
        Msg,
        fun(
            #st{unacked = Unacked0, buffer = Buffer0} = St0,
            Handler0,
            #extsub_info{
                seq_id = SeqId, handler_ref = HandlerRef, original_qos = OriginalQos
            }
        ) ->
            Buffer = emqx_extsub_buffer:set_delivered(Buffer0, HandlerRef, SeqId),
            St = St0#st{buffer = Buffer},
            AckCtx = ack_ctx(St, Handler0, HandlerRef, OriginalQos),
            Handler = emqx_extsub_handler:delivered(Handler0, AckCtx, Msg, ReasonCode),
            %% Update the unacked window
            Unacked = maps:remove(SeqId, Unacked0),
            ?tp_debug(extsub_on_delivered_remove_unacked, #{
                seq_id => SeqId, unacked_cnt => map_size(Unacked)
            }),
            {ok, ensure_deliver_retry_timer(0, St#st{unacked = Unacked}), Handler}
        end
    ).

on_message_nack(Msg, false) ->
    ?tp_debug(extsub_on_message_nack, #{message => Msg}),
    with_msg_handler(
        Msg,
        fun(
            #st{unacked = Unacked0, buffer = Buffer0} = St0,
            Handler,
            #extsub_info{seq_id = SeqId, handler_ref = HandlerRef}
        ) ->
            Unacked = maps:remove(SeqId, Unacked0),
            UnackedCnt = map_size(Unacked),
            Buffer = emqx_extsub_buffer:add_back(Buffer0, HandlerRef, SeqId, Msg),
            St = St0#st{unacked = Unacked, buffer = Buffer},
            case UnackedCnt of
                0 ->
                    ?tp_debug(extsub_on_message_nack_schedule_retry, #{seq_id => SeqId}),
                    {ok, ensure_deliver_retry_timer(St), Handler, true};
                _ ->
                    ?tp_debug(extsub_on_message_nack_no_schedule_retry, #{seq_id => SeqId}),
                    {ok, St, Handler, true}
            end
        end,
        ok
    );
%% Already nacked by some other hook
on_message_nack(_Msg, true) ->
    ok.

on_session_subscribed(ClientInfo, TopicFilter, _SubOpts) ->
    ?tp_debug(extsub_on_session_subscribed, #{topic_filter => TopicFilter}),
    on_subscribed(subscribe, ClientInfo, [TopicFilter]).

on_session_created(_ClientInfo, SessionInfo) ->
    ?tp_debug(extsub_on_session_created, #{session_info => SessionInfo}),
    ok = set_can_receive_acks(SessionInfo).

on_session_resumed(ClientInfo, #{subscriptions := Subs} = SessionInfo) ->
    ?tp_debug(extsub_on_session_resumed, #{client_info => ClientInfo, subscriptions => Subs}),
    ok = set_can_receive_acks(SessionInfo),
    on_subscribed(resume, ClientInfo, maps:keys(Subs)).

on_subscribed(SubscribeType, ClientInfo, TopicFilters) ->
    SubscribeCtx = subscribe_ctx(ClientInfo),
    with_st(fun(#st{registry = HandlerRegistry} = St) ->
        {ok, St#st{
            registry = emqx_extsub_handler_registry:subscribe(
                HandlerRegistry, SubscribeType, SubscribeCtx, TopicFilters
            )
        }}
    end).

on_session_unsubscribed(_ClientInfo, TopicFilter, _SubOpts) ->
    ?tp_debug(extsub_on_session_unsubscribed, #{topic_filter => TopicFilter}),
    on_unsubscribed(unsubscribe, [TopicFilter]).

on_session_disconnected(_ClientInfo, #{subscriptions := Subs} = _SessionInfo) ->
    ?tp_debug(extsub_on_session_disconnected, #{subscriptions => Subs}),
    on_unsubscribed(disconnect, maps:keys(Subs)).

on_unsubscribed(UnsubscribeType, TopicFilters) ->
    with_st(fun(#st{registry = HandlerRegistry} = St) ->
        {ok, St#st{
            registry = emqx_extsub_handler_registry:unsubscribe(
                HandlerRegistry, UnsubscribeType, TopicFilters
            )
        }}
    end).

on_client_handle_info(
    #info_to_extsub{handler_ref = HandlerRef, info = InfoMsg},
    #{session_info_fn := SessionInfoFn} = HookContext,
    #{deliver := Delivers} = Acc
) ->
    %% Message to a specific handler
    with_st(
        fun(St0) ->
            case do_handle_info(St0, HookContext, HandlerRef, InfoMsg) of
                {ok, St1} ->
                    {ok, St1};
                {try_deliver, St1} ->
                    {St, NewDelivers} = try_deliver(SessionInfoFn, St1),
                    {ok, St, {ok, Acc#{deliver => NewDelivers ++ Delivers}}}
            end
        end,
        {ok, Acc}
    );
on_client_handle_info(
    #info_extsub_try_deliver{},
    #{session_info_fn := SessionInfoFn} = _HookContext,
    #{deliver := Delivers} = Acc
) ->
    ?tp_debug(extsub_on_client_handle_info_try_deliver, #{}),
    {ok, Acc#{deliver => try_deliver(SessionInfoFn) ++ Delivers}};
on_client_handle_info(
    Info, #{session_info_fn := SessionInfoFn} = HookContext, #{deliver := Delivers} = Acc0
) ->
    %% Generic info
    with_st(fun(#st{registry = HandlerRegistry} = St0) ->
        GenericMessageHandlers = emqx_extsub_handler_registry:generic_message_handlers(
            HandlerRegistry
        ),
        {St1, NeedTryDeliver} = lists:foldl(
            fun(HandlerRef, {StAcc0, NeedTryDeliverAcc} = StAcc) ->
                case do_handle_info(StAcc0, HookContext, HandlerRef, {generic, Info}) of
                    {ok, StAcc} ->
                        {StAcc, NeedTryDeliverAcc};
                    {try_deliver, StAcc} ->
                        {StAcc, true}
                end
            end,
            {St0, false},
            GenericMessageHandlers
        ),
        {St, Acc} =
            case NeedTryDeliver of
                true ->
                    {St2, NewDelivers} = try_deliver(SessionInfoFn, St1),
                    {St2, Acc0#{deliver => NewDelivers ++ Delivers}};
                false ->
                    {St1, Acc0}
            end,
        {ok, St, {ok, Acc}}
    end).

do_handle_info(St0, #{chan_info_fn := ChanInfoFn} = _HookContext, HandlerRef, InfoMsg) ->
    {St1, InfoHandleResult} = with_handler(
        St0,
        HandlerRef,
        fun(#st{buffer = Buffer0} = St, Handler0) ->
            case
                emqx_extsub_handler:info(
                    Handler0, info_ctx(St, Handler0, HandlerRef), InfoMsg
                )
            of
                {ok, Handler} ->
                    {ok, St, Handler, ok};
                {ok, Handler, Messages} ->
                    ?tp_debug(extsub_on_client_handle_info_add_new, #{
                        handler_ref => HandlerRef, messages => length(Messages)
                    }),
                    Buffer = emqx_extsub_buffer:add_new(Buffer0, HandlerRef, Messages),
                    {ok, St#st{buffer = Buffer}, Handler, try_deliver};
                recreate ->
                    {ok, St, Handler0, recreate}
            end
        end,
        ok
    ),
    case InfoHandleResult of
        recreate ->
            ClientInfo = ChanInfoFn(clientinfo),
            {ok, recreate_handler(St1, ClientInfo, HandlerRef)};
        ok ->
            {ok, St1};
        try_deliver ->
            {try_deliver, St1}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

recreate_handler(#st{registry = HandlerRegistry, buffer = Buffer} = St, ClientInfo, OldHandlerRef) ->
    SubscribeCtx = subscribe_ctx(ClientInfo),
    St#st{
        registry = emqx_extsub_handler_registry:recreate(
            HandlerRegistry, SubscribeCtx, OldHandlerRef
        ),
        buffer = emqx_extsub_buffer:drop_handler(Buffer, OldHandlerRef)
    }.

try_deliver(SessionInfoFn) ->
    with_st(fun(St0) ->
        {St, Delivers} = try_deliver(SessionInfoFn, St0),
        {ok, St, Delivers}
    end).

try_deliver(SessionInfoFn, #st{buffer = Buffer0, unacked = Unacked0} = St0) ->
    St = cancel_deliver_retry_timer(St0),
    BufferSize = emqx_extsub_buffer:size(Buffer0),
    UnackedCnt = map_size(Unacked0),
    DeliverCnt = deliver_count(SessionInfoFn, UnackedCnt),
    ?tp_debug(extsub_try_deliver, #{
        deliver_cnt => DeliverCnt, buffer_size => BufferSize, unacked_cnt => UnackedCnt
    }),
    case DeliverCnt of
        0 when BufferSize > 0 andalso UnackedCnt =:= 0 ->
            {ensure_deliver_retry_timer(St), []};
        0 ->
            %% * No room but noting to deliver (BufferSize =:= 0)
            %% * No room and there are unacked messages (UnackedCnt > 0) — we will retry
            %% on their ack callback.
            {St, []};
        Room ->
            {MessageEntries, Buffer} = emqx_extsub_buffer:take(Buffer0, Room),
            Unacked = add_unacked(Unacked0, MessageEntries),
            {St#st{buffer = Buffer, unacked = Unacked}, delivers(MessageEntries)}
    end.

add_unacked(Unacked, MessageEntries) ->
    lists:foldl(
        fun({_HandlerRef, SeqId, _Msg}, UnackedAcc) ->
            UnackedAcc#{SeqId => true}
        end,
        Unacked,
        MessageEntries
    ).

delivers(MessageEntries) ->
    lists:map(
        fun({HandlerRef, SeqId, Msg0}) ->
            Topic = emqx_message:topic(Msg0),
            Msg = emqx_message:set_headers(
                #{
                    ?EXTSUB_HEADER_INFO => #extsub_info{
                        handler_ref = HandlerRef,
                        seq_id = SeqId,
                        original_qos = emqx_message:qos(Msg0)
                    }
                },
                Msg0
            ),
            {deliver, Topic, Msg}
        end,
        MessageEntries
    ).

with_st(Fun) ->
    with_st(Fun, ok).

with_st(Fun, DefaultResult) ->
    St0 = get_st(),
    case Fun(St0) of
        {ok, St} ->
            ok = put_st(St),
            DefaultResult;
        {ok, St, Result} ->
            ok = put_st(St),
            Result
    end.

with_msg_handler(Msgs, Fun) ->
    with_msg_handler(Msgs, Fun, ok).
with_msg_handler(#message{} = Msg, Fun, DefaultResult) ->
    case emqx_message:get_header(?EXTSUB_HEADER_INFO, Msg) of
        undefined ->
            DefaultResult;
        #extsub_info{handler_ref = HandlerRef} = ExtSubInfo ->
            with_st(fun(St0) ->
                {St, Result} = with_handler(St0, HandlerRef, Fun, [ExtSubInfo], DefaultResult),
                {ok, St, Result}
            end)
    end.

with_handler(St, HandlerRef, Fun, DefaultResult) when is_reference(HandlerRef) ->
    with_handler(St, HandlerRef, Fun, [], DefaultResult).

with_handler(#st{registry = HandlerRegistry0} = St0, HandlerRef, Fun, Args, DefaultResult) ->
    case emqx_extsub_handler_registry:find(HandlerRegistry0, HandlerRef) of
        undefined ->
            {St0, DefaultResult};
        Handler0 ->
            case erlang:apply(Fun, [St0, Handler0 | Args]) of
                {ok, St, Handler} ->
                    HandlerRegistry = emqx_extsub_handler_registry:update(
                        HandlerRegistry0, HandlerRef, Handler
                    ),
                    {St#st{registry = HandlerRegistry}, DefaultResult};
                {ok, St, Handler, Result} ->
                    HandlerRegistry = emqx_extsub_handler_registry:update(
                        HandlerRegistry0, HandlerRef, Handler
                    ),
                    {St#st{registry = HandlerRegistry}, Result}
            end
    end.

deliver_count(SessionInfoFn, UnackedCnt) ->
    InflightMax = SessionInfoFn(inflight_max),
    InflightCnt = SessionInfoFn(inflight_cnt),
    ?tp_debug(extsub_deliver_count_calc, #{
        inflight_max => InflightMax, inflight_cnt => InflightCnt, unacked_cnt => UnackedCnt
    }),
    case InflightMax of
        0 -> ?EXTSUB_MAX_UNACKED - UnackedCnt;
        _ -> max(?EXTSUB_MAX_UNACKED - UnackedCnt, InflightMax - InflightCnt)
    end.

subscribe_ctx(ClientInfo) ->
    #{
        clientinfo => ClientInfo,
        can_receive_acks => can_receive_acks()
    }.

info_ctx(State, Handler, HandlerRef) ->
    #{desired_message_count => desired_message_count(State, Handler, HandlerRef)}.

ack_ctx(State, Handler, HandlerRef, OriginalQos) ->
    #{
        desired_message_count => desired_message_count(State, Handler, HandlerRef),
        qos => OriginalQos
    }.

desired_message_count(#st{buffer = Buffer}, Handler, HandlerRef) ->
    BufferSize = emqx_extsub_handler:get_option(buffer_size, Handler, ?EXTSUB_BUFFER_SIZE),
    DeliveringCnt = emqx_extsub_buffer:delivering_count(Buffer, HandlerRef),
    ?tp_debug(extsub_desired_message_count, #{
        configured_buffer_size => BufferSize, delivering_cnt => DeliveringCnt
    }),
    case DeliveringCnt of
        N when N < BufferSize ->
            BufferSize;
        _ ->
            0
    end.

ensure_deliver_retry_timer(St) ->
    ensure_deliver_retry_timer(?EXTSUB_DELIVER_RETRY_INTERVAL, St).

ensure_deliver_retry_timer(Interval, #st{deliver_retry_tref = undefined, buffer = Buffer} = St) ->
    case emqx_extsub_buffer:size(Buffer) > 0 of
        true ->
            ?tp_debug(extsub_ensure_deliver_retry_timer_schedule, #{interval => Interval}),
            TRef = erlang:send_after(Interval, self(), #info_extsub_try_deliver{}),
            St#st{deliver_retry_tref = TRef};
        false ->
            ?tp_debug(extsub_ensure_deliver_retry_timer_no_buffer, #{}),
            St
    end;
ensure_deliver_retry_timer(_Interval, #st{deliver_retry_tref = TRef} = St) when
    is_reference(TRef)
->
    ?tp_debug(extsub_ensure_deliver_retry_timer_already_scheduled, #{}),
    St.

cancel_deliver_retry_timer(#st{deliver_retry_tref = TRef} = St) ->
    ok = emqx_utils:cancel_timer(TRef),
    St#st{deliver_retry_tref = undefined}.

get_st() ->
    case erlang:get(?ST_PD_KEY) of
        undefined -> new_st();
        St -> St
    end.

new_st() ->
    #st{
        registry = emqx_extsub_handler_registry:new(),
        buffer = emqx_extsub_buffer:new(),
        deliver_retry_tref = undefined,
        unacked = #{}
    }.

put_st(#st{} = St) ->
    _ = erlang:put(?ST_PD_KEY, St),
    ok.

set_can_receive_acks(#{impl := emqx_session_mem} = _SessionInfo) ->
    _ = erlang:put(?CAN_RECEIVE_ACKS_PD_KEY, true),
    ok;
set_can_receive_acks(_SessionInfo) ->
    _ = erlang:put(?CAN_RECEIVE_ACKS_PD_KEY, false),
    ok.

can_receive_acks() ->
    case erlang:get(?CAN_RECEIVE_ACKS_PD_KEY) of
        undefined ->
            false;
        CanReceiveAcks ->
            CanReceiveAcks
    end.
