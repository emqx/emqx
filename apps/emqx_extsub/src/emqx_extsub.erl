%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    on_session_save_subopts/2,
    on_delivery_completed/2,
    on_message_delivered/2,
    on_message_nack/2,
    on_client_handle_info/3
]).

-export([
    set_max_unacked/1,
    max_unacked/0,
    inspect/1
]).

%% Internal exports (for `emqx_extsub` application)
-export([filter_saved_subopts/2]).

-define(ST_PD_KEY, extsub_st).
-define(CHANNEL_INFO_PD_KEY, extsub_channel_info).

-define(MAX_UNACKED_PT_KEY, extsub_max_unacked).

-record(st, {
    registry :: emqx_extsub_handler_registry:t(),
    deliver_retry_tref :: reference() | undefined,
    unacked :: #{emqx_extsub_buffer:seq_id() => true},
    tombstone :: #{
        saved_subopts => #{
            emqx_types:topic() | emqx_types:share() => #{module() => term()}
        }
    }
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
    ok = emqx_hooks:add('session.save_subopts', {?MODULE, on_session_save_subopts, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('message.nack', {?MODULE, on_message_nack, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_LOWEST),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    emqx_hooks:del('message.delivered', {?MODULE, on_message_delivered}),
    emqx_hooks:del('session.created', {?MODULE, on_session_created}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    emqx_hooks:del('session.disconnected', {?MODULE, on_session_disconnected}),
    emqx_hooks:del('session.save_subopts', {?MODULE, on_session_save_subopts}),
    emqx_hooks:del('message.nack', {?MODULE, on_message_nack}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}),
    ok.

-spec inspect(pid()) -> {ok, map()} | {error, timeout}.
inspect(ChannelPid) ->
    Self = alias([reply]),
    erlang:send(ChannelPid, #info_extsub_inspect{receiver = Self}),
    receive
        {Self, Info} ->
            {ok, Info}
    after 5000 ->
        {error, timeout}
    end.

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

on_delivered(#message{} = Msg, ReasonCode) ->
    case emqx_message:get_header(?EXTSUB_HEADER_INFO, Msg) of
        undefined ->
            ok;
        #extsub_info{} = ExtSubInfo ->
            with_st(fun(#st{} = St) ->
                do_on_delivered(St, ExtSubInfo, Msg, ReasonCode)
            end)
    end.

do_on_delivered(
    #st{unacked = Unacked0, registry = HandlerRegistry0} = St0,
    #extsub_info{seq_id = SeqId, handler_ref = HandlerRef, original_qos = OriginalQos},
    Msg,
    ReasonCode
) ->
    ?tp_debug(extsub_on_delivered, #{
        seq_id => SeqId, unacked_cnt => map_size(Unacked0)
    }),
    Unacked = maps:remove(SeqId, Unacked0),
    InitAckCtx = init_ack_ctx(OriginalQos, Unacked),
    Registry = emqx_extsub_handler_registry:delivered(
        HandlerRegistry0, HandlerRef, InitAckCtx, Msg, SeqId, ReasonCode
    ),
    St1 = St0#st{unacked = Unacked, registry = Registry},
    St = ensure_deliver_retry_timer(0, St1),
    {ok, St}.

on_message_nack(Msg, false) ->
    case emqx_message:get_header(?EXTSUB_HEADER_INFO, Msg) of
        undefined ->
            ok;
        #extsub_info{} = ExtSubInfo ->
            _ = with_st(fun(#st{} = St) ->
                do_on_message_nack(St, ExtSubInfo, Msg)
            end),
            {ok, true}
    end;
%% Already nacked by some other hook
on_message_nack(_Msg, true) ->
    ok.

do_on_message_nack(
    #st{unacked = Unacked0, registry = HandlerRegistry0} = St0,
    #extsub_info{seq_id = SeqId, handler_ref = HandlerRef} = _ExtSubInfo,
    Msg
) ->
    Unacked = maps:remove(SeqId, Unacked0),
    UnackedCnt = map_size(Unacked),
    Registry = emqx_extsub_handler_registry:buffer_add_back(
        HandlerRegistry0, HandlerRef, SeqId, Msg
    ),
    St = St0#st{unacked = Unacked, registry = Registry},
    case UnackedCnt of
        0 ->
            ?tp_debug(extsub_on_message_nack_schedule_retry, #{seq_id => SeqId}),
            {ok, ensure_deliver_retry_timer(St)};
        _ ->
            ?tp_debug(extsub_on_message_nack_no_schedule_retry, #{seq_id => SeqId}),
            {ok, St}
    end.

on_session_subscribed(ClientInfo, TopicFilter, SubOpts) ->
    ?tp_debug(extsub_on_session_subscribed, #{topic_filter => TopicFilter}),
    on_subscribed(subscribe, ClientInfo, #{TopicFilter => SubOpts}).

on_session_created(_ClientInfo, SessionInfo) ->
    SessionCreatedCtx = emqx_hooks:context('session.created'),
    ?tp_debug(extsub_on_session_created, #{session_info => SessionInfo}),
    ok = save_channel_info(SessionCreatedCtx, SessionInfo).

on_session_resumed(ClientInfo, #{subscriptions := Subs} = SessionInfo) ->
    SessionResumedCtx = emqx_hooks:context('session.resumed'),
    ?tp_debug(extsub_on_session_resumed, #{client_info => ClientInfo, subscriptions => Subs}),
    ok = save_channel_info(SessionResumedCtx, SessionInfo),
    on_subscribed(resume, ClientInfo, Subs).

on_subscribed(SubscribeType, ClientInfo, TopicFiltersToSubOpts) ->
    SubscribeCtx = subscribe_ctx(ClientInfo),
    with_st(fun(#st{registry = HandlerRegistry} = St) ->
        {ok, St#st{
            registry = emqx_extsub_handler_registry:subscribe(
                HandlerRegistry, SubscribeType, SubscribeCtx, TopicFiltersToSubOpts
            )
        }}
    end).

on_session_unsubscribed(_ClientInfo, TopicFilter, SubOpts) ->
    ?tp_debug(extsub_on_session_unsubscribed, #{topic_filter => TopicFilter, sub_opts => SubOpts}),
    on_unsubscribed(unsubscribe, #{TopicFilter => SubOpts}).

on_session_disconnected(_ClientInfo, #{subscriptions := Subs} = _SessionInfo) ->
    ?tp_debug(extsub_on_session_disconnected, #{subscriptions => Subs}),
    tombstone_subopts(Subs),
    on_unsubscribed(disconnect, Subs).

on_session_save_subopts(Context, SubOpts0) ->
    #{topic_filter := TopicFilter} = Context,
    with_st(
        fun
            (#st{tombstone = #{saved_subopts := SavedSubOpts}} = St) ->
                %% Already disconnected and uninstalled all handlers.
                case SavedSubOpts of
                    #{TopicFilter := SubOpts} ->
                        {ok, St, {ok, SubOpts}};
                    _ ->
                        {ok, St, {ok, SubOpts0}}
                end;
            (#st{} = St0) ->
                {SubOpts, St} = do_save_subopts(St0, Context, SubOpts0),
                {ok, St, {ok, SubOpts}}
        end,
        {ok, SubOpts0}
    ).

do_save_subopts(#st{registry = HandlerRegistry0} = St0, Context, SubOpts0) ->
    {Res, HandlerRegistry} = emqx_extsub_handler_registry:save_subopts(
        HandlerRegistry0, Context, SubOpts0
    ),
    SubOpts =
        case map_size(Res) > 0 of
            true ->
                SubOpts0#{?MODULE => Res};
            false ->
                maps:remove(?MODULE, SubOpts0)
        end,
    {SubOpts, St0#st{registry = HandlerRegistry}}.

on_unsubscribed(UnsubscribeType, Subs) ->
    with_st(fun(#st{registry = HandlerRegistry} = St) ->
        {ok, St#st{
            registry = emqx_extsub_handler_registry:unsubscribe(
                HandlerRegistry, UnsubscribeType, Subs
            )
        }}
    end).

on_client_handle_info(
    _ClientInfo,
    #info_to_extsub{handler_ref = HandlerRef, info = InfoMsg},
    #{deliver := Delivers} = Acc
) ->
    ?tp_debug(extsub_on_client_handle_info_to_extsub, #{handler_ref => HandlerRef, info => InfoMsg}),
    %% Message to a specific handler
    with_st(
        fun(#st{registry = HandlerRegistry0} = St0) ->
            #{session_info_fn := SessionInfoFn} =
                HookContext = emqx_hooks:context('client.handle_info'),
            InfoCtx = info_ctx(HandlerRegistry0, HandlerRef, HookContext),
            Res = emqx_extsub_handler_registry:info(HandlerRegistry0, HandlerRef, InfoCtx, InfoMsg),
            case Res of
                {ok, HandlerRegistry} ->
                    {ok, St0#st{registry = HandlerRegistry}};
                {ok, HandlerRegistry, Messages} ->
                    {St, NewDelivers} = try_deliver(
                        SessionInfoFn, St0#st{registry = HandlerRegistry}, [{HandlerRef, Messages}]
                    ),
                    {ok, St, {ok, Acc#{deliver := NewDelivers ++ Delivers}}}
            end
        end,
        {ok, Acc}
    );
on_client_handle_info(
    _ClientInfo,
    #info_extsub_try_deliver{},
    #{deliver := Delivers} = Acc
) ->
    ?tp_debug(extsub_on_client_handle_info_try_deliver, #{}),
    #{session_info_fn := SessionInfoFn} = emqx_hooks:context('client.handle_info'),
    {ok, Acc#{deliver := try_deliver(SessionInfoFn) ++ Delivers}};
on_client_handle_info(
    _ClientInfo,
    #info_extsub_inspect{receiver = Receiver},
    Acc
) ->
    ?tp_debug(extsub_on_client_handle_info_inspect, #{}),
    _ = with_st(fun(St) ->
        Info = do_inspect(St),
        erlang:send(Receiver, {Receiver, Info}),
        {ok, St}
    end),
    {ok, Acc};
on_client_handle_info(
    _ClientInfo, Info, #{deliver := Delivers} = Acc0
) ->
    ?tp_debug(extsub_on_client_handle_info_generic, #{info => Info}),
    %% Generic info
    with_st(fun(#st{registry = HandlerRegistry0} = St0) ->
        #{session_info_fn := SessionInfoFn} =
            HookContext = emqx_hooks:context('client.handle_info'),
        GenericMessageHandlers = emqx_extsub_handler_registry:generic_message_handlers(
            HandlerRegistry0
        ),
        {HandlerRegistry, Messages} = lists:foldl(
            fun(HandlerRef, {HandlerRegistryAcc0, MessagesAcc}) ->
                InfoCtx = info_ctx(HandlerRegistryAcc0, HandlerRef, HookContext),
                case
                    emqx_extsub_handler_registry:info(
                        HandlerRegistryAcc0, HandlerRef, InfoCtx, {generic, Info}
                    )
                of
                    {ok, HandlerRegistryAcc} ->
                        {HandlerRegistryAcc, MessagesAcc};
                    {ok, HandlerRegistryAcc, Messages} ->
                        {HandlerRegistryAcc, [{HandlerRef, Messages} | MessagesAcc]}
                end
            end,
            {HandlerRegistry0, []},
            GenericMessageHandlers
        ),
        St1 = St0#st{registry = HandlerRegistry},
        {St, Acc} =
            case Messages of
                [] ->
                    {St1, Acc0};
                _ ->
                    {St2, NewDelivers} = try_deliver(SessionInfoFn, St1, Messages),
                    {St2, Acc0#{deliver => NewDelivers ++ Delivers}}
            end,
        {ok, St, {ok, Acc}}
    end).

set_max_unacked(MaxUnacked) ->
    _ = persistent_term:put(?MAX_UNACKED_PT_KEY, MaxUnacked),
    ok.

max_unacked() ->
    persistent_term:get(?MAX_UNACKED_PT_KEY, ?EXTSUB_MAX_UNACKED).

%%--------------------------------------------------------------------
%% Internal exports (for `emqx_extsub` application)
%%--------------------------------------------------------------------

filter_saved_subopts(Module, SubOpts0) ->
    case SubOpts0 of
        #{subopts := InnerSubOpts0} ->
            %% DS session
            case maps:take(?MODULE, InnerSubOpts0) of
                {#{Module := SavedSt}, InnerSubOpts1} ->
                    InnerSubOpts = maybe_unwrap_ds_subopts(InnerSubOpts1),
                    InnerSubOpts#{Module => SavedSt};
                {_, InnerSubOpts} ->
                    maybe_unwrap_ds_subopts(InnerSubOpts);
                _ ->
                    maybe_unwrap_ds_subopts(InnerSubOpts0)
            end;
        #{} ->
            %% in-memory session
            case maps:take(?MODULE, SubOpts0) of
                {#{Module := SavedSt}, SubOpts} ->
                    SubOpts#{Module => SavedSt};
                {_, SubOpts} ->
                    SubOpts;
                error ->
                    SubOpts0
            end
    end.

maybe_unwrap_ds_subopts(#{subopts := SubOpts}) ->
    SubOpts;
maybe_unwrap_ds_subopts(#{} = SubOpts) ->
    SubOpts.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

tombstone_subopts(Subs) ->
    _ = with_st(fun(#st{} = St0) ->
        {Tombstone, St3} = maps:fold(
            fun(TopicFilter, SubOpts0, {TombstoneAcc0, St1}) ->
                Context = #{topic_filter => TopicFilter},
                {SubOpts, St2} = do_save_subopts(St1, Context, SubOpts0),
                HasData =
                    case SubOpts of
                        #{subopts := #{?MODULE := _}} ->
                            %% DS session
                            true;
                        #{?MODULE := _} ->
                            %% In-memory session
                            true;
                        _ ->
                            false
                    end,
                TombstoneAcc = emqx_utils_maps:put_if(TombstoneAcc0, TopicFilter, SubOpts, HasData),
                {TombstoneAcc, St2}
            end,
            {#{}, St0},
            Subs
        ),
        St = St3#st{tombstone = #{saved_subopts => Tombstone}},
        {ok, St}
    end),
    ok.

try_deliver(SessionInfoFn) ->
    with_st(fun(St0) ->
        {St, Delivers} = try_deliver(SessionInfoFn, St0, []),
        {ok, St, Delivers}
    end).

try_deliver(SessionInfoFn, #st{unacked = Unacked0, registry = HandlerRegistry0} = St0, Messages) ->
    HandlerRegistry1 = lists:foldl(
        %% Messages is a list of lists of messages
        fun({HandlerRef, Msgs}, HandlerRegistryAcc) ->
            emqx_extsub_handler_registry:buffer_add(HandlerRegistryAcc, HandlerRef, Msgs)
        end,
        HandlerRegistry0,
        Messages
    ),
    St = cancel_deliver_retry_timer(St0),
    BufferSize = emqx_extsub_handler_registry:buffer_size(HandlerRegistry1),
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
            {St#st{registry = HandlerRegistry1}, []};
        Room ->
            {MessageEntries, HandlerRegistry} = emqx_extsub_handler_registry:buffer_take(
                HandlerRegistry1, Room
            ),
            Unacked = add_unacked(Unacked0, MessageEntries),
            {St#st{registry = HandlerRegistry, unacked = Unacked}, delivers(MessageEntries)}
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

deliver_count(SessionInfoFn, UnackedCnt) ->
    InflightMax = SessionInfoFn(inflight_max),
    InflightCnt = SessionInfoFn(inflight_cnt),
    ?tp_debug(extsub_deliver_count_calc, #{
        inflight_max => InflightMax, inflight_cnt => InflightCnt, unacked_cnt => UnackedCnt
    }),
    MaxUnacked = max_unacked(),
    case InflightMax of
        0 -> MaxUnacked - UnackedCnt;
        _ -> max(MaxUnacked - UnackedCnt, InflightMax - InflightCnt)
    end.

subscribe_ctx(ClientInfo) ->
    maps:merge(
        #{
            clientinfo => ClientInfo
        },
        get_channel_info()
    ).

info_ctx(
    HandlerRegistry,
    HandlerRef,
    #{chan_info_fn := ClientInfoFn} = _HookContext
) ->
    {DeliveringCnt, DesiredMsgCount} = emqx_extsub_handler_registry:message_counts(
        HandlerRegistry, HandlerRef
    ),
    #{
        desired_message_count => DesiredMsgCount,
        delivering_count => DeliveringCnt,
        can_receive_acks => can_receive_acks(),
        clientinfo => ClientInfoFn(clientinfo)
    }.

init_ack_ctx(OriginalQos, Unacked) ->
    #{
        unacked_count => map_size(Unacked),
        qos => OriginalQos
    }.

ensure_deliver_retry_timer(St) ->
    ensure_deliver_retry_timer(?EXTSUB_DELIVER_RETRY_INTERVAL, St).

ensure_deliver_retry_timer(
    Interval, #st{deliver_retry_tref = undefined, registry = HandlerRegistry} = St
) ->
    case emqx_extsub_handler_registry:buffer_size(HandlerRegistry) > 0 of
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
        deliver_retry_tref = undefined,
        unacked = #{},
        tombstone = #{}
    }.

put_st(#st{} = St) ->
    _ = erlang:put(?ST_PD_KEY, St),
    ok.

save_channel_info(#{conn_info_fn := ConnInfoFn} = _Ctx, SessionInfo) ->
    CanReceiveAcks = maps:get(impl, SessionInfo, undefined) =:= emqx_session_mem,
    _ = erlang:put(?CHANNEL_INFO_PD_KEY, #{
        can_receive_acks => CanReceiveAcks, conninfo_fn => ConnInfoFn
    }),
    ok.

get_channel_info() ->
    #{} = erlang:get(?CHANNEL_INFO_PD_KEY).

can_receive_acks() ->
    #{can_receive_acks := CanReceiveAcks} = get_channel_info(),
    CanReceiveAcks.

do_inspect(#st{
    unacked = Unacked,
    deliver_retry_tref = DeliverRetryTRef,
    tombstone = Tombstone,
    registry = HandlerRegistry
}) ->
    #{
        unacked_count => maps:size(Unacked),
        deliver_retry_scheduled => DeliverRetryTRef =/= undefined,
        tombstone => Tombstone,
        registry => emqx_extsub_handler_registry:inspect(HandlerRegistry)
    }.
