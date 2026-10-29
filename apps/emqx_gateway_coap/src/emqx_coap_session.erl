%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_coap_session).

%% Exempt the pre-existing calls that elvis 5.0.4 reports in this module.
-elvis([
    {elvis_style, no_invalid_dynamic_calls, disable},
    {elvis_style, simplify_anonymous_functions, disable}
]).

-include("emqx_coap.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

%% API
-export([
    new/0,
    resume/2,
    process_subscribe/4
]).

-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    handle_request/2,
    handle_response/2,
    handle_out/2,
    set_reply/2,
    deliver/3,
    deliver/5,
    drain_pending_observe_notifications/2,
    next_timeout/1,
    timeout/2
]).

-ifdef(TEST).
-export([map_notify_block2_prepare_result/4]).
-endif.

-export_type([session/0]).

-record(session, {
    transport_manager :: emqx_coap_tm:manager(),
    observe_manager :: emqx_coap_observe_res:manager(),
    observe_inflight = 0 :: non_neg_integer(),
    observe_pending :: queue:queue(),
    observe_pending_len = 0 :: non_neg_integer(),
    observe_pending_dropped = 0 :: non_neg_integer(),
    observe_non_until :: integer() | undefined,
    observe_latest = #{} :: map(),
    created_at :: pos_integer()
}).

-type session() :: #session{}.

%% steal from emqx_session
-define(INFO_KEYS, [
    subscriptions,
    upgrade_qos,
    retry_interval,
    await_rel_timeout,
    created_at
]).

-define(STATS_KEYS, [
    subscriptions_cnt,
    subscriptions_max,
    inflight_cnt,
    inflight_max,
    mqueue_len,
    mqueue_max,
    mqueue_dropped,
    next_pkt_id,
    awaiting_rel_cnt,
    awaiting_rel_max
]).

-define(OBSERVE_INFLIGHT_WINDOW, 1).
-define(OBSERVE_NOTIFICATION_QUEUE_MAX_LEN, 100).

-import(emqx_coap_medium, [iter/3]).
-import(emqx_coap_channel, [metrics_inc/2]).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------
-spec new() -> session().
new() ->
    _ = emqx_utils:rand_seed(),
    #session{
        transport_manager = emqx_coap_tm:new(),
        observe_manager = emqx_coap_observe_res:new_manager(),
        observe_pending = queue:new(),
        created_at = erlang:system_time(millisecond)
    }.

-spec resume(emqx_types:clientinfo(), session()) -> session().
resume(_ClientInfo, Session = #session{}) ->
    Session.

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------
%% @doc Compatible with emqx_session
%% do we need use inflight and mqueue in here?
-spec info(session()) -> emqx_types:infos().
info(Session) ->
    maps:from_list(info(?INFO_KEYS, Session)).

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(subscriptions, #session{observe_manager = OM}) ->
    emqx_coap_observe_res:subscriptions(OM);
info(subscriptions_cnt, #session{observe_manager = OM}) ->
    maps:size(emqx_coap_observe_res:subscriptions(OM));
info(subscriptions_max, _) ->
    infinity;
info(upgrade_qos, _) ->
    ?QOS_0;
info(inflight, _) ->
    emqx_inflight:new();
info(inflight_cnt, #session{observe_inflight = Inflight}) ->
    Inflight;
info(inflight_max, _) ->
    ?OBSERVE_INFLIGHT_WINDOW;
info(retry_interval, _) ->
    infinity;
info(mqueue, _) ->
    emqx_mqueue:init(#{max_len => 0, store_qos0 => false});
info(mqueue_len, #session{observe_pending_len = Len}) ->
    Len;
info(mqueue_max, _) ->
    ?OBSERVE_NOTIFICATION_QUEUE_MAX_LEN;
info(mqueue_dropped, #session{observe_pending_dropped = Dropped}) ->
    Dropped;
info(next_pkt_id, _) ->
    0;
info(awaiting_rel, _) ->
    #{};
info(awaiting_rel_cnt, _) ->
    0;
info(awaiting_rel_max, _) ->
    infinity;
info(await_rel_timeout, _) ->
    infinity;
info(created_at, #session{created_at = CreatedAt}) ->
    CreatedAt.

%% @doc Get stats of the session.
-spec stats(session()) -> emqx_types:stats().
stats(Session) -> info(?STATS_KEYS, Session).

%%%-------------------------------------------------------------------
%%% Process Message
%%%-------------------------------------------------------------------
handle_request(Msg, Session) ->
    call_transport_manager(
        ?FUNCTION_NAME,
        Msg,
        Session
    ).

handle_response(Msg, Session) ->
    call_transport_manager(?FUNCTION_NAME, Msg, Session).

handle_out(Msg, Session) ->
    call_transport_manager(?FUNCTION_NAME, Msg, Session).

set_reply(Msg, #session{transport_manager = TM} = Session) ->
    TM2 = emqx_coap_tm:set_reply(Msg, TM),
    Session#session{transport_manager = TM2}.

deliver(
    Delivers,
    Ctx,
    #session{} = Session
) ->
    do_deliver(Delivers, Ctx, Session, undefined, undefined).

deliver(
    Delivers,
    Ctx,
    #session{} = Session,
    BW0,
    PeerKey
) ->
    do_deliver(Delivers, Ctx, Session, BW0, PeerKey).

do_deliver(
    Delivers,
    Ctx,
    #session{} = Session,
    BW0,
    PeerKey
) ->
    Fun = fun(
        {_, Topic, Message}, {OutAcc, #session{observe_manager = OMAcc} = SessionAcc, BWAcc} = Acc
    ) ->
        case emqx_coap_observe_res:res_changed(Topic, OMAcc) of
            undefined ->
                metrics_inc('delivery.dropped', Ctx),
                metrics_inc('delivery.dropped.no_subid', Ctx),
                Acc;
            {Notifications, OM2} ->
                lists:foldl(
                    fun({Token, SeqId}, {Outs, S0, B0}) ->
                        metrics_inc('messages.delivered', Ctx),
                        Msg0 = mqtt_to_coap(Message, Token, SeqId),
                        {Out, S1, B1} = send_or_queue_observe_notification(
                            Topic, Msg0, Message, Ctx, S0, B0, PeerKey
                        ),
                        {[Out | Outs], S1, B1}
                    end,
                    {OutAcc, SessionAcc#session{observe_manager = OM2}, BWAcc},
                    [
                        N
                     || {Token, _} = N <- Notifications,
                        allow_observe_delivery(Topic, Token, Message, OM2)
                    ]
                )
        end
    end,
    {Outs, Session2, BW2} = lists:foldl(Fun, {[], Session, BW0}, Delivers),

    BaseResult = #{
        out => lists:flatten(lists:reverse(Outs)),
        session => Session2
    },
    Result0 = maybe_attach_blockwise_result(BaseResult, BW0, BW2),
    {Result, Session3} = drain_observe_notifications(Result0, Session2),
    Result#{session => Session3}.

allow_observe_delivery(Topic, Token, #message{from = From}, OM) ->
    case emqx_coap_observe_res:lookup(Topic, Token, OM) of
        #{clientid := From, subopts := #{nl := 1}} when From =/= undefined -> false;
        _ -> true
    end.

maybe_attach_blockwise_result(BaseResult, BW0, BW2) when is_map(BW0) ->
    BaseResult#{blockwise => BW2};
maybe_attach_blockwise_result(BaseResult, _BW0, _BW2) ->
    BaseResult.

timeout(observe, Session) ->
    drain_pending_observe_notifications(Session, undefined);
timeout(Timer, Session) ->
    call_transport_manager(?FUNCTION_NAME, Timer, Session).

drain_pending_observe_notifications(#session{} = Session0, BW0) ->
    Result0 =
        case is_map(BW0) of
            true -> #{blockwise => BW0};
            false -> #{}
        end,
    {Result, Session} = drain_observe_notifications(Result0, Session0),
    Result#{session => Session}.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
call_transport_manager(
    Fun,
    Msg,
    #session{transport_manager = TM} = Session
) ->
    Result = emqx_coap_tm:Fun(Msg, TM),
    iter(
        [tm, fun process_tm/4, fun process_session/3],
        Result,
        Session
    ).

process_tm(TM, Result, Session, Cursor) ->
    iter(Cursor, Result, Session#session{transport_manager = TM}).

process_session(_, Result, Session0) ->
    {Result1, Session1} = maybe_cancel_failed_observe(Result, Session0),
    {Result2, Session2} = maybe_drain_observe_notifications(Result1, Session1),
    Result2#{session => Session2}.

maybe_cancel_failed_observe(
    #{observe_notification_failed := {observe_notification, Topic, Id}} = Result,
    #session{observe_manager = OM} = Session
) ->
    Tokens = [
        Token
     || Token <- emqx_coap_observe_res:tokens(Topic, OM),
        #{id := CurrentId} <- [emqx_coap_observe_res:lookup(Topic, Token, OM)],
        CurrentId =:= Id
    ],
    case Tokens of
        [Token] ->
            cancel_observe(Topic, Token, Result, Session);
        [] ->
            {Result, Session}
    end;
maybe_cancel_failed_observe(Result, Session) ->
    {Result, Session}.

send_or_queue_observe_notification(ObservedTopic, Msg0, MQTT, Ctx, Session0, BW0, PeerKey) ->
    Session = enqueue_observe_confirmations(
        remember_observe_notification(ObservedTopic, Msg0, MQTT, Ctx, Session0, BW0, PeerKey)
    ),
    Token = Msg0#coap_message.token,
    case maps:get({ObservedTopic, Token}, Session#session.observe_latest) of
        #{confirm_queued := true} ->
            {[], Session, BW0};
        _ ->
            queue_or_send_observe_notification(
                ObservedTopic, Msg0, MQTT, Ctx, Session, BW0, PeerKey
            )
    end.

queue_or_send_observe_notification(ObservedTopic, Msg0, MQTT, Ctx, Session, BW0, PeerKey) ->
    TrackInflight = is_confirmable_observe_notification(Msg0),
    NeedsBlockwise = observe_notification_needs_block2(Msg0, BW0),
    DeferBlockwise = should_defer_observe_blockwise(Msg0, Session, BW0, PeerKey),
    case should_queue_observe_notification(Session) orelse DeferBlockwise of
        true ->
            Session1 = enqueue_observe_notification(
                Msg0, MQTT, ObservedTopic, undefined, PeerKey, Ctx, NeedsBlockwise, Session
            ),
            {[], Session1, BW0};
        false ->
            maybe_send_or_enqueue_observe_notification(
                ObservedTopic, Msg0, MQTT, Ctx, Session, BW0, PeerKey, TrackInflight
            )
    end.

should_queue_observe_notification(
    #session{
        observe_inflight = Inflight,
        observe_pending_len = PendingLen
    } = Session
) ->
    Inflight >= ?OBSERVE_INFLIGHT_WINDOW orelse PendingLen > 0 orelse waiting_for_non(Session).

maybe_send_or_enqueue_observe_notification(
    ObservedTopic,
    Msg0,
    MQTT,
    Ctx,
    Session,
    BW0,
    PeerKey,
    TrackInflight
) ->
    case maybe_split_notify_block2(Msg0, PeerKey, BW0, Ctx) of
        {busy, BW1} ->
            Session1 = enqueue_observe_notification(
                Msg0, MQTT, ObservedTopic, undefined, PeerKey, Ctx, true, Session
            ),
            {[], Session1, BW1};
        {too_large, BW1} ->
            {[], Session, BW1};
        {Msg, BW1} ->
            case send_observe_notification_now(ObservedTopic, Msg, TrackInflight, Session) of
                {[], Session1} ->
                    NeedsBlockwise = observe_notification_needs_block2(Msg0, BW0),
                    Session2 = enqueue_observe_notification(
                        Msg0,
                        MQTT,
                        ObservedTopic,
                        undefined,
                        PeerKey,
                        Ctx,
                        NeedsBlockwise,
                        Session1
                    ),
                    {[], Session2, BW0};
                {Out, Session1} ->
                    maybe_inc_notify_block2_started(Msg, Ctx),
                    {Out, Session1, BW1}
            end
    end.

send_observe_notification_now(
    Topic,
    #coap_message{token = Token} = Msg0,
    TrackInflight,
    #session{transport_manager = TM0, observe_manager = OM} = Session
) ->
    #{id := Id} = emqx_coap_observe_res:lookup(Topic, Token, OM),
    Msg = emqx_coap_message:set(observe, emqx_coap_observe_res:next_value(), Msg0),
    Ctx = {observe_notification, Topic, Id},
    case emqx_coap_tm:handle_out(Msg, Ctx, TM0) of
        #{out := Out, tm := TM1} ->
            Session1 = Session#session{transport_manager = TM1},
            {Out,
                record_observe_sent(
                    Topic, Msg, maybe_inc_observe_inflight(TrackInflight, Session1)
                )};
        #{tm := TM1} ->
            {[], Session#session{transport_manager = TM1}};
        Empty when map_size(Empty) =:= 0 ->
            {[], Session}
    end.

maybe_inc_observe_inflight(true, #session{observe_inflight = Inflight} = Session) ->
    Session#session{observe_inflight = Inflight + 1};
maybe_inc_observe_inflight(false, Session) ->
    Session.

enqueue_observe_notification(
    Msg,
    MQTT,
    ObservedTopic,
    BW,
    PeerKey,
    Ctx,
    ForceBlockwiseKey,
    #session{
        observe_pending = Queue0,
        observe_pending_len = Len0,
        observe_pending_dropped = Dropped0
    } = Session
) when Len0 < ?OBSERVE_NOTIFICATION_QUEUE_MAX_LEN ->
    Pending = new_pending_observe_notification(
        Msg, MQTT, ObservedTopic, BW, PeerKey, Ctx, ForceBlockwiseKey
    ),
    Session#session{
        observe_pending = queue:in(Pending, Queue0),
        observe_pending_len = Len0 + 1,
        observe_pending_dropped = Dropped0
    };
enqueue_observe_notification(
    Msg,
    MQTT,
    ObservedTopic,
    BW,
    PeerKey,
    Ctx,
    ForceBlockwiseKey,
    #session{
        observe_pending = Queue0,
        observe_pending_dropped = Dropped0
    } = Session
) ->
    {{value, Dropped}, Queue1} = queue:out(Queue0),
    DroppedMQTT = pending_mqtt(Dropped),
    Pending = new_pending_observe_notification(
        Msg, MQTT, ObservedTopic, BW, PeerKey, Ctx, ForceBlockwiseKey
    ),
    metrics_inc('delivery.dropped', Ctx),
    metrics_inc('delivery.dropped.queue_full', Ctx),
    log_observe_notification_queue_full(DroppedMQTT),
    Session1 = Session#session{
        observe_pending = queue:in(Pending, Queue1),
        observe_pending_dropped = Dropped0 + 1
    },
    retry_dropped_confirmation(Dropped, Session1).

retry_dropped_confirmation(
    Pending, #session{observe_latest = Latest, observe_pending = Queue} = Session
) ->
    Key = {pending_observed_topic(Pending), pending_token(Pending)},
    case maps:find(Key, Latest) of
        {ok, #{confirm_queued := true} = Entry} ->
            HasConfirmation = lists:any(
                fun(P) ->
                    {pending_observed_topic(P), pending_token(P)} =:= Key andalso
                        (pending_msg(P))#coap_message.type =:= con
                end,
                queue:to_list(Queue)
            ),
            Session#session{
                observe_latest = Latest#{Key := Entry#{confirm_queued => HasConfirmation}}
            };
        _ ->
            Session
    end.

maybe_drain_observe_notifications(Result0, Session0) ->
    Done = maps:get(observe_notification_done, Result0, 0),
    Result1 = maps:remove(observe_notification_done, Result0),
    Session1 = release_observe_inflight(Done, Session0),
    drain_observe_notifications(Result1, Session1).

release_observe_inflight(0, Session) ->
    Session;
release_observe_inflight(Done, #session{observe_inflight = Inflight} = Session) ->
    Session#session{observe_inflight = max(0, Inflight - Done)}.

drain_observe_notifications(Result, Session0) ->
    Session = enqueue_observe_confirmations(Session0),
    case waiting_for_non(Session) of
        true -> {Result, Session};
        false -> do_drain_observe_notifications(Result, Session)
    end.

do_drain_observe_notifications(
    Result,
    #session{observe_inflight = Inflight} = Session
) when Inflight >= ?OBSERVE_INFLIGHT_WINDOW ->
    {Result, Session};
do_drain_observe_notifications(Result, #session{observe_pending_len = 0} = Session) ->
    {Result, Session};
do_drain_observe_notifications(
    Result,
    #session{observe_pending = Queue0, observe_pending_len = Len0} = Session0
) ->
    case queue:out(Queue0) of
        {{value, Pending}, Queue1} ->
            Session1 = Session0#session{
                observe_pending = Queue1,
                observe_pending_len = Len0 - 1
            },
            case pending_blockwise_active(Pending, Result) of
                true ->
                    {Result, Session1#session{
                        observe_pending = queue:in_r(Pending, Queue1),
                        observe_pending_len = Len0
                    }};
                false ->
                    case prepare_pending_observe_notification(Pending, Result) of
                        wait ->
                            {Result, Session1#session{
                                observe_pending = queue:in_r(Pending, Queue1),
                                observe_pending_len = Len0
                            }};
                        {busy, BW} ->
                            {maybe_attach_pending_blockwise_result(Result, BW), Session1#session{
                                observe_pending = queue:in_r(Pending, Queue1),
                                observe_pending_len = Len0
                            }};
                        {too_large, BW} ->
                            Result1 = maybe_attach_pending_blockwise_result(Result, BW),
                            drain_observe_notifications(Result1, Session1);
                        {Msg, BW} ->
                            do_drain_observe_notification(
                                Msg, BW, Pending, Queue1, Len0, Result, Session1
                            )
                    end
            end;
        {empty, _} ->
            {Result, Session0#session{observe_pending_len = 0}}
    end.

do_drain_observe_notification(Msg, BW, Pending, Queue, Len0, Result, Session0) ->
    TrackInflight = is_confirmable_observe_notification(Msg),
    case
        send_observe_notification_now(pending_observed_topic(Pending), Msg, TrackInflight, Session0)
    of
        {[], Session} ->
            {Result, Session#session{
                observe_pending = queue:in_r(Pending, Queue),
                observe_pending_len = Len0
            }};
        {Out, Session} ->
            maybe_inc_notify_block2_started(Msg, pending_ctx(Pending)),
            Result1 = add_outs(Out, maybe_attach_pending_blockwise_result(Result, BW)),
            drain_observe_notifications(Result1, Session)
    end.

prepare_pending_observe_notification(Pending, Result) ->
    Msg = pending_msg(Pending),
    case pending_blockwise(Pending) of
        BW when is_map(BW) ->
            CurrentBW = maps:get(blockwise, Result, BW),
            case emqx_coap_blockwise:merge_within_limits(CurrentBW, BW) of
                {ok, MergedBW} ->
                    {Msg, MergedBW};
                {error, _Reason} ->
                    {busy, CurrentBW}
            end;
        _ ->
            case {pending_blockwise_key(Pending), maps:get(blockwise, Result, undefined)} of
                {undefined, _} ->
                    {Msg, undefined};
                {_BlockwiseKey, BW0} when is_map(BW0) ->
                    maybe_split_notify_block2(
                        Msg,
                        pending_peer_key(Pending),
                        BW0,
                        pending_ctx(Pending)
                    );
                {_BlockwiseKey, _} ->
                    wait
            end
    end.

should_defer_observe_blockwise(Msg, Session, BW, PeerKey) ->
    case observe_notification_needs_block2(Msg, BW) of
        false ->
            false;
        true ->
            BlockwiseKey = observe_blockwise_key(Msg, PeerKey),
            blockwise_key_active(BlockwiseKey, BW) orelse
                pending_blockwise_key_exists(BlockwiseKey, Session)
    end.

observe_notification_needs_block2(#coap_message{options = Opts, payload = Payload}, BW) when
    is_map(Opts), is_map(BW)
->
    emqx_coap_blockwise:enabled(BW) andalso
        byte_size(Payload) > emqx_coap_blockwise:blockwise_size(BW) andalso
        not maps:is_key(block2, Opts);
observe_notification_needs_block2(_Msg, _BW) ->
    false.

pending_blockwise_key_exists(undefined, _Session) ->
    false;
pending_blockwise_key_exists(BlockwiseKey, #session{observe_pending = Queue}) ->
    lists:any(
        fun(Pending) -> pending_blockwise_key(Pending) =:= BlockwiseKey end,
        queue:to_list(Queue)
    ).

blockwise_key_active(undefined, _BW) ->
    false;
blockwise_key_active(BlockwiseKey, #{server_tx_block2 := ServerTx}) when is_map(ServerTx) ->
    maps:is_key(BlockwiseKey, ServerTx);
blockwise_key_active(_BlockwiseKey, _BW) ->
    false.

new_pending_observe_notification(Msg, MQTT, ObservedTopic, BW, PeerKey, Ctx, true) ->
    {Msg, MQTT, ObservedTopic, BW, observe_blockwise_key(Msg, PeerKey), PeerKey, Ctx};
new_pending_observe_notification(Msg, MQTT, ObservedTopic, BW, PeerKey, Ctx, false) ->
    {Msg, MQTT, ObservedTopic, BW, pending_blockwise_key(Msg, PeerKey), PeerKey, Ctx}.

pending_msg({Msg, _MQTT, _ObservedTopic, _BW, _BlockwiseKey, _PeerKey, _Ctx}) ->
    Msg;
pending_msg({Msg, _MQTT, _BW}) ->
    Msg;
pending_msg({Msg, _MQTT, _BW, _BlockwiseKey}) ->
    Msg;
pending_msg({Msg, _MQTT, _BW, _BlockwiseKey, _PeerKey, _Ctx}) ->
    Msg.

pending_mqtt({_Msg, MQTT, _ObservedTopic, _BW, _BlockwiseKey, _PeerKey, _Ctx}) ->
    MQTT;
pending_mqtt({_Msg, MQTT, _BW}) ->
    MQTT;
pending_mqtt({_Msg, MQTT, _BW, _BlockwiseKey}) ->
    MQTT;
pending_mqtt({_Msg, MQTT, _BW, _BlockwiseKey, _PeerKey, _Ctx}) ->
    MQTT.

pending_observed_topic({_Msg, _MQTT, ObservedTopic, _BW, _BlockwiseKey, _PeerKey, _Ctx}) ->
    ObservedTopic;
pending_observed_topic(_Pending) ->
    undefined.

pending_blockwise({_Msg, _MQTT, _ObservedTopic, BW, _BlockwiseKey, _PeerKey, _Ctx}) ->
    BW;
pending_blockwise({_Msg, _MQTT, BW}) ->
    BW;
pending_blockwise({_Msg, _MQTT, BW, _BlockwiseKey}) ->
    BW;
pending_blockwise({_Msg, _MQTT, BW, _BlockwiseKey, _PeerKey, _Ctx}) ->
    BW.

pending_blockwise_key({_Msg, _MQTT, _ObservedTopic, _BW, BlockwiseKey, _PeerKey, _Ctx}) ->
    BlockwiseKey;
pending_blockwise_key({_Msg, _MQTT, _BW}) ->
    undefined;
pending_blockwise_key({_Msg, _MQTT, _BW, BlockwiseKey}) ->
    BlockwiseKey;
pending_blockwise_key({_Msg, _MQTT, _BW, BlockwiseKey, _PeerKey, _Ctx}) ->
    BlockwiseKey.

pending_peer_key({_Msg, _MQTT, _ObservedTopic, _BW, _BlockwiseKey, PeerKey, _Ctx}) ->
    PeerKey;
pending_peer_key({_Msg, _MQTT, _BW, _BlockwiseKey, PeerKey, _Ctx}) ->
    PeerKey;
pending_peer_key(_Pending) ->
    undefined.

pending_ctx({_Msg, _MQTT, _ObservedTopic, _BW, _BlockwiseKey, _PeerKey, Ctx}) ->
    Ctx;
pending_ctx({_Msg, _MQTT, _BW, _BlockwiseKey, _PeerKey, Ctx}) ->
    Ctx;
pending_ctx(_Pending) ->
    undefined.

pending_blockwise_key(#coap_message{token = Token} = Msg, PeerKey) when Token =/= <<>> ->
    case emqx_coap_message:get_option(block2, Msg, undefined) of
        {_, true, _} ->
            observe_blockwise_key_from_token(Token, PeerKey);
        _ ->
            undefined
    end;
pending_blockwise_key(_Msg, _PeerKey) ->
    undefined.

observe_blockwise_key(#coap_message{token = Token}, PeerKey) when Token =/= <<>> ->
    observe_blockwise_key_from_token(Token, PeerKey);
observe_blockwise_key(_Msg, _PeerKey) ->
    undefined.

observe_blockwise_key_from_token(Token, PeerKey) ->
    {server_tx_block2, PeerKey, Token}.

pending_blockwise_active(Pending, Result) ->
    case pending_blockwise_key(Pending) of
        undefined ->
            false;
        BlockwiseKey ->
            case maps:get(blockwise, Result, undefined) of
                #{server_tx_block2 := ServerTx} when is_map(ServerTx) ->
                    maps:is_key(BlockwiseKey, ServerTx);
                _ ->
                    false
            end
    end.

maybe_attach_pending_blockwise_result(Result, BW) when is_map(BW) ->
    Result#{blockwise => BW};
maybe_attach_pending_blockwise_result(Result, _BW) ->
    Result.

add_outs(Outs, Result) ->
    lists:foldl(
        fun(Out, Acc) -> emqx_coap_medium:out(Out, Acc) end,
        Result,
        Outs
    ).

is_confirmable_observe_notification(#coap_message{type = con, method = {ok, _}} = Msg) ->
    emqx_coap_message:get_option(observe, Msg, undefined) =/= undefined;
is_confirmable_observe_notification(_) ->
    false.

log_observe_notification_queue_full(#message{topic = Topic, payload = Payload}) ->
    ?SLOG_THROTTLE(
        warning,
        #{
            msg => dropped_msg_due_to_mqueue_is_full,
            queue => coap_observe_notification,
            payload => Payload
        },
        #{topic => Topic}
    ).

process_subscribe(
    Sub,
    Msg,
    Result,
    #session{observe_manager = OM} = Session
) ->
    case Sub of
        undefined ->
            Result;
        #{
            topic := _Topic
        } = SubData ->
            {_, OM2} = emqx_coap_observe_res:insert(SubData, OM),
            sync_observe_subscription(maps:get(topic, SubData), OM2),
            Payload = latest_observe_representation(maps:get(topic, SubData), Session),
            Replay = emqx_coap_message:piggyback({ok, content}, Payload, Msg),
            %% RFC 7641 Section 4.4: Observe option carries 24-bit sequence.
            Replay2 = Replay#coap_message{
                options = #{observe => emqx_coap_observe_res:next_value()}
            },
            Result#{
                reply => Replay2,
                session => Session#session{observe_manager = OM2}
            };
        {Topic, Token} ->
            cancel_observe_reply(Topic, [Token], Msg, Result, Session);
        Topic ->
            cancel_observe_reply(
                Topic, emqx_coap_observe_res:tokens(Topic, OM), Msg, Result, Session
            )
    end.

cancel_observe_reply(Topic, Tokens, Msg, Result, Session) ->
    Payload = latest_observe_representation(Topic, Session),
    {Result1, Session1} = lists:foldl(
        fun(Token, {R, S}) ->
            cancel_observe(Topic, Token, R, S)
        end,
        {Result, Session},
        Tokens
    ),
    Result1#{
        reply => emqx_coap_message:piggyback({ok, content}, Payload, Msg), session => Session1
    }.

latest_observe_representation(Topic, #session{observe_latest = Latest}) ->
    Entries = [
        {Updated, pending_msg(Pending)}
     || {{T, _}, #{updated_at := Updated, pending := Pending}} <-
            maps:to_list(Latest),
        T =:= Topic
    ],
    case lists:reverse(lists:sort(Entries)) of
        [{_, #coap_message{payload = Payload}} | _] -> Payload;
        [] -> <<>>
    end.

cancel_observe(
    Topic,
    Token,
    Result,
    #session{
        observe_manager = OM, transport_manager = TM, observe_latest = Latest
    } = Session
) ->
    case emqx_coap_observe_res:lookup(Topic, Token, OM) of
        undefined ->
            {Result, Session};
        #{id := Id} ->
            {Done, TM1} = emqx_coap_tm:abort_observe_notifications(
                {observe_notification, Topic, Id}, TM
            ),
            OM1 = emqx_coap_observe_res:remove(Topic, Token, OM),
            Result1 = maybe_unsubscribe_observe_topic(Topic, OM1, Result),
            Session1 = purge_pending_observe_notifications(Topic, Token, Session),
            {
                Result1#{
                    observe_cancelled_tokens =>
                        [Token | maps:get(observe_cancelled_tokens, Result1, [])]
                },
                release_observe_inflight(Done, Session1#session{
                    observe_manager = OM1,
                    transport_manager = TM1,
                    observe_latest = maps:remove({Topic, Token}, Latest)
                })
            }
    end.

maybe_unsubscribe_observe_topic(Topic, OM, Result) ->
    case emqx_coap_observe_res:tokens(Topic, OM) of
        [] ->
            emqx_broker:unsubscribe(Topic),
            Result#{observe_unsubscribed => [Topic | maps:get(observe_unsubscribed, Result, [])]};
        _ ->
            sync_observe_subscription(Topic, OM),
            Result
    end.

sync_observe_subscription(Topic, OM) ->
    case emqx_broker:subscribed(self(), Topic) of
        true ->
            emqx_broker:subscribe(Topic, maps:get(Topic, emqx_coap_observe_res:subscriptions(OM)));
        false ->
            ok
    end.

waiting_for_non(#session{observe_non_until = undefined}) -> false;
waiting_for_non(#session{observe_non_until = Until}) -> Until > erlang:monotonic_time(millisecond).

remember_observe_notification(
    Topic,
    #coap_message{token = Token} = Msg,
    MQTT,
    Ctx,
    #session{observe_latest = Latest} = Session,
    BW,
    PeerKey
) ->
    Now = erlang:monotonic_time(millisecond),
    Old0 = maps:get({Topic, Token}, Latest, #{last_con_at => Now, confirm_at => infinity}),
    Old =
        case {Msg#coap_message.type, Old0} of
            {non, #{confirm_at := infinity, last_con_at := LastCon}} ->
                Old0#{confirm_at => LastCon + emqx_coap_observe_res:confirmation_interval()};
            _ ->
                Old0
        end,
    NeedsBlockwise = observe_notification_needs_block2(Msg, BW),
    Pending = new_pending_observe_notification(
        Msg, MQTT, Topic, undefined, PeerKey, Ctx, NeedsBlockwise
    ),
    Session#session{
        observe_latest = Latest#{
            {Topic, Token} => Old#{
                pending => Pending,
                updated_at => erlang:monotonic_time(),
                confirm_queued => false
            }
        }
    }.

record_observe_sent(
    Topic,
    #coap_message{type = con, token = Token},
    #session{observe_latest = Latest} = Session
) ->
    case maps:find({Topic, Token}, Latest) of
        {ok, Entry} ->
            Session#session{
                observe_latest = Latest#{
                    {Topic, Token} := Entry#{
                        last_con_at => erlang:monotonic_time(millisecond),
                        confirm_at => infinity,
                        confirm_queued => false
                    }
                }
            };
        error ->
            Session
    end;
record_observe_sent(
    Topic,
    #coap_message{type = non, token = Token},
    #session{observe_latest = Latest} = Session
) ->
    Latest1 =
        case maps:find({Topic, Token}, Latest) of
            {ok, #{confirm_at := infinity} = Entry} ->
                Due = maps:get(last_con_at, Entry) + emqx_coap_observe_res:confirmation_interval(),
                Latest#{{Topic, Token} := Entry#{confirm_at => Due}};
            _ ->
                Latest
        end,
    Session#session{
        observe_latest = Latest1,
        observe_non_until = erlang:monotonic_time(millisecond) + 3000
    }.

next_timeout(
    #session{
        observe_latest = Latest,
        observe_non_until = Until,
        observe_pending_len = Len,
        observe_inflight = Inflight
    } = Session
) ->
    Deadlines = [
        Due
     || {Key, #{confirm_at := Due} = Entry} <- maps:to_list(Latest),
        can_queue_observe_confirmation(Key, Session),
        is_integer(Due),
        not maps:get(confirm_queued, Entry, false)
    ],
    Pacing =
        case Len > 0 andalso Inflight =:= 0 andalso is_integer(Until) of
            true -> [Until];
            false -> []
        end,
    Now = erlang:monotonic_time(millisecond),
    case Deadlines ++ [Due || Due <- Pacing, Due > Now] of
        [] -> undefined;
        All -> lists:min(All)
    end.

enqueue_observe_confirmations(#session{observe_latest = Latest} = Session) ->
    Now = erlang:monotonic_time(millisecond),
    lists:foldl(
        fun
            ({_, #{confirm_queued := true}}, S0) ->
                S0;
            ({{Topic, Token}, #{confirm_at := Due, pending := Pending}}, S0) when
                is_integer(Due), Due =< Now
            ->
                case can_queue_observe_confirmation({Topic, Token}, S0) of
                    true -> prioritize_observe_confirmation(Topic, Token, Pending, S0);
                    false -> S0
                end;
            (_, S0) ->
                S0
        end,
        Session,
        maps:to_list(Latest)
    ).

can_queue_observe_confirmation(_Key, #session{observe_pending_len = Len}) when
    Len < ?OBSERVE_NOTIFICATION_QUEUE_MAX_LEN
->
    true;
can_queue_observe_confirmation({Topic, Token}, #session{observe_pending = Queue}) ->
    lists:any(
        fun(Pending) -> is_pending_observe_notification(Topic, Token, Pending) end,
        queue:to_list(Queue)
    ).

prioritize_observe_confirmation(Topic, Token, Pending, Session0) ->
    Session1 = purge_pending_observe_notifications(Topic, Token, Session0),
    Msg = (pending_msg(Pending))#coap_message{type = con},
    Session2 = enqueue_observe_notification(
        Msg,
        pending_mqtt(Pending),
        Topic,
        undefined,
        pending_peer_key(Pending),
        pending_ctx(Pending),
        pending_blockwise_key(Pending) =/= undefined,
        Session1
    ),
    #session{observe_pending = Queue, observe_latest = Latest} = Session2,
    {{value, Confirmation}, Rest} = queue:out_r(Queue),
    Entry = maps:get({Topic, Token}, Latest),
    Session2#session{
        observe_pending = queue:in_r(Confirmation, Rest),
        observe_latest = Latest#{{Topic, Token} := Entry#{confirm_queued => true}}
    }.

purge_pending_observe_notifications(
    Topic,
    Token,
    #session{observe_pending = Queue0, observe_pending_len = Len0} = Session
) ->
    Pending0 = queue:to_list(Queue0),
    Pending = [
        Entry
     || Entry <- Pending0, not is_pending_observe_notification(Topic, Token, Entry)
    ],
    case length(Pending) of
        Len0 ->
            Session;
        Len ->
            Session#session{
                observe_pending = queue:from_list(Pending),
                observe_pending_len = Len
            }
    end.

is_pending_observe_notification(Topic, Token, Pending) ->
    pending_token(Pending) =:= Token andalso
        case pending_observed_topic(Pending) of
            Topic ->
                true;
            undefined ->
                pending_message_topic(Pending) =:= Topic;
            _ ->
                false
        end.

pending_token(Pending) ->
    case pending_msg(Pending) of
        #coap_message{token = Token} ->
            Token;
        _ ->
            undefined
    end.

pending_message_topic(Pending) ->
    case pending_mqtt(Pending) of
        #message{topic = Topic} ->
            Topic;
        _ ->
            undefined
    end.

mqtt_to_coap(MQTT, Token, SeqId) ->
    #message{payload = Payload} = MQTT,
    #coap_message{
        type = get_notify_type(MQTT),
        method = {ok, content},
        token = Token,
        payload = Payload,
        %% RFC 7641 Section 4.4: Observe option carries 24-bit sequence.
        options = #{observe => emqx_coap_observe_res:observe_value(SeqId)}
    }.

get_notify_type(#message{qos = Qos}) ->
    case emqx_conf:get([gateway, coap, notify_type], qos) of
        qos ->
            case Qos of
                ?QOS_0 ->
                    non;
                _ ->
                    con
            end;
        Other ->
            Other
    end.

maybe_split_notify_block2(Msg, _PeerKey, undefined, _Ctx) ->
    {Msg, undefined};
maybe_split_notify_block2(Msg, PeerKey, BW0, Ctx) ->
    map_notify_block2_prepare_result(
        emqx_coap_blockwise:server_prepare_out_response(undefined, Msg, PeerKey, BW0),
        Msg,
        PeerKey,
        Ctx
    ).

map_notify_block2_prepare_result({single, Msg1, BW1}, _Msg, _PeerKey, _Ctx) ->
    {Msg1, BW1};
map_notify_block2_prepare_result({chunked, Msg1, BW1}, _Msg, _PeerKey, _Ctx) ->
    {Msg1, BW1};
map_notify_block2_prepare_result({busy, _Reason, BW1}, _Msg, _PeerKey, _Ctx) ->
    {busy, BW1};
map_notify_block2_prepare_result({too_large, BW1}, _Msg, PeerKey, Ctx) ->
    ?SLOG(warning, #{msg => "coap_notify_block2_too_large", peer_key => PeerKey}),
    metrics_inc('delivery.dropped', Ctx),
    metrics_inc('blockwise.tx_block2.failed', Ctx),
    {too_large, BW1};
map_notify_block2_prepare_result({error, _ErrorReply, BW1}, _Msg, PeerKey, Ctx) ->
    ?SLOG(warning, #{
        msg => "coap_notify_block2_prepare_failed",
        peer_key => PeerKey
    }),
    metrics_inc('delivery.dropped', Ctx),
    metrics_inc('blockwise.tx_block2.failed', Ctx),
    {too_large, BW1}.

maybe_inc_notify_block2_started(Msg, Ctx) when is_map(Ctx) ->
    case emqx_coap_message:get_option(block2, Msg, undefined) of
        {0, true, _} -> metrics_inc('blockwise.tx_block2.started', Ctx);
        _ -> ok
    end;
maybe_inc_notify_block2_started(_Msg, _Ctx) ->
    ok.
