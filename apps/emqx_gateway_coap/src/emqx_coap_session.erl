%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_coap_session).

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
            {Token, SeqId, OM2} ->
                metrics_inc('messages.delivered', Ctx),
                Msg0 = mqtt_to_coap(Message, Token, SeqId),
                SessionAcc1 = SessionAcc#session{observe_manager = OM2},
                {Out, SessionAcc2, BW2} =
                    send_or_queue_observe_notification(
                        Msg0, Message, Ctx, SessionAcc1, BWAcc, PeerKey
                    ),
                {[Out | OutAcc], SessionAcc2, BW2}
        end
    end,
    {Outs, Session2, BW2} = lists:foldl(Fun, {[], Session, BW0}, lists:reverse(Delivers)),

    BaseResult = #{
        out => lists:flatten(lists:reverse(Outs)),
        session => Session2
    },
    maybe_attach_blockwise_result(BaseResult, BW0, BW2).

maybe_attach_blockwise_result(BaseResult, BW0, BW2) when is_map(BW0) ->
    BaseResult#{blockwise => BW2};
maybe_attach_blockwise_result(BaseResult, _BW0, _BW2) ->
    BaseResult.

timeout(Timer, Session) ->
    call_transport_manager(?FUNCTION_NAME, Timer, Session).

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
    {Result1, Session1} = maybe_drain_observe_notifications(Result, Session0),
    Result1#{session => Session1}.

send_or_queue_observe_notification(Msg0, MQTT, Ctx, Session, BW0, PeerKey) ->
    TrackInflight = is_confirmable_observe_notification(Msg0),
    case should_queue_observe_notification(Session) of
        true ->
            {Msg, BW1} = maybe_split_notify_block2(Msg0, PeerKey, BW0, Ctx),
            Session1 = enqueue_observe_notification(Msg, MQTT, BW1, Ctx, Session),
            {[], Session1, BW0};
        false ->
            maybe_send_or_enqueue_observe_notification(
                Msg0, MQTT, Ctx, Session, BW0, PeerKey, TrackInflight
            )
    end.

should_queue_observe_notification(#session{
    observe_inflight = Inflight,
    observe_pending_len = PendingLen
}) ->
    Inflight >= ?OBSERVE_INFLIGHT_WINDOW orelse PendingLen > 0.

maybe_send_or_enqueue_observe_notification(
    Msg0,
    MQTT,
    Ctx,
    Session,
    BW0,
    PeerKey,
    TrackInflight
) ->
    {Msg, BW1} = maybe_split_notify_block2(Msg0, PeerKey, BW0, Ctx),
    case send_observe_notification_now(Msg, TrackInflight, Session) of
        {[], Session1} ->
            Session2 = enqueue_observe_notification(Msg, MQTT, BW1, Ctx, Session1),
            {[], Session2, BW0};
        {Out, Session1} ->
            {Out, Session1, BW1}
    end.

send_observe_notification_now(Msg, TrackInflight, #session{transport_manager = TM0} = Session) ->
    case emqx_coap_tm:handle_out(Msg, TM0) of
        #{out := Out, tm := TM1} ->
            Session1 = Session#session{transport_manager = TM1},
            {Out, maybe_inc_observe_inflight(TrackInflight, Session1)};
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
    BW,
    _Ctx,
    #session{
        observe_pending = Queue0,
        observe_pending_len = Len0,
        observe_pending_dropped = Dropped0
    } = Session
) when Len0 < ?OBSERVE_NOTIFICATION_QUEUE_MAX_LEN ->
    Session#session{
        observe_pending = queue:in({Msg, MQTT, BW}, Queue0),
        observe_pending_len = Len0 + 1,
        observe_pending_dropped = Dropped0
    };
enqueue_observe_notification(
    Msg,
    MQTT,
    BW,
    Ctx,
    #session{
        observe_pending = Queue0,
        observe_pending_dropped = Dropped0
    } = Session
) ->
    {{value, {_DroppedMsg, DroppedMQTT, _DroppedBW}}, Queue1} = queue:out(Queue0),
    metrics_inc('delivery.dropped', Ctx),
    metrics_inc('delivery.dropped.queue_full', Ctx),
    log_observe_notification_queue_full(DroppedMQTT),
    Session#session{
        observe_pending = queue:in({Msg, MQTT, BW}, Queue1),
        observe_pending_dropped = Dropped0 + 1
    }.

maybe_drain_observe_notifications(Result0, Session0) ->
    Done = maps:get(observe_notification_done, Result0, 0),
    Result1 = maps:remove(observe_notification_done, Result0),
    Session1 = release_observe_inflight(Done, Session0),
    drain_observe_notifications(Result1, Session1).

release_observe_inflight(0, Session) ->
    Session;
release_observe_inflight(Done, #session{observe_inflight = Inflight} = Session) ->
    Session#session{observe_inflight = max(0, Inflight - Done)}.

drain_observe_notifications(
    Result,
    #session{observe_inflight = Inflight} = Session
) when Inflight >= ?OBSERVE_INFLIGHT_WINDOW ->
    {Result, Session};
drain_observe_notifications(Result, #session{observe_pending_len = 0} = Session) ->
    {Result, Session};
drain_observe_notifications(
    Result,
    #session{observe_pending = Queue0, observe_pending_len = Len0} = Session0
) ->
    case queue:out(Queue0) of
        {{value, {Msg, _MQTT, BW} = Pending}, Queue1} ->
            Session1 = Session0#session{
                observe_pending = Queue1,
                observe_pending_len = Len0 - 1
            },
            TrackInflight = is_confirmable_observe_notification(Msg),
            case send_observe_notification_now(Msg, TrackInflight, Session1) of
                {[], Session2} ->
                    {Result, Session2#session{
                        observe_pending = queue:in_r(Pending, Queue1),
                        observe_pending_len = Len0
                    }};
                {Out, Session2} ->
                    Result1 = add_outs(Out, maybe_attach_pending_blockwise_result(Result, BW)),
                    drain_observe_notifications(Result1, Session2)
            end;
        {empty, _} ->
            {Result, Session0#session{observe_pending_len = 0}}
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
            {SeqId, OM2} = emqx_coap_observe_res:insert(SubData, OM),
            Replay = emqx_coap_message:piggyback({ok, content}, Msg),
            %% RFC 7641 Section 4.4: Observe option carries 24-bit sequence.
            Replay2 = Replay#coap_message{
                options = #{observe => emqx_coap_observe_res:observe_value(SeqId)}
            },
            Result#{
                reply => Replay2,
                session => Session#session{observe_manager = OM2}
            };
        Topic ->
            OM2 = emqx_coap_observe_res:remove(Topic, OM),
            Replay = emqx_coap_message:piggyback({ok, nocontent}, Msg),
            Session1 = purge_pending_observe_notifications(Topic, Session),
            Result#{
                reply => Replay,
                session => Session1#session{observe_manager = OM2}
            }
    end.

purge_pending_observe_notifications(
    Topic,
    #session{observe_pending = Queue0, observe_pending_len = Len0} = Session
) ->
    Pending0 = queue:to_list(Queue0),
    Pending = [Entry || Entry <- Pending0, not is_pending_observe_topic(Topic, Entry)],
    case length(Pending) of
        Len0 ->
            Session;
        Len ->
            Session#session{
                observe_pending = queue:from_list(Pending),
                observe_pending_len = Len
            }
    end.

is_pending_observe_topic(Topic, {_Msg, #message{topic = Topic}, _BW}) ->
    true;
is_pending_observe_topic(_Topic, _Pending) ->
    false.

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
map_notify_block2_prepare_result({chunked, Msg1, BW1}, _Msg, _PeerKey, Ctx) ->
    metrics_inc('blockwise.tx_block2.started', Ctx),
    {Msg1, BW1};
map_notify_block2_prepare_result({error, _ErrorReply, BW1}, Msg, PeerKey, _Ctx) ->
    ?SLOG(warning, #{
        msg => "coap_notify_block2_prepare_failed",
        peer_key => PeerKey
    }),
    {Msg, BW1}.
