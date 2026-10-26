%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_pg_extsub).
-moduledoc "Ordered QoS 1 delivery of live copies with database recovery.".
-behaviour(emqx_extsub_handler).
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_omp_pg.hrl").

-export([
    handle_subscribe/4, handle_unsubscribe/4, handle_terminate/1, handle_info/3, handle_delivered/4
]).

-define(RETRY_INTERVAL, 1000).
-define(REQUEST_TIMEOUT_MARGIN, 5000).

-record(h, {
    config,
    target,
    key,
    topic,
    clientid,
    subopts,
    topic_id,
    %% Last offset passed to ExtSub or skipped.
    offset_cursor = 0,
    %% Latest acknowledged offset. A negative acknowledgement or a session drop also counts.
    offset_acked = 0,
    %% Latest acknowledged offset sent to the database as a checkpoint. The write is asynchronous.
    offset_sent_to_commit = 0,
    %% Database catch-up is required.
    dirty = true,
    %% Live copies above `offset_cursor + 1`, by offset.
    reorder_buffer = #{},
    reorder_timer,
    last_activity = 0,
    %% Time when the last checkpoint was sent. Each checkpoint also renews the subscription row.
    checkpoint_sent_at = 0,
    %% Keeps reference to the current async read request (at most one may be pending).
    %% `{Ref, initialize | page, Timer}`.
    pending,
    flush_timer,
    retry_timer,
    retry_delay,
    idle_timer,
    %% Shortens the first idle poll interval after initialization by a random amount.
    idle_jitter = 0,
    scheduled = false,
    stopped = false
}).

%%------------------------------------------------------------------------------
%% Handler callbacks
%%------------------------------------------------------------------------------

-doc "Create a reader for an exact managed QoS 1 subscription that reports acknowledgements.".
handle_subscribe(_Type, #{can_receive_acks := false}, _State, _Topic) ->
    ignore;
handle_subscribe(_Type, _Context, _State, #share{}) ->
    ignore;
handle_subscribe(_Type, #{subopts := #{qos := QoS}}, _State, _Topic) when QoS =/= ?QOS_1 ->
    %% Authorization denies these subscriptions. Subscriptions that skip authorization
    %% receive no managed messages.
    ignore;
handle_subscribe(_Type, Context, undefined, Topic) ->
    #{message_target := Target, clientinfo := #{clientid := ClientId}, subopts := Options} =
        Context,
    Config = emqx_offline_messages_pg:config(),
    case
        emqx_offline_messages_pg:is_managed(Topic, Config) andalso not emqx_topic:wildcard(Topic)
    of
        true ->
            Handler = #h{
                config = Config,
                target = Target,
                key = make_ref(),
                topic = Topic,
                clientid = ClientId,
                subopts = maps:with([qos, nl, rap, rh, subid], Options)
            },
            {ok, nudge(Handler)};
        false ->
            ignore
    end.

-doc "Remove explicit subscriptions or save acknowledged progress before disconnecting.".
handle_unsubscribe(unsubscribe, _Context, Handler, _TopicFilter) ->
    #h{config = #{namespace := Namespace} = Config, clientid = ClientId, topic = Topic} = Handler,
    log_error(unsubscribe, emqx_omp_pg_store:unsubscribe(Namespace, ClientId, Topic, Config)),
    Handler#h{stopped = true};
handle_unsubscribe(disconnect, _Context, Handler0, _TopicFilter) ->
    Handler = checkpoint(Handler0),
    Handler#h{stopped = true}.

-doc "Cancel the reader timers and save progress unless an unsubscribe stopped the reader.".
handle_terminate(#h{pending = Pending} = Handler) ->
    _ = clear_reorder_buffer(Handler),
    emqx_utils:cancel_timer(Handler#h.flush_timer),
    emqx_utils:cancel_timer(Handler#h.retry_timer),
    emqx_utils:cancel_timer(Handler#h.idle_timer),
    case Pending of
        {_Ref, _Kind, Timer} -> emqx_utils:cancel_timer(Timer);
        undefined -> ok
    end,
    case Handler of
        #h{stopped = false} -> _ = checkpoint(Handler);
        _ -> ok
    end,
    ok.

-doc "Record an acknowledged offset and save progress when enough acknowledgements are unsent.".
handle_delivered(Handler0, _Context, Msg, Ack) ->
    #h{config = #{namespace := Namespace, checkpoint := #{max_pending := Max}}, topic_id = Id} =
        Handler0,
    {Namespace, Id, Offset} = emqx_message:get_header(?OMP_POSITION, Msg),
    report_ack(Ack),
    Handler1 = Handler0#h{offset_acked = max(Handler0#h.offset_acked, Offset)},
    Handler =
        case unsent_ack_count(Handler1) >= Max of
            true -> checkpoint(Handler1);
            false -> Handler1
        end,
    {ok, nudge(Handler)}.

-doc "Recreate the reader after a configuration change and ignore events after it stops.".
handle_info(#h{stopped = true} = Handler, _Context, _Info) ->
    {ok, Handler};
handle_info(#h{config = #{instance := Instance}} = Handler, Context, Info) ->
    case emqx_offline_messages_pg:config() of
        #{instance := Instance} -> info(Handler, Context, Info);
        undefined -> destroy;
        _ -> recreate
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

-doc "Process live batches, timer events and matching database replies.".
info(
    #h{topic_id = Id, config = #{namespace := Namespace}} = Handler0,
    Context,
    {generic, ?OMP_LIVE(Namespace, Id, Offset, Batch)}
) when Id =/= undefined ->
    #{desired_message_count := Demand} = Context,
    case live_batch(Offset, Batch, Demand, Handler0, []) of
        {Handler, []} -> {ok, nudge(Handler)};
        {Handler, Messages} -> {ok, nudge(Handler), Messages}
    end;
info(Handler, _Context, {generic, _Info}) ->
    {ok, Handler};
info(Handler, Context, pg_tick) ->
    {ok, pump(Handler#h{scheduled = false}, Context)};
info(Handler, Context, pg_retry) ->
    {ok, pump(Handler#h{retry_timer = undefined}, Context)};
info(Handler, _Context, pg_flush) ->
    {ok, flush(Handler#h{flush_timer = undefined})};
info(#h{config = #{read := #{idle_poll_interval := Interval}}} = Handler0, _Context, pg_idle) ->
    #h{dirty = Dirty, last_activity = Last, idle_jitter = Jitter} = Handler0,
    Handler = Handler0#h{
        dirty = Dirty orelse now_ms() - Last >= Interval - Jitter,
        idle_jitter = 0
    },
    {ok, nudge(ensure_idle_timer(Handler))};
info(#h{reorder_timer = {_, Token}} = Handler, _Context, {pg_reorder, Token}) ->
    {ok, nudge(Handler#h{dirty = true, reorder_timer = undefined})};
info(#h{pending = {Ref, Kind, _Timer}} = Handler, _Context, {pg_timeout, Ref}) ->
    {ok, retry(failed(Kind, Handler#h{pending = undefined}))};
info(#h{pending = {Ref, Kind, Timer}} = Handler, _Context, {pg_result, Ref, Result}) ->
    emqx_utils:cancel_timer(Timer),
    result(Handler#h{pending = undefined}, Kind, Result);
info(Handler, _Context, _Info) ->
    {ok, Handler}.

-doc "Apply a database result, release consecutive buffered copies and schedule more reads.".
result(Handler, initialize, {ok, #{topic_id := Id, cursor := Cursor}}) ->
    #h{config = #{read := #{idle_poll_interval := Interval}}} = Handler,
    Now = now_ms(),
    Init = Handler#h{
        %% Spread the polls of handlers that start together after a mass reconnect.
        idle_jitter = rand:uniform(max(1, Interval div 2)) - 1,
        topic_id = Id,
        offset_cursor = Cursor,
        offset_acked = Cursor,
        offset_sent_to_commit = Cursor,
        last_activity = Now,
        checkpoint_sent_at = Now,
        retry_delay = undefined
    },
    {ok, nudge(ensure_idle_timer(ensure_flush_timer(Init)))};
result(Handler0, page, {ok, [_ | _] = Rows}) ->
    %% Decode the rows and advance the cursor over them.
    {Handler1, Messages0} = lists:foldl(fun row/2, {Handler0, []}, Rows),
    Tip = element(1, hd(Rows)),
    %% A single row without a message means that no stored message follows the cursor.
    Cursor =
        case Rows of
            [{_, null, _, _, _, _, _, _, _, _}] -> Tip;
            _ -> Handler1#h.offset_cursor
        end,
    %% Drop buffered live copies that the page already returned.
    ReorderBuffer = maps:filter(fun(Offset, _) -> Offset > Cursor end, Handler1#h.reorder_buffer),
    Handler2 = Handler1#h{
        offset_cursor = Cursor,
        reorder_buffer = ReorderBuffer,
        last_activity = now_ms(),
        retry_delay = undefined
    },
    %% Release consecutive buffered copies and read again while the cursor is below the tip.
    {Handler3, Messages} = drain(Handler2, Messages0),
    Handler = ensure_reorder_timer(Handler3#h{
        dirty = Handler3#h.dirty orelse Handler3#h.offset_cursor < Tip
    }),
    ?tp(omp_pg_page, #{
        clientid => Handler#h.clientid,
        topic => Handler#h.topic,
        cursor => Handler#h.offset_cursor,
        dirty => Handler#h.dirty
    }),
    {ok, nudge(ensure_idle_timer(Handler)), lists:reverse(Messages)};
result(Handler, Kind, Error) ->
    log_error(Kind, Error),
    {ok, retry(failed(Kind, Handler))}.

failed(initialize, Handler) -> Handler;
failed(page, Handler) -> Handler#h{dirty = true}.

-doc "Reduce the remaining ExtSub demand by the messages returned earlier in this batch.".
live_batch(_Offset, [], _Demand, Handler, Acc) ->
    {Handler, lists:reverse(Acc)};
live_batch(Offset, [Msg | Rest], Demand, Handler0, Acc) ->
    {Handler, Messages} = live(Offset, Msg, Demand, Handler0),
    live_batch(
        Offset + 1, Rest, max(0, Demand - length(Messages)), Handler, lists:reverse(Messages, Acc)
    ).

-doc "Deliver consecutive copies, buffer small gaps and recover dropped copies from storage.".
live(
    Offset, _Msg, _Demand, #h{offset_cursor = Cursor, reorder_buffer = ReorderBuffer} = Handler
) when
    Offset =< Cursor; is_map_key(Offset, ReorderBuffer)
->
    {Handler, []};
live(Offset, Msg0, Demand, Handler0) ->
    #h{
        config = #{read := #{reorder_max_distance := Distance}},
        offset_cursor = Cursor,
        reorder_buffer = ReorderBuffer
    } =
        Handler0,
    Msg = message(Offset, Msg0, Handler0),
    case {Demand > 0, Offset =:= Cursor + 1 andalso Handler0#h.pending =:= undefined} of
        {true, true} ->
            %% The next offset: deliver it and the buffered copies that follow it.
            {Handler, Messages} = drain(
                Handler0#h{offset_cursor = Offset, last_activity = now_ms()}, [Msg]
            ),
            {ensure_reorder_timer(Handler), lists:reverse(Messages)};
        {false, _} ->
            %% No ExtSub demand: drop this copy and recover it from the database.
            {Handler0#h{dirty = true}, []};
        {true, false} when Offset =< Cursor + Distance, map_size(ReorderBuffer) < Distance ->
            %% A small gap or a pending read: keep the copy until the missing offsets arrive.
            Handler = Handler0#h{
                reorder_buffer = ReorderBuffer#{Offset => Msg}, last_activity = now_ms()
            },
            {ensure_reorder_timer(Handler), []};
        {true, false} ->
            %% A large gap: drop the buffered copies and read the messages from the database.
            {clear_reorder_buffer(Handler0#h{dirty = true}), []}
    end.

message(Offset, Msg, #h{config = #{namespace := Namespace}, topic = Topic, topic_id = Id}) ->
    Msg1 = emqx_message:set_headers(
        #{sub_topic => Topic, ?OMP_POSITION => {Namespace, Id, Offset}}, Msg
    ),
    Msg1#message{qos = ?QOS_1}.

-doc "Release buffered copies while their offsets follow the current cursor without a gap.".
drain(#h{offset_cursor = Cursor, reorder_buffer = ReorderBuffer0} = Handler, Messages) ->
    case maps:take(Cursor + 1, ReorderBuffer0) of
        {Msg, ReorderBuffer} ->
            drain(Handler#h{offset_cursor = Cursor + 1, reorder_buffer = ReorderBuffer}, [
                Msg | Messages
            ]);
        error ->
            {Handler, Messages}
    end.

-doc "Start one gap timer while the reorder buffer contains messages.".
ensure_reorder_timer(#h{reorder_buffer = ReorderBuffer} = Handler) when
    map_size(ReorderBuffer) =:= 0
->
    clear_reorder_buffer(Handler);
ensure_reorder_timer(
    #h{
        reorder_timer = undefined,
        config = #{read := #{reorder_timeout := Timeout}},
        target = Target
    } = Handler
) ->
    Token = make_ref(),
    Timer = emqx_extsub_handler:send_after(Timeout, Target, {pg_reorder, Token}),
    Handler#h{reorder_timer = {Timer, Token}};
ensure_reorder_timer(Handler) ->
    Handler.

clear_reorder_buffer(#h{reorder_timer = {Timer, _Token}} = Handler) ->
    emqx_utils:cancel_timer(Timer),
    Handler#h{reorder_buffer = #{}, reorder_timer = undefined};
clear_reorder_buffer(Handler) ->
    Handler#h{reorder_buffer = #{}, reorder_timer = undefined}.

-doc "Start initialization or recovery only when no request or retry is pending.".
pump(#h{retry_timer = Timer} = Handler, _Context) when Timer =/= undefined -> Handler;
pump(#h{pending = Pending} = Handler, _Context) when Pending =/= undefined -> Handler;
pump(#h{topic_id = undefined} = Handler, _Context) ->
    #h{config = Config, topic = Topic, clientid = ClientId, subopts = SubOpts} = Handler,
    #{namespace := Namespace, new_subscription_start := Start} = Config,
    request(Handler, initialize, fun(Target, Ref) ->
        emqx_omp_pg_store:subscribe_async(Namespace, ClientId, Topic, SubOpts, Start, Target, Ref)
    end);
pump(#h{dirty = true} = Handler, Context) ->
    read_page(Handler, Context);
pump(Handler, _Context) ->
    Handler.

-doc "Read a page no larger than the ExtSub demand. Without demand, wait for an acknowledgement.".
read_page(#h{config = #{read := #{page_max_messages := PageSize}}} = Handler, Context) ->
    case Context of
        #{desired_message_count := Demand} when Demand > 0 ->
            #h{topic_id = Id, key = Key, offset_cursor = Cursor} = Handler,
            Count = min(Demand, PageSize),
            request(Handler#h{dirty = false}, page, fun(Target, Ref) ->
                emqx_omp_pg_store:page_async(Key, Id, Cursor, Count, Target, Ref)
            end);
        _ ->
            Handler
    end.

-doc "Send an asynchronous database request with a timeout for missing pool callbacks.".
request(#h{target = Target, config = #{query_timeout := Timeout}} = Handler, Kind, Send) ->
    Ref = make_ref(),
    ok = Send(Target, Ref),
    Timer = emqx_extsub_handler:send_after(
        Timeout + ?REQUEST_TIMEOUT_MARGIN, Target, {pg_timeout, Ref}
    ),
    Handler#h{pending = {Ref, Kind, Timer}}.

-doc "Advance over each stored row and return only valid messages that have not expired.".
row({_, null, _, _, _, _, _, _, _, _}, Acc) ->
    Acc;
row({_, Offset, _, _, _, _, _, _, _, Expiry} = Row, {Handler0, Messages}) ->
    Handler = Handler0#h{offset_cursor = Offset},
    #h{config = #{namespace := Namespace}, topic = Topic, topic_id = Id} = Handler,
    case emqx_omp_pg_store:decode(Topic, {Namespace, Id}, Row) of
        {ok, Msg} ->
            case Expiry =/= null andalso Expiry =< erlang:system_time(millisecond) of
                true -> {Handler, Messages};
                false -> {Handler, [Msg | Messages]}
            end;
        {error, Reason} ->
            emqx_offline_messages_pg:count(skipped, 1),
            ?SLOG(error, #{
                msg => offline_messages_pg_decode_failed,
                topic => Topic,
                offset => Offset,
                reason => Reason
            }),
            {Handler, Messages}
    end.

-doc "Save unsent acknowledgements or renew an active subscription before its expiry.".
flush(#h{config = #{subscription_expiry := Expiry}, checkpoint_sent_at = SentAt} = Handler0) ->
    Renew = now_ms() - SentAt >= Expiry div 3,
    Handler =
        case unsent_ack_count(Handler0) > 0 orelse Renew of
            true -> checkpoint(Handler0);
            false -> Handler0
        end,
    ensure_flush_timer(Handler).

ensure_flush_timer(
    #h{flush_timer = undefined, config = #{checkpoint := #{interval := Interval}}, target = Target} =
        Handler
) ->
    Handler#h{flush_timer = emqx_extsub_handler:send_after(Interval, Target, pg_flush)};
ensure_flush_timer(Handler) ->
    Handler.

-doc "Schedule the next idle poll from recent activity and the initial reconnect jitter.".
ensure_idle_timer(
    #h{
        config = #{read := #{idle_poll_interval := Interval}},
        target = Target,
        idle_timer = Timer,
        idle_jitter = Jitter
    } = Handler
) ->
    emqx_utils:cancel_timer(Timer),
    Delay =
        case Handler of
            #h{dirty = false, pending = undefined, last_activity = Last} ->
                max(1, Last + Interval - Jitter - now_ms());
            _ ->
                Interval - Jitter
        end,
    Handler#h{idle_timer = emqx_extsub_handler:send_after(Delay, Target, pg_idle)}.

-doc "Send acknowledged progress to the client's database worker without blocking delivery.".
checkpoint(#h{topic_id = undefined} = Handler) ->
    Handler;
checkpoint(
    #h{
        config = #{namespace := Namespace},
        clientid = ClientId,
        topic_id = Id,
        offset_acked = Acked
    } =
        Handler
) ->
    ok = emqx_omp_pg_store:checkpoint(Namespace, ClientId, Id, Acked),
    Handler#h{offset_sent_to_commit = Acked, checkpoint_sent_at = now_ms()}.

unsent_ack_count(#h{offset_acked = Acked, offset_sent_to_commit = Sent}) ->
    Acked - Sent.

nudge(#h{scheduled = true} = Handler) ->
    Handler;
nudge(#h{dirty = true, target = Target} = Handler) ->
    emqx_extsub_handler:send(Target, pg_tick),
    Handler#h{scheduled = true};
nudge(Handler) ->
    Handler.

now_ms() -> erlang:monotonic_time(millisecond).

-doc "Back off exponentially up to the idle poll interval, with jitter of +-50%.".
retry(#h{retry_timer = undefined, target = Target, retry_delay = Previous} = Handler) ->
    #h{config = #{read := #{idle_poll_interval := Interval}}} = Handler,
    Delay =
        case Previous of
            undefined -> min(?RETRY_INTERVAL, Interval);
            _ -> min(2 * Previous, Interval)
        end,
    Jittered = Delay div 2 + rand:uniform(Delay),
    ?tp(omp_pg_retry, #{timeout => Jittered}),
    Handler#h{
        retry_delay = Delay,
        retry_timer = emqx_extsub_handler:send_after(Jittered, Target, pg_retry)
    };
retry(Handler) ->
    Handler.

report_ack(Ack) when is_integer(Ack), Ack >= 16#80 ->
    emqx_offline_messages_pg:count(rejected, 1),
    ?SLOG_THROTTLE(warning, #{msg => offline_messages_pg_rejected, reason_code => Ack});
report_ack(_) ->
    ok.

log_error(_Operation, {ok, _}) ->
    ok;
log_error(Operation, Error) ->
    ?SLOG_THROTTLE(error, #{
        msg => offline_messages_pg_query_failed, operation => Operation, reason => Error
    }).
