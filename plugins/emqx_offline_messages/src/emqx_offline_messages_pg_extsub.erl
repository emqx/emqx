%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_offline_messages_pg_extsub).
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
    cursor = 0,
    %% Latest completed offset.
    acked = 0,
    %% Last checkpointed offset.
    durable = 0,
    %% Database catch-up is required.
    dirty = true,
    %% Live copies above `cursor + 1`, by offset.
    pocket = #{},
    reorder_timer,
    last_activity = 0,
    last_write = 0,
    %% `{Ref, initialize | {page, ReservedCount}, Timer}`.
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
    case emqx_offline_messages_pg:managed(Topic, Config) andalso not emqx_topic:wildcard(Topic) of
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

handle_unsubscribe(unsubscribe, _Context, Handler, _TopicFilter) ->
    #h{config = #{namespace := Namespace} = Config, clientid = ClientId, topic = Topic} = Handler,
    Operation = {unsubscribe, Namespace, ClientId, Topic},
    log_error(unsubscribe, emqx_omp_pg_store:call(omp_pg_read, ClientId, Operation, Config)),
    Handler#h{stopped = true};
handle_unsubscribe(disconnect, _Context, Handler0, _TopicFilter) ->
    Handler = checkpoint(Handler0),
    Handler#h{stopped = true}.

handle_terminate(#h{pending = Pending} = Handler) ->
    cancel(Handler#h.reorder_timer),
    cancel(Handler#h.flush_timer),
    cancel(Handler#h.retry_timer),
    cancel(Handler#h.idle_timer),
    case Pending of
        {_Ref, _Kind, Timer} -> cancel(Timer);
        undefined -> ok
    end,
    case Handler of
        #h{stopped = false} -> _ = checkpoint(Handler);
        _ -> ok
    end,
    ok.

handle_delivered(Handler0, _Context, Msg, Ack) ->
    #h{config = #{namespace := Namespace, checkpoint := #{max_pending := Max}}, topic_id = Id} =
        Handler0,
    {Namespace, Id, Offset} = emqx_message:get_header(?OMP_POSITION, Msg),
    report_ack(Ack),
    Handler1 = Handler0#h{acked = max(Handler0#h.acked, Offset)},
    Handler =
        case pending_completions(Handler1) >= Max of
            true -> checkpoint(Handler1);
            false -> Handler1
        end,
    {ok, nudge(Handler)}.

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

info(
    #h{topic_id = Id, config = #{namespace := Namespace}} = Handler0,
    Context,
    {generic, ?OMP_LIVE(Namespace, Id, Offset, Msg)}
) when Id =/= undefined ->
    case live(Offset, Msg, Context, Handler0) of
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
    {ok, flush(Handler)};
info(#h{config = #{read := #{idle_poll_interval := Interval}}} = Handler0, _Context, pg_idle) ->
    #h{dirty = Dirty, last_activity = Last, idle_jitter = Jitter} = Handler0,
    Handler = Handler0#h{
        dirty = Dirty orelse now_ms() - Last >= Interval - Jitter, idle_jitter = 0
    },
    {ok, nudge(idle_timer(Handler))};
info(#h{reorder_timer = {_, Token}} = Handler, _Context, {pg_reorder, Token}) ->
    {ok, nudge(Handler#h{dirty = true, reorder_timer = undefined})};
info(#h{pending = {Ref, Kind, _Timer}} = Handler, _Context, {pg_timeout, Ref}) ->
    {ok, retry(failed(Kind, Handler#h{pending = undefined}))};
info(#h{pending = {Ref, Kind, Timer}} = Handler, _Context, {pg_result, Ref, Result}) ->
    cancel(Timer),
    result(Handler#h{pending = undefined}, Kind, Result);
info(Handler, _Context, _Info) ->
    {ok, Handler}.

result(Handler, initialize, {ok, #{topic_id := Id, cursor := Cursor}}) ->
    #h{config = #{read := #{idle_poll_interval := Interval}}} = Handler,
    Now = now_ms(),
    Init = Handler#h{
        %% Spread the polls of handlers that start together after a mass reconnect.
        idle_jitter = rand:uniform(max(1, Interval div 2)) - 1,
        topic_id = Id,
        cursor = Cursor,
        acked = Cursor,
        durable = Cursor,
        last_activity = Now,
        last_write = Now,
        retry_delay = undefined
    },
    {ok, nudge(idle_timer(flush_timer(Init)))};
result(Handler0, {page, _Count}, {ok, [_ | _] = Rows}) ->
    {Handler1, Messages0} = lists:foldl(fun row/2, {Handler0, []}, Rows),
    Tip = element(1, hd(Rows)),
    Cursor =
        case Rows of
            [{_, null, _, _, _, _, _, _, _, _}] -> Tip;
            _ -> Handler1#h.cursor
        end,
    Pocket = maps:filter(fun(Offset, _) -> Offset > Cursor end, Handler1#h.pocket),
    Handler2 = Handler1#h{
        cursor = Cursor, pocket = Pocket, last_activity = now_ms(), retry_delay = undefined
    },
    {Handler3, Messages} = drain(Handler2, Messages0),
    Handler = reorder_timer(Handler3#h{dirty = Handler3#h.dirty orelse Handler3#h.cursor < Tip}),
    ?tp(omp_pg_page, #{
        clientid => Handler#h.clientid,
        topic => Handler#h.topic,
        cursor => Handler#h.cursor,
        dirty => Handler#h.dirty
    }),
    {ok, nudge(idle_timer(Handler)), lists:reverse(Messages)};
result(Handler, Kind, Error) ->
    log_error(Kind, Error),
    {ok, retry(failed(Kind, Handler))}.

failed(initialize, Handler) -> Handler;
failed({page, _}, Handler) -> Handler#h{dirty = true}.

live(Offset, _Msg, _Context, #h{cursor = Cursor, pocket = Pocket} = Handler) when
    Offset =< Cursor; is_map_key(Offset, Pocket)
->
    {Handler, []};
live(Offset, Msg0, Context, Handler0) ->
    #h{config = #{read := #{reorder_max_distance := Distance}}, cursor = Cursor, pocket = Pocket} =
        Handler0,
    Msg = message(Offset, Msg0, Handler0),
    Capacity = capacity(Handler0, Context),
    case {Capacity > 0, Offset =:= Cursor + 1 andalso Handler0#h.pending =:= undefined} of
        {true, true} ->
            {Handler, Messages} = drain(
                Handler0#h{cursor = Offset, last_activity = now_ms()}, [Msg]
            ),
            {reorder_timer(Handler), lists:reverse(Messages)};
        {false, _} ->
            %% No capacity: drop this copy and recover it from the database.
            {Handler0#h{dirty = true}, []};
        {true, false} when Offset =< Cursor + Distance, map_size(Pocket) < Distance ->
            Handler = Handler0#h{pocket = Pocket#{Offset => Msg}, last_activity = now_ms()},
            {reorder_timer(Handler), []};
        {true, false} ->
            {clear_pocket(Handler0#h{dirty = true}), []}
    end.

message(Offset, Msg, #h{config = #{namespace := Namespace}, topic = Topic, topic_id = Id}) ->
    Msg1 = emqx_message:set_headers(
        #{sub_topic => Topic, ?OMP_POSITION => {Namespace, Id, Offset}}, Msg
    ),
    Msg1#message{qos = ?QOS_1}.

drain(#h{cursor = Cursor, pocket = Pocket0} = Handler, Messages) ->
    case maps:take(Cursor + 1, Pocket0) of
        {Msg, Pocket} -> drain(Handler#h{cursor = Cursor + 1, pocket = Pocket}, [Msg | Messages]);
        error -> {Handler, Messages}
    end.

reorder_timer(#h{pocket = Pocket} = Handler) when map_size(Pocket) =:= 0 ->
    clear_pocket(Handler);
reorder_timer(
    #h{
        reorder_timer = undefined,
        config = #{read := #{reorder_timeout := Timeout}},
        target = Target
    } = Handler
) ->
    Token = make_ref(),
    Timer = emqx_extsub_handler:send_after(Timeout, Target, {pg_reorder, Token}),
    Handler#h{reorder_timer = {Timer, Token}};
reorder_timer(Handler) ->
    Handler.

clear_pocket(#h{reorder_timer = Timer} = Handler) ->
    cancel(Timer),
    Handler#h{pocket = #{}, reorder_timer = undefined}.

pump(#h{retry_timer = Timer} = Handler, _Context) when Timer =/= undefined -> Handler;
pump(#h{pending = Pending} = Handler, _Context) when Pending =/= undefined -> Handler;
pump(#h{topic_id = undefined} = Handler, _Context) ->
    #h{config = Config, topic = Topic, clientid = ClientId, subopts = SubOpts} = Handler,
    #{namespace := Namespace, new_subscription_start := Start} = Config,
    request(
        Handler,
        ClientId,
        initialize,
        {subscribe, Namespace, ClientId, Topic, SubOpts, Start}
    );
pump(#h{dirty = true} = Handler, Context) ->
    read_page(Handler, Context);
pump(Handler, _Context) ->
    Handler.

read_page(Handler0, Context) ->
    %% Release live copies when they occupy all capacity needed for recovery.
    Handler =
        case capacity(Handler0, Context) =< 0 of
            true -> clear_pocket(Handler0);
            false -> Handler0
        end,
    case capacity(Handler, Context) of
        Capacity when Capacity > 0 ->
            #h{config = #{read := #{page_max_messages := PageSize}}, topic_id = Id, key = Key} =
                Handler,
            Count = min(Capacity, PageSize),
            request(
                Handler#h{dirty = false},
                Key,
                {page, Count},
                {page, Id, Handler#h.cursor, Count}
            );
        _ ->
            Handler
    end.

capacity(
    #h{config = #{read := #{window := Window}}, pocket = Pocket, pending = Pending},
    #{delivering_count := Delivering}
) ->
    Reserved =
        case Pending of
            {_, {page, Count}, _} -> Count;
            _ -> 0
        end,
    Window - Delivering - map_size(Pocket) - Reserved.

request(#h{target = Target, config = #{query_timeout := Timeout}} = Handler, Key, Kind, Operation) ->
    Ref = make_ref(),
    ok = emqx_omp_pg_store:async(omp_pg_read, Key, Operation, Target, Ref),
    Timer = emqx_extsub_handler:send_after(
        Timeout + ?REQUEST_TIMEOUT_MARGIN, Target, {pg_timeout, Ref}
    ),
    Handler#h{pending = {Ref, Kind, Timer}}.

row({_, null, _, _, _, _, _, _, _, _}, Acc) ->
    Acc;
row({_, Offset, _, _, _, _, _, _, _, Expiry} = Row, {Handler0, Messages}) ->
    Handler = Handler0#h{cursor = Offset},
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

flush(#h{config = #{subscription_expiry := Expiry}, last_write = LastWrite} = Handler0) ->
    Renew = now_ms() - LastWrite >= Expiry div 3,
    Handler =
        case pending_completions(Handler0) > 0 orelse Renew of
            true -> checkpoint(Handler0);
            false -> Handler0
        end,
    flush_timer(Handler).

flush_timer(#h{config = #{checkpoint := #{interval := Interval}}, target = Target} = Handler) ->
    Handler#h{flush_timer = emqx_extsub_handler:send_after(Interval, Target, pg_flush)}.

idle_timer(
    #h{
        config = #{read := #{idle_poll_interval := Interval}},
        target = Target,
        idle_timer = Timer,
        idle_jitter = Jitter
    } = Handler
) ->
    cancel(Timer),
    Delay =
        case Handler of
            #h{dirty = false, pending = undefined, last_activity = Last} ->
                max(1, Last + Interval - Jitter - now_ms());
            _ ->
                Interval - Jitter
        end,
    Handler#h{idle_timer = emqx_extsub_handler:send_after(Delay, Target, pg_idle)}.

checkpoint(#h{topic_id = undefined} = Handler) ->
    Handler;
checkpoint(
    #h{config = #{namespace := Namespace}, clientid = ClientId, topic_id = Id, acked = Acked} =
        Handler
) ->
    ok = emqx_omp_pg_store:cast(
        omp_pg_read, ClientId, {checkpoint, Namespace, ClientId, Id, Acked}
    ),
    Handler#h{durable = Acked, last_write = now_ms()}.

pending_completions(#h{acked = Acked, durable = Durable}) ->
    Acked - Durable.

nudge(#h{scheduled = true} = Handler) ->
    Handler;
nudge(#h{dirty = true, target = Target} = Handler) ->
    emqx_extsub_handler:send(Target, pg_tick),
    Handler#h{scheduled = true};
nudge(Handler) ->
    Handler.

now_ms() -> erlang:monotonic_time(millisecond).

%% Back off exponentially up to the idle poll interval, with jitter of +-50%.
retry(#h{retry_timer = undefined, target = Target, retry_delay = Previous} = Handler) ->
    #h{config = #{read := #{idle_poll_interval := Interval}}} = Handler,
    Delay =
        case Previous of
            undefined -> min(?RETRY_INTERVAL, Interval);
            _ -> min(2 * Previous, Interval)
        end,
    Jittered = Delay div 2 + rand:uniform(Delay),
    Handler#h{
        retry_delay = Delay,
        retry_timer = emqx_extsub_handler:send_after(Jittered, Target, pg_retry)
    };
retry(Handler) ->
    Handler.

cancel(undefined) ->
    ok;
cancel({Ref, _Token}) ->
    cancel(Ref);
cancel(Ref) ->
    _ = erlang:cancel_timer(Ref),
    ok.

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
