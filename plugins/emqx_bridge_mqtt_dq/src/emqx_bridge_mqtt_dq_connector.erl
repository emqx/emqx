%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_connector).

-behaviour(gen_server).

-include("emqx_bridge_mqtt_dq.hrl").

-export([start_link/2, sync_metrics/1, status/1]).
-export([on_async_publish_ack/3]).

-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2]).

-define(MIN_RECONNECT_DELAY_MS, 1000).
-define(MAX_RECONNECT_DELAY_MS, 60000).
-define(DEFAULT_MAX_PUBLISH_RETRIES, infinity).
-define(MIN_DROP_LOG_INTERVAL_MS, 5000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(map(), non_neg_integer()) -> emqx_types:startlink_ret().
start_link(BridgeConfig, Index) ->
    gen_server:start_link(?MODULE, {BridgeConfig, Index}, []).

-spec sync_metrics(pid()) -> ok.
sync_metrics(Pid) ->
    gen_server:call(Pid, sync_metrics).

-spec status(pid()) -> map().
status(Pid) ->
    gen_server:call(Pid, status).

%% Called from emqtt process context when PUBACK/PUBREC is received
%% or when QoS 0 message is written to TCP send buffer.
on_async_publish_ack(ConnPid, SeqNo, Result) ->
    ConnPid ! {async_pub_ack, SeqNo, Result}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init({BridgeConfig, Index}) ->
    process_flag(trap_exit, true),
    #{
        name := BridgeName,
        server := Server,
        proto_ver := ProtoVer,
        clientid_prefix := ClientidPrefix,
        username := Username,
        password := Password,
        keepalive_s := Keepalive,
        max_inflight := MaxInflight,
        ssl := SslConf
    } = BridgeConfig,
    {Host, Port} = parse_server(Server),
    ClientId = make_clientid(ClientidPrefix, Index),
    ConnOpts = #{
        host => Host,
        port => Port,
        clientid => ClientId,
        proto_ver => ProtoVer,
        clean_start => true,
        keepalive => Keepalive,
        max_inflight => MaxInflight,
        force_ping => true,
        connect_timeout => 10
    },
    ConnOpts1 = maybe_add_credentials(ConnOpts, Username, Password),
    ConnOpts2 = maybe_add_ssl(ConnOpts1, Host, SslConf),
    MaxRetries = maps:get(max_publish_retries, BridgeConfig, ?DEFAULT_MAX_PUBLISH_RETRIES),
    State = #{
        bridge_name => BridgeName,
        index => Index,
        conn_opts => ConnOpts2,
        bridge_config => BridgeConfig,
        client_pid => undefined,
        connected => false,
        max_publish_retries => MaxRetries,
        reconnect_delay_ms => ?MIN_RECONNECT_DELAY_MS,
        reconnect_tref => undefined,
        %% Same max_inflight value is used for three things:
        %% 1. emqtt connection option (wire-level send window)
        %% 2. Connector inflight cap (items dispatched, awaiting ack)
        %% 3. Buffer batch pop size (items per flush)
        %% Keeping one knob simplifies tuning: the entire pipeline from
        %% disk queue → connector → emqtt → remote broker is aligned.
        max_inflight => MaxInflight,
        %% Monotonically increasing sequence number for items dispatched to
        %% emqtt. Used as the callback key so acks can be matched to items
        %% regardless of arrival order (QoS 0 acks immediately, QoS 1/2
        %% waits for PUBACK/PUBREC).
        next_seq => 0,
        %% Items waiting to be dispatched to emqtt. Appended when batches
        %% arrive from the buffer; drained into inflight up to max_inflight.
        %% Each entry: {SeqNo, BatchRef, Item, Retries}
        %% SeqNo is assigned when the item first enters backlog (or is
        %% re-queued for retry with a fresh SeqNo).
        backlog => queue:new(),
        backlog_len => 0,
        %% Items dispatched to emqtt, keyed by SeqNo. At most max_inflight
        %% entries. Only these items lose a retry credit on disconnect.
        %% SeqNo => {BatchRef, Item, Retries}
        inflight => #{},
        %% Batch completion tracking. Each buffer batch is registered here
        %% when received. Acked back to buffer when pending set is empty.
        %% BatchRef => #{from => pid(), pending => #{ItemSeqNo => []}}
        batches => #{}
    },
    self() ! reconnect,
    {ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(sync_metrics, _From, State) ->
    {reply, ok, update_metrics(State)};
handle_call(status, _From, State) ->
    {reply, #{connected => maps:get(connected, State, false)}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_info({publish_batch, Items, From, Ref}, State) ->
    {noreply, accept_batch(Items, From, Ref, State)};
handle_info({async_pub_ack, SeqNo, Result}, State) ->
    {noreply, handle_async_ack(SeqNo, Result, State)};
handle_info(reconnect, State) ->
    {noreply, do_connect(State#{reconnect_tref := undefined})};
handle_info({'EXIT', Pid, Reason}, #{client_pid := Pid} = State) ->
    #{bridge_name := BridgeName, index := Index} = State,
    ?LOG(warning, #{
        msg => "mqtt_dq_connector_down",
        bridge => BridgeName,
        index => Index,
        reason => Reason
    }),
    ok = incr_retry_reason(State, connection_lost, 1),
    State1 = on_client_exit(State),
    {noreply, schedule_reconnect(State1)};
handle_info({'EXIT', _Pid, _Reason}, State) ->
    %% Stale EXIT from a previously linked emqtt client after reconnect.
    %% Supervisor shutdown never reaches here — gen_server intercepts
    %% parent EXIT internally and calls terminate/2 directly.
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    cancel_reconnect_timer(State),
    ok = clear_metrics(State),
    maybe_disconnect(maps:get(client_pid, State, undefined)),
    ok.

%%--------------------------------------------------------------------
%% Internal: batch acceptance
%%--------------------------------------------------------------------

%% Accept a batch from the buffer: register in batches map, assign SeqNos,
%% append to backlog, then try to drain backlog into inflight.
%%
%% Backlog size is bounded by buffer-side flow control:
%% each buffer sends at most MAX_INFLIGHT_BATCHES (2) batches before
%% waiting for acks, each batch has at most max_inflight items.
%% With K buffers per connector (buffer_pool_size / pool_size),
%% the worst-case backlog is K * 2 * max_inflight items.
accept_batch(Items, From, Ref, State) ->
    {Entries, State1} = assign_seqnos(Items, Ref, State),
    SeqNos = [SeqNo || {SeqNo, _, _, _} <- Entries],
    Pending = maps:from_keys(SeqNos, []),
    #{batches := Batches, backlog := Backlog} = State1,
    Batches1 = Batches#{Ref => #{from => From, pending => Pending}},
    Backlog1 = lists:foldl(fun(E, Q) -> queue:in(E, Q) end, Backlog, Entries),
    BacklogLen1 = maps:get(backlog_len, State1, 0) + length(Entries),
    maybe_drain(
        update_metrics(State1#{batches := Batches1, backlog := Backlog1, backlog_len := BacklogLen1})
    ).

assign_seqnos(Items, BatchRef, #{next_seq := Seq, max_publish_retries := MaxRetries} = State) ->
    {Entries, Seq1} = lists:foldl(
        fun(Item, {Acc, S}) ->
            Entry = {S, BatchRef, Item, MaxRetries},
            {[Entry | Acc], S + 1}
        end,
        {[], Seq},
        Items
    ),
    {lists:reverse(Entries), State#{next_seq := Seq1}}.

%%--------------------------------------------------------------------
%% Internal: flow-controlled dispatch
%%--------------------------------------------------------------------

%% Drain backlog into inflight up to max_inflight, dispatching each item
%% to emqtt via publish_async.
maybe_drain(#{connected := false} = State) ->
    State;
maybe_drain(#{inflight := Inflight, max_inflight := Max} = State) when
    map_size(Inflight) >= Max
->
    State;
maybe_drain(#{backlog := Backlog, inflight := Inflight} = State) ->
    case queue:out(Backlog) of
        {empty, _} ->
            update_metrics(State);
        {{value, {SeqNo, BatchRef, Item, Retries}}, Backlog1} ->
            BacklogLen1 = maps:get(backlog_len, State, 0) - 1,
            State1 = State#{
                backlog := Backlog1,
                backlog_len := ensure_non_negative_backlog_len(BacklogLen1)
            },
            ok = dispatch_one(SeqNo, Item, State1),
            ok = maybe_sync_after_dispatch(SeqNo, State1),
            Inflight1 = Inflight#{SeqNo => {BatchRef, Item, Retries}},
            maybe_drain(update_metrics(State1#{inflight := Inflight1}))
    end.

dispatch_one(SeqNo, Item, State) ->
    #{
        client_pid := ClientPid,
        bridge_config := #{
            remote_topic := TopicTpl,
            remote_qos := QoSConf,
            remote_retain := RetainConf
        }
    } = State,
    #{
        topic := OrigTopic,
        payload := Payload,
        qos := OrigQoS,
        retain := OrigRetain,
        properties := Props
    } = Item,
    Topic = render_topic(TopicTpl, OrigTopic),
    QoS = resolve_qos(QoSConf, OrigQoS),
    Retain = resolve_retain(RetainConf, OrigRetain),
    PubOpts = [{qos, QoS}, {retain, Retain}],
    Callback = {fun ?MODULE:on_async_publish_ack/3, [self(), SeqNo]},
    ok = emqtt:publish_async(ClientPid, Topic, Props, Payload, PubOpts, infinity, Callback).

%% Make a sync gen_statem:call to emqtt so that all preceding async
%% messages in its mailbox are processed before we continue dispatching.
sync_barrier(#{client_pid := ClientPid}) ->
    try
        _ = emqtt:info(ClientPid, n_queued),
        ok
    catch
        _:_ -> ok
    end.

maybe_sync_after_dispatch(SeqNo, #{max_inflight := Max} = State) when
    is_integer(Max), Max > 0, ((SeqNo + 1) rem Max) =:= 0
->
    %% Periodically make a sync gen_statem:call to emqtt so preceding
    %% async publish messages in its mailbox are processed before we keep
    %% dispatching, which limits mailbox growth for fast async acks.
    sync_barrier(State);
maybe_sync_after_dispatch(_SeqNo, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal: async ack handling
%%--------------------------------------------------------------------

handle_async_ack(SeqNo, Result, #{inflight := Inflight} = State) ->
    case maps:get(SeqNo, Inflight, undefined) of
        {BatchRef, Item, Retries} ->
            State1 = handle_item_result(SeqNo, BatchRef, Item, Retries, Result, State),
            maybe_drain(State1);
        undefined ->
            %% Stale ack for an item already salvaged to backlog; ignore.
            State
    end.

handle_item_result(SeqNo, BatchRef, Item, Retries, Result, State) ->
    case should_retry(Result) of
        false ->
            #{bridge_name := BridgeName} = State,
            ok = emqx_bridge_mqtt_dq_metrics:incr_bridge_publish(BridgeName, 1),
            finish_item(SeqNo, BatchRef, remove_inflight(SeqNo, State));
        true ->
            case should_retry_again(Retries) of
                true ->
                    %% Keep the item inflight and let on_client_exit/1 salvage it,
                    %% so retry accounting is centralized in one place.
                    ok = incr_retry_reason(State, normalize_retry_reason(Result), 1),
                    maybe_log_reason_code(Result, Item, retry, State),
                    disconnect_and_reconnect(Result, State);
                false ->
                    #{bridge_name := BridgeName} = State,
                    ok = emqx_bridge_mqtt_dq_metrics:incr_bridge_drop(BridgeName, 1),
                    maybe_log_reason_code(Result, Item, drop, State),
                    log_drop(State, classify_error(Result)),
                    finish_item(SeqNo, BatchRef, remove_inflight(SeqNo, State))
            end
    end.

disconnect_and_reconnect(Result, State) ->
    #{bridge_name := BridgeName, index := Index} = State,
    ?LOG(warning, #{
        msg => "mqtt_dq_connector_puback_error",
        bridge => BridgeName,
        index => Index,
        reason => classify_error(Result)
    }),
    maybe_disconnect(maps:get(client_pid, State, undefined)),
    State1 = on_client_exit(State),
    schedule_reconnect(State1).

should_retry(ok) -> false;
should_retry({ok, #{reason_code := RC}}) when RC =:= 0; RC =:= 16 -> false;
should_retry(_) -> true.

should_retry_again(infinity) -> true;
should_retry_again(Retries) when is_integer(Retries) -> Retries > 1.

%% Item delivered or dropped — remove from batch pending set.
finish_item(SeqNo, BatchRef, #{batches := Batches} = State) ->
    case maps:find(BatchRef, Batches) of
        {ok, #{pending := Pending} = Ctx} ->
            Pending1 = maps:remove(SeqNo, Pending),
            check_batch_complete(BatchRef, Ctx#{pending := Pending1}, State);
        error ->
            %% Batch already completed (shouldn't happen)
            State
    end.

remove_inflight(SeqNo, #{inflight := Inflight} = State) ->
    update_metrics(State#{
        inflight := maps:remove(SeqNo, Inflight)
    }).

%% Batch is complete when pending set is empty (map_size/1 is O(1)).
check_batch_complete(BatchRef, #{pending := Pending, from := From}, State) when
    map_size(Pending) =:= 0
->
    #{batches := Batches, bridge_name := BridgeName} = State,
    From ! {batch_ack, BatchRef, ok},
    ?tp(mqtt_dq_batch_complete, #{bridge => BridgeName}),
    State#{batches := maps:remove(BatchRef, Batches)};
check_batch_complete(BatchRef, Ctx, #{batches := Batches} = State) ->
    State#{batches := Batches#{BatchRef := Ctx}}.

classify_error({ok, #{reason_code := RC}}) -> {reason_code, RC};
classify_error({error, Reason}) -> Reason;
classify_error(Other) -> Other.

maybe_log_reason_code({ok, #{reason_code := RC}}, Item, Action, State) ->
    log_reason_code(State, Item, RC, Action);
maybe_log_reason_code(_Result, _Item, _Action, _State) ->
    ok.

log_reason_code(
    #{bridge_name := BridgeName, index := Index, bridge_config := #{remote_topic := TopicTpl}},
    #{topic := SourceTopic},
    RC,
    Action
) ->
    RemoteTopic = render_topic(TopicTpl, SourceTopic),
    ?LOG(warning, #{
        msg => "mqtt_dq_publish_reason_code",
        bridge => BridgeName,
        index => Index,
        action => Action,
        reason_code => RC,
        source_topic => SourceTopic,
        remote_topic => RemoteTopic
    }).

%% Rate-limited drop logging to avoid log storms during sustained failures.
log_drop(State, Reason) ->
    log_drop(State, Reason, 1).

log_drop(#{bridge_name := BridgeName}, Reason, Count) ->
    Key = {?MODULE, drop_log},
    Now = erlang:monotonic_time(millisecond),
    {LastTs, AccDropped} =
        case get(Key) of
            undefined -> {0, 0};
            V -> V
        end,
    NewAcc = AccDropped + Count,
    case Now - LastTs > ?MIN_DROP_LOG_INTERVAL_MS of
        true ->
            ?LOG(warning, #{
                msg => "mqtt_dq_publish_dropped",
                bridge => BridgeName,
                dropped => NewAcc,
                reason => Reason
            }),
            put(Key, {Now, 0});
        false ->
            put(Key, {LastTs, NewAcc})
    end.

%%--------------------------------------------------------------------
%% Internal: disconnect / reconnect
%%--------------------------------------------------------------------

%% Called when emqtt exits.
%% Inflight items lose one retry credit and move to backlog head.
%% Backlog items are untouched — they never reached emqtt.
on_client_exit(#{inflight := Inflight, backlog := Backlog, batches := Batches} = State) ->
    %% Sort by SeqNo to preserve dispatch order when prepending to backlog.
    Sorted = lists:keysort(1, maps:to_list(Inflight)),
    {Backlog1, Batches1, DropCount, RequeuedCount} = lists:foldr(
        fun({SeqNo, {BatchRef, Item, Retries}}, {QAcc, BAcc, Drops, Requeued}) ->
            Retries1 = decrement_retries_on_disconnect(Retries),
            case Retries1 of
                0 ->
                    %% Exhausted — drop item, remove from batch pending
                    BAcc1 = remove_from_batch_pending(SeqNo, BatchRef, BAcc),
                    {QAcc, BAcc1, Drops + 1, Requeued};
                _ ->
                    %% Re-queue keeping the same SeqNo
                    {queue:in_r({SeqNo, BatchRef, Item, Retries1}, QAcc), BAcc, Drops, Requeued + 1}
            end
        end,
        {Backlog, Batches, 0, 0},
        Sorted
    ),
    case DropCount of
        0 ->
            ok;
        _ ->
            #{bridge_name := BridgeName} = State,
            ok = emqx_bridge_mqtt_dq_metrics:incr_bridge_drop(BridgeName, DropCount),
            log_drop(State, retries_exhausted, DropCount)
    end,
    %% Check if any batches completed due to dropped items
    Batches2 = flush_completed_batches(Batches1),
    BacklogLen1 = maps:get(backlog_len, State, 0) + RequeuedCount,
    update_metrics(State#{
        client_pid := undefined,
        connected := false,
        inflight := #{},
        backlog := Backlog1,
        backlog_len := BacklogLen1,
        batches := Batches2
    }).

decrement_retries_on_disconnect(infinity) -> infinity;
decrement_retries_on_disconnect(Retries) when is_integer(Retries) -> max(0, Retries - 1).

remove_from_batch_pending(SeqNo, BatchRef, Batches) ->
    case maps:find(BatchRef, Batches) of
        {ok, #{pending := Pending} = Ctx} ->
            Batches#{BatchRef := Ctx#{pending := maps:remove(SeqNo, Pending)}};
        error ->
            Batches
    end.

flush_completed_batches(Batches) ->
    maps:fold(
        fun(Ref, #{pending := Pending, from := From} = Ctx, Acc) ->
            case map_size(Pending) of
                0 ->
                    From ! {batch_ack, Ref, ok},
                    Acc;
                _ ->
                    Acc#{Ref => Ctx}
            end
        end,
        #{},
        Batches
    ).

do_connect(#{conn_opts := ConnOpts, bridge_name := BridgeName, index := Index} = State) ->
    maybe_disconnect(maps:get(client_pid, State, undefined)),
    case start_and_connect(ConnOpts) of
        {ok, Pid} ->
            ?LOG(info, #{
                msg => "mqtt_dq_connector_connected",
                bridge => BridgeName,
                index => Index
            }),
            ?tp(mqtt_dq_connector_connected, #{bridge => BridgeName, index => Index}),
            State1 = reset_reconnect_backoff(State#{client_pid := Pid, connected := true}),
            State2 = update_metrics(State1),
            maybe_drain(State2);
        {error, Reason} ->
            ?LOG(warning, #{
                msg => "mqtt_dq_connector_connect_failed",
                bridge => BridgeName,
                index => Index,
                reason => Reason
            }),
            ?tp(mqtt_dq_connector_connect_failed, #{bridge => BridgeName, index => Index}),
            ok = incr_retry_reason(State, connect_failed, 1),
            State1 = update_metrics(State#{client_pid := undefined, connected := false}),
            schedule_reconnect(State1)
    end.

render_topic(<<"${topic}">>, OrigTopic) ->
    OrigTopic;
render_topic(Template, OrigTopic) ->
    binary:replace(Template, <<"${topic}">>, OrigTopic, [global]).

resolve_qos('${qos}', OrigQoS) -> OrigQoS;
resolve_qos(QoS, _OrigQoS) -> QoS.

resolve_retain('${retain}', OrigRetain) -> OrigRetain;
resolve_retain(Retain, _OrigRetain) -> Retain.

%%--------------------------------------------------------------------
%% Internal: connection helpers
%%--------------------------------------------------------------------

start_and_connect(ConnOpts) ->
    try
        do_start_and_connect(ConnOpts)
    catch
        Class:Error -> {error, {Class, Error}}
    end.

do_start_and_connect(ConnOpts) ->
    case emqtt:start_link(ConnOpts) of
        {ok, Pid} -> try_connect(Pid);
        {error, Reason} -> {error, Reason}
    end.

try_connect(Pid) ->
    case emqtt:connect(Pid) of
        {ok, _Props} ->
            {ok, Pid};
        {error, Reason} ->
            catch emqtt:stop(Pid),
            {error, Reason}
    end.

schedule_reconnect(#{reconnect_delay_ms := DelayMs} = State0) ->
    cancel_reconnect_timer(State0),
    TRef = erlang:send_after(DelayMs, self(), reconnect),
    State0#{
        reconnect_tref := TRef,
        reconnect_delay_ms := min(DelayMs * 2, ?MAX_RECONNECT_DELAY_MS)
    }.

reset_reconnect_backoff(State0) ->
    cancel_reconnect_timer(State0),
    State0#{reconnect_tref := undefined, reconnect_delay_ms := ?MIN_RECONNECT_DELAY_MS}.

cancel_reconnect_timer(#{reconnect_tref := undefined}) ->
    ok;
cancel_reconnect_timer(#{reconnect_tref := TRef}) ->
    _ = erlang:cancel_timer(TRef),
    ok.

maybe_disconnect(undefined) ->
    ok;
maybe_disconnect(Pid) when is_pid(Pid) ->
    exit(Pid, kill),
    ok.

incr_retry_reason(#{bridge_name := BridgeName}, Reason, Count) ->
    emqx_bridge_mqtt_dq_metrics:incr_bridge_retried(BridgeName, Reason, Count).

parse_server(Server) when is_list(Server) ->
    parse_server(list_to_binary(Server));
parse_server(Server) when is_binary(Server) ->
    case binary:split(Server, <<":">>) of
        [Host, PortBin] ->
            {binary_to_list(Host), binary_to_integer(PortBin)};
        [Host] ->
            {binary_to_list(Host), 1883}
    end.

make_clientid(Prefix, Index) ->
    NodeHash = node_hash(),
    iolist_to_binary([Prefix, NodeHash, "-", integer_to_binary(Index)]).

node_hash() ->
    <<Int:32>> = binary:part(crypto:hash(sha, atom_to_binary(node())), 0, 4),
    string:lowercase(integer_to_binary(Int, 16)).

maybe_add_credentials(Opts, <<>>, _) ->
    Opts;
maybe_add_credentials(Opts, Username, Password) ->
    Opts#{username => Username, password => Password}.

maybe_add_ssl(Opts, Host, #{enable := true} = SslConf) ->
    SslOpts = maybe_add_verify(
        maybe_add_cert_files(maybe_add_sni([], Host, SslConf), SslConf), SslConf
    ),
    Opts#{ssl => true, ssl_opts => SslOpts};
maybe_add_ssl(Opts, _Host, _) ->
    Opts.

maybe_add_sni(SslOpts, _Host, #{server_name_indication := disable}) ->
    [{server_name_indication, disable} | SslOpts];
maybe_add_sni(SslOpts, _Host, #{server_name_indication := SNI}) when is_list(SNI), SNI =/= [] ->
    [{server_name_indication, SNI} | SslOpts];
maybe_add_sni(SslOpts, Host, _) ->
    [{server_name_indication, Host} | SslOpts].

maybe_add_cert_files(SslOpts, #{cacertfile := Cacertfile, certfile := Certfile, keyfile := Keyfile}) ->
    add_ssl_opt_if_set(
        keyfile,
        Keyfile,
        add_ssl_opt_if_set(certfile, Certfile, add_ssl_opt_if_set(cacertfile, Cacertfile, SslOpts))
    );
maybe_add_cert_files(SslOpts, _) ->
    SslOpts.

add_ssl_opt_if_set(_Key, undefined, SslOpts) ->
    SslOpts;
add_ssl_opt_if_set(Key, Value, SslOpts) ->
    [{Key, Value} | SslOpts].

maybe_add_verify(SslOpts, #{verify := Verify}) ->
    [{verify, Verify} | SslOpts];
maybe_add_verify(SslOpts, _) ->
    [{verify, verify_none} | SslOpts].

update_metrics(
    #{bridge_name := BridgeName, index := Index, backlog_len := BacklogLen, inflight := Inflight} =
        State
) ->
    ok = emqx_bridge_mqtt_dq_metrics:set_connector_backlog(BridgeName, Index, BacklogLen),
    ok = emqx_bridge_mqtt_dq_metrics:set_connector_connected(
        BridgeName, Index, bool_to_int(maps:get(connected, State, false))
    ),
    ok = emqx_bridge_mqtt_dq_metrics:set_connector_inflight(BridgeName, Index, map_size(Inflight)),
    State.

clear_metrics(#{bridge_name := BridgeName, index := Index}) ->
    ok = emqx_bridge_mqtt_dq_metrics:set_connector_backlog(BridgeName, Index, 0),
    ok = emqx_bridge_mqtt_dq_metrics:set_connector_connected(BridgeName, Index, 0),
    ok = emqx_bridge_mqtt_dq_metrics:set_connector_inflight(BridgeName, Index, 0).

normalize_retry_reason({ok, #{reason_code := _RC}}) ->
    reason_code;
normalize_retry_reason({error, timeout}) ->
    timeout;
normalize_retry_reason({error, _Reason}) ->
    connect_failed;
normalize_retry_reason(timeout) ->
    timeout;
normalize_retry_reason(_Other) ->
    other.

bool_to_int(true) ->
    1;
bool_to_int(false) ->
    0.

ensure_non_negative_backlog_len(BacklogLen) when BacklogLen >= 0 ->
    BacklogLen;
ensure_non_negative_backlog_len(BacklogLen) ->
    error({invalid_backlog_len, BacklogLen}).
