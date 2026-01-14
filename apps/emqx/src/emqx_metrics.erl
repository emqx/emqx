%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_metrics).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").
-include("emqx_mqtt.hrl").
-include("emqx_config.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    start_link/0,
    stop/0
]).

-export([
    new/1,
    new/2,
    ensure/1,
    ensure/2
]).

%% RPC targets (`emqx_proto_v{1..2}`).
-export([all/0]).
%% RPC targets (`emqx_proto_v3`).
-export([all_v3/1]).

-export([
    all_global/0,
    val_global/1,
    inc_global/1,
    inc_global/2,
    dec_global/1,
    dec_global/2,
    set_global/2
]).

-export([register_namespace/1, unregister_namespace/1]).
-export([all_ns/0]).

-export([
    all/1,
    val/2,
    inc/2,
    inc/3,
    dec/2,
    dec/3,
    set/3
]).

-export([inc_safe/2, inc_safe/3]).

%% Inc received/sent metrics
-export([
    inc_msg/1,
    inc_recv/2,
    inc_sent/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% {Type, Name, Description} for API schema definition.
-export([all_metrics/0]).

-export([is_olp_metric/1]).

-export_type([metric_idx/0, metric_name/0]).

-compile({inline, [inc_global/1, inc_global/2, dec_global/1, dec_global/2]}).
-compile({inline, [inc/2, inc/3, dec/2, dec/3]}).
-compile({inline, [inc_recv/2, inc_sent/2]}).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(MAX_SIZE, 256).
-define(RESERVED_IDX, 128).

-opaque metric_idx() :: 1..?MAX_SIZE.

-type metric_name() :: atom() | string() | binary().

-type maybe_namespace() :: emqx_config:maybe_namespace().

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

-define(PT_KEY(NS), {?MODULE, cref, NS}).

-record(state, {next_idx = 1}).
-record(metric, {name, type, idx}).

%% Calls/casts/infos
-record(register_namespace, {namespace :: binary()}).
-record(unregister_namespace, {namespace :: binary()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start the metrics server.
-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    try
        gen_server:stop(?SERVER)
    catch
        exit:R when R =:= noproc orelse R =:= timeout ->
            %% pid is killed after timeout
            ok
    end.

%%--------------------------------------------------------------------
%% Metrics API
%%--------------------------------------------------------------------

is_olp_metric(Name) ->
    lists:keymember(Name, 2, olp_metrics()).

-spec new(metric_name()) -> ok.
new(Name) ->
    new(counter, Name).

-spec new(gauge | counter, metric_name()) -> ok.
new(gauge, Name) ->
    create(gauge, Name);
new(counter, Name) ->
    create(counter, Name).

-spec ensure(metric_name()) -> ok.
ensure(Name) ->
    ensure(counter, Name).

-spec ensure(gauge | counter, metric_name()) -> ok.
ensure(Type, Name) when Type =:= gauge; Type =:= counter ->
    case ets:lookup(?TAB, Name) of
        [] -> create(Type, Name);
        _ -> ok
    end.

%% @private
create(Type, Name) ->
    case gen_server:call(?SERVER, {create, Type, Name}) of
        {ok, _Idx} -> ok;
        {error, Reason} -> error(Reason)
    end.

%% RPC target (`emqx_proto_v{1..2}`).
all() ->
    all_global().

%% @doc Get all metrics
-spec all_global() -> [{metric_name(), non_neg_integer()}].
all_global() ->
    all(?global_ns).

-spec all(maybe_namespace()) -> [{metric_name(), non_neg_integer()}].
all(Namespace) ->
    try
        CRef = get_counters_ref(Namespace),
        [
            {Name, counters:get(CRef, Idx)}
         || #metric{name = Name, idx = Idx} <- ets:tab2list(?TAB)
        ]
    catch
        error:badarg ->
            []
    end.

%% RPC target (`emqx_proto_v3`).
all_v3(Namespace) ->
    all(Namespace).

-spec all_ns() -> #{maybe_namespace() => [{metric_name(), non_neg_integer()}]}.
all_ns() ->
    RegisteredMetrics = ets:tab2list(?TAB),
    lists:foldl(
        fun
            ({?PT_KEY(Namespace), CRef}, Acc) ->
                Metrics = [
                    {Name, counters:get(CRef, Idx)}
                 || #metric{name = Name, idx = Idx} <- RegisteredMetrics
                ],
                Acc#{Namespace => Metrics};
            (_, Acc) ->
                Acc
        end,
        #{},
        persistent_term:get()
    ).

%% @doc Get metric value
-spec val_global(metric_name()) -> non_neg_integer().
val_global(Name) ->
    val(?global_ns, Name).

-spec val(maybe_namespace(), metric_name()) -> non_neg_integer().
val(Namespace, Name) ->
    try
        case ets:lookup(?TAB, Name) of
            [#metric{idx = Idx}] ->
                CRef = get_counters_ref(Namespace),
                counters:get(CRef, Idx);
            [] ->
                0
        end
    catch
        error:badarg ->
            %% When application is restarting to join a new cluster, ETS table might be
            %% temporarily absent.
            0
    end.

%% @doc Increase counter
-spec inc_global(metric_name()) -> ok.
inc_global(Name) ->
    inc_global(Name, 1).

%% @doc Increase metric value
-spec inc_global(metric_name(), pos_integer()) -> ok.
inc_global(Name, Value) ->
    update_counter(?global_ns, Name, Value).

-spec inc(maybe_namespace(), metric_name()) -> ok.
inc(Namespace, Name) ->
    inc(Namespace, Name, 1).

-spec inc(maybe_namespace(), metric_name(), pos_integer()) -> ok.
inc(Namespace, Name, Value) ->
    update_counter(Namespace, Name, Value).

-spec inc_safe(maybe_namespace(), metric_name()) -> ok | {error, not_found}.
inc_safe(Namespace, Name) ->
    inc_safe(Namespace, Name, 1).

-spec inc_safe(maybe_namespace(), metric_name(), pos_integer()) -> ok | {error, not_found}.
inc_safe(Namespace, Name, Value) ->
    try
        inc(Namespace, Name, Value)
    catch
        error:badarg ->
            {error, not_found}
    end.

%% @doc Decrease metric value
-spec dec_global(metric_name()) -> ok.
dec_global(Name) ->
    dec_global(Name, 1).

%% @doc Decrease metric value
-spec dec_global(metric_name(), pos_integer()) -> ok.
dec_global(Name, Value) ->
    update_counter(?global_ns, Name, -Value).

-spec dec(maybe_namespace(), metric_name()) -> ok.
dec(Namespace, Name) ->
    dec(Namespace, Name, 1).

-spec dec(maybe_namespace(), metric_name(), pos_integer()) -> ok.
dec(Namespace, Name, Value) ->
    update_counter(Namespace, Name, -Value).

%% @doc Set metric value
-spec set_global(metric_name(), integer()) -> ok.
set_global(Name, Value) ->
    set(?global_ns, Name, Value).

-spec set(maybe_namespace(), metric_name(), integer()) -> ok.
set(Namespace, Name, Value) ->
    CRef = get_counters_ref(Namespace),
    Idx = ets:lookup_element(?TAB, Name, 4),
    counters:put(CRef, Idx, Value).

update_counter(Namespace, Name, Value) ->
    CRef = get_counters_ref(Namespace),
    CIdx =
        case reserved_idx(Name) of
            Idx when is_integer(Idx) -> Idx;
            undefined -> ets:lookup_element(?TAB, Name, 4)
        end,
    counters:add(CRef, CIdx, Value).

register_namespace(Namespace) when is_binary(Namespace) ->
    gen_server:call(?MODULE, #register_namespace{namespace = Namespace}, infinity).

unregister_namespace(Namespace) when is_binary(Namespace) ->
    gen_server:call(?MODULE, #unregister_namespace{namespace = Namespace}, infinity).

%%--------------------------------------------------------------------
%% Inc received/sent metrics
%%--------------------------------------------------------------------

-spec inc_msg(emqx_types:message()) -> ok.
inc_msg(Msg) ->
    case Msg#message.qos of
        0 -> inc_global('messages.qos0.received');
        1 -> inc_global('messages.qos1.received');
        2 -> inc_global('messages.qos2.received')
    end,
    inc_global('messages.received').

%% @doc Inc packets received.
-spec inc_recv(emqx_types:packet(), emqx_config:maybe_namespace()) -> ok.
inc_recv(Packet, Namespace) ->
    inc(?global_ns, 'packets.received'),
    do_inc_recv(Packet, ?global_ns),
    try
        case is_binary(Namespace) of
            true ->
                inc(Namespace, 'packets.received'),
                do_inc_recv(Packet, Namespace);
            false ->
                ok
        end
    catch
        error:badarg ->
            %% Not an explicitly managed namespace or race condition while creating or
            %% deleting namespace.
            ok
    end.

do_inc_recv(?PACKET(?CONNECT), Namespace) ->
    inc(Namespace, 'packets.connect.received');
do_inc_recv(?PUBLISH_PACKET(QoS), Namespace) ->
    inc(Namespace, 'messages.received'),
    case QoS of
        ?QOS_0 -> inc(Namespace, 'messages.qos0.received');
        ?QOS_1 -> inc(Namespace, 'messages.qos1.received');
        ?QOS_2 -> inc(Namespace, 'messages.qos2.received');
        _other -> ok
    end,
    inc(Namespace, 'packets.publish.received');
do_inc_recv(?PACKET(?PUBACK), Namespace) ->
    inc(Namespace, 'packets.puback.received');
do_inc_recv(?PACKET(?PUBREC), Namespace) ->
    inc(Namespace, 'packets.pubrec.received');
do_inc_recv(?PACKET(?PUBREL), Namespace) ->
    inc(Namespace, 'packets.pubrel.received');
do_inc_recv(?PACKET(?PUBCOMP), Namespace) ->
    inc(Namespace, 'packets.pubcomp.received');
do_inc_recv(?PACKET(?SUBSCRIBE), Namespace) ->
    inc(Namespace, 'packets.subscribe.received');
do_inc_recv(?PACKET(?UNSUBSCRIBE), Namespace) ->
    inc(Namespace, 'packets.unsubscribe.received');
do_inc_recv(?PACKET(?PINGREQ), Namespace) ->
    inc(Namespace, 'packets.pingreq.received');
do_inc_recv(?PACKET(?DISCONNECT), Namespace) ->
    inc(Namespace, 'packets.disconnect.received');
do_inc_recv(?PACKET(?AUTH), Namespace) ->
    inc(Namespace, 'packets.auth.received');
do_inc_recv(_Packet, _Namespace) ->
    ok.

%% @doc Inc packets sent. Will not count $SYS PUBLISH.
-spec inc_sent(emqx_types:packet(), emqx_config:maybe_namespace()) -> ok.
inc_sent(?PUBLISH_PACKET(_QoS, <<"$SYS/", _/binary>>, _, _), _Namespace) ->
    ok;
inc_sent(Packet, Namespace) ->
    inc(?global_ns, 'packets.sent'),
    do_inc_sent(Packet, ?global_ns),
    try
        case is_binary(Namespace) of
            true ->
                inc(Namespace, 'packets.sent'),
                do_inc_sent(Packet, Namespace);
            false ->
                ok
        end
    catch
        error:badarg ->
            %% Not an explicitly managed namespace or race condition while creating or
            %% deleting namespace.
            ok
    end.

do_inc_sent(?CONNACK_PACKET(ReasonCode, _SessPresent), Namespace) ->
    (ReasonCode == ?RC_SUCCESS) orelse inc(Namespace, 'packets.connack.error'),
    ((ReasonCode == ?RC_NOT_AUTHORIZED) orelse
        (ReasonCode == ?CONNACK_AUTH)) andalso
        inc(Namespace, 'packets.connack.auth_error'),
    ((ReasonCode == ?RC_BAD_USER_NAME_OR_PASSWORD) orelse
        (ReasonCode == ?CONNACK_CREDENTIALS)) andalso
        inc(Namespace, 'packets.connack.auth_error'),
    inc(Namespace, 'packets.connack.sent');
do_inc_sent(?PUBLISH_PACKET(QoS), Namespace) ->
    inc(Namespace, 'messages.sent'),
    case QoS of
        ?QOS_0 -> inc(Namespace, 'messages.qos0.sent');
        ?QOS_1 -> inc(Namespace, 'messages.qos1.sent');
        ?QOS_2 -> inc(Namespace, 'messages.qos2.sent');
        _other -> ok
    end,
    inc(Namespace, 'packets.publish.sent');
do_inc_sent(?PUBACK_PACKET(_PacketId, ReasonCode), Namespace) ->
    (ReasonCode >= ?RC_UNSPECIFIED_ERROR) andalso inc(Namespace, 'packets.publish.error'),
    (ReasonCode == ?RC_NOT_AUTHORIZED) andalso inc(Namespace, 'packets.publish.auth_error'),
    inc(Namespace, 'packets.puback.sent');
do_inc_sent(?PUBREC_PACKET(_PacketId, ReasonCode), Namespace) ->
    (ReasonCode >= ?RC_UNSPECIFIED_ERROR) andalso inc(Namespace, 'packets.publish.error'),
    (ReasonCode == ?RC_NOT_AUTHORIZED) andalso inc(Namespace, 'packets.publish.auth_error'),
    inc(Namespace, 'packets.pubrec.sent');
do_inc_sent(?PACKET(?PUBREL), Namespace) ->
    inc(Namespace, 'packets.pubrel.sent');
do_inc_sent(?PACKET(?PUBCOMP), Namespace) ->
    inc(Namespace, 'packets.pubcomp.sent');
do_inc_sent(?SUBACK_PACKET(_PacketId, ReasonCodes), Namespace) ->
    lists:any(fun(Code) -> Code >= ?RC_UNSPECIFIED_ERROR end, ReasonCodes) andalso
        inc(Namespace, 'packets.subscribe.error'),
    lists:member(?RC_NOT_AUTHORIZED, ReasonCodes) andalso
        inc(Namespace, 'packets.subscribe.auth_error'),
    inc(Namespace, 'packets.suback.sent');
do_inc_sent(?PACKET(?UNSUBACK), Namespace) ->
    inc(Namespace, 'packets.unsuback.sent');
do_inc_sent(?PACKET(?PINGRESP), Namespace) ->
    inc(Namespace, 'packets.pingresp.sent');
do_inc_sent(?PACKET(?DISCONNECT), Namespace) ->
    inc(Namespace, 'packets.disconnect.sent');
do_inc_sent(?PACKET(?AUTH), Namespace) ->
    inc(Namespace, 'packets.auth.sent');
do_inc_sent(_Packet, _Namespace) ->
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    % Create counters array
    CRef = counters:new(?MAX_SIZE, [write_concurrency]),
    ok = persistent_term:put(?PT_KEY(?global_ns), CRef),
    % Create index mapping table
    ok = emqx_utils_ets:new(?TAB, [{keypos, 2}, {read_concurrency, true}]),
    Metrics = all_metrics(),
    initialize_counters(CRef, Metrics),
    {ok, #state{next_idx = ?RESERVED_IDX + 1}, hibernate}.

handle_call({create, Type, Name}, _From, State = #state{next_idx = ?MAX_SIZE}) ->
    ?SLOG(error, #{
        msg => "failed_to_create_type_name_for_index_exceeded",
        type => Type,
        name => Name
    }),
    {reply, {error, metric_index_exceeded}, State};
handle_call({create, Type, Name}, _From, State = #state{next_idx = NextIdx}) ->
    case ets:lookup(?TAB, Name) of
        [#metric{idx = Idx}] ->
            ?SLOG(info, #{msg => "name_already_exists", name => Name}),
            {reply, {ok, Idx}, State};
        [] ->
            Metric = #metric{name = Name, type = Type, idx = NextIdx},
            true = ets:insert(?TAB, Metric),
            {reply, {ok, NextIdx}, State#state{next_idx = NextIdx + 1}}
    end;
handle_call({set_type_to_counter, Keys}, _From, State) ->
    lists:foreach(
        fun(K) ->
            ets:update_element(?TAB, K, {#metric.type, counter})
        end,
        Keys
    ),
    {reply, ok, State};
handle_call(#register_namespace{namespace = Namespace}, _From, State) ->
    case persistent_term:get(?PT_KEY(Namespace), undefined) of
        undefined ->
            CRef = counters:new(?MAX_SIZE, [write_concurrency]),
            ok = persistent_term:put(?PT_KEY(Namespace), CRef),
            Metrics = all_metrics(),
            initialize_counters(CRef, Metrics),
            ok;
        _CRef ->
            ok
    end,
    {reply, ok, State};
handle_call(#unregister_namespace{namespace = Namespace}, _From, State) ->
    _ = persistent_term:erase(?PT_KEY(Namespace)),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", req => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

reserved_idx('bytes.received') -> 1;
reserved_idx('bytes.sent') -> 2;
reserved_idx('packets.received') -> 3;
reserved_idx('packets.sent') -> 4;
reserved_idx('packets.connect.received') -> 5;
reserved_idx('packets.connack.sent') -> 6;
reserved_idx('packets.connack.error') -> 7;
reserved_idx('packets.connack.auth_error') -> 8;
reserved_idx('packets.publish.received') -> 9;
reserved_idx('packets.publish.sent') -> 10;
reserved_idx('packets.publish.inuse') -> 11;
reserved_idx('packets.publish.error') -> 12;
reserved_idx('packets.publish.auth_error') -> 13;
reserved_idx('packets.puback.received') -> 14;
reserved_idx('packets.puback.sent') -> 15;
reserved_idx('packets.puback.inuse') -> 16;
reserved_idx('packets.puback.missed') -> 17;
reserved_idx('packets.pubrec.received') -> 18;
reserved_idx('packets.pubrec.sent') -> 19;
reserved_idx('packets.pubrec.inuse') -> 20;
reserved_idx('packets.pubrec.missed') -> 21;
reserved_idx('packets.pubrel.received') -> 22;
reserved_idx('packets.pubrel.sent') -> 23;
reserved_idx('packets.pubrel.missed') -> 24;
reserved_idx('packets.pubcomp.received') -> 25;
reserved_idx('packets.pubcomp.sent') -> 26;
reserved_idx('packets.pubcomp.inuse') -> 27;
reserved_idx('packets.pubcomp.missed') -> 28;
reserved_idx('packets.subscribe.received') -> 29;
reserved_idx('packets.subscribe.error') -> 30;
reserved_idx('packets.subscribe.auth_error') -> 31;
reserved_idx('packets.suback.sent') -> 32;
reserved_idx('packets.unsubscribe.received') -> 33;
reserved_idx('packets.unsubscribe.error') -> 34;
reserved_idx('packets.unsuback.sent') -> 35;
reserved_idx('packets.pingreq.received') -> 36;
reserved_idx('packets.pingresp.sent') -> 37;
reserved_idx('packets.disconnect.received') -> 38;
reserved_idx('packets.disconnect.sent') -> 39;
reserved_idx('packets.auth.received') -> 40;
reserved_idx('packets.auth.sent') -> 41;
reserved_idx('packets.publish.quota_exceeded') -> 42;
reserved_idx('messages.received') -> 43;
reserved_idx('messages.sent') -> 44;
reserved_idx('messages.qos0.received') -> 45;
reserved_idx('messages.qos0.sent') -> 46;
reserved_idx('messages.qos1.received') -> 47;
reserved_idx('messages.qos1.sent') -> 48;
reserved_idx('messages.qos2.received') -> 49;
reserved_idx('messages.qos2.sent') -> 50;
reserved_idx('messages.publish') -> 51;
reserved_idx('messages.dropped') -> 52;
reserved_idx('messages.dropped.await_pubrel_timeout') -> 53;
reserved_idx('messages.dropped.no_subscribers') -> 54;
reserved_idx('messages.forward') -> 55;
reserved_idx('messages.dropped.quota_exceeded') -> 56;
reserved_idx('messages.delayed') -> 57;
reserved_idx('messages.delivered') -> 58;
reserved_idx('messages.acked') -> 59;
reserved_idx('messages.dropped.receive_maximum') -> 60;
reserved_idx('delivery.dropped') -> 61;
reserved_idx('delivery.dropped.no_local') -> 62;
reserved_idx('delivery.dropped.too_large') -> 63;
reserved_idx('delivery.dropped.qos0_msg') -> 64;
reserved_idx('delivery.dropped.queue_full') -> 65;
reserved_idx('delivery.dropped.expired') -> 66;
reserved_idx('client.connect') -> 67;
reserved_idx('client.connack') -> 68;
reserved_idx('client.connected') -> 69;
reserved_idx('client.authenticate') -> 70;
reserved_idx('client.ping') -> 71;
reserved_idx('client.auth.anonymous') -> 72;
reserved_idx('client.authorize') -> 73;
reserved_idx('client.subscribe') -> 74;
reserved_idx('client.unsubscribe') -> 75;
reserved_idx('client.disconnected') -> 76;
reserved_idx('session.created') -> 77;
reserved_idx('session.resumed') -> 78;
reserved_idx('session.takenover') -> 79;
reserved_idx('session.discarded') -> 80;
reserved_idx('session.terminated') -> 81;
reserved_idx('session.disconnected') -> 82;
reserved_idx('authorization.allow') -> 83;
reserved_idx('authorization.deny') -> 84;
reserved_idx('authorization.cache_hit') -> 85;
reserved_idx('authorization.cache_miss') -> 86;
reserved_idx('authentication.success') -> 87;
reserved_idx('authentication.success.anonymous') -> 88;
reserved_idx('authentication.failure') -> 89;
reserved_idx('overload_protection.delay.ok') -> 90;
reserved_idx('overload_protection.delay.timeout') -> 91;
reserved_idx('overload_protection.hibernation') -> 92;
reserved_idx('overload_protection.gc') -> 93;
reserved_idx('overload_protection.new_conn') -> 94;
reserved_idx('messages.validation_succeeded') -> 95;
reserved_idx('messages.validation_failed') -> 96;
reserved_idx('messages.persisted') -> 97;
reserved_idx('messages.transformation_succeeded') -> 98;
reserved_idx('messages.transformation_failed') -> 99;
reserved_idx('rules.matched') -> 100;
reserved_idx('actions.executed') -> 101;
reserved_idx(_) -> undefined.

all_metrics() ->
    lists:append([
        bytes_metrics(),
        packet_metrics(),
        message_metrics(),
        delivery_metrics(),
        client_metrics(),
        session_metrics(),
        stats_acl_metrics(),
        stats_authn_metrics(),
        olp_metrics(),
        data_integration_metrics()
    ]).

%% Bytes sent and received
bytes_metrics() ->
    [
        {counter, 'bytes.received', ?DESC("bytes_received")},
        {counter, 'bytes.sent', ?DESC("bytes_sent")}
    ].

%% Packets sent and received
packet_metrics() ->
    [
        {counter, 'packets.received', ?DESC("packets_received")},
        {counter, 'packets.sent', ?DESC("packets_sent")},
        {counter, 'packets.connect.received', ?DESC("packets_connect_received")},
        {counter, 'packets.connack.sent', ?DESC("packets_connack_sent")},
        {counter, 'packets.connack.error', ?DESC("packets_connack_error")},
        {counter, 'packets.connack.auth_error', ?DESC("packets_connack_auth_error")},
        {counter, 'packets.publish.received', ?DESC("packets_publish_received")},
        {counter, 'packets.publish.sent', ?DESC("packets_publish_sent")},
        {counter, 'packets.publish.inuse', ?DESC("packets_publish_inuse")},
        {counter, 'packets.publish.error', ?DESC("packets_publish_error")},
        {counter, 'packets.publish.auth_error', ?DESC("packets_publish_auth_error")},
        {counter, 'packets.publish.quota_exceeded', ?DESC("packets_publish_quota_exceeded")},
        {counter, 'packets.puback.received', ?DESC("packets_puback_received")},
        {counter, 'packets.puback.sent', ?DESC("packets_puback_sent")},
        {counter, 'packets.puback.inuse', ?DESC("packets_puback_inuse")},
        {counter, 'packets.puback.missed', ?DESC("packets_puback_missed")},
        {counter, 'packets.pubrec.received', ?DESC("packets_pubrec_received")},
        {counter, 'packets.pubrec.sent', ?DESC("packets_pubrec_sent")},
        {counter, 'packets.pubrec.inuse', ?DESC("packets_pubrec_inuse")},
        {counter, 'packets.pubrec.missed', ?DESC("packets_pubrec_missed")},
        {counter, 'packets.pubrel.received', ?DESC("packets_pubrel_received")},
        {counter, 'packets.pubrel.sent', ?DESC("packets_pubrel_sent")},
        {counter, 'packets.pubrel.missed', ?DESC("packets_pubrel_missed")},
        {counter, 'packets.pubcomp.received', ?DESC("packets_pubcomp_received")},
        {counter, 'packets.pubcomp.sent', ?DESC("packets_pubcomp_sent")},
        {counter, 'packets.pubcomp.inuse', ?DESC("packets_pubcomp_inuse")},
        {counter, 'packets.pubcomp.missed', ?DESC("packets_pubcomp_missed")},
        {counter, 'packets.subscribe.received', ?DESC("packets_subscribe_received")},
        {counter, 'packets.subscribe.error', ?DESC("packets_subscribe_error")},
        {counter, 'packets.subscribe.auth_error', ?DESC("packets_subscribe_auth_error")},
        {counter, 'packets.suback.sent', ?DESC("packets_suback_sent")},
        {counter, 'packets.unsubscribe.received', ?DESC("packets_unsubscribe_received")},
        {counter, 'packets.unsubscribe.error', ?DESC("packets_unsubscribe_error")},
        {counter, 'packets.unsuback.sent', ?DESC("packets_unsuback_sent")},
        {counter, 'packets.pingreq.received', ?DESC("packets_pingreq_received")},
        {counter, 'packets.pingresp.sent', ?DESC("packets_pingresp_sent")},
        {counter, 'packets.disconnect.received', ?DESC("packets_disconnect_received")},
        {counter, 'packets.disconnect.sent', ?DESC("packets_disconnect_sent")},
        {counter, 'packets.auth.received', ?DESC("packets_auth_received")},
        {counter, 'packets.auth.sent', ?DESC("packets_auth_sent")}
    ].

%% Messages sent/received and pubsub
message_metrics() ->
    [
        {counter, 'messages.received', ?DESC("messages_received")},
        {counter, 'messages.sent', ?DESC("messages_sent")},
        {counter, 'messages.qos0.received', ?DESC("messages_qos0_received")},
        {counter, 'messages.qos0.sent', ?DESC("messages_qos0_sent")},
        {counter, 'messages.qos1.received', ?DESC("messages_qos1_received")},
        {counter, 'messages.qos1.sent', ?DESC("messages_qos1_sent")},
        {counter, 'messages.qos2.received', ?DESC("messages_qos2_received")},
        {counter, 'messages.qos2.sent', ?DESC("messages_qos2_sent")},
        {counter, 'messages.publish', ?DESC("messages_publish")},
        {counter, 'messages.dropped', ?DESC("messages_dropped")},
        {counter, 'messages.dropped.await_pubrel_timeout',
            ?DESC("messages_dropped_await_pubrel_timeout")},
        {counter, 'messages.dropped.no_subscribers', ?DESC("messages_dropped_no_subscribers")},
        {counter, 'messages.dropped.quota_exceeded', ?DESC("messages_dropped_quota_exceeded")},
        {counter, 'messages.dropped.receive_maximum', ?DESC("messages_dropped_receive_maximum")},
        {counter, 'messages.forward', ?DESC("messages_forward")},
        {counter, 'messages.delayed', ?DESC("messages_delayed")},
        {counter, 'messages.delivered', ?DESC("messages_delivered")},
        {counter, 'messages.acked', ?DESC("messages_acked")},
        {counter, 'messages.validation_failed', ?DESC("messages_validation_failed")},
        {counter, 'messages.validation_succeeded', ?DESC("messages_validation_succeeded")},
        {counter, 'messages.transformation_failed', ?DESC("messages_transformation_failed")},
        {counter, 'messages.transformation_succeeded', ?DESC("messages_transformation_succeeded")},
        {counter, 'messages.persisted', ?DESC("messages_persisted")}
    ].

%% Rule and Action related metrics
data_integration_metrics() ->
    [
        {counter, 'rules.matched', ?DESC("rules_matched")},
        {counter, 'actions.executed', ?DESC("actions_executed")}
    ].

%% Delivery metrics
delivery_metrics() ->
    [
        {counter, 'delivery.dropped', ?DESC("delivery_dropped")},
        {counter, 'delivery.dropped.no_local', ?DESC("delivery_dropped_no_local")},
        {counter, 'delivery.dropped.too_large', ?DESC("delivery_dropped_too_large")},
        {counter, 'delivery.dropped.qos0_msg', ?DESC("delivery_dropped_qos0_msg")},
        {counter, 'delivery.dropped.queue_full', ?DESC("delivery_dropped_queue_full")},
        {counter, 'delivery.dropped.expired', ?DESC("delivery_dropped_expired")}
    ].

%% Client Lifecycle metrics
client_metrics() ->
    [
        {counter, 'client.connect', ?DESC("client_connect")},
        {counter, 'client.connack', ?DESC("client_connack")},
        {counter, 'client.connected', ?DESC("client_connected")},
        {counter, 'client.authenticate', ?DESC("client_authenticate")},
        {counter, 'client.auth.anonymous', ?DESC("client_auth_anonymous")},
        {counter, 'client.authorize', ?DESC("client_authorize")},
        {counter, 'client.subscribe', ?DESC("client_subscribe")},
        {counter, 'client.unsubscribe', ?DESC("client_unsubscribe")},
        {counter, 'client.disconnected', ?DESC("client_disconnected")}
    ].

%% Session Lifecycle metrics
session_metrics() ->
    [
        {counter, 'session.created', ?DESC("session_created")},
        {counter, 'session.disconnected', ?DESC("session_disonnected")},
        {counter, 'session.resumed', ?DESC("session_resumed")},
        {counter, 'session.takenover', ?DESC("session_takenover")},
        {counter, 'session.discarded', ?DESC("session_discarded")},
        {counter, 'session.terminated', ?DESC("session_terminated")}
    ].

%% Statistic metrics for ACL checking
stats_acl_metrics() ->
    [
        {counter, 'authorization.allow', ?DESC("authorization_allow")},
        {counter, 'authorization.deny', ?DESC("authorization_deny")},
        {counter, 'authorization.cache_hit', ?DESC("authorization_cache_hit")},
        {counter, 'authorization.cache_miss', ?DESC("authorization_cache_miss")}
    ].

stats_authn_metrics() ->
    %% Statistic metrics for auth checking
    [
        {counter, 'authentication.success', ?DESC("authentication_success")},
        {counter, 'authentication.success.anonymous', ?DESC("authentication_success_anonymous")},
        {counter, 'authentication.failure', ?DESC("authentication_failure")}
    ].

olp_metrics() ->
    [
        {counter, 'overload_protection.delay.ok', ?DESC("overload_protection_delay_ok")},
        {counter, 'overload_protection.delay.timeout', ?DESC("overload_protection_delay_timeout")},
        {counter, 'overload_protection.hibernation', ?DESC("overload_protection_hibernation")},
        {counter, 'overload_protection.gc', ?DESC("overload_protection_gc")},
        {counter, 'overload_protection.new_conn', ?DESC("overload_protection_new_conn")}
    ].

%% Store reserved indices
initialize_counters(CRef, Metrics) ->
    lists:foreach(
        fun({Type, Name, _Desc}) ->
            Idx = reserved_idx(Name),
            Metric = #metric{name = Name, type = Type, idx = Idx},
            true = ets:insert(?TAB, Metric),
            ok = counters:put(CRef, Idx, 0)
        end,
        Metrics
    ).

get_counters_ref(?global_ns) ->
    persistent_term:get(?PT_KEY(?global_ns));
get_counters_ref(Namespace) when is_binary(Namespace) ->
    persistent_term:get(?PT_KEY(Namespace)).
