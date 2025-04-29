%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_metrics).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").
-include("emqx_mqtt.hrl").

-export([
    start_link/0,
    stop/0
]).

-export([
    new/1,
    new/2,
    ensure/1,
    ensure/2,
    all/0
]).

-export([
    val/1,
    inc/1,
    inc/2,
    dec/1,
    dec/2,
    set/2
]).

-export([
    trans/2,
    trans/3,
    commit/0
]).

%% Inc received/sent metrics
-export([
    inc_msg/1,
    inc_recv/1,
    inc_sent/1
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

%% BACKW: v4.3.0
-export([upgrade_retained_delayed_counter_type/0]).

-export_type([metric_idx/0, metric_name/0]).

-compile({inline, [inc/1, inc/2, dec/1, dec/2]}).
-compile({inline, [inc_recv/1, inc_sent/1]}).

-opaque metric_idx() :: 1..1024.

-type metric_name() :: atom() | string() | binary().

-define(MAX_SIZE, 1024).
-define(RESERVED_IDX, 512).
-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

is_olp_metric(Name) ->
    lists:keymember(Name, 2, olp_metrics()).

-record(state, {next_idx = 1}).

-record(metric, {name, type, idx}).

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

%% BACKW: v4.3.0
upgrade_retained_delayed_counter_type() ->
    Ks = ['messages.delayed'],
    gen_server:call(?SERVER, {set_type_to_counter, Ks}, infinity).

%%--------------------------------------------------------------------
%% Metrics API
%%--------------------------------------------------------------------

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

%% @doc Get all metrics
-spec all() -> [{metric_name(), non_neg_integer()}].
all() ->
    CRef = persistent_term:get(?MODULE),
    [
        {Name, counters:get(CRef, Idx)}
     || #metric{name = Name, idx = Idx} <- ets:tab2list(?TAB)
    ].

%% @doc Get metric value
-spec val(metric_name()) -> non_neg_integer().
val(Name) ->
    try
        case ets:lookup(?TAB, Name) of
            [#metric{idx = Idx}] ->
                CRef = persistent_term:get(?MODULE),
                counters:get(CRef, Idx);
            [] ->
                0
        end
        %% application will restart when join cluster, then ets not exist.
    catch
        error:badarg ->
            0
    end.

%% @doc Increase counter
-spec inc(metric_name()) -> ok.
inc(Name) ->
    inc(Name, 1).

%% @doc Increase metric value
-spec inc(metric_name(), pos_integer()) -> ok.
inc(Name, Value) ->
    update_counter(Name, Value).

%% @doc Decrease metric value
-spec dec(metric_name()) -> ok.
dec(Name) ->
    dec(Name, 1).

%% @doc Decrease metric value
-spec dec(metric_name(), pos_integer()) -> ok.
dec(Name, Value) ->
    update_counter(Name, -Value).

%% @doc Set metric value
-spec set(metric_name(), integer()) -> ok.
set(Name, Value) ->
    CRef = persistent_term:get(?MODULE),
    Idx = ets:lookup_element(?TAB, Name, 4),
    counters:put(CRef, Idx, Value).

-spec trans(inc | dec, metric_name()) -> ok.
trans(Op, Name) when Op =:= inc; Op =:= dec ->
    trans(Op, Name, 1).

-spec trans(inc | dec, metric_name(), pos_integer()) -> ok.
trans(inc, Name, Value) ->
    cache(Name, Value);
trans(dec, Name, Value) ->
    cache(Name, -Value).

-spec cache(metric_name(), integer()) -> ok.
cache(Name, Value) ->
    put(
        '$metrics',
        case get('$metrics') of
            undefined ->
                #{Name => Value};
            Metrics ->
                maps:update_with(Name, fun(Cnt) -> Cnt + Value end, Value, Metrics)
        end
    ),
    ok.

-spec commit() -> ok.
commit() ->
    case get('$metrics') of
        undefined ->
            ok;
        Metrics ->
            _ = erase('$metrics'),
            lists:foreach(fun update_counter/1, maps:to_list(Metrics))
    end.

update_counter({Name, Value}) ->
    update_counter(Name, Value).

update_counter(Name, Value) ->
    CRef = persistent_term:get(?MODULE),
    CIdx =
        case reserved_idx(Name) of
            Idx when is_integer(Idx) -> Idx;
            undefined -> ets:lookup_element(?TAB, Name, 4)
        end,
    counters:add(CRef, CIdx, Value).

%%--------------------------------------------------------------------
%% Inc received/sent metrics
%%--------------------------------------------------------------------

-spec inc_msg(emqx_types:message()) -> ok.
inc_msg(Msg) ->
    case Msg#message.qos of
        0 -> inc('messages.qos0.received');
        1 -> inc('messages.qos1.received');
        2 -> inc('messages.qos2.received')
    end,
    inc('messages.received').

%% @doc Inc packets received.
-spec inc_recv(emqx_types:packet()) -> ok.
inc_recv(Packet) ->
    inc('packets.received'),
    do_inc_recv(Packet).

do_inc_recv(?PACKET(?CONNECT)) ->
    inc('packets.connect.received');
do_inc_recv(?PUBLISH_PACKET(QoS)) ->
    inc('messages.received'),
    case QoS of
        ?QOS_0 -> inc('messages.qos0.received');
        ?QOS_1 -> inc('messages.qos1.received');
        ?QOS_2 -> inc('messages.qos2.received');
        _other -> ok
    end,
    inc('packets.publish.received');
do_inc_recv(?PACKET(?PUBACK)) ->
    inc('packets.puback.received');
do_inc_recv(?PACKET(?PUBREC)) ->
    inc('packets.pubrec.received');
do_inc_recv(?PACKET(?PUBREL)) ->
    inc('packets.pubrel.received');
do_inc_recv(?PACKET(?PUBCOMP)) ->
    inc('packets.pubcomp.received');
do_inc_recv(?PACKET(?SUBSCRIBE)) ->
    inc('packets.subscribe.received');
do_inc_recv(?PACKET(?UNSUBSCRIBE)) ->
    inc('packets.unsubscribe.received');
do_inc_recv(?PACKET(?PINGREQ)) ->
    inc('packets.pingreq.received');
do_inc_recv(?PACKET(?DISCONNECT)) ->
    inc('packets.disconnect.received');
do_inc_recv(?PACKET(?AUTH)) ->
    inc('packets.auth.received');
do_inc_recv(_Packet) ->
    ok.

%% @doc Inc packets sent. Will not count $SYS PUBLISH.
-spec inc_sent(emqx_types:packet()) -> ok.
inc_sent(?PUBLISH_PACKET(_QoS, <<"$SYS/", _/binary>>, _, _)) ->
    ok;
inc_sent(Packet) ->
    inc('packets.sent'),
    do_inc_sent(Packet).

do_inc_sent(?CONNACK_PACKET(ReasonCode, _SessPresent)) ->
    (ReasonCode == ?RC_SUCCESS) orelse inc('packets.connack.error'),
    ((ReasonCode == ?RC_NOT_AUTHORIZED) orelse
        (ReasonCode == ?CONNACK_AUTH)) andalso
        inc('packets.connack.auth_error'),
    ((ReasonCode == ?RC_BAD_USER_NAME_OR_PASSWORD) orelse
        (ReasonCode == ?CONNACK_CREDENTIALS)) andalso
        inc('packets.connack.auth_error'),
    inc('packets.connack.sent');
do_inc_sent(?PUBLISH_PACKET(QoS)) ->
    inc('messages.sent'),
    case QoS of
        ?QOS_0 -> inc('messages.qos0.sent');
        ?QOS_1 -> inc('messages.qos1.sent');
        ?QOS_2 -> inc('messages.qos2.sent');
        _other -> ok
    end,
    inc('packets.publish.sent');
do_inc_sent(?PUBACK_PACKET(_PacketId, ReasonCode)) ->
    (ReasonCode >= ?RC_UNSPECIFIED_ERROR) andalso inc('packets.publish.error'),
    (ReasonCode == ?RC_NOT_AUTHORIZED) andalso inc('packets.publish.auth_error'),
    inc('packets.puback.sent');
do_inc_sent(?PUBREC_PACKET(_PacketId, ReasonCode)) ->
    (ReasonCode >= ?RC_UNSPECIFIED_ERROR) andalso inc('packets.publish.error'),
    (ReasonCode == ?RC_NOT_AUTHORIZED) andalso inc('packets.publish.auth_error'),
    inc('packets.pubrec.sent');
do_inc_sent(?PACKET(?PUBREL)) ->
    inc('packets.pubrel.sent');
do_inc_sent(?PACKET(?PUBCOMP)) ->
    inc('packets.pubcomp.sent');
do_inc_sent(?PACKET(?SUBACK)) ->
    inc('packets.suback.sent');
do_inc_sent(?PACKET(?UNSUBACK)) ->
    inc('packets.unsuback.sent');
do_inc_sent(?PACKET(?PINGRESP)) ->
    inc('packets.pingresp.sent');
do_inc_sent(?PACKET(?DISCONNECT)) ->
    inc('packets.disconnect.sent');
do_inc_sent(?PACKET(?AUTH)) ->
    inc('packets.auth.sent');
do_inc_sent(_Packet) ->
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    % Create counters array
    CRef = counters:new(?MAX_SIZE, [write_concurrency]),
    ok = persistent_term:put(?MODULE, CRef),
    % Create index mapping table
    ok = emqx_utils_ets:new(?TAB, [{keypos, 2}, {read_concurrency, true}]),
    Metrics = all_metrics(),
    % Store reserved indices
    ok = lists:foreach(
        fun({Type, Name, _Desc}) ->
            Idx = reserved_idx(Name),
            Metric = #metric{name = Name, type = Type, idx = Idx},
            true = ets:insert(?TAB, Metric),
            ok = counters:put(CRef, Idx, 0)
        end,
        Metrics
    ),
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

reserved_idx('bytes.received') -> 01;
reserved_idx('bytes.sent') -> 02;
%% Reserved indices of packet's metrics
reserved_idx('packets.received') -> 10;
reserved_idx('packets.sent') -> 11;
reserved_idx('packets.connect.received') -> 12;
reserved_idx('packets.connack.sent') -> 13;
reserved_idx('packets.connack.error') -> 14;
reserved_idx('packets.connack.auth_error') -> 15;
reserved_idx('packets.publish.received') -> 16;
reserved_idx('packets.publish.sent') -> 17;
reserved_idx('packets.publish.inuse') -> 18;
reserved_idx('packets.publish.error') -> 19;
reserved_idx('packets.publish.auth_error') -> 20;
reserved_idx('packets.puback.received') -> 21;
reserved_idx('packets.puback.sent') -> 22;
reserved_idx('packets.puback.inuse') -> 23;
reserved_idx('packets.puback.missed') -> 24;
reserved_idx('packets.pubrec.received') -> 25;
reserved_idx('packets.pubrec.sent') -> 26;
reserved_idx('packets.pubrec.inuse') -> 27;
reserved_idx('packets.pubrec.missed') -> 28;
reserved_idx('packets.pubrel.received') -> 29;
reserved_idx('packets.pubrel.sent') -> 30;
reserved_idx('packets.pubrel.missed') -> 31;
reserved_idx('packets.pubcomp.received') -> 32;
reserved_idx('packets.pubcomp.sent') -> 33;
reserved_idx('packets.pubcomp.inuse') -> 34;
reserved_idx('packets.pubcomp.missed') -> 35;
reserved_idx('packets.subscribe.received') -> 36;
reserved_idx('packets.subscribe.error') -> 37;
reserved_idx('packets.subscribe.auth_error') -> 38;
reserved_idx('packets.suback.sent') -> 39;
reserved_idx('packets.unsubscribe.received') -> 40;
reserved_idx('packets.unsubscribe.error') -> 41;
reserved_idx('packets.unsuback.sent') -> 42;
reserved_idx('packets.pingreq.received') -> 43;
reserved_idx('packets.pingresp.sent') -> 44;
reserved_idx('packets.disconnect.received') -> 45;
reserved_idx('packets.disconnect.sent') -> 46;
reserved_idx('packets.auth.received') -> 47;
reserved_idx('packets.auth.sent') -> 48;
%% reserved_idx('packets.publish.dropped') -> 49; %% deprecated; new metrics may use this index.
reserved_idx('packets.publish.quota_exceeded') -> 50;
%% Reserved indices of message's metrics
reserved_idx('messages.received') -> 100;
reserved_idx('messages.sent') -> 101;
reserved_idx('messages.qos0.received') -> 102;
reserved_idx('messages.qos0.sent') -> 103;
reserved_idx('messages.qos1.received') -> 104;
reserved_idx('messages.qos1.sent') -> 105;
reserved_idx('messages.qos2.received') -> 106;
reserved_idx('messages.qos2.sent') -> 107;
reserved_idx('messages.publish') -> 108;
reserved_idx('messages.dropped') -> 109;
reserved_idx('messages.dropped.await_pubrel_timeout') -> 110;
reserved_idx('messages.dropped.no_subscribers') -> 111;
reserved_idx('messages.forward') -> 112;
reserved_idx('messages.dropped.quota_exceeded') -> 113;
reserved_idx('messages.delayed') -> 114;
reserved_idx('messages.delivered') -> 115;
reserved_idx('messages.acked') -> 116;
reserved_idx('messages.dropped.receive_maximum') -> 117;
reserved_idx('delivery.dropped') -> 118;
reserved_idx('delivery.dropped.no_local') -> 119;
reserved_idx('delivery.dropped.too_large') -> 120;
reserved_idx('delivery.dropped.qos0_msg') -> 121;
reserved_idx('delivery.dropped.queue_full') -> 122;
reserved_idx('delivery.dropped.expired') -> 123;
reserved_idx('client.connect') -> 200;
reserved_idx('client.connack') -> 201;
reserved_idx('client.connected') -> 202;
reserved_idx('client.authenticate') -> 203;
%% reserved_idx('client.enhanced_authenticate') -> 204; %% have never used
reserved_idx('client.auth.anonymous') -> 205;
reserved_idx('client.authorize') -> 206;
reserved_idx('client.subscribe') -> 207;
reserved_idx('client.unsubscribe') -> 208;
reserved_idx('client.disconnected') -> 209;
reserved_idx('session.created') -> 220;
reserved_idx('session.resumed') -> 221;
reserved_idx('session.takenover') -> 222;
reserved_idx('session.discarded') -> 223;
reserved_idx('session.terminated') -> 224;
reserved_idx('authorization.allow') -> 300;
reserved_idx('authorization.deny') -> 301;
reserved_idx('authorization.cache_hit') -> 302;
reserved_idx('authorization.cache_miss') -> 303;
reserved_idx('authentication.success') -> 310;
reserved_idx('authentication.success.anonymous') -> 311;
reserved_idx('authentication.failure') -> 312;
reserved_idx('overload_protection.delay.ok') -> 400;
reserved_idx('overload_protection.delay.timeout') -> 401;
reserved_idx('overload_protection.hibernation') -> 402;
reserved_idx('overload_protection.gc') -> 403;
reserved_idx('overload_protection.new_conn') -> 404;
reserved_idx('messages.validation_succeeded') -> 405;
reserved_idx('messages.validation_failed') -> 406;
reserved_idx('messages.persisted') -> 407;
reserved_idx('messages.transformation_succeeded') -> 408;
reserved_idx('messages.transformation_failed') -> 409;
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
        olp_metrics()
    ]).

%% Bytes sent and received
bytes_metrics() ->
    [
        {counter, 'bytes.received', <<"Number of bytes received ">>},
        {counter, 'bytes.sent', <<"Number of bytes sent on this connection">>}
    ].

%% Packets sent and received
packet_metrics() ->
    [
        {counter, 'packets.received', <<"Number of received packet">>},
        {counter, 'packets.sent', <<"Number of sent packet">>},
        {counter, 'packets.connect.received', <<"Number of received CONNECT packet">>},
        {counter, 'packets.connack.sent', <<"Number of sent CONNACK packet">>},
        {counter, 'packets.connack.error',
            <<"Number of received CONNECT packet with unsuccessful connections">>},
        {counter, 'packets.connack.auth_error',
            <<"Number of received CONNECT packet with failed Authentication">>},
        {counter, 'packets.publish.received', <<"Number of received PUBLISH packet">>},
        %% PUBLISH packets sent
        {counter, 'packets.publish.sent', <<"Number of sent PUBLISH packet">>},
        %% PUBLISH packet_id inuse
        {counter, 'packets.publish.inuse',
            <<"Number of received PUBLISH packet with occupied identifiers">>},
        %% PUBLISH failed for error
        {counter, 'packets.publish.error',
            <<"Number of received PUBLISH packet that cannot be published">>},
        %% PUBLISH failed for auth error
        {counter, 'packets.publish.auth_error',
            <<"Number of received PUBLISH packets with failed the Authorization check">>},
        %% PUBLISH failed due to rate limiting
        {counter, 'packets.publish.quota_exceeded',
            <<"Number of received PUBLISH packets that exceeded the quota">>},
        %% PUBACK packets received
        {counter, 'packets.puback.received', <<"Number of received PUBACK packet">>},
        %% PUBACK packets sent
        {counter, 'packets.puback.sent', <<"Number of sent PUBACK packet">>},
        %% PUBACK packet_id inuse
        {counter, 'packets.puback.inuse',
            <<"Number of received PUBACK packet with occupied identifiers">>},
        %% PUBACK packets missed
        {counter, 'packets.puback.missed', <<"Number of received packet with identifiers.">>},
        %% PUBREC packets received
        {counter, 'packets.pubrec.received', <<"Number of received PUBREC packet">>},
        %% PUBREC packets sent
        {counter, 'packets.pubrec.sent', <<"Number of sent PUBREC packet">>},
        %% PUBREC packet_id inuse
        {counter, 'packets.pubrec.inuse',
            <<"Number of received PUBREC packet with occupied identifiers">>},
        %% PUBREC packets missed
        {counter, 'packets.pubrec.missed',
            <<"Number of received PUBREC packet with unknown identifiers">>},
        %% PUBREL packets received
        {counter, 'packets.pubrel.received', <<"Number of received PUBREL packet">>},
        %% PUBREL packets sent
        {counter, 'packets.pubrel.sent', <<"Number of sent PUBREL packet">>},
        %% PUBREL packets missed
        {counter, 'packets.pubrel.missed',
            <<"Number of received PUBREC packet with unknown identifiers">>},
        %% PUBCOMP packets received
        {counter, 'packets.pubcomp.received', <<"Number of received PUBCOMP packet">>},
        %% PUBCOMP packets sent
        {counter, 'packets.pubcomp.sent', <<"Number of sent PUBCOMP packet">>},
        %% PUBCOMP packet_id inuse
        {counter, 'packets.pubcomp.inuse',
            <<"Number of received PUBCOMP packet with occupied identifiers">>},
        %% PUBCOMP packets missed
        {counter, 'packets.pubcomp.missed', <<"Number of missed PUBCOMP packet">>},
        %% SUBSCRIBE Packets received
        {counter, 'packets.subscribe.received', <<"Number of received SUBSCRIBE packet">>},
        %% SUBSCRIBE error
        {counter, 'packets.subscribe.error',
            <<"Number of received SUBSCRIBE packet with failed subscriptions">>},
        %% SUBSCRIBE failed for not auth
        {counter, 'packets.subscribe.auth_error',
            <<"Number of received SUBACK packet with failed Authorization check">>},
        %% SUBACK packets sent
        {counter, 'packets.suback.sent', <<"Number of sent SUBACK packet">>},
        %% UNSUBSCRIBE Packets received
        {counter, 'packets.unsubscribe.received', <<"Number of received UNSUBSCRIBE packet">>},
        %% UNSUBSCRIBE error
        {counter, 'packets.unsubscribe.error',
            <<"Number of received UNSUBSCRIBE packet with failed unsubscriptions">>},
        %% UNSUBACK Packets sent
        {counter, 'packets.unsuback.sent', <<"Number of sent UNSUBACK packet">>},
        %% PINGREQ packets received
        {counter, 'packets.pingreq.received', <<"Number of received PINGREQ packet">>},
        %% PINGRESP Packets sent
        {counter, 'packets.pingresp.sent', <<"Number of sent PUBRESP packet">>},
        %% DISCONNECT Packets received
        {counter, 'packets.disconnect.received', <<"Number of received DISCONNECT packet">>},
        %% DISCONNECT Packets sent
        {counter, 'packets.disconnect.sent', <<"Number of sent DISCONNECT packet">>},
        %% Auth Packets received
        {counter, 'packets.auth.received', <<"Number of received AUTH packet">>},
        %% Auth Packets sent
        {counter, 'packets.auth.sent', <<"Number of sent AUTH packet">>}
    ].

%% Messages sent/received and pubsub
message_metrics() ->
    [
        %% All Messages received
        {counter, 'messages.received', <<
            "Number of messages received from the client, equal to the sum of "
            "messages.qos0.received, messages.qos1.received and messages.qos2.received"
        >>},
        %% All Messages sent
        {counter, 'messages.sent', <<
            "Number of messages sent to the client, equal to the sum of "
            "messages.qos0.sent, messages.qos1.sent and messages.qos2.sent"
        >>},
        %% QoS0 Messages received
        {counter, 'messages.qos0.received', <<"Number of QoS 0 messages received from clients">>},
        %% QoS0 Messages sent
        {counter, 'messages.qos0.sent', <<"Number of QoS 0 messages sent to clients">>},
        %% QoS1 Messages received
        {counter, 'messages.qos1.received', <<"Number of QoS 1 messages received from clients">>},
        %% QoS1 Messages sent
        {counter, 'messages.qos1.sent', <<"Number of QoS 1 messages sent to clients">>},
        %% QoS2 Messages received
        {counter, 'messages.qos2.received', <<"Number of QoS 2 messages received from clients">>},
        %% QoS2 Messages sent
        {counter, 'messages.qos2.sent', <<"Number of QoS 2 messages sent to clients">>},
        %% PubSub Metrics

        %% Messages Publish
        {counter, 'messages.publish',
            <<"Number of messages published in addition to system messages">>},
        %% Messages dropped due to no subscribers
        {counter, 'messages.dropped',
            <<"Number of messages dropped before forwarding to the subscription process">>},
        %% Messages that failed validations
        {counter, 'messages.validation_failed', <<"Number of message validation failed">>},
        %% Messages that passed validations
        {counter, 'messages.validation_succeeded', <<"Number of message validation successful">>},
        %% % Messages that failed transformations
        {counter, 'messages.transformation_failed', <<"Number fo message transformation failed">>},
        %% % Messages that passed transformations
        {counter, 'messages.transformation_succeeded',
            <<"Number fo message transformation succeeded">>},
        %% QoS2 Messages expired
        {counter, 'messages.dropped.await_pubrel_timeout',
            <<"Number of messages dropped due to waiting PUBREL timeout">>},
        %% Messages dropped
        {counter, 'messages.dropped.no_subscribers',
            <<"Number of messages dropped due to no subscribers">>},
        %% Messages dropped - quota exceeded
        {counter, 'messages.dropped.quota_exceeded',
            <<"Number of messages dropped due to quota being exceeded">>},
        %% Messages dropped - receive maximum
        {counter, 'messages.dropped.receive_maximum',
            <<"Number of messages dropped due to receive maximum being reached">>},
        %% Messages forward
        {counter, 'messages.forward', <<"Number of messages forwarded to other nodes">>},
        %% Messages delayed
        {counter, 'messages.delayed', <<"Number of delay-published messages">>},
        %% Messages delivered
        {counter, 'messages.delivered',
            <<"Number of messages forwarded to the subscription process internally">>},
        %% Messages acked
        {counter, 'messages.acked', <<"Number of received PUBACK and PUBREC packet">>},
        %% Messages persistently stored
        {counter, 'messages.persisted', <<"Number of message persisted">>}
    ].

%% Delivery metrics
delivery_metrics() ->
    [
        %% All Dropped during delivery
        {counter, 'delivery.dropped', <<"Total number of discarded messages when sending">>},
        %% Dropped due to no_local
        {counter, 'delivery.dropped.no_local', <<
            "Number of messages that were dropped due to the No Local subscription "
            "option when sending"
        >>},
        %% Dropped due to message too large
        {counter, 'delivery.dropped.too_large', <<
            "The number of messages that were dropped because the length exceeded "
            "the limit when sending"
        >>},
        %% Dropped qos0 message
        {counter, 'delivery.dropped.qos0_msg', <<
            "Number of messages with QoS 0 that were dropped because "
            "mqtt.mqueue_store_qos0=false when sending"
        >>},
        %% Dropped due to queue full
        {counter, 'delivery.dropped.queue_full', <<
            "Number of messages that were dropped because the "
            "message queue was full when sending"
        >>},
        %% Dropped due to expired
        {counter, 'delivery.dropped.expired',
            <<"Number of messages dropped due to message expiration on sending">>}
    ].

%% Client Lifecircle metrics
client_metrics() ->
    [
        {counter, 'client.connect', <<"Number of client connections">>},
        {counter, 'client.connack', <<"Number of CONNACK packet sent">>},
        {counter, 'client.connected', <<"Number of successful client connected">>},
        {counter, 'client.authenticate', <<"Number of client Authentication">>},
        {counter, 'client.auth.anonymous', <<"Number of clients who log in anonymously">>},
        {counter, 'client.authorize', <<"Number of Authorization rule checks">>},
        {counter, 'client.subscribe', <<"Number of client subscriptions">>},
        {counter, 'client.unsubscribe', <<"Number of client unsubscriptions">>},
        {counter, 'client.disconnected', <<"Number of client disconnects">>}
    ].

%% Session Lifecircle metrics
session_metrics() ->
    [
        {counter, 'session.created', <<"Number of sessions created">>},
        {counter, 'session.resumed',
            <<"Number of sessions resumed because Clean Session or Clean Start is false">>},
        {counter, 'session.takenover',
            <<"Number of sessions takenover because Clean Session or Clean Start is false">>},
        {counter, 'session.discarded',
            <<"Number of sessions dropped because Clean Session or Clean Start is true">>},
        {counter, 'session.terminated', <<"Number of terminated sessions">>}
    ].

%% Statistic metrics for ACL checking
stats_acl_metrics() ->
    [
        {counter, 'authorization.allow', <<"Number of Authorization allow">>},
        {counter, 'authorization.deny', <<"Number of Authorization deny">>},
        {counter, 'authorization.cache_hit', <<"Number of Authorization hits the cache">>},
        {counter, 'authorization.cache_miss', <<"Number of Authorization cache missing">>}
    ].

stats_authn_metrics() ->
    %% Statistic metrics for auth checking
    [
        {counter, 'authentication.success', <<"Number of successful client Authentication">>},
        {counter, 'authentication.success.anonymous',
            <<"Number of successful client Authentication due to anonymous">>},
        {counter, 'authentication.failure', <<"Number of failed client Authentication">>}
    ].

olp_metrics() ->
    [
        {counter, 'overload_protection.delay.ok', <<"Number of overload protection delayed">>},
        {counter, 'overload_protection.delay.timeout',
            <<"Number of overload protection delay timeout">>},
        {counter, 'overload_protection.hibernation',
            <<"Number of overload protection hibernation">>},
        {counter, 'overload_protection.gc', <<"Number of overload protection garbage collection">>},
        {counter, 'overload_protection.new_conn',
            <<"Number of overload protection close new incoming connection">>}
    ].
