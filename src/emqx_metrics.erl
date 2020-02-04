%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_metrics).

-behavior(gen_server).

-include("logger.hrl").
-include("types.hrl").
-include("emqx_mqtt.hrl").

-logger_header("[Metrics]").

-export([ start_link/0
        , stop/0
        ]).

-export([ new/1
        , new/2
        , all/0
        ]).

-export([ val/1
        , inc/1
        , inc/2
        , dec/1
        , dec/2
        , set/2
        ]).

-export([ trans/2
        , trans/3
        , commit/0
        ]).

%% Inc received/sent metrics
-export([ inc_recv/1
        , inc_sent/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export_type([metric_idx/0]).

-compile({inline, [inc/1, inc/2, dec/1, dec/2]}).
-compile({inline, [inc_recv/1, inc_sent/1]}).

-opaque(metric_idx() :: 1..1024).

-type(metric_name() :: atom() | string() | binary()).

-define(MAX_SIZE, 1024).
-define(RESERVED_IDX, 512).
-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

%% Bytes sent and received
-define(BYTES_METRICS,
        [{counter, 'bytes.received'}, % Total bytes received
         {counter, 'bytes.sent'}      % Total bytes sent
        ]).

%% Packets sent and received
-define(PACKET_METRICS,
        [{counter, 'packets.received'},              % All Packets received
         {counter, 'packets.sent'},                  % All Packets sent
         {counter, 'packets.connect.received'},      % CONNECT Packets received
         {counter, 'packets.connack.sent'},          % CONNACK Packets sent
         {counter, 'packets.connack.error'},         % CONNACK error sent
         {counter, 'packets.connack.auth_error'},    % CONNACK auth_error sent
         {counter, 'packets.publish.received'},      % PUBLISH packets received
         {counter, 'packets.publish.sent'},          % PUBLISH packets sent
         {counter, 'packets.publish.inuse'},         % PUBLISH packet_id inuse
         {counter, 'packets.publish.error'},         % PUBLISH failed for error
         {counter, 'packets.publish.auth_error'},    % PUBLISH failed for auth error
         {counter, 'packets.publish.dropped'},       % PUBLISH(QoS2) packets dropped
         {counter, 'packets.puback.received'},       % PUBACK packets received
         {counter, 'packets.puback.sent'},           % PUBACK packets sent
         {counter, 'packets.puback.inuse'},          % PUBACK packet_id inuse
         {counter, 'packets.puback.missed'},         % PUBACK packets missed
         {counter, 'packets.pubrec.received'},       % PUBREC packets received
         {counter, 'packets.pubrec.sent'},           % PUBREC packets sent
         {counter, 'packets.pubrec.inuse'},          % PUBREC packet_id inuse
         {counter, 'packets.pubrec.missed'},         % PUBREC packets missed
         {counter, 'packets.pubrel.received'},       % PUBREL packets received
         {counter, 'packets.pubrel.sent'},           % PUBREL packets sent
         {counter, 'packets.pubrel.missed'},         % PUBREL packets missed
         {counter, 'packets.pubcomp.received'},      % PUBCOMP packets received
         {counter, 'packets.pubcomp.sent'},          % PUBCOMP packets sent
         {counter, 'packets.pubcomp.inuse'},         % PUBCOMP packet_id inuse
         {counter, 'packets.pubcomp.missed'},        % PUBCOMP packets missed
         {counter, 'packets.subscribe.received'},    % SUBSCRIBE Packets received
         {counter, 'packets.subscribe.error'},       % SUBSCRIBE error
         {counter, 'packets.subscribe.auth_error'},  % SUBSCRIBE failed for not auth
         {counter, 'packets.suback.sent'},           % SUBACK packets sent
         {counter, 'packets.unsubscribe.received'},  % UNSUBSCRIBE Packets received
         {counter, 'packets.unsubscribe.error'},     % UNSUBSCRIBE error
         {counter, 'packets.unsuback.sent'},         % UNSUBACK Packets sent
         {counter, 'packets.pingreq.received'},      % PINGREQ packets received
         {counter, 'packets.pingresp.sent'},         % PINGRESP Packets sent
         {counter, 'packets.disconnect.received'},   % DISCONNECT Packets received
         {counter, 'packets.disconnect.sent'},       % DISCONNECT Packets sent
         {counter, 'packets.auth.received'},         % Auth Packets received
         {counter, 'packets.auth.sent'}              % Auth Packets sent
        ]).

%% Messages sent/received and pubsub
-define(MESSAGE_METRICS,
        [{counter, 'messages.received'},      % All Messages received
         {counter, 'messages.sent'},          % All Messages sent
         {counter, 'messages.qos0.received'}, % QoS0 Messages received
         {counter, 'messages.qos0.sent'},     % QoS0 Messages sent
         {counter, 'messages.qos1.received'}, % QoS1 Messages received
         {counter, 'messages.qos1.sent'},     % QoS1 Messages sent
         {counter, 'messages.qos2.received'}, % QoS2 Messages received
         {counter, 'messages.qos2.sent'},     % QoS2 Messages sent
         %% PubSub Metrics
         {counter, 'messages.publish'},       % Messages Publish
         {counter, 'messages.dropped'},       % Messages dropped due to no subscribers
         {counter, 'messages.dropped.expired'},  % QoS2 Messages expired
         {counter, 'messages.dropped.no_subscribers'},  % Messages dropped
         {counter, 'messages.forward'},       % Messages forward
         {gauge,   'messages.retained'},      % Messages retained
         {gauge,   'messages.delayed'},       % Messages delayed
         {counter, 'messages.delivered'},     % Messages delivered
         {counter, 'messages.acked'}          % Messages acked
        ]).

%% Delivery metrics
-define(DELIVERY_METRICS,
        [{counter, 'delivery.dropped'},
         {counter, 'delivery.dropped.no_local'},
         {counter, 'delivery.dropped.too_large'},
         {counter, 'delivery.dropped.qos0_msg'},
         {counter, 'delivery.dropped.queue_full'},
         {counter, 'delivery.dropped.expired'}
        ]).

%% Client Lifecircle metrics
-define(CLIENT_METRICS,
        [{counter, 'client.connect'},
         {counter, 'client.connack'},
         {counter, 'client.connected'},
         {counter, 'client.authenticate'},
         {counter, 'client.auth.anonymous'},
         {counter, 'client.check_acl'},
         {counter, 'client.subscribe'},
         {counter, 'client.unsubscribe'},
         {counter, 'client.disconnected'}
        ]).

%% Session Lifecircle metrics
-define(SESSION_METRICS,
        [{counter, 'session.created'},
         {counter, 'session.resumed'},
         {counter, 'session.takeovered'},
         {counter, 'session.discarded'},
         {counter, 'session.terminated'}
        ]).

-record(state, {next_idx = 1}).

-record(metric, {name, type, idx}).

%% @doc Start the metrics server.
-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(stop() -> ok).
stop() -> gen_server:stop(?SERVER).

%%--------------------------------------------------------------------
%% Metrics API
%%--------------------------------------------------------------------

-spec(new(metric_name()) -> ok).
new(Name) ->
    new(counter, Name).

-spec(new(gauge|counter, metric_name()) -> ok).
new(gauge, Name) ->
    create(gauge, Name);
new(counter, Name) ->
    create(counter, Name).

%% @private
create(Type, Name) ->
    case gen_server:call(?SERVER, {create, Type, Name}) of
        {ok, _Idx} -> ok;
        {error, Reason} -> error(Reason)
    end.

%% @doc Get all metrics
-spec(all() -> [{metric_name(), non_neg_integer()}]).
all() ->
    CRef = persistent_term:get(?MODULE),
    [{Name, counters:get(CRef, Idx)}
     || #metric{name = Name, idx = Idx} <- ets:tab2list(?TAB)].

%% @doc Get metric value
-spec(val(metric_name()) -> maybe(non_neg_integer())).
val(Name) ->
    case ets:lookup(?TAB, Name) of
        [#metric{idx = Idx}] ->
            CRef = persistent_term:get(?MODULE),
            counters:get(CRef, Idx);
        [] -> undefined
    end.

%% @doc Increase counter
-spec(inc(metric_name()) -> ok).
inc(Name) ->
    inc(Name, 1).

%% @doc Increase metric value
-spec(inc(metric_name(), pos_integer()) -> ok).
inc(Name, Value) ->
    update_counter(Name, Value).

%% @doc Decrease metric value
-spec(dec(metric_name()) -> ok).
dec(Name) ->
    dec(Name, 1).

%% @doc Decrease metric value
-spec(dec(metric_name(), pos_integer()) -> ok).
dec(Name, Value) ->
    update_counter(Name, -Value).

%% @doc Set metric value
-spec(set(metric_name(), integer()) -> ok).
set(Name, Value) ->
    CRef = persistent_term:get(?MODULE),
    Idx = ets:lookup_element(?TAB, Name, 4),
    counters:put(CRef, Idx, Value).

-spec(trans(inc | dec, metric_name()) -> ok).
trans(Op, Name) when Op =:= inc; Op =:= dec ->
    trans(Op, Name, 1).

-spec(trans(inc | dec, metric_name(), pos_integer()) -> ok).
trans(inc, Name, Value) ->
    cache(Name, Value);
trans(dec, Name, Value) ->
    cache(Name, -Value).

-spec(cache(metric_name(), integer()) -> ok).
cache(Name, Value) ->
    put('$metrics', case get('$metrics') of
                        undefined ->
                            #{Name => Value};
                        Metrics ->
                            maps:update_with(Name, fun(Cnt) -> Cnt + Value end, Value, Metrics)
                    end),
    ok.

-spec(commit() -> ok).
commit() ->
    case get('$metrics') of
        undefined -> ok;
        Metrics ->
            _ = erase('$metrics'),
            lists:foreach(fun update_counter/1, maps:to_list(Metrics))
    end.

update_counter({Name, Value}) ->
    update_counter(Name, Value).

update_counter(Name, Value) ->
    CRef = persistent_term:get(?MODULE),
    CIdx = case reserved_idx(Name) of
               Idx when is_integer(Idx) -> Idx;
               undefined ->
                   ets:lookup_element(?TAB, Name, 4)
           end,
    counters:add(CRef, CIdx, Value).

%%--------------------------------------------------------------------
%% Inc received/sent metrics
%%--------------------------------------------------------------------

%% @doc Inc packets received.
-spec(inc_recv(emqx_types:packet()) -> ok).
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
do_inc_recv(_Packet) -> ok.

%% @doc Inc packets sent. Will not count $SYS PUBLISH.
-spec(inc_sent(emqx_types:packet()) -> ok).
inc_sent(?PUBLISH_PACKET(_QoS, <<"$SYS/", _/binary>>, _, _)) ->
    ok;
inc_sent(Packet) ->
    inc('packets.sent'),
    do_inc_sent(Packet).

do_inc_sent(?CONNACK_PACKET(ReasonCode)) ->
    (ReasonCode == ?RC_SUCCESS) orelse inc('packets.connack.error'),
    (ReasonCode == ?RC_NOT_AUTHORIZED) andalso inc('packets.connack.auth_error'),
    (ReasonCode == ?RC_BAD_USER_NAME_OR_PASSWORD) andalso inc('packets.connack.auth_error'),
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
do_inc_sent(_Packet) -> ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    % Create counters array
    CRef = counters:new(?MAX_SIZE, [write_concurrency]),
    ok = persistent_term:put(?MODULE, CRef),
    % Create index mapping table
    ok = emqx_tables:new(?TAB, [{keypos, 2}, {read_concurrency, true}]),
    Metrics = lists:append([?BYTES_METRICS,
                            ?PACKET_METRICS,
                            ?MESSAGE_METRICS,
                            ?DELIVERY_METRICS,
                            ?CLIENT_METRICS,
                            ?SESSION_METRICS
                           ]),
    % Store reserved indices
    ok = lists:foreach(fun({Type, Name}) ->
                               Idx = reserved_idx(Name),
                               Metric = #metric{name = Name, type = Type, idx = Idx},
                               true = ets:insert(?TAB, Metric),
                               ok = counters:put(CRef, Idx, 0)
                       end, Metrics),
    {ok, #state{next_idx = ?RESERVED_IDX + 1}, hibernate}.

handle_call({create, Type, Name}, _From, State = #state{next_idx = ?MAX_SIZE}) ->
    ?LOG(error, "Failed to create ~s:~s for index exceeded.", [Type, Name]),
    {reply, {error, metric_index_exceeded}, State};

handle_call({create, Type, Name}, _From, State = #state{next_idx = NextIdx}) ->
    case ets:lookup(?TAB, Name) of
        [#metric{idx = Idx}] ->
            ?LOG(info, "~s already exists.", [Name]),
            {reply, {ok, Idx}, State};
        [] ->
            Metric = #metric{name = Name, type = Type, idx = NextIdx},
            true = ets:insert(?TAB, Metric),
            {reply, {ok, NextIdx}, State#state{next_idx = NextIdx + 1}}
    end;

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

reserved_idx('bytes.received')               -> 01;
reserved_idx('bytes.sent')                   -> 02;
%% Reserved indices of packet's metrics
reserved_idx('packets.received')             -> 10;
reserved_idx('packets.sent')                 -> 11;
reserved_idx('packets.connect.received')     -> 12;
reserved_idx('packets.connack.sent')         -> 13;
reserved_idx('packets.connack.error')        -> 14;
reserved_idx('packets.connack.auth_error')   -> 15;
reserved_idx('packets.publish.received')     -> 16;
reserved_idx('packets.publish.sent')         -> 17;
reserved_idx('packets.publish.inuse')        -> 18;
reserved_idx('packets.publish.error')        -> 19;
reserved_idx('packets.publish.auth_error')   -> 20;
reserved_idx('packets.puback.received')      -> 21;
reserved_idx('packets.puback.sent')          -> 22;
reserved_idx('packets.puback.inuse')         -> 23;
reserved_idx('packets.puback.missed')        -> 24;
reserved_idx('packets.pubrec.received')      -> 25;
reserved_idx('packets.pubrec.sent')          -> 26;
reserved_idx('packets.pubrec.inuse')         -> 27;
reserved_idx('packets.pubrec.missed')        -> 28;
reserved_idx('packets.pubrel.received')      -> 29;
reserved_idx('packets.pubrel.sent')          -> 30;
reserved_idx('packets.pubrel.missed')        -> 31;
reserved_idx('packets.pubcomp.received')     -> 32;
reserved_idx('packets.pubcomp.sent')         -> 33;
reserved_idx('packets.pubcomp.inuse')        -> 34;
reserved_idx('packets.pubcomp.missed')       -> 35;
reserved_idx('packets.subscribe.received')   -> 36;
reserved_idx('packets.subscribe.error')      -> 37;
reserved_idx('packets.subscribe.auth_error') -> 38;
reserved_idx('packets.suback.sent')          -> 39;
reserved_idx('packets.unsubscribe.received') -> 40;
reserved_idx('packets.unsubscribe.error')    -> 41;
reserved_idx('packets.unsuback.sent')        -> 42;
reserved_idx('packets.pingreq.received')     -> 43;
reserved_idx('packets.pingresp.sent')        -> 44;
reserved_idx('packets.disconnect.received')  -> 45;
reserved_idx('packets.disconnect.sent')      -> 46;
reserved_idx('packets.auth.received')        -> 47;
reserved_idx('packets.auth.sent')            -> 48;
reserved_idx('packets.publish.dropped')      -> 49;
%% Reserved indices of message's metrics
reserved_idx('messages.received')            -> 100;
reserved_idx('messages.sent')                -> 101;
reserved_idx('messages.qos0.received')       -> 102;
reserved_idx('messages.qos0.sent')           -> 103;
reserved_idx('messages.qos1.received')       -> 104;
reserved_idx('messages.qos1.sent')           -> 105;
reserved_idx('messages.qos2.received')       -> 106;
reserved_idx('messages.qos2.sent')           -> 107;
reserved_idx('messages.publish')             -> 108;
reserved_idx('messages.dropped')             -> 109;
reserved_idx('messages.dropped.expired')     -> 110;
reserved_idx('messages.dropped.no_subscribers') -> 111;
reserved_idx('messages.forward')             -> 112;
reserved_idx('messages.retained')            -> 113;
reserved_idx('messages.delayed')             -> 114;
reserved_idx('messages.delivered')           -> 115;
reserved_idx('messages.acked')               -> 116;
reserved_idx('delivery.expired')             -> 117;
reserved_idx('delivery.dropped')             -> 118;
reserved_idx('delivery.dropped.no_local')    -> 119;
reserved_idx('delivery.dropped.too_large')   -> 120;
reserved_idx('delivery.dropped.qos0_msg')    -> 121;
reserved_idx('delivery.dropped.queue_full')  -> 122;
reserved_idx('delivery.dropped.expired')     -> 123;

reserved_idx('client.connect')               -> 200;
reserved_idx('client.connack')               -> 201;
reserved_idx('client.connected')             -> 202;
reserved_idx('client.authenticate')          -> 203;
reserved_idx('client.auth.anonymous')        -> 204;
reserved_idx('client.check_acl')             -> 205;
reserved_idx('client.subscribe')             -> 206;
reserved_idx('client.unsubscribe')           -> 207;
reserved_idx('client.disconnected')          -> 208;

reserved_idx('session.created')              -> 220;
reserved_idx('session.resumed')              -> 221;
reserved_idx('session.takeovered')           -> 222;
reserved_idx('session.discarded')            -> 223;
reserved_idx('session.terminated')           -> 224;

reserved_idx(_)                              -> undefined.

