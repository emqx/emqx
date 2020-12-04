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

-module(emqx_sn_gateway).

-behaviour(gen_statem).

-include("emqx_sn.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%% API.
-export([start_link/3]).

-export([ info/1
        , stats/1
        ]).

-export([call/2]).

%% SUB/UNSUB Asynchronously, called by plugins.
-export([ subscribe/2
        , unsubscribe/2
        ]).

-export([kick/1]).

%% state functions
-export([ idle/3
        , wait_for_will_topic/3
        , wait_for_will_msg/3
        , connected/3
        , asleep/3
        , awake/3
        ]).

%% gen_statem callbacks
-export([ init/1
        , callback_mode/0
        , handle_event/4
        , terminate/3
        , code_change/4
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-type(maybe(T) :: T | undefined).

-record(will_msg, {retain = false  :: boolean(),
                   qos    = ?QOS_0 :: emqx_mqtt_types:qos(),
                   topic           :: maybe(binary()),
                   payload         :: maybe(binary())
                  }).

-record(state, {gwid                 :: integer(),
                socket               :: port(),
                sockpid              :: pid(),
                sockstate            :: emqx_types:sockstate(),
                sockname             :: {inet:ip_address(), inet:port()},
                peername             :: {inet:ip_address(), inet:port()},
                channel              :: emqx_channel:channel(),
                registry             :: emqx_sn_registry:registry(),
                clientid             :: maybe(binary()),
                username             :: maybe(binary()),
                password             :: maybe(binary()),
                will_msg             :: maybe(#will_msg{}),
                keepalive_interval   :: maybe(integer()),
                connpkt              :: term(),
                asleep_timer         :: tuple(),
                asleep_msg_queue     :: list(),
                enable_stats         :: boolean(),
                stats_timer          :: maybe(reference()),
                idle_timeout         :: integer(),
                enable_qos3 = false  :: boolean(),
                transform            :: fun((emqx_types:packet(), #state{}) -> tuple())
               }).

-define(INFO_KEYS, [socktype, peername, sockname, sockstate]). %, active_n]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(STAT_TIMEOUT, 10000).
-define(IDLE_TIMEOUT, 30000).
-define(DEFAULT_CHAN_OPTIONS, [{max_packet_size, 256}, {zone, external}]).
-define(LOG(Level, Format, Args, State),
        emqx_logger:Level("MQTT-SN(~s): " ++ Format, [esockd:format(State#state.peername) | Args])).

-define(NEG_QOS_CLIENT_ID, <<"NegQoS-Client">>).

-define(NO_PEERCERT, undefined).


%%--------------------------------------------------------------------
%% Exported APIs
%%--------------------------------------------------------------------

start_link(Transport, Peername, Options) ->
    gen_statem:start_link(?MODULE, [Transport, Peername, Options], [{hibernate_after, 60000}]).

subscribe(GwPid, TopicTable) ->
    gen_statem:cast(GwPid, {subscribe, TopicTable}).

unsubscribe(GwPid, Topics) ->
    gen_statem:cast(GwPid, {unsubscribe, Topics}).

kick(GwPid) ->
    gen_statem:call(GwPid, kick).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

init([{_, SockPid, Sock}, Peername, Options]) ->
    GwId = proplists:get_value(gateway_id, Options),
    Registry = proplists:get_value(registry, Options),
    Username = proplists:get_value(username, Options, undefined),
    Password = proplists:get_value(password, Options, undefined),
    EnableQos3 = proplists:get_value(enable_qos3, Options, false),
    IdleTimeout = proplists:get_value(idle_timeout, Options, 30000),
    EnableStats = proplists:get_value(enable_stats, Options, false),
    case inet:sockname(Sock) of
        {ok, Sockname} ->
            Channel = emqx_channel:init(#{sockname => Sockname,
                                          peername => Peername,
                                          protocol => 'mqtt-sn',
                                          peercert => ?NO_PEERCERT,
                                          conn_mod => ?MODULE
                                         }, ?DEFAULT_CHAN_OPTIONS),
            State = #state{gwid             = GwId,
                           username         = Username,
                           password         = Password,
                           socket           = Sock,
                           sockstate        = running,
                           sockpid          = SockPid,
                           sockname         = Sockname,
                           peername         = Peername,
                           channel          = Channel,
                           registry         = Registry,
                           asleep_timer     = emqx_sn_asleep_timer:init(),
                           asleep_msg_queue = [],
                           enable_stats     = EnableStats,
                           enable_qos3      = EnableQos3,
                           idle_timeout     = IdleTimeout,
                           transform        = transform_fun()
                          },
            {ok, idle, State, [IdleTimeout]};
        {error, Reason} when Reason =:= enotconn;
                             Reason =:= einval;
                             Reason =:= closed ->
            {stop, normal};
        {error, Reason} -> {stop, Reason}
    end.

callback_mode() -> state_functions.

idle(cast, {incoming, ?SN_SEARCHGW_MSG(_Radius)}, State = #state{gwid = GwId}) ->
    send_message(?SN_GWINFO_MSG(GwId, <<>>), State),
    {keep_state, State, State#state.idle_timeout};

idle(cast, {incoming, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId)}, State) ->
    #mqtt_sn_flags{will = Will, clean_start = CleanStart} = Flags,
    do_connect(ClientId, CleanStart, Will, Duration, State);

idle(cast, {incoming, Packet = ?CONNECT_PACKET(_ConnPkt)}, State) ->
    handle_incoming(Packet, State);

idle(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, State) ->
    % ignore
    {keep_state, State, State#state.idle_timeout};

idle(cast, {incoming, ?SN_DISCONNECT_MSG(_Duration)}, State) ->
    % ignore
    {keep_state, State, State#state.idle_timeout};

idle(cast, {incoming, ?SN_PUBLISH_MSG(_Flag, _TopicId, _MsgId, _Data)}, State = #state{enable_qos3 = false}) ->
    ?LOG(debug, "The enable_qos3 is false, ignore the received publish with QoS=-1 in idle mode!", [], State),
    {keep_state_and_data, State#state.idle_timeout};

idle(cast, {incoming, ?SN_PUBLISH_MSG(#mqtt_sn_flags{qos = ?QOS_NEG1,
                                                     topic_id_type = TopicIdType
                                                    }, TopicId, _MsgId, Data)},
     State = #state{clientid = ClientId, registry = Registry}) ->
    TopicName = case (TopicIdType =:= ?SN_SHORT_TOPIC) of
                    false ->
                        emqx_sn_registry:lookup_topic(Registry, ClientId, TopicId);
                    true  -> <<TopicId:16>>
                end,
    Msg = emqx_message:make({?NEG_QOS_CLIENT_ID, State#state.username},
                            ?QOS_0, TopicName, Data),
    (TopicName =/= undefined) andalso emqx_broker:publish(Msg),
    ?LOG(debug, "Client id=~p receives a publish with QoS=-1 in idle mode!", [ClientId], State),
    {keep_state_and_data, State#state.idle_timeout};

idle(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(_ClientId)}, State) ->
    handle_ping(PingReq, State);

idle(cast, {outgoing, Packet}, State) ->
    ok = handle_outgoing(Packet, State),
    {keep_state, State};

idle(cast, {connack, ConnAck}, State) ->
    ok = handle_outgoing(ConnAck, State),
    {next_state, connected, State};

idle(timeout, _Timeout, State) ->
    stop({shutdown, idle_timeout}, State);

idle(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, idle, State).

wait_for_will_topic(cast, {incoming, ?SN_WILLTOPIC_EMPTY_MSG}, State = #state{connpkt = ConnPkt}) ->
    %% 6.3:
    %% Note that if a client wants to delete only its Will data at connection setup,
    %% it could send a CONNECT message with 'CleanSession=false' and 'Will=true',
    %% and sends an empty WILLTOPIC message to the GW when prompted to do so
    NState = State#state{will_msg = undefined},
    handle_incoming(?CONNECT_PACKET(ConnPkt), NState);

wait_for_will_topic(cast, {incoming, ?SN_WILLTOPIC_MSG(Flags, Topic)}, State) ->
    #mqtt_sn_flags{qos = QoS, retain = Retain} = Flags,
    WillMsg = #will_msg{retain = Retain, qos = QoS, topic = Topic},
    send_message(?SN_WILLMSGREQ_MSG(), State),
    {next_state, wait_for_will_msg, State#state{will_msg = WillMsg}};

wait_for_will_topic(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, _State) ->
    % ignore
    keep_state_and_data;

wait_for_will_topic(cast, {incoming, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId)}, State) ->
    do_2nd_connect(Flags, Duration, ClientId, State);

wait_for_will_topic(cast, {outgoing, Packet}, State) ->
    ok = handle_outgoing(Packet, State),
    {keep_state, State};

wait_for_will_topic(cast, {connack, ConnAck}, State) ->
    ok = handle_outgoing(ConnAck, State),
    {next_state, connected, State};

wait_for_will_topic(cast, Event, State) ->
    ?LOG(error, "wait_for_will_topic UNEXPECTED Event: ~p", [Event], State),
    keep_state_and_data;

wait_for_will_topic(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, wait_for_will_topic, State).

wait_for_will_msg(cast, {incoming, ?SN_WILLMSG_MSG(Payload)},
                  State = #state{will_msg = WillMsg, connpkt = ConnPkt}) ->
    NState = State#state{will_msg = WillMsg#will_msg{payload = Payload}},
    handle_incoming(?CONNECT_PACKET(ConnPkt), NState);

wait_for_will_msg(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, _State) ->
    % ignore
    keep_state_and_data;

%% XXX: ?? Why we will handling the 2nd CONNECT packet ??
wait_for_will_msg(cast, {incoming, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId)}, State) ->
    do_2nd_connect(Flags, Duration, ClientId, State);

wait_for_will_msg(cast, {outgoing, Packet}, State) ->
    ok = handle_outgoing(Packet, State),
    {keep_state, State};

wait_for_will_msg(cast, {connack, ConnAck}, State) ->
    ok = handle_outgoing(ConnAck, State),
    {next_state, connected, State};

wait_for_will_msg(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, wait_for_will_msg, State).

connected(cast, {incoming, ?SN_REGISTER_MSG(_TopicId, MsgId, TopicName)},
          State = #state{clientid = ClientId, registry = Registry}) ->
    case emqx_sn_registry:register_topic(Registry, ClientId, TopicName) of
        TopicId when is_integer(TopicId) ->
            ?LOG(debug, "register ClientId=~p, TopicName=~p, TopicId=~p", [ClientId, TopicName, TopicId], State),
            send_message(?SN_REGACK_MSG(TopicId, MsgId, ?SN_RC_ACCEPTED), State);
        {error, too_large} ->
            ?LOG(error, "TopicId is full! ClientId=~p, TopicName=~p", [ClientId, TopicName], State),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_NOT_SUPPORTED), State);
        {error, wildcard_topic} ->
            ?LOG(error, "wildcard topic can not be registered! ClientId=~p, TopicName=~p", [ClientId, TopicName], State),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_NOT_SUPPORTED), State)
    end,
    {keep_state, State};

connected(cast, {incoming, ?SN_PUBLISH_MSG(Flags, TopicId, MsgId, Data)},
          State = #state{enable_qos3 = EnableQoS3}) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType, qos = QoS} = Flags,
    Skip = (EnableQoS3 =:= false) andalso (QoS =:= ?QOS_NEG1),
    case Skip of
        true  ->
            ?LOG(debug, "The enable_qos3 is false, ignore the received publish with QoS=-1 in connected mode!", [], State),
            {keep_state, State};
        false ->
            do_publish(TopicIdType, TopicId, Data, Flags, MsgId, State)
    end;

connected(cast, {incoming, ?SN_PUBACK_MSG(TopicId, MsgId, RC)}, State) ->
    do_puback(TopicId, MsgId, RC, connected, State);

connected(cast, {incoming, ?SN_PUBREC_MSG(PubRec, MsgId)}, State)
    when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    do_pubrec(PubRec, MsgId, State);

connected(cast, {incoming, ?SN_SUBSCRIBE_MSG(Flags, MsgId, TopicId)}, State) ->
    #mqtt_sn_flags{qos = QoS, topic_id_type = TopicIdType} = Flags,
    handle_subscribe(TopicIdType, TopicId, QoS, MsgId, State);

connected(cast, {incoming, ?SN_UNSUBSCRIBE_MSG(Flags, MsgId, TopicId)}, State) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType} = Flags,
    handle_unsubscribe(TopicIdType, TopicId, MsgId, State);

connected(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(_ClientId)}, State) ->
    handle_ping(PingReq, State);

connected(cast, {incoming, ?SN_REGACK_MSG(_TopicId, _MsgId, ?SN_RC_ACCEPTED)}, State) ->
    {keep_state, State};
connected(cast, {incoming, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode)}, State) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p",
         [TopicId, MsgId, ReturnCode], State),
    {keep_state, State};

connected(cast, {incoming, ?SN_DISCONNECT_MSG(Duration)}, State) ->
    ok = send_message(?SN_DISCONNECT_MSG(undefined), State),
    case Duration of
        undefined ->
            handle_incoming(?DISCONNECT_PACKET(), State);
        _Other -> goto_asleep_state(Duration, State)
    end;

connected(cast, {incoming, ?SN_WILLTOPICUPD_MSG(Flags, Topic)}, State = #state{will_msg = WillMsg}) ->
    WillMsg1 = case Topic of
                   undefined -> undefined;
                   _         -> update_will_topic(WillMsg, Flags, Topic)
               end,
    send_message(?SN_WILLTOPICRESP_MSG(0), State),
    {keep_state, State#state{will_msg = WillMsg1}};

connected(cast, {incoming, ?SN_WILLMSGUPD_MSG(Payload)}, State = #state{will_msg = WillMsg}) ->
    ok = send_message(?SN_WILLMSGRESP_MSG(0), State),
    {keep_state, State#state{will_msg = update_will_msg(WillMsg, Payload)}};

connected(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, State) ->
    % ignore
    {keep_state, State};

connected(cast, {incoming, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId)}, State) ->
    do_2nd_connect(Flags, Duration, ClientId, State);

connected(cast, {outgoing, Packet}, State) ->
    ok = handle_outgoing(Packet, State),
    {keep_state, State};

%% XXX: It's so strange behavoir!!!
connected(cast, {connack, ConnAck}, State) ->
    ok = handle_outgoing(ConnAck, State),
    {keep_state, State};

connected(cast, {shutdown, Reason, Packet}, State) ->
    ok = handle_outgoing(Packet, State),
    {stop, {shutdown, Reason}, State};

connected(cast, {shutdown, Reason}, State) ->
    {stop, {shutdown, Reason}, State};

connected(cast, {close, Reason}, State) ->
    ?LOG(debug, "Force to close the socket due to ~p", [Reason], State),
    handle_info({sock_closed, Reason}, close_socket(State));

connected(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, connected, State).

asleep(cast, {incoming, ?SN_DISCONNECT_MSG(Duration)}, State) ->
    ok = send_message(?SN_DISCONNECT_MSG(undefined), State),
    case Duration of
        undefined ->
            handle_incoming(?PACKET(?DISCONNECT), State);
        _Other ->
            goto_asleep_state(Duration, State)
    end;

asleep(cast, {incoming, ?SN_PINGREQ_MSG(undefined)}, State) ->
    % ClientId in PINGREQ is mandatory
    {keep_state, State};

asleep(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(ClientIdPing)},
       State = #state{clientid = ClientId}) ->
    case ClientIdPing of
        ClientId ->
            _ = handle_ping(PingReq, State),
            self() ! do_awake_jobs,
            % it is better to go awake state, since the jobs in awake may take long time
            % and asleep timer get timeout, it will cause disaster
            {next_state, awake, State};
        _Other   ->
            {next_state, asleep, State}
    end;

asleep(cast, {incoming, ?SN_PUBACK_MSG(TopicId, MsgId, ReturnCode)}, State) ->
    do_puback(TopicId, MsgId, ReturnCode, asleep, State);

asleep(cast, {incoming, ?SN_PUBREC_MSG(PubRec, MsgId)}, State)
  when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    do_pubrec(PubRec, MsgId, State);

% NOTE: what about following scenario:
%    1) client go to sleep
%    2) client reboot for manual reset or other reasons
%    3) client send a CONNECT
%    4) emq-sn regard this CONNECT as a signal to connected state, not a bootup CONNECT. For this reason, will procedure is lost
% this should be a bug in mqtt-sn channel.
asleep(cast, {incoming, ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId)},
       State = #state{keepalive_interval = _Interval}) ->
    % device wakeup and goto connected state
    % keepalive timer may timeout in asleep state and delete itself, need to restart keepalive
    % TODO: Fixme later.
    %% self() ! {keepalive, start, Interval},
    send_connack(State),
    {next_state, connected, State};

asleep(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, asleep, State).

awake(cast, {incoming, ?SN_REGACK_MSG(_TopicId, _MsgId, ?SN_RC_ACCEPTED)}, State) ->
    {keep_state, State};

awake(cast, {incoming, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode)}, State) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p",
         [TopicId, MsgId, ReturnCode], State),
    {keep_state, State};

awake(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(_ClientId)}, State) ->
    handle_ping(PingReq, State);

awake(cast, {outgoing, Packet}, State) ->
    ok = handle_outgoing(Packet, State),
    {keep_state, State};

awake(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, awake, State).

handle_event({call, From}, Req, _StateName, State) ->
    case handle_call(From, Req, State) of
        {reply, Reply, NState} ->
            gen_server:reply(From, Reply),
            {keep_state, NState};
        {stop, Reason, Reply, NState} ->
            case NState#state.sockstate of
                running ->
                    send_message(?SN_DISCONNECT_MSG(undefined), NState);
                _ -> ok
            end,
            gen_server:reply(From, Reply),
            stop(Reason, NState)
    end;

handle_event(info, {datagram, SockPid, Data}, StateName,
             State = #state{sockpid = SockPid, channel = _Channel}) ->
    ?LOG(debug, "RECV ~p", [Data], State),
    Oct = iolist_size(Data),
    emqx_pd:inc_counter(recv_oct, Oct),
    try emqx_sn_frame:parse(Data) of
        {ok, Msg} ->
            emqx_pd:inc_counter(recv_cnt, 1),
            ?LOG(info, "RECV ~s at state ~s",
                 [emqx_sn_frame:format(Msg), StateName], State),
            {keep_state, State, next_event({incoming, Msg})}
    catch
        error:Error:Stacktrace ->
            ?LOG(info, "Parse frame error: ~p at state ~s, Stacktrace: ~p",
                 [Error, StateName, Stacktrace], State),
            shutdown(frame_error, State)
    end;

handle_event(info, Deliver = {deliver, _Topic, Msg}, asleep,
             State = #state{asleep_msg_queue = AsleepMsgQ}) ->
    % section 6.14, Support of sleeping clients
    ?LOG(debug, "enqueue downlink message in asleep state Msg=~p", [Msg], State),
    {keep_state, State#state{asleep_msg_queue = [Deliver|AsleepMsgQ]}};

handle_event(info, Deliver = {deliver, _Topic, _Msg}, _StateName,
             State = #state{channel = Channel}) ->
    handle_return(emqx_channel:handle_deliver([Deliver], Channel), State);

handle_event(info, {redeliver, {?PUBREL, MsgId}}, _StateName, State) ->
    send_message(?SN_PUBREC_MSG(?SN_PUBREL, MsgId), State),
    {keep_state, State};

%% FIXME: Is not unused in v4.x
handle_event(info, {timeout, TRef, emit_stats}, _StateName,
             State = #state{channel = Channel}) ->
    case emqx_channel:info(clientinfo, Channel) of
        #{clientid := undefined} -> {keep_state, State};
        _ -> handle_timeout(TRef, {emit_stats, stats(State)}, State)
    end;

handle_event(info, {timeout, TRef, keepalive}, _StateName, State) ->
    RecvOct = emqx_pd:get_counter(recv_oct),
    handle_timeout(TRef, {keepalive, RecvOct}, State);

handle_event(info, {timeout, TRef, TMsg}, _StateName, State) ->
    handle_timeout(TRef, TMsg, State);

handle_event(info, do_awake_jobs, StateName, State=#state{clientid = ClientId}) ->
    ?LOG(debug, "Do awake jobs, statename : ~p", [StateName], State),
    case process_awake_jobs(ClientId, State) of
        {keep_state, NewState} ->
            case StateName of
                awake  -> goto_asleep_state(NewState);
                _Other -> {keep_state, NewState}
                          %% device send a CONNECT immediately before this do_awake_jobs is handled
            end;
        Stop -> Stop
    end;

handle_event(info, asleep_timeout, asleep, State) ->
    ?LOG(debug, "asleep timer timeout, shutdown now", [], State),
    stop(asleep_timeout, State);

handle_event(info, asleep_timeout, StateName, State) ->
    ?LOG(debug, "asleep timer timeout on StateName=~p, ignore it", [StateName], State),
    {keep_state, State};

handle_event(cast, {event, connected}, _StateName, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:register_channel(ClientId, info(State), stats(State)),
    {keep_state, State};

handle_event(cast, {event, disconnected}, _StateName, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_info(ClientId, info(State)),
    emqx_cm:connection_closed(ClientId),
    {keep_state, State};

handle_event(cast, {event, _Other}, _StateName, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_info(ClientId, info(State)),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    {keep_state, State};

handle_event(EventType, EventContent, StateName, State) ->
    ?LOG(error, "StateName: ~s, Unexpected Event: ~p",
         [StateName, {EventType, EventContent}], State),
    {keep_state, State}.

terminate(Reason, _StateName, #state{clientid = ClientId,
                                     channel  = Channel,
                                     registry = Registry}) ->
    emqx_sn_registry:unregister_topic(Registry, ClientId),
    case {Channel, Reason} of
        {undefined, _} -> ok;
        {_, _} ->
            emqx_channel:terminate(Reason, Channel)
    end.

code_change(_Vsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%--------------------------------------------------------------------
%% Handle Call/Info
%%--------------------------------------------------------------------

handle_call(_From, info, State) ->
    {reply, info(State), State};

handle_call(_From, stats, State) ->
    {reply, stats(State), State};

handle_call(_From, Req, State = #state{channel = Channel}) ->
    case emqx_channel:handle_call(Req, Channel) of
        {reply, Reply, NChannel} ->
            {reply, Reply, State#state{channel = NChannel}};
        {shutdown, Reason, Reply, NChannel} ->
            shutdown(Reason, Reply, State#state{channel = NChannel});
        {shutdown, Reason, Reply, OutPacket, NChannel} ->
            NState = State#state{channel = NChannel},
            ok = handle_outgoing(OutPacket, NState),
            shutdown(Reason, Reply, NState)
    end.

handle_info(Info, State = #state{channel = Channel}) ->
   handle_return(emqx_channel:handle_info(Info, Channel), State).

handle_ping(_PingReq, State) ->
    emqx_pd:inc_counter(recv_oct, 2),
    emqx_pd:inc_counter(recv_msg, 1),
    ok = send_message(?SN_PINGRESP_MSG(), State),
    {keep_state, State}.

handle_timeout(TRef, TMsg, State = #state{channel = Channel}) ->
    handle_return(emqx_channel:handle_timeout(TRef, TMsg, Channel), State).

handle_return(ok, State) ->
    {keep_state, State};
handle_return({ok, NChannel}, State) ->
    {keep_state, State#state{channel = NChannel}};
handle_return({ok, Replies, NChannel}, State) ->
    {keep_state, State#state{channel = NChannel}, next_events(Replies)};

handle_return({shutdown, Reason, NChannel}, State) ->
    stop({shutdown, Reason}, State#state{channel = NChannel});
handle_return({shutdown, Reason, OutPacket, NChannel}, State) ->
    NState = State#state{channel = NChannel},
    ok = handle_outgoing(OutPacket, NState),
    stop({shutdown, Reason}, NState);
handle_return({stop, Reason, NChannel}, State) ->
    stop(Reason, State#state{channel = NChannel});
handle_return({stop, Reason, OutPacket, NChannel}, State) ->
    NState = State#state{channel = NChannel},
    ok = handle_outgoing(OutPacket, NState),
    stop(Reason, NState).

next_events(Packet) when is_record(Packet, mqtt_packet) ->
    next_event({outgoing, Packet});
next_events(Action) when is_tuple(Action) ->
    next_event(Action);
next_events(Actions) when is_list(Actions) ->
    lists:map(fun next_event/1, Actions).

close_socket(State = #state{sockstate = closed}) -> State;
close_socket(State = #state{socket = _Socket}) ->
    %ok = gen_udp:close(Socket),
    State#state{sockstate = closed}.

%%--------------------------------------------------------------------
%% Info & Stats
%%--------------------------------------------------------------------

%% @doc Get infos of the connection/channel.
info(CPid) when is_pid(CPid) ->
    call(CPid, info);
info(State = #state{channel = Channel}) ->
    ChanInfo = upgrade_infos(emqx_channel:info(Channel)),
    SockInfo = maps:from_list(
                 info(?INFO_KEYS, State)),
    ChanInfo#{sockinfo => SockInfo}.

info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(socktype, _State) ->
    udp;
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(sockstate, #state{sockstate = SockSt}) ->
    SockSt.

upgrade_infos(ChanInfo = #{conninfo := ConnInfo}) ->
    ChanInfo#{conninfo => ConnInfo#{proto_name => <<"MQTT-SN">>,
                                    proto_ver  => 1}}.

%% @doc Get stats of the connection/channel.
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#state{socket = Socket, channel = Channel}) ->
    SockStats = case inet:getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = emqx_channel:stats(Channel),
    ProcStats = emqx_misc:proc_stats(),
    lists:append([SockStats, ConnStats, ChanStats, ProcStats]).

call(Pid, Req) ->
    gen_server:call(Pid, Req, infinity).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

transform(?CONNACK_PACKET(0, _SessPresent), _FuncMsgIdToTopicId, _State) ->
    ?SN_CONNACK_MSG(0);

transform(?CONNACK_PACKET(_ReturnCode, _SessPresent), _FuncMsgIdToTopicId, _State) ->
    ?SN_CONNACK_MSG(?SN_RC_CONGESTION);

transform(?PUBLISH_PACKET(QoS, Topic, PacketId, Payload), _FuncMsgIdToTopicId, #state{registry = Registry}) ->
    NewPacketId = if
                      QoS =:= ?QOS_0 -> 0;
                      true           -> PacketId
                  end,
    ClientId = get(clientid),
    {TopicIdType, TopicContent} = case emqx_sn_registry:lookup_topic_id(Registry, ClientId, Topic) of
                                      {predef, PredefTopicId} ->
                                          {?SN_PREDEFINED_TOPIC, PredefTopicId};
                                      TopicId when is_integer(TopicId) ->
                                          {?SN_NORMAL_TOPIC, TopicId};
                                      undefined ->
                                          {?SN_SHORT_TOPIC, Topic}
                                  end,

    Flags = #mqtt_sn_flags{qos = QoS, topic_id_type = TopicIdType},
    ?SN_PUBLISH_MSG(Flags, TopicContent, NewPacketId, Payload);

transform(?PUBACK_PACKET(MsgId, _ReasonCode), FuncMsgIdToTopicId, _State) ->
    TopicIdFinal =  get_topic_id(puback, MsgId, FuncMsgIdToTopicId),
    ?SN_PUBACK_MSG(TopicIdFinal, MsgId, ?SN_RC_ACCEPTED);

transform(?PUBREC_PACKET(MsgId), _FuncMsgIdToTopicId, _State) ->
    ?SN_PUBREC_MSG(?SN_PUBREC, MsgId);

transform(?PUBREL_PACKET(MsgId), _FuncMsgIdToTopicId, _State) ->
    ?SN_PUBREC_MSG(?SN_PUBREL, MsgId);

transform(?PUBCOMP_PACKET(MsgId), _FuncMsgIdToTopicId, _State) ->
    ?SN_PUBREC_MSG(?SN_PUBCOMP, MsgId);

transform(?SUBACK_PACKET(MsgId, ReturnCodes), FuncMsgIdToTopicId, _State)->
    % if success, suback is sent by handle_info({suback, MsgId, [GrantedQoS]}, ...)
    % if failure, suback is sent in this function.
    [ReturnCode | _ ] = ReturnCodes,
    {QoS, TopicId, NewReturnCode}
        = case ?IS_QOS(ReturnCode) of
              true ->
                  {ReturnCode, get_topic_id(suback, MsgId, FuncMsgIdToTopicId), ?SN_RC_ACCEPTED};
              _ ->
                  {?QOS_0, get_topic_id(suback, MsgId, FuncMsgIdToTopicId), ?SN_RC_NOT_SUPPORTED}
          end,
    Flags = #mqtt_sn_flags{qos = QoS},
    ?SN_SUBACK_MSG(Flags, TopicId, MsgId, NewReturnCode);

transform(?UNSUBACK_PACKET(MsgId), _FuncMsgIdToTopicId, _State)->
    ?SN_UNSUBACK_MSG(MsgId).

send_register(TopicName, TopicId, MsgId, State) ->
    send_message(?SN_REGISTER_MSG(TopicId, MsgId, TopicName), State).

send_connack(State) ->
    send_message(?SN_CONNACK_MSG(?SN_RC_ACCEPTED), State).

send_message(Msg = #mqtt_sn_message{type = Type},
             State = #state{sockpid = SockPid, peername = Peername}) ->
    ?LOG(debug, "SEND ~s~n", [emqx_sn_frame:format(Msg)], State),
    inc_outgoing_stats(Type),
    Data = emqx_sn_frame:serialize(Msg),
    ok = emqx_metrics:inc('bytes.sent', iolist_size(Data)),
    SockPid ! {datagram, Peername, Data},
    ok.

goto_asleep_state(State) ->
    goto_asleep_state(undefined, State).
goto_asleep_state(Duration, State=#state{asleep_timer = AsleepTimer}) ->
    ?LOG(debug, "goto_asleep_state Duration=~p", [Duration], State),
    NewTimer = emqx_sn_asleep_timer:ensure(Duration, AsleepTimer),
    {next_state, asleep, State#state{asleep_timer = NewTimer}, hibernate}.

%%--------------------------------------------------------------------
%% Helper funcs
%%--------------------------------------------------------------------

-compile({inline, [shutdown/2, shutdown/3]}).
shutdown(Reason, State) ->
    ?LOG(error, "shutdown due to ~p", [Reason], State),
    stop({shutdown, Reason}, State).

shutdown(Reason, Reply, State) ->
    ?LOG(error, "shutdown due to ~p", [Reason], State),
    stop({shutdown, Reason}, Reply, State).

-compile({inline, [stop/2, stop/3]}).
stop(Reason, State) ->
    case Reason of
        %% FIXME: The Will-Msg should publish when a Session terminated!
        asleep_timeout                    -> do_publish_will(State);
        {shutdown, keepalive_timeout}     -> do_publish_will(State);
        _                                 -> ok
    end,
    {stop, Reason, State}.

stop(Reason, Reply, State) ->
    {stop, Reason, Reply, State}.

mqttsn_to_mqtt(?SN_PUBACK, MsgId)  ->
    ?PUBACK_PACKET(MsgId);
mqttsn_to_mqtt(?SN_PUBREC, MsgId)  ->
    ?PUBREC_PACKET(MsgId);
mqttsn_to_mqtt(?SN_PUBREL, MsgId)  ->
    ?PUBREL_PACKET(MsgId);
mqttsn_to_mqtt(?SN_PUBCOMP, MsgId) ->
    ?PUBCOMP_PACKET(MsgId).

do_connect(ClientId, CleanStart, WillFlag, Duration, State) ->
    ConnPkt = #mqtt_packet_connect{clientid    = ClientId,
                                   clean_start = CleanStart,
                                   username    = State#state.username,
                                   password    = State#state.password,
                                   keepalive   = Duration
                                  },
    put(clientid, ClientId),
    case WillFlag of
        true -> send_message(?SN_WILLTOPICREQ_MSG(), State),
                NState = State#state{connpkt  = ConnPkt,
                                     clientid = ClientId,
                                     keepalive_interval = Duration
                                    },
                {next_state, wait_for_will_topic, NState};
        false ->
            NState = State#state{clientid = ClientId,
                                 keepalive_interval = Duration
                                },
            handle_incoming(?CONNECT_PACKET(ConnPkt), NState)
    end.

do_2nd_connect(Flags, Duration, ClientId, State = #state{sockname = Sockname,
                                                         peername = Peername,
                                                         clientid = OldClientId,
                                                         registry = Registry,
                                                         channel  = Channel}) ->
    #mqtt_sn_flags{will = Will, clean_start = CleanStart} = Flags,
    NChannel = case CleanStart of
                   true ->
                       emqx_channel:terminate(normal, Channel),
                       emqx_sn_registry:unregister_topic(Registry, OldClientId),
                       emqx_channel:init(#{socktype => udp,
                                           sockname => Sockname,
                                           peername => Peername,
                                           peercert => ?NO_PEERCERT,
                                           conn_mod => ?MODULE
                                          }, ?DEFAULT_CHAN_OPTIONS);
                   false -> Channel
               end,
    NState = State#state{channel = NChannel},
    do_connect(ClientId, CleanStart, Will, Duration, NState).

handle_subscribe(?SN_NORMAL_TOPIC, TopicName, QoS, MsgId,
                 State=#state{clientid = ClientId, registry = Registry}) ->
    case emqx_sn_registry:register_topic(Registry, ClientId, TopicName) of
        {error, too_large} ->
            ok = send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                             ?SN_INVALID_TOPIC_ID,
                                             MsgId,
                                             ?SN_RC_INVALID_TOPIC_ID), State),
            {keep_state, State};
        {error, wildcard_topic} ->
            proto_subscribe(TopicName, QoS, MsgId, ?SN_INVALID_TOPIC_ID, State);
        NewTopicId when is_integer(NewTopicId) ->
            proto_subscribe(TopicName, QoS, MsgId, NewTopicId, State)
    end;

handle_subscribe(?SN_PREDEFINED_TOPIC, TopicId, QoS, MsgId,
                 State = #state{clientid = ClientId, registry = Registry}) ->
    case emqx_sn_registry:lookup_topic(Registry, ClientId, TopicId) of
        undefined ->
            ok = send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                             TopicId,
                                             MsgId,
                                             ?SN_RC_INVALID_TOPIC_ID), State),
            {next_state, connected, State};
        PredefinedTopic ->
            proto_subscribe(PredefinedTopic, QoS, MsgId, TopicId, State)
    end;

handle_subscribe(?SN_SHORT_TOPIC, TopicId, QoS, MsgId, State) ->
    TopicName = case is_binary(TopicId) of
                    true  -> TopicId;
                    false -> <<TopicId:16>>
                end,
    proto_subscribe(TopicName, QoS, MsgId, ?SN_INVALID_TOPIC_ID, State);

handle_subscribe(_, _TopicId, QoS, MsgId, State) ->
    ok = send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                     ?SN_INVALID_TOPIC_ID,
                                     MsgId,
                                     ?SN_RC_INVALID_TOPIC_ID), State),
    {keep_state, State}.

handle_unsubscribe(?SN_NORMAL_TOPIC, TopicId, MsgId, State) ->
    proto_unsubscribe(TopicId, MsgId, State);

handle_unsubscribe(?SN_PREDEFINED_TOPIC, TopicId, MsgId,
                   State = #state{clientid = ClientId, registry = Registry}) ->
    case emqx_sn_registry:lookup_topic(Registry, ClientId, TopicId) of
        undefined ->
            ok = send_message(?SN_UNSUBACK_MSG(MsgId), State),
            {keep_state, State};
        PredefinedTopic ->
            proto_unsubscribe(PredefinedTopic, MsgId, State)
    end;

handle_unsubscribe(?SN_SHORT_TOPIC, TopicId, MsgId, State) ->
    TopicName = case is_binary(TopicId) of
                    true  -> TopicId;
                    false -> <<TopicId:16>>
                end,
    proto_unsubscribe(TopicName, MsgId, State);

handle_unsubscribe(_, _TopicId, MsgId, State) ->
    send_message(?SN_UNSUBACK_MSG(MsgId), State),
    {keep_state, State}.

do_publish(?SN_NORMAL_TOPIC, TopicName, Data, Flags, MsgId, State) ->
    %% XXX: Handle normal topic id as predefined topic id, to be compatible with paho mqtt-sn library
    <<TopicId:16>> = TopicName,
    do_publish(?SN_PREDEFINED_TOPIC, TopicId, Data, Flags, MsgId, State);
do_publish(?SN_PREDEFINED_TOPIC, TopicId, Data, Flags, MsgId,
           State=#state{clientid = ClientId, registry = Registry}) ->
    #mqtt_sn_flags{qos = QoS, dup = Dup, retain = Retain} = Flags,
    NewQoS = get_corrected_qos(QoS, State),
    case emqx_sn_registry:lookup_topic(Registry, ClientId, TopicId) of
        undefined ->
            (NewQoS =/= ?QOS_0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID), State),
            {keep_state, State};
        TopicName ->
            proto_publish(TopicName, Data, Dup, NewQoS, Retain, MsgId, TopicId, State)
    end;
do_publish(?SN_SHORT_TOPIC, STopicName, Data, Flags, MsgId, State) ->
    #mqtt_sn_flags{qos = QoS, dup = Dup, retain = Retain} = Flags,
    NewQoS = get_corrected_qos(QoS, State),
    <<TopicId:16>> = STopicName ,
    case emqx_topic:wildcard(STopicName) of
        true ->
            (NewQoS =/= ?QOS_0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_NOT_SUPPORTED), State),
            {keep_state, State};
        false ->
            proto_publish(STopicName, Data, Dup, NewQoS, Retain, MsgId, TopicId, State)
    end;
do_publish(_, TopicId, _Data, #mqtt_sn_flags{qos = QoS}, MsgId, State) ->
    (QoS =/= ?QOS_0) andalso send_message(?SN_PUBACK_MSG(TopicId, MsgId, ?SN_RC_NOT_SUPPORTED), State),
    {keep_state, State}.

do_publish_will(#state{will_msg = undefined}) ->
    ok;
do_publish_will(#state{will_msg = #will_msg{payload = undefined}}) ->
    ok;
do_publish_will(#state{will_msg = #will_msg{topic = undefined}}) ->
    ok;
do_publish_will(#state{channel = Channel, will_msg = WillMsg}) ->
    #will_msg{qos = QoS, retain = Retain, topic = Topic, payload = Payload} = WillMsg,
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = false,
                                                          qos = QoS, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = Topic, packet_id = 1000},
                           payload  = Payload},
    ClientInfo = emqx_channel:info(clientinfo, Channel),
    emqx_broker:publish(emqx_packet:to_message(ClientInfo, Publish)),
    ok.

do_puback(TopicId, MsgId, ReturnCode, _StateName,
          State=#state{clientid = ClientId, registry = Registry}) ->
    case ReturnCode of
        ?SN_RC_ACCEPTED ->
            handle_incoming(?PUBACK_PACKET(MsgId), State);
        ?SN_RC_INVALID_TOPIC_ID ->
            case emqx_sn_registry:lookup_topic(Registry, ClientId, TopicId) of
                undefined -> ok;
                TopicName ->
                    %%notice that this TopicName maybe normal or predefined,
                    %% involving the predefined topic name in register to enhance the gateway's robustness even inconsistent with MQTT-SN channels
                    send_register(TopicName, TopicId, MsgId, State),
                    {keep_state, State}
            end;
        _ ->
            ?LOG(error, "CAN NOT handle PUBACK ReturnCode=~p", [ReturnCode], State),
            {keep_state, State}
    end.

do_pubrec(PubRec, MsgId, State) ->
    handle_incoming(mqttsn_to_mqtt(PubRec, MsgId), State).

proto_subscribe(TopicName, QoS, MsgId, TopicId, State) ->
    ?LOG(debug, "subscribe Topic=~p, MsgId=~p, TopicId=~p",
         [TopicName, MsgId, TopicId], State),
    enqueue_msgid(suback, MsgId, TopicId),
    SubOpts = maps:put(qos, QoS, ?DEFAULT_SUBOPTS),
    handle_incoming(?SUBSCRIBE_PACKET(MsgId, [{TopicName, SubOpts}]), State).

proto_unsubscribe(TopicName, MsgId, State) ->
    ?LOG(debug, "unsubscribe Topic=~p, MsgId=~p", [TopicName, MsgId], State),
    handle_incoming(?UNSUBSCRIBE_PACKET(MsgId, [TopicName]), State).

proto_publish(TopicName, Data, Dup, QoS, Retain, MsgId, TopicId, State) ->
    (QoS =/= ?QOS_0) andalso enqueue_msgid(puback, MsgId, TopicId),
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = QoS, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId},
                           payload  = Data},
    ?LOG(debug, "[publish] Msg: ~p~n", [Publish], State),
    handle_incoming(Publish, State).

update_will_topic(undefined, #mqtt_sn_flags{qos = QoS, retain = Retain}, Topic) ->
    #will_msg{qos = QoS, retain = Retain, topic = Topic};
update_will_topic(Will=#will_msg{}, #mqtt_sn_flags{qos = QoS, retain = Retain}, Topic) ->
    Will#will_msg{qos = QoS, retain = Retain, topic = Topic}.

update_will_msg(undefined, Msg) ->
    #will_msg{payload = Msg};
update_will_msg(Will = #will_msg{}, Msg) ->
    Will#will_msg{payload = Msg}.

process_awake_jobs(_ClientId, State = #state{asleep_msg_queue = []}) ->
    {keep_state, State};
process_awake_jobs(_ClientId, State = #state{channel = Channel,
                                             asleep_msg_queue = AsleepMsgQ}) ->
    Delivers = lists:reverse(AsleepMsgQ),
    NState = State#state{asleep_msg_queue = []},
    Result = emqx_channel:handle_deliver(Delivers, Channel),
    handle_return(Result, NState).

enqueue_msgid(suback, MsgId, TopicId) ->
    put({suback, MsgId}, TopicId);
enqueue_msgid(puback, MsgId, TopicId) ->
    put({puback, MsgId}, TopicId).

dequeue_msgid(suback, MsgId) ->
    erase({suback, MsgId});
dequeue_msgid(puback, MsgId) ->
    erase({puback, MsgId}).

get_corrected_qos(?QOS_NEG1, State) ->
    ?LOG(debug, "Receive a publish with QoS=-1", [], State),
    ?QOS_0;

get_corrected_qos(QoS, _State) ->
    QoS.

get_topic_id(Type, MsgId, Func) ->
    case Func(Type, MsgId) of
        undefined -> 0;
        TopicId -> TopicId
    end.

handle_incoming(Packet = ?PACKET(Type), State = #state{channel = Channel}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)], State),
    Result = emqx_channel:handle_in(Packet, Channel),
    handle_return(Result, State).

handle_outgoing(Packets, State) when is_list(Packets) ->
    lists:foreach(fun(Packet) -> handle_outgoing(Packet, State) end, Packets);

handle_outgoing(PubPkt = ?PUBLISH_PACKET(QoS, TopicName, PacketId, Payload),
                State = #state{clientid = ClientId, registry = Registry, transform = Transform}) ->
    #mqtt_packet{header = #mqtt_packet_header{dup = Dup, retain = Retain}} = PubPkt,
    MsgId = message_id(PacketId),
    ?LOG(debug, "Handle outgoing: ~p", [PubPkt], State),

    (emqx_sn_registry:lookup_topic_id(Registry, ClientId, TopicName) == undefined)
        andalso (byte_size(TopicName) =/= 2)
            andalso register_and_notify_client(TopicName, Payload, Dup, QoS,
                                               Retain, MsgId, ClientId, State),
    send_message(Transform(PubPkt, State), State);


handle_outgoing(Packet, State = #state{transform = Transform}) ->
    send_message(Transform(Packet, State), State).

register_and_notify_client(TopicName, Payload, Dup, QoS, Retain, MsgId, ClientId,
                           State = #state{registry = Registry}) ->
    TopicId = emqx_sn_registry:register_topic(Registry, ClientId, TopicName),
    ?LOG(debug, "register TopicId=~p, TopicName=~p, Payload=~p, Dup=~p, QoS=~p, Retain=~p, MsgId=~p",
        [TopicId, TopicName, Payload, Dup, QoS, Retain, MsgId], State),
    send_register(TopicName, TopicId, MsgId, State).

message_id(undefined) ->
    rand:uniform(16#FFFF);
message_id(MsgId) -> MsgId.

transform_fun() ->
    FunMsgIdToTopicId = fun(Type, MsgId) -> dequeue_msgid(Type, MsgId) end,
    fun(Packet, State) -> transform(Packet, FunMsgIdToTopicId, State) end.

inc_incoming_stats(Type) ->
    emqx_pd:inc_counter(recv_pkt, 1),
    case Type == ?PUBLISH of
        true ->
            emqx_pd:inc_counter(recv_msg, 1),
            emqx_pd:inc_counter(incoming_pubs, 1);
        false -> ok
    end.

inc_outgoing_stats(Type) ->
    emqx_pd:inc_counter(send_pkt, 1),
    (Type == ?SN_PUBLISH)
        andalso emqx_pd:inc_counter(send_msg, 1).

next_event(Content) ->
    {next_event, cast, Content}.
