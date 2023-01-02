%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/logger.hrl").

-logger_header("[MQTT-SN]").

%% API.
-export([start_link/3]).

-export([ info/1
        , stats/1
        ]).

-export([ call/2
        , call/3
        ]).

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
        , registering/3
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

-type(pending_msgs() :: #{integer() => [#mqtt_sn_message{}]}).

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
                channel              :: maybe(emqx_channel:channel()),
                clientid             :: maybe(binary()),
                username             :: maybe(binary()),
                password             :: maybe(binary()),
                will_msg             :: maybe(#will_msg{}),
                keepalive_interval   :: maybe(integer()),
                connpkt              :: term(),
                asleep_timer         :: tuple(),
                enable_stats         :: boolean(),
                stats_timer          :: maybe(reference()),
                idle_timeout         :: integer(),
                enable_qos3 = false  :: boolean(),
                has_pending_pingresp = false :: boolean(),
                %% Store all qos0 messages for waiting REGACK
                %% Note: QoS1/QoS2 messages will kept inflight queue
                pending_topic_ids = #{} :: pending_msgs(),
                subs_resume         = false,
                waiting_sync_topics = [],
                previous_outgoings_and_state = undefined
               }).

-define(INFO_KEYS, [socktype, peername, sockname, sockstate]). %, active_n]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(STAT_TIMEOUT, 10000).
-define(IDLE_TIMEOUT, 30000).
-define(DEFAULT_CHAN_OPTIONS, [{max_packet_size, 256}, {zone, external}]).

-define(NEG_QOS_CLIENT_ID, <<"NegQoS-Client">>).

-define(NO_PEERCERT, undefined).

-define(CONN_INFO(Sockname, Peername),
    #{socktype => udp,
      sockname => Sockname,
      peername => Peername,
      protocol => 'mqtt-sn',
      peercert => ?NO_PEERCERT,
      conn_mod => ?MODULE
    }).

-define(is_non_error_reason(Reason),
        Reason =:= normal;
        Reason =:= idle_timeout;
        Reason =:= asleep_timeout;
        Reason =:= keepalive_timeout).

-define(RETRY_TIMEOUT, 5000).
-define(MAX_RETRY_TIMES, 3).

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
    Username = proplists:get_value(username, Options, undefined),
    Password = proplists:get_value(password, Options, undefined),
    EnableQos3 = proplists:get_value(enable_qos3, Options, false),
    IdleTimeout = proplists:get_value(idle_timeout, Options, 30000),
    SubsResume = proplists:get_value(subs_resume, Options, false),
    EnableStats = proplists:get_value(enable_stats, Options, false),
    case inet:sockname(Sock) of
        {ok, Sockname} ->
            Channel = emqx_channel:init(?CONN_INFO(Sockname, Peername), ?DEFAULT_CHAN_OPTIONS),
            State = #state{gwid             = GwId,
                           username         = Username,
                           password         = Password,
                           socket           = Sock,
                           sockstate        = running,
                           sockpid          = SockPid,
                           sockname         = Sockname,
                           peername         = Peername,
                           channel          = Channel,
                           asleep_timer     = emqx_sn_asleep_timer:init(),
                           enable_stats     = EnableStats,
                           enable_qos3      = EnableQos3,
                           idle_timeout     = IdleTimeout,
                           subs_resume      = SubsResume
                          },
            emqx_logger:set_metadata_peername(esockd:format(Peername)),
            {ok, idle, State, [IdleTimeout]};
        {error, Reason} when Reason =:= enotconn;
                             Reason =:= einval;
                             Reason =:= closed ->
            {stop, normal};
        {error, Reason} -> {stop, Reason}
    end.

callback_mode() -> state_functions.

idle(cast, {incoming, ?SN_SEARCHGW_MSG(_Radius)}, State = #state{gwid = GwId}) ->
    State0 = send_message(?SN_GWINFO_MSG(GwId, <<>>), State),
    {keep_state, State0, State0#state.idle_timeout};

idle(cast, {incoming, ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId)}, State) ->
    #mqtt_sn_flags{will = Will, clean_start = CleanStart} = Flags,
    do_connect(ClientId, CleanStart, Will, Duration, State);

idle(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, State) ->
    % ignore
    {keep_state, State, State#state.idle_timeout};

idle(cast, {incoming, ?SN_DISCONNECT_MSG(_Duration)}, State) ->
    % ignore
    {keep_state, State, State#state.idle_timeout};

idle(cast, {incoming, ?SN_PUBLISH_MSG(_Flag, _TopicId, _MsgId, _Data)}, State = #state{enable_qos3 = false}) ->
    ?LOG(debug, "The enable_qos3 is false, ignore the received publish with QoS=-1 in idle mode!"),
    {keep_state_and_data, State#state.idle_timeout};

idle(cast, {incoming, ?SN_PUBLISH_MSG(#mqtt_sn_flags{qos = ?QOS_NEG1,
                                                     topic_id_type = TopicIdType
                                                    }, TopicId, _MsgId, Data)},
    State = #state{clientid = ClientId}) ->
    TopicName = case (TopicIdType =:= ?SN_SHORT_TOPIC) of
                    false -> emqx_sn_registry:lookup_topic(ClientId, TopicId);
                    true  -> <<TopicId:16>>
                end,
    _ = case TopicName =/= undefined of
        true ->
            Msg = emqx_message:make(?NEG_QOS_CLIENT_ID, ?QOS_0, TopicName, Data),
            emqx_broker:publish(Msg);
        false ->
            ok
    end,
    ?LOG(debug, "Client id=~p receives a publish with QoS=-1 in idle mode!", [ClientId]),
    {keep_state_and_data, State#state.idle_timeout};

idle(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(_ClientId)}, State) ->
    handle_ping(PingReq, State);

idle(cast, {outgoing, Packet}, State) ->
    {keep_state, handle_outgoing(Packet, State)};

idle(cast, {connack, ConnAck}, State) ->
    {next_state, connected, handle_outgoing(ConnAck, State)};

idle(timeout, _Timeout, State) ->
    stop(idle_timeout, State);

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
    State0 = send_message(?SN_WILLMSGREQ_MSG(), State),
    {next_state, wait_for_will_msg, State0#state{will_msg = WillMsg}};

wait_for_will_topic(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, _State) ->
    % ignore
    keep_state_and_data;

wait_for_will_topic(cast, {incoming, ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId)}, _State) ->
    ?LOG(warning, "Receive connect packet in wait_for_will_topic state", []),
    keep_state_and_data;

wait_for_will_topic(cast, {outgoing, Packet}, State) ->
    {keep_state, handle_outgoing(Packet, State)};

wait_for_will_topic(cast, {connack, ConnAck}, State) ->
    {next_state, connected, handle_outgoing(ConnAck, State)};

wait_for_will_topic(cast, Event, _State) ->
    ?LOG(error, "wait_for_will_topic UNEXPECTED Event: ~p", [Event]),
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

wait_for_will_msg(cast, {incoming, ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId)}, _State) ->
    ?LOG(warning, "Receive connect packet in wait_for_will_msg state", []),
    keep_state_and_data;

wait_for_will_msg(cast, {outgoing, Packet}, State) ->
    {keep_state, handle_outgoing(Packet, State)};

wait_for_will_msg(cast, {connack, ConnAck}, State) ->
    {next_state, connected, handle_outgoing(ConnAck, State)};

wait_for_will_msg(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, wait_for_will_msg, State).

connected(cast, {incoming, ?SN_REGISTER_MSG(_TopicId, MsgId, TopicName)},
          State = #state{clientid = ClientId}) ->
    State0 =
    case emqx_sn_registry:register_topic(ClientId, TopicName) of
        TopicId when is_integer(TopicId) ->
            ?LOG(debug, "register ClientId=~p, TopicName=~p, TopicId=~p", [ClientId, TopicName, TopicId]),
            send_message(?SN_REGACK_MSG(TopicId, MsgId, ?SN_RC_ACCEPTED), State);
        {error, too_large} ->
            ?LOG(error, "TopicId is full! ClientId=~p, TopicName=~p", [ClientId, TopicName]),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_NOT_SUPPORTED), State);
        {error, wildcard_topic} ->
            ?LOG(error, "wildcard topic can not be registered! ClientId=~p, TopicName=~p", [ClientId, TopicName]),
            send_message(?SN_REGACK_MSG(?SN_INVALID_TOPIC_ID, MsgId, ?SN_RC_NOT_SUPPORTED), State)
    end,
    {keep_state, State0};

connected(cast, {incoming, ?SN_PUBLISH_MSG(Flags, TopicId, MsgId, Data)},
          State = #state{enable_qos3 = EnableQoS3}) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType, qos = QoS} = Flags,
    Skip = (EnableQoS3 =:= false) andalso (QoS =:= ?QOS_NEG1),
    case Skip of
        true  ->
            ?LOG(debug, "The enable_qos3 is false, ignore the received publish with QoS=-1 in connected mode!"),
            {keep_state, State};
        false ->
            do_publish(TopicIdType, TopicId, Data, Flags, MsgId, State)
    end;

connected(cast, {incoming, ?SN_PUBACK_MSG(TopicId, MsgId, RC)}, State) ->
    do_puback(TopicId, MsgId, RC, connected, State);

connected(cast, {incoming, ?SN_PUBREC_MSG(PubRec, MsgId)}, State)
    when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    do_pubrec(PubRec, MsgId, connected, State);

connected(cast, {incoming, ?SN_SUBSCRIBE_MSG(Flags, MsgId, TopicId)}, State) ->
    #mqtt_sn_flags{qos = QoS, topic_id_type = TopicIdType} = Flags,
    handle_subscribe(TopicIdType, TopicId, QoS, MsgId, State);

connected(cast, {incoming, ?SN_UNSUBSCRIBE_MSG(Flags, MsgId, TopicId)}, State) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType} = Flags,
    handle_unsubscribe(TopicIdType, TopicId, MsgId, State);

connected(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(_ClientId)}, State) ->
    handle_ping(PingReq, State);

connected(cast, {incoming, ?SN_REGACK_MSG(TopicId, _MsgId, ?SN_RC_ACCEPTED)}, State) ->
    {keep_state, replay_no_reg_pending_publishes(TopicId, State)};
connected(cast, {incoming, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode)}, State) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p",
         [TopicId, MsgId, ReturnCode]),
    {keep_state, State};

connected(cast, {incoming, ?SN_DISCONNECT_MSG(Duration)}, State) ->
    State0 = send_message(?SN_DISCONNECT_MSG(undefined), State),
    case Duration of
        undefined ->
            handle_incoming(?DISCONNECT_PACKET(), State0);
        _Other -> goto_asleep_state(Duration, State0)
    end;

connected(cast, {incoming, ?SN_WILLTOPICUPD_MSG(Flags, Topic)}, State = #state{will_msg = WillMsg}) ->
    WillMsg1 = case Topic of
                   undefined -> undefined;
                   _         -> update_will_topic(WillMsg, Flags, Topic)
               end,
    State0 = send_message(?SN_WILLTOPICRESP_MSG(0), State),
    {keep_state, State0#state{will_msg = WillMsg1}};

connected(cast, {incoming, ?SN_WILLMSGUPD_MSG(Payload)}, State = #state{will_msg = WillMsg}) ->
    State0 = send_message(?SN_WILLMSGRESP_MSG(0), State),
    {keep_state, State0#state{will_msg = update_will_msg(WillMsg, Payload)}};

connected(cast, {incoming, ?SN_ADVERTISE_MSG(_GwId, _Radius)}, State) ->
    % ignore
    {keep_state, State};

connected(cast, {incoming, ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId)}, _State) ->
    ?LOG(warning, "Receive connect packet in wait_for_will_topic state", []),
    keep_state_and_data;

connected(cast, {outgoing, Packet}, State) ->
    {keep_state, handle_outgoing(Packet, State)};

%% XXX: It's so strange behavoir!!!
connected(cast, {connack, ConnAck}, State) ->
    {keep_state, handle_outgoing(ConnAck, State)};

connected(cast, {register, TopicNames, BlockedOutgoins}, State) ->
    NState = State#state{
               waiting_sync_topics = TopicNames,
               previous_outgoings_and_state = {BlockedOutgoins, ?FUNCTION_NAME}
              },
    {next_state, registering, NState, [next_event(shooting)]};

connected(cast, {shutdown, Reason, Packet}, State) ->
    stop(Reason, handle_outgoing(Packet, State));

connected(cast, {shutdown, Reason}, State) ->
    stop(Reason, State);

connected(cast, {close, Reason}, State) ->
    ?LOG(debug, "Force to close the socket due to ~p", [Reason]),
    handle_info({sock_closed, Reason}, close_socket(State));

connected(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, connected, State).

registering(cast, shooting,
            State = #state{
                       channel = Channel,
                       waiting_sync_topics = [],
                       previous_outgoings_and_state = {Outgoings, StateName}}) ->
    Session = emqx_channel:get_session(Channel),
    ClientInfo = emqx_channel:info(clientinfo, Channel),
    {Outgoings2, NChannel} =
        case emqx_session:dequeue(ClientInfo, Session) of
            {ok, NSession} ->
                {[], emqx_channel:set_session(NSession, Channel)};
            {ok, Pubs, NSession} ->
                emqx_channel:do_deliver(
                  Pubs,
                  emqx_channel:set_session(NSession, Channel)
                 )
        end,
    NState = State#state{
               channel = NChannel,
               previous_outgoings_and_state = undefined},
    {next_state, StateName, NState, outgoing_events(Outgoings ++ Outgoings2)};

registering(cast, shooting,
            State = #state{
                       clientid = ClientId,
                       waiting_sync_topics = [TopicName | Remainings]}) ->
    TopicId = emqx_sn_registry:lookup_topic_id(ClientId, TopicName),
    NState = send_register(
               TopicName,
               TopicId,
               16#FFFF, %% FIXME: msgid ?
               State#state{waiting_sync_topics = [{TopicId, TopicName, 0} | Remainings]}
              ),
    {keep_state, NState, {{timeout, wait_regack}, ?RETRY_TIMEOUT, nocontent}};

registering(cast, {incoming, ?SN_REGACK_MSG(TopicId, _MsgId, ?SN_RC_ACCEPTED)},
            State = #state{waiting_sync_topics = [{TopicId, TopicName, _} | Remainings]}) ->
    ?LOG(debug, "Register topic name ~s with id ~w successfully!", [TopicName, TopicId]),
    {keep_state, State#state{waiting_sync_topics = Remainings}, [next_event(shooting)]};

registering(cast, {incoming, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode)},
            State = #state{waiting_sync_topics = [{TopicId, TopicName, _} | Remainings]}) ->
    ?LOG(error, "client does not accept register TopicName=~s, TopicId=~p, MsgId=~p, ReturnCode=~p",
         [TopicName, TopicId, MsgId, ReturnCode]),
    {keep_state, State#state{waiting_sync_topics = Remainings}, [next_event(shooting)]};

registering(cast, {incoming, Packet},
            State = #state{previous_outgoings_and_state = {_, StateName}})
  when is_record(Packet, mqtt_sn_message) ->
    apply(?MODULE, StateName, [cast, {incoming, Packet}, State]);

registering({timeout, wait_regack}, _,
            State = #state{waiting_sync_topics = [{TopicId, TopicName, Times} | Remainings]})
  when Times < ?MAX_RETRY_TIMES ->
    ?LOG(warning, "Waiting REGACK timeout for TopicName=~s, TopicId=~w, try it again(~w)",
                  [TopicName, TopicId, Times+1]),
    NState = send_register(
               TopicName,
               TopicId,
               16#FFFF, %% FIXME: msgid?
               State#state{waiting_sync_topics = [{TopicId, TopicName, Times + 1} | Remainings]}
              ),
    {keep_state, NState, {{timeout, wait_regack}, ?RETRY_TIMEOUT, nocontent}};

registering({timeout, wait_regack}, _,
            State = #state{waiting_sync_topics = [{TopicId, TopicName, ?MAX_RETRY_TIMES} | _]}) ->
    ?LOG(error, "Retry register TopicName=~s, TopicId=~w reached the max retry times",
                [TopicId, TopicName]),
    NState = send_message(?SN_DISCONNECT_MSG(undefined), State),
    stop(reached_max_retry_times, NState);

registering(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, ?FUNCTION_NAME, State).

asleep(cast, {incoming, ?SN_DISCONNECT_MSG(Duration)}, State) ->
    State0 = send_message(?SN_DISCONNECT_MSG(undefined), State),
    case Duration of
        undefined ->
            handle_incoming(?DISCONNECT_PACKET(), State0);
        _Other ->
            goto_asleep_state(Duration, State0)
    end;

asleep(cast, {incoming, ?SN_PINGREQ_MSG(undefined)}, State) ->
    % ClientId in PINGREQ is mandatory
    {keep_state, State};

asleep(cast, {incoming, ?SN_PINGREQ_MSG(ClientIdPing)},
       State = #state{clientid = ClientId, channel = Channel}) ->
    inc_ping_counter(),
    case ClientIdPing of
        ClientId ->
            case emqx_session:replay(emqx_channel:info(clientinfo, Channel),
                        emqx_channel:get_session(Channel)) of
                {ok, [], Session0} ->
                    State0 = send_message(?SN_PINGRESP_MSG(), State),
                    {keep_state, State0#state{
                        channel = emqx_channel:set_session(Session0, Channel)}};
                {ok, Publishes, Session0} ->
                    {Packets, Channel1} = emqx_channel:do_deliver(Publishes,
                        emqx_channel:set_session(Session0, Channel)),
                    {next_state, awake,
                        State#state{channel = Channel1, has_pending_pingresp = true},
                        outgoing_events(Packets ++ [try_goto_asleep])}
            end;
        _Other ->
            {next_state, asleep, State}
    end;

asleep(cast, {incoming, ?SN_PUBACK_MSG(TopicId, MsgId, ReturnCode)}, State) ->
    do_puback(TopicId, MsgId, ReturnCode, asleep, State);

asleep(cast, {incoming, ?SN_PUBREC_MSG(PubRec, MsgId)}, State)
  when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    do_pubrec(PubRec, MsgId, asleep, State);

% NOTE: what about following scenario:
%    1) client go to sleep
%    2) client reboot for manual reset or other reasons
%    3) client send a CONNECT
%    4) emq-sn regard this CONNECT as a signal to connected state, not a bootup CONNECT. For this reason, will procedure is lost
% this should be a bug in mqtt-sn channel.
asleep(cast, {incoming, ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId)},
       State = #state{channel = Channel, asleep_timer = Timer}) ->
    NChannel = emqx_channel:ensure_keepalive(#{}, Channel),
    emqx_sn_asleep_timer:cancel(Timer),
    {next_state, connected, send_connack(State#state{channel = NChannel,
                                                     asleep_timer = emqx_sn_asleep_timer:init()})};

asleep(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, asleep, State).

awake(cast, {incoming, ?SN_REGACK_MSG(TopicId, _MsgId, ?SN_RC_ACCEPTED)}, State) ->
    {keep_state, replay_no_reg_pending_publishes(TopicId, State)};

awake(cast, {incoming, ?SN_REGACK_MSG(TopicId, MsgId, ReturnCode)}, State) ->
    ?LOG(error, "client does not accept register TopicId=~p, MsgId=~p, ReturnCode=~p",
         [TopicId, MsgId, ReturnCode]),
    {keep_state, State};

awake(cast, {incoming, PingReq = ?SN_PINGREQ_MSG(_ClientId)}, State) ->
    handle_ping(PingReq, State);

awake(cast, {outgoing, Packet}, State) ->
    {keep_state, handle_outgoing(Packet, State)};

awake(cast, {incoming, ?SN_PUBACK_MSG(TopicId, MsgId, ReturnCode)}, State) ->
    do_puback(TopicId, MsgId, ReturnCode, awake, State);

awake(cast, {incoming, ?SN_PUBREC_MSG(PubRec, MsgId)}, State)
  when PubRec == ?SN_PUBREC; PubRec == ?SN_PUBREL; PubRec == ?SN_PUBCOMP ->
    do_pubrec(PubRec, MsgId, awake, State);

awake(cast, try_goto_asleep, State=#state{channel = Channel,
        has_pending_pingresp = PingPending}) ->
    Inflight = emqx_session:info(inflight, emqx_channel:get_session(Channel)),
    case emqx_inflight:size(Inflight) of
        0 when PingPending =:= true ->
            State0 = send_message(?SN_PINGRESP_MSG(), State),
            goto_asleep_state(State0#state{has_pending_pingresp = false});
        0 when PingPending =:= false ->
            goto_asleep_state(State);
        _Size ->
            keep_state_and_data
    end;

awake(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, awake, State).

handle_event({call, From}, Req, _StateName, State) ->
    case handle_call(From, Req, State) of
        {reply, Reply, NState} ->
            gen_server:reply(From, Reply),
            {keep_state, NState};
        {shutdown, Reason, Reply, NState} ->
            State0 = case NState#state.sockstate of
                running ->
                    send_message(?SN_DISCONNECT_MSG(undefined), NState);
                _ -> NState
            end,
            gen_server:reply(From, Reply),
            stop(Reason, State0)
    end;

handle_event(info, {datagram, SockPid, Data}, StateName,
             State = #state{sockpid = SockPid, channel = _Channel}) ->
    ?LOG(debug, "RECV ~0p", [Data]),
    Oct = iolist_size(Data),
    inc_counter(recv_oct, Oct),
    try emqx_sn_frame:parse(Data) of
        {ok, Msg} ->
            inc_counter(recv_cnt, 1),
            ?LOG(info, "RECV ~s at state ~s", [emqx_sn_frame:format(Msg), StateName]),
            {keep_state, State, next_event({incoming, Msg})}
    catch
        error:Error:Stacktrace ->
            ?LOG(info, "Parse frame error: ~p at state ~s, Stacktrace: ~p",
                 [Error, StateName, Stacktrace]),
            stop(frame_error, State)
    end;

handle_event(info, {deliver, _Topic, Msg}, StateName,
             State = #state{channel = Channel})
  when StateName == alseep;
       StateName == registering ->
    % section 6.14, Support of sleeping clients
    ?LOG(debug, "enqueue downlink message in ~s state, msg: ~0p",
                [StateName, Msg]),
    Session = emqx_session:enqueue(emqx_channel:info(clientinfo, Channel),
                Msg, emqx_channel:get_session(Channel)),
    {keep_state, State#state{channel = emqx_channel:set_session(Session, Channel)}};

handle_event(info, Deliver = {deliver, _Topic, _Msg}, _StateName,
             State = #state{channel = Channel}) ->
    handle_return(emqx_channel:handle_deliver([Deliver], Channel), State);

handle_event(info, {redeliver, {?PUBREL, MsgId}}, _StateName, State) ->
    {keep_state, send_message(?SN_PUBREC_MSG(?SN_PUBREL, MsgId), State)};

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

handle_event(info, asleep_timeout, asleep, State) ->
    ?LOG(debug, "asleep timer timeout, shutdown now"),
    stop(asleep_timeout, State);

handle_event(info, asleep_timeout, StateName, State) ->
    ?LOG(debug, "asleep timer timeout on StateName=~p, ignore it", [StateName]),
    {keep_state, State};

handle_event(cast, {close, Reason}, _StateName, State) ->
    stop(Reason, State);

handle_event(cast, {event, connected}, _StateName, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:insert_channel_info(ClientId, info(State), stats(State)),
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
    ?LOG(error, "StateName: ~s, Unexpected Event: ~0p",
         [StateName, {EventType, EventContent}]),
    {keep_state, State}.

terminate(Reason, _StateName, #state{channel  = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    case Reason of
        {shutdown, takeovered} ->
            ok;
        _ ->
            emqx_sn_registry:unregister_topic(ClientId)
        end,
    emqx_channel:terminate(Reason, Channel),
    ok.

%% in the emqx_sn:v4.3.6, we have added two new fields in the state last:
%%  - waiting_sync_topics
%%  - previous_outgoings_and_state
code_change({down, _Vsn}, StateName, State, [ToVsn]) ->
    case re:run(ToVsn, "4\\.3\\.[2-5]") of
        {match, _} ->
            NState0 = lists:droplast(
                        lists:droplast(
                          lists:droplast(tuple_to_list(State)))),
            NState = list_to_tuple(NState0),
            {ok, StateName, NState};
        _ ->
            {ok, StateName, State}
    end;

code_change(_Vsn, StateName, State, [FromVsn]) ->
    case re:run(FromVsn, "4\\.3\\.[2-5]") of
        {match, _} ->
            NState = list_to_tuple(
                       tuple_to_list(State) ++ [false, [], undefined]
                      ),
            {ok, StateName, NState};
        _ ->
            {ok, StateName, State}
    end.

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
            {shutdown, Reason, Reply, State#state{channel = NChannel}}
    end.

handle_info({sock_closed, Reason} = Info, State = #state{channel = Channel}) ->
    maybe_send_will_msg(Reason, State),
    handle_return(emqx_channel:handle_info(Info, Channel), State).

handle_timeout(TRef, TMsg, State = #state{channel = Channel}) ->
    handle_return(emqx_channel:handle_timeout(TRef, TMsg, Channel), State).

handle_return(Return, State) ->
    handle_return(Return, State, []).

handle_return({ok, NChannel}, State, AddEvents) ->
    handle_return({ok, AddEvents, NChannel}, State, []);
handle_return({ok, Replies, NChannel}, State, AddEvents) ->
    {keep_state, State#state{channel = NChannel}, outgoing_events(append(Replies, AddEvents))};
handle_return({shutdown, Reason, NChannel}, State, _AddEvents) ->
    stop(Reason, State#state{channel = NChannel});
handle_return({shutdown, Reason, OutPacket, NChannel}, State, _AddEvents) ->
    NState = State#state{channel = NChannel},
    stop(Reason, handle_outgoing(OutPacket, NState)).

outgoing_events(Actions) ->
    lists:map(fun outgoing_event/1, Actions).

outgoing_event(Packet) when is_record(Packet, mqtt_packet);
                            is_record(Packet, mqtt_sn_message)->
    next_event({outgoing, Packet});
outgoing_event(Action) ->
    next_event(Action).

next_event(Content) ->
    {next_event, cast, Content}.

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
    call(Pid, Req, infinity).

call(Pid, Req, Timeout) ->
    gen_server:call(Pid, Req, Timeout).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
handle_ping(_PingReq, State) ->
    State0 = send_message(?SN_PINGRESP_MSG(), State),
    inc_ping_counter(),
    {keep_state, State0}.

inc_ping_counter() ->
    inc_counter(recv_msg, 1).

mqtt2sn(?CONNACK_PACKET(0, _SessPresent),  _State) ->
    ?SN_CONNACK_MSG(0);

mqtt2sn(?CONNACK_PACKET(_ReturnCode, _SessPresent), _State) ->
    ?SN_CONNACK_MSG(?SN_RC_CONGESTION);

mqtt2sn(?PUBACK_PACKET(MsgId, _ReasonCode), _State) ->
    TopicIdFinal =  get_topic_id(puback, MsgId),
    ?SN_PUBACK_MSG(TopicIdFinal, MsgId, ?SN_RC_ACCEPTED);

mqtt2sn(?PUBREC_PACKET(MsgId, _ReturnCode), _State) ->
    ?SN_PUBREC_MSG(?SN_PUBREC, MsgId);
mqtt2sn(?PUBREL_PACKET(MsgId, _ReturnCode), _State) ->
    ?SN_PUBREC_MSG(?SN_PUBREL, MsgId);
mqtt2sn(?PUBCOMP_PACKET(MsgId, _ReturnCode), _State) ->
    ?SN_PUBREC_MSG(?SN_PUBCOMP, MsgId);

mqtt2sn(?UNSUBACK_PACKET(MsgId), _State)->
    ?SN_UNSUBACK_MSG(MsgId);

mqtt2sn(
  #mqtt_packet{header = #mqtt_packet_header{
                           type = ?PUBLISH,
                           qos = QoS,
                           %dup = Dup,
                           retain = Retain},
               variable = #mqtt_packet_publish{
                             topic_name = Topic,
                             packet_id = PacketId},
               payload = Payload}, #state{clientid = ClientId}) ->
    NPacketId = if QoS =:= ?QOS_0 -> 0;
                     true -> PacketId
                  end,
    {TopicIdType, TopicContent} = case emqx_sn_registry:lookup_topic_id(ClientId, Topic) of
                                      {predef, PredefTopicId} ->
                                          {?SN_PREDEFINED_TOPIC, PredefTopicId};
                                      TopicId when is_integer(TopicId) ->
                                          {?SN_NORMAL_TOPIC, TopicId};
                                      undefined ->
                                          {?SN_SHORT_TOPIC, Topic}
                                  end,

    Flags = #mqtt_sn_flags{
               %dup = Dup,
               qos = QoS,
               retain = Retain,
               topic_id_type = TopicIdType},
    ?SN_PUBLISH_MSG(Flags, TopicContent, NPacketId, Payload);

mqtt2sn(?SUBACK_PACKET(MsgId, ReturnCodes), _State)->
    % if success, suback is sent by handle_info({suback, MsgId, [GrantedQoS]}, ...)
    % if failure, suback is sent in this function.
    [ReturnCode | _ ] = ReturnCodes,
    {QoS, TopicId, NewReturnCode}
        = case ?IS_QOS(ReturnCode) of
              true ->
                  {ReturnCode, get_topic_id(suback, MsgId), ?SN_RC_ACCEPTED};
              _ ->
                  {?QOS_0, get_topic_id(suback, MsgId), ?SN_RC_NOT_SUPPORTED}
          end,
    Flags = #mqtt_sn_flags{qos = QoS},
    ?SN_SUBACK_MSG(Flags, TopicId, MsgId, NewReturnCode).

send_register(TopicName, TopicId, MsgId, State) ->
    send_message(?SN_REGISTER_MSG(TopicId, MsgId, TopicName), State).

send_connack(State) ->
    send_message(?SN_CONNACK_MSG(?SN_RC_ACCEPTED), State).

send_message(Msg = #mqtt_sn_message{type = Type},
             State = #state{sockpid = SockPid, peername = Peername}) ->
    ?LOG(info, "SEND ~s~n", [emqx_sn_frame:format(Msg)]),
    inc_outgoing_stats(Type),
    Data = emqx_sn_frame:serialize(Msg),
    ?LOG(debug, "SEND ~0p", [Data]),
    ok = emqx_metrics:inc('bytes.sent', iolist_size(Data)),
    SockPid ! {datagram, Peername, Data},
    State.

goto_asleep_state(State) ->
    goto_asleep_state(undefined, State).
goto_asleep_state(Duration, State=#state{asleep_timer = AsleepTimer,
                                         channel = Channel}) ->
    ?LOG(debug, "goto_asleep_state Duration=~p", [Duration]),
    NewTimer = emqx_sn_asleep_timer:ensure(Duration, AsleepTimer),
    NChannel = emqx_channel:clear_keepalive(Channel),
    {next_state, asleep, State#state{asleep_timer = NewTimer,
                                     channel = NChannel}, hibernate}.

%%--------------------------------------------------------------------
%% Helper funcs
%%--------------------------------------------------------------------
stop({shutdown, Reason}, State) ->
    stop(Reason, State);
stop(Reason, State) ->
    ?LOG(stop_log_level(Reason), "stop due to ~p", [Reason]),
    maybe_send_will_msg(Reason, State),
    {stop, {shutdown, Reason}, State}.

maybe_send_will_msg(normal, _State) ->
    ok;
maybe_send_will_msg(_Reason, State) ->
    do_publish_will(State).

stop_log_level(Reason) when ?is_non_error_reason(Reason) ->
    debug;
stop_log_level(_) ->
    error.

mqttsn_to_mqtt(?SN_PUBACK, MsgId)  ->
    ?PUBACK_PACKET(MsgId);
mqttsn_to_mqtt(?SN_PUBREC, MsgId)  ->
    ?PUBREC_PACKET(MsgId);
mqttsn_to_mqtt(?SN_PUBREL, MsgId)  ->
    ?PUBREL_PACKET(MsgId);
mqttsn_to_mqtt(?SN_PUBCOMP, MsgId) ->
    ?PUBCOMP_PACKET(MsgId).

do_connect(ClientId, CleanStart, WillFlag, Duration, State) ->
    emqx_logger:set_metadata_clientid(ClientId),
    %% 6.6 Client’s Publish Procedure
    %% At any point in time a client may have only one QoS level 1 or 2 PUBLISH message
    %% outstanding, i.e. it has to wait for the termination of this PUBLISH message exchange
    %% before it could start a new level 1 or 2 transaction.
    %%
    %% FIXME: But we should have a re-try timer to re-send the inflight
    %% qos1/qos2 message
    OnlyOneInflight = #{'Receive-Maximum' => 1},
    ConnPkt = #mqtt_packet_connect{clientid    = ClientId,
                                   clean_start = CleanStart,
                                   username    = State#state.username,
                                   password    = State#state.password,
                                   proto_name  = <<"MQTT-SN">>,
                                   keepalive   = Duration,
                                   properties  = OnlyOneInflight,
                                   proto_ver   = 1
                                  },
    case WillFlag of
        true -> State0 = send_message(?SN_WILLTOPICREQ_MSG(), State),
                NState = State0#state{connpkt  = ConnPkt,
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

handle_subscribe(?SN_NORMAL_TOPIC, TopicName, QoS, MsgId,
                 State=#state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    case emqx_sn_registry:register_topic(ClientId, TopicName) of
        {error, too_large} ->
            State0 = send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                             ?SN_INVALID_TOPIC_ID,
                                             MsgId,
                                             ?SN_RC_INVALID_TOPIC_ID), State),
            {keep_state, State0};
        {error, wildcard_topic} ->
            proto_subscribe(TopicName, QoS, MsgId, ?SN_INVALID_TOPIC_ID, State);
        NewTopicId when is_integer(NewTopicId) ->
            proto_subscribe(TopicName, QoS, MsgId, NewTopicId, State)
    end;

handle_subscribe(?SN_PREDEFINED_TOPIC, TopicId, QoS, MsgId,
                 State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            State0 = send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                             TopicId,
                                             MsgId,
                                             ?SN_RC_INVALID_TOPIC_ID), State),
            {next_state, connected, State0};
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
    State0 = send_message(?SN_SUBACK_MSG(#mqtt_sn_flags{qos = QoS},
                                     ?SN_INVALID_TOPIC_ID,
                                     MsgId,
                                     ?SN_RC_INVALID_TOPIC_ID), State),
    {keep_state, State0}.

handle_unsubscribe(?SN_NORMAL_TOPIC, TopicId, MsgId, State) ->
    proto_unsubscribe(TopicId, MsgId, State);

handle_unsubscribe(?SN_PREDEFINED_TOPIC, TopicId, MsgId,
                   State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            {keep_state, send_message(?SN_UNSUBACK_MSG(MsgId), State)};
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
    {keep_state, send_message(?SN_UNSUBACK_MSG(MsgId), State)}.

do_publish(?SN_NORMAL_TOPIC, TopicName, Data, Flags, MsgId, State) ->
    %% XXX: Handle normal topic id as predefined topic id, to be compatible with paho mqtt-sn library
    <<TopicId:16>> = TopicName,
    do_publish(?SN_PREDEFINED_TOPIC, TopicId, Data, Flags, MsgId, State);
do_publish(?SN_PREDEFINED_TOPIC, TopicId, Data, Flags, MsgId,
           State=#state{channel = Channel}) ->
    #mqtt_sn_flags{qos = QoS, dup = Dup, retain = Retain} = Flags,
    NewQoS = get_corrected_qos(QoS),
    ClientId = emqx_channel:info(clientid, Channel),
    case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
        undefined ->
            {keep_state, maybe_send_puback(NewQoS, TopicId, MsgId, ?SN_RC_INVALID_TOPIC_ID,
                State)};
        TopicName ->
            proto_publish(TopicName, Data, Dup, NewQoS, Retain, MsgId, TopicId, State)
    end;

do_publish(?SN_SHORT_TOPIC, STopicName, Data, Flags, MsgId, State) ->
    #mqtt_sn_flags{qos = QoS, dup = Dup, retain = Retain} = Flags,
    NewQoS = get_corrected_qos(QoS),
    <<TopicId:16>> = STopicName,
    case emqx_topic:wildcard(STopicName) of
        true ->
            {keep_state, maybe_send_puback(NewQoS, TopicId, MsgId, ?SN_RC_NOT_SUPPORTED,
                State)};
        false ->
            proto_publish(STopicName, Data, Dup, NewQoS, Retain, MsgId, TopicId, State)
    end;
do_publish(_, TopicId, _Data, #mqtt_sn_flags{qos = QoS}, MsgId, State) ->
    {keep_state, maybe_send_puback(QoS, TopicId, MsgId, ?SN_RC_NOT_SUPPORTED,
        State)}.

do_publish_will(#state{will_msg = undefined}) ->
    ok;
do_publish_will(#state{will_msg = #will_msg{payload = undefined}}) ->
    ok;
do_publish_will(#state{will_msg = #will_msg{topic = undefined}}) ->
    ok;
do_publish_will(#state{will_msg = WillMsg, clientid = ClientId}) ->
    #will_msg{qos = QoS, retain = Retain, topic = Topic, payload = Payload} = WillMsg,
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = false,
                                                          qos = QoS, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = Topic, packet_id = 1000},
                           payload  = Payload},
    _ = emqx_broker:publish(emqx_packet:to_message(Publish, ClientId)),
    ok.

do_puback(TopicId, MsgId, ReturnCode, StateName,
          State=#state{channel = Channel}) ->
    case ReturnCode of
        ?SN_RC_ACCEPTED ->
            handle_incoming(?PUBACK_PACKET(MsgId), StateName, State);
        ?SN_RC_INVALID_TOPIC_ID ->
            ClientId = emqx_channel:info(clientid, Channel),
            case emqx_sn_registry:lookup_topic(ClientId, TopicId) of
                undefined -> {keep_state, State};
                TopicName ->
                    %% notice that this TopicName maybe normal or predefined,
                    %% involving the predefined topic name in register to
                    %% enhance the gateway's robustness even inconsistent
                    %% with MQTT-SN channel
                    {keep_state,
                     send_register(TopicName, TopicId, MsgId, State)}
            end;
        _ ->
            %% XXX: We need to handle others error code
            %% 'Rejection: congestion'
            ?LOG(error, "CAN NOT handle PUBACK ReturnCode=~p", [ReturnCode]),
            {keep_state, State}
    end.

do_pubrec(PubRec, MsgId, StateName, State) ->
    handle_incoming(mqttsn_to_mqtt(PubRec, MsgId), StateName, State).

proto_subscribe(TopicName, QoS, MsgId, TopicId, State) ->
    ?LOG(debug, "subscribe Topic=~p, MsgId=~p, TopicId=~p",
         [TopicName, MsgId, TopicId]),
    enqueue_msgid(suback, MsgId, TopicId),
    SubOpts = maps:put(qos, QoS, ?DEFAULT_SUBOPTS),
    handle_incoming(?SUBSCRIBE_PACKET(MsgId, [{TopicName, SubOpts}]), State).

proto_unsubscribe(TopicName, MsgId, State) ->
    ?LOG(debug, "unsubscribe Topic=~p, MsgId=~p", [TopicName, MsgId]),
    handle_incoming(?UNSUBSCRIBE_PACKET(MsgId, [TopicName]), State).

proto_publish(TopicName, Data, Dup, QoS, Retain, MsgId, TopicId, State) ->
    (QoS =/= ?QOS_0) andalso enqueue_msgid(puback, MsgId, TopicId),
    Publish = #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH, dup = Dup, qos = QoS, retain = Retain},
                           variable = #mqtt_packet_publish{topic_name = TopicName, packet_id = MsgId},
                           payload  = Data},
    ?LOG(debug, "[publish] Msg: ~0p~n", [Publish]),
    handle_incoming(Publish, State).

update_will_topic(undefined, #mqtt_sn_flags{qos = QoS, retain = Retain}, Topic) ->
    #will_msg{qos = QoS, retain = Retain, topic = Topic};
update_will_topic(Will=#will_msg{}, #mqtt_sn_flags{qos = QoS, retain = Retain}, Topic) ->
    Will#will_msg{qos = QoS, retain = Retain, topic = Topic}.

update_will_msg(undefined, Msg) ->
    #will_msg{payload = Msg};
update_will_msg(Will = #will_msg{}, Msg) ->
    Will#will_msg{payload = Msg}.

enqueue_msgid(suback, MsgId, TopicId) ->
    put({suback, MsgId}, TopicId);
enqueue_msgid(puback, MsgId, TopicId) ->
    put({puback, MsgId}, TopicId).

dequeue_msgid(suback, MsgId) ->
    erase({suback, MsgId});
dequeue_msgid(puback, MsgId) ->
    erase({puback, MsgId}).

get_corrected_qos(?QOS_NEG1) ->
    ?LOG(debug, "Receive a publish with QoS=-1"),
    ?QOS_0;
get_corrected_qos(QoS) ->
    QoS.

get_topic_id(Type, MsgId) ->
    case dequeue_msgid(Type, MsgId) of
        undefined -> 0;
        TopicId -> TopicId
    end.

handle_incoming(Packet, State) ->
    handle_incoming(Packet, unknown, State).

handle_incoming(#mqtt_packet{variable = #mqtt_packet_puback{}} = Packet, awake, State) ->
    Result = channel_handle_in(Packet, State),
    handle_return(Result, State, [try_goto_asleep]);

handle_incoming(
  #mqtt_packet{
     variable = #mqtt_packet_connect{
                   clean_start = false}
    } = Packet,
  _,
  State = #state{subs_resume = SubsResume}) ->
    Result = channel_handle_in(Packet, State),
    case {SubsResume, Result} of
        {true, {ok, Replies, NChannel}} ->
            case maps:get(
                   subscriptions,
                   emqx_channel:info(session, NChannel)
                  ) of
                Subs when map_size(Subs) == 0 ->
                    handle_return(Result, State);
                Subs ->
                    TopicNames = lists:filter(
                                   fun(T) -> not emqx_topic:wildcard(T)
                                   end, maps:keys(Subs)),
                    {ConnackEvents, Outgoings} = split_connack_replies(
                                                   Replies),
                    Events = outgoing_events(
                               ConnackEvents ++
                               [{register, TopicNames, Outgoings}]
                              ),
                    {keep_state, State#state{channel = NChannel}, Events}
            end;
        _ ->
            handle_return(Result, State)
    end;

handle_incoming(Packet, _StName, State) ->
    Result = channel_handle_in(Packet, State),
    handle_return(Result, State).

channel_handle_in(Packet = ?PACKET(Type), #state{channel = Channel}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "Transed-RECV ~s", [emqx_packet:format(Packet)]),
    emqx_channel:handle_in(Packet, Channel).

handle_outgoing(Packets, State) when is_list(Packets) ->
    lists:foldl(fun(Packet, State0) ->
        handle_outgoing(Packet, State0)
    end, State, Packets);

handle_outgoing(PubPkt = ?PUBLISH_PACKET(_, TopicName, _, _),
                State = #state{channel = Channel}) ->
    ?LOG(debug, "Handle outgoing publish: ~0p", [PubPkt]),
    ClientId = emqx_channel:info(clientid, Channel),
    TopicId = emqx_sn_registry:lookup_topic_id(ClientId, TopicName),
    case (TopicId == undefined) andalso (byte_size(TopicName) =/= 2) of
        true ->
            %% TODO: only one REGISTER inflight if qos=0??
            register_and_notify_client(PubPkt, State);
        false -> send_message(mqtt2sn(PubPkt, State), State)
    end;

handle_outgoing(Packet, State) ->
    send_message(mqtt2sn(Packet, State), State).

cache_no_reg_publish_message(Pendings, TopicId, PubPkt, State) ->
    ?LOG(debug, "cache non-registered publish message for topic-id: ~p, msg: ~0p, pendings: ~0p",
        [TopicId, PubPkt, Pendings]),
    Msgs = maps:get(pending_topic_ids, Pendings, []),
    Pendings#{TopicId => Msgs ++ [mqtt2sn(PubPkt, State)]}.

replay_no_reg_pending_publishes(TopicId,
                                State0 = #state{
                                            pending_topic_ids = Pendings}) ->
    ?LOG(debug, "replay non-registered qos0 publish message for "
                "topic-id: ~p, pendings: ~0p", [TopicId, Pendings]),
    State = lists:foldl(fun(Msg, State1) ->
        send_message(Msg, State1)
    end, State0, maps:get(TopicId, Pendings, [])),

    NState = State#state{pending_topic_ids = maps:remove(TopicId, Pendings)},
    case replay_inflight_messages(TopicId, State#state.channel) of
        [] -> ok;
        Outgoings ->
            ?LOG(debug, "replay non-registered qos1/qos2 publish message "
                        "for topic-id: ~0p, messages: ~0p",
                        [TopicId, Outgoings]),
            handle_outgoing(Outgoings, NState)
    end.

replay_inflight_messages(TopicId, Channel) ->
    Inflight = emqx_session:info(inflight, emqx_channel:get_session(Channel)),

    case emqx_inflight:to_list(Inflight) of
        [] -> [];
        [{PktId, {Msg, _Ts}}] -> %% Fixed inflight size 1
            ClientId = emqx_channel:info(clientid, Channel),
            ReplayTopic = emqx_sn_registry:lookup_topic(ClientId, TopicId),
            case ReplayTopic =:= emqx_message:topic(Msg) of
                false -> [];
                true ->
                    NMsg = emqx_message:set_flag(dup, true, Msg),
                    [emqx_message:to_packet(PktId, NMsg)]
            end
    end.

register_and_notify_client(?PUBLISH_PACKET(QoS, TopicName, PacketId, Payload) = PubPkt,
        State = #state{pending_topic_ids = Pendings, channel = Channel}) ->
    MsgId = message_id(PacketId),
    #mqtt_packet{header = #mqtt_packet_header{dup = Dup, retain = Retain}} = PubPkt,
    ClientId = emqx_channel:info(clientid, Channel),
    TopicId = emqx_sn_registry:register_topic(ClientId, TopicName),
    ?LOG(debug, "Register TopicId=~p, TopicName=~p, Payload=~p, Dup=~p, "
                "QoS=~p,Retain=~p, MsgId=~p",
                [TopicId, TopicName, Payload, Dup, QoS, Retain, MsgId]),
    NPendings = case QoS == ?QOS_0 of
                    true ->
                        cache_no_reg_publish_message(
                          Pendings, TopicId, PubPkt, State);
                    _ -> Pendings
                end,
    send_register(TopicName, TopicId, MsgId,
                  State#state{pending_topic_ids = NPendings}).

message_id(undefined) ->
    rand:uniform(16#FFFF);
message_id(MsgId) -> MsgId.

inc_incoming_stats(Type) ->
    inc_counter(recv_pkt, 1),
    case Type == ?PUBLISH of
        true ->
            inc_counter(recv_msg, 1),
            inc_counter(incoming_pubs, 1);
        false -> ok
    end.

inc_outgoing_stats(Type) ->
    inc_counter(send_pkt, 1),
    case Type =:= ?SN_PUBLISH of
        true -> inc_counter(send_msg, 1);
        false -> ok
    end.

inc_counter(Key, Inc) ->
    _ = emqx_pd:inc_counter(Key, Inc),
    ok.

append(Replies, AddEvents) when is_list(Replies) ->
    Replies ++ AddEvents;
append(Replies, AddEvents) ->
    [Replies] ++ AddEvents.

maybe_send_puback(?QOS_0, _TopicId, _MsgId, _ReasonCode, State) ->
    State;
maybe_send_puback(_QoS, TopicId, MsgId, ReasonCode, State) ->
    send_message(?SN_PUBACK_MSG(TopicId, MsgId, ReasonCode), State).

%% Replies = [{event, connected}, {connack, ConnAck}, {outgoing, Pkts}]
split_connack_replies([A = {event, connected},
                       B = {connack, _ConnAck} | Outgoings]) ->
   {[A, B], Outgoings}.
